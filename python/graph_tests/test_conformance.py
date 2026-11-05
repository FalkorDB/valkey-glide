# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Real pinned-server gate: explicitly invoke this target, never plain Valkey CI."""

import copy
import gc
import importlib
import json
import os
import socket
import subprocess
import sys
import threading
import uuid
import weakref
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import anyio
import pytest
from glide_shared import (
    ClosingError,
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
    GraphMap,
    GraphNode,
    GraphPath,
    GraphPoint,
    GraphRelationship,
    GraphTemporal,
    GraphVector,
    NodeAddress,
    ProtocolVersion,
    ReadFrom,
    RequestError,
    TimeoutError,
)

ALL_TAGS = (
    "CREATE p=(a:Person:Researcher {name:'Ada',age:42})"
    "-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN "
    "null, 'text', -9223372036854775808, true, 1.5, "
    "[1,null,{nested:[false,'x']}], r, a, p, "
    "{first:1,second:'two'}, point({latitude:1.5,longitude:2.5}), "
    "vecf32([1,2.5,-3]), localdatetime('2020-01-02T03:04:05'), "
    "date('2020-01-02'), localtime('03:04:05'), duration('P1Y2M3DT4H5M6S')"
)
REPLICA_CREATE = (
    "CREATE p=(a:ReplicaPerson {name:$first})"
    "-[r:REPLICA_LINK {weight:$weight}]->(b:ReplicaPeer {name:$second}) "
    "RETURN p,a,r,b"
)
REPLICA_READ = (
    "MATCH p=(a:ReplicaPerson)-[r:REPLICA_LINK]->(b:ReplicaPeer) RETURN p,a,r,b"
)
REPLICA_PARAMS = {"first": "Ada", "second": "Bob", "weight": 7}


@pytest.fixture(params=["standalone", "cluster"])
def topology(request):
    endpoint = os.environ.get(f"FALKORDB_{request.param.upper()}_ENDPOINT")
    if not endpoint:
        pytest.fail(
            f"Dedicated graph target requires FALKORDB_{request.param.upper()}_ENDPOINT"
        )
    host, port = endpoint.rsplit(":", 1)
    return request.param, NodeAddress(host, int(port))


@pytest.fixture(params=[ProtocolVersion.RESP2, ProtocolVersion.RESP3])
def config(topology, request):
    mode, address = topology
    config_type = (
        GlideClusterClientConfiguration
        if mode == "cluster"
        else GlideClientConfiguration
    )
    return config_type(
        addresses=[address], protocol=request.param, request_timeout=10000
    )


@pytest.fixture(params=["asyncio", "trio", ("asyncio", {"use_uvloop": True})])
def anyio_backend(request):
    return request.param


@pytest.fixture(
    params=[ProtocolVersion.RESP2, ProtocolVersion.RESP3], ids=["resp2", "resp3"]
)
def replica_config(request):
    addresses = []
    for role in ("STANDALONE", "REPLICA"):
        endpoint = os.environ.get(f"FALKORDB_{role}_ENDPOINT")
        if not endpoint:
            pytest.fail(f"Dedicated graph target requires FALKORDB_{role}_ENDPOINT")
        host, port = endpoint.rsplit(":", 1)
        addresses.append(NodeAddress(host, int(port)))
    return GlideClientConfiguration(
        addresses=addresses,
        protocol=request.param,
        read_from=ReadFrom.PREFER_REPLICA,
        request_timeout=10000,
    )


@contextmanager
def _monitor_replica(key: str) -> Iterator[None]:
    helper = Path(__file__).resolve().parents[2] / "utils/falkordb_monitor.py"
    endpoint = os.environ["FALKORDB_REPLICA_ENDPOINT"]
    with subprocess.Popen(
        [
            sys.executable,
            str(helper),
            "--endpoint",
            endpoint,
            "--graph",
            key,
            "--timeout",
            "30",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert process.stdout is not None
            ready = process.stdout.readline()
            if ready != "READY\n":
                output, errors = process.communicate(timeout=5)
                pytest.fail(f"Replica monitor failed to start: {ready}{output}{errors}")
            yield
            output, errors = process.communicate("STOP\n", timeout=10)
            assert process.returncode == 0, (output, errors)
            evidence = json.loads(output)
            assert evidence["endpoint"] == endpoint
            assert evidence["graph"] == key
            assert evidence["role"] == "replica"
            assert evidence["complete"] is True, evidence
            assert evidence["same_connection"] is True, evidence
            assert evidence["counts"] == {
                "query": 1,
                "labels": 1,
                "relationship_types": 1,
                "property_keys": 1,
                "write": 0,
            }, evidence
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


def assert_all_tags(result):
    assert len(result.columns) == 16
    row = result.rows[0]
    assert row[:5] == [None, "text", -(2**63), True, 1.5]
    assert row[5] == [1, None, GraphMap([("nested", [False, "x"])])]
    assert isinstance(row[6], GraphRelationship)
    assert row[6].type == "KNOWS"
    assert row[6].properties == GraphMap([("since", 2020)])
    assert isinstance(row[7], GraphNode)
    assert set(row[7].labels) == {"Person", "Researcher"}
    assert row[7].properties == GraphMap([("name", "Ada"), ("age", 42)])
    assert isinstance(row[8], GraphPath)
    assert len(row[8].nodes) == 2 and row[8].relationships == [row[6]]
    assert row[9] == GraphMap([("first", 1), ("second", "two")])
    assert row[10] == GraphPoint(1.5, 2.5)
    assert row[11] == GraphVector([1.0, 2.5, -3.0])
    assert row[12] == GraphTemporal("datetime", 1577934245)
    assert row[14] == GraphTemporal("time", 11045)
    assert all(isinstance(value, GraphTemporal) for value in row[12:])
    assert result.statistics.nodes_created == 2
    assert result.statistics.relationships_created == 1
    assert result.statistics.raw


def graph_key():
    return f"glide-python-graph-{uuid.uuid4().hex}"


class _GraphResponseGate:
    """Forward real server traffic while holding graph replies for two callers."""

    def __init__(self, address):
        self.address = address
        self.armed = threading.Event()
        self.submitted = threading.Event()
        self.release = threading.Event()
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.query_count = 0
        self.connections = []
        self.threads = []
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.accept_thread = self._start(self._accept)

    def _start(self, target, *args):
        thread = threading.Thread(target=target, args=args, daemon=True)
        self.threads.append(thread)
        thread.start()
        return thread

    def _accept(self):
        while not self.stopped.is_set():
            try:
                incoming, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            upstream = socket.create_connection(self.address, timeout=5)
            upstream.settimeout(None)
            self.connections.extend((incoming, upstream))
            self._start(self._forward, incoming, upstream, False)
            self._start(self._forward, upstream, incoming, True)

    def _forward(self, source, destination, response):
        tail = b""
        marker = b"GRAPH.RO_QUERY"
        try:
            while not self.stopped.is_set():
                data = source.recv(65536)
                if not data:
                    return
                if self.armed.is_set():
                    if response:
                        if not self.release.wait(10):
                            return
                    else:
                        combined = tail + data
                        with self.lock:
                            self.query_count += combined.count(marker)
                            submitted = self.query_count >= 2
                        tail = combined[-(len(marker) - 1) :]
                        if submitted:
                            self.submitted.set()
                destination.sendall(data)
        except OSError:
            if not self.stopped.is_set():
                raise

    def close(self):
        self.stopped.set()
        self.release.set()
        self.listener.close()
        self.accept_thread.join(timeout=5)
        assert not self.accept_thread.is_alive()
        for connection in self.connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive()


@pytest.mark.parametrize("protocol", [ProtocolVersion.RESP2, ProtocolVersion.RESP3])
@pytest.mark.parametrize("close_inflight", [False, True], ids=["deadline", "close"])
def test_sync_native_graph_queries_overlap_and_keep_close_safe(
    protocol, close_inflight
):
    from glide_sync import GlideClient

    endpoint = os.environ.get("FALKORDB_STANDALONE_ENDPOINT")
    assert endpoint, "Dedicated graph target requires FALKORDB_STANDALONE_ENDPOINT"
    host, port = endpoint.rsplit(":", 1)
    gate = _GraphResponseGate((host, int(port)))
    client = None
    keys = [graph_key(), graph_key()]
    try:
        client = GlideClient.create(
            GlideClientConfiguration(
                [NodeAddress("127.0.0.1", gate.port)],
                protocol=protocol,
                request_timeout=1000,
            )
        )
        graphs = [client.select_graph(key) for key in keys]
        for graph in graphs:
            graph.query("CREATE ()")
        gate.armed.set()
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [
                pool.submit(graph.ro_query, "RETURN $value", {"value": index})
                for index, graph in enumerate(graphs)
            ]
            try:
                assert gate.submitted.wait(3), "native graph calls serialized"
                assert not any(call.done() for call in calls)
                if close_inflight:
                    client.close()
                    assert client._deferred_core_client != client._ffi.NULL
                    gate.release.set()
                    assert [call.result(timeout=3).rows for call in calls] == [
                        [[0]],
                        [[1]],
                    ]
                    assert client._deferred_core_client == client._ffi.NULL
                else:
                    for call in calls:
                        with pytest.raises(TimeoutError):
                            call.result(timeout=3)
                assert gate.query_count == 2, "a graph operation was replayed"
            finally:
                gate.release.set()
    finally:
        if client is not None:
            client.close()
        gate.close()
        with GlideClient.create(
            GlideClientConfiguration([NodeAddress(host, int(port))])
        ) as cleanup:
            cleanup.delete(keys)


def test_sync_graph_conformance(config, topology):
    from glide_sync import GlideClient, GlideClusterClient

    cls = GlideClusterClient if topology[0] == "cluster" else GlideClient
    client = cls.create(config)
    key = graph_key()
    graph = client.select_graph(key)
    try:
        assert client.exists([key]) == 0
        assert_all_tags(graph.query(ALL_TAGS))
        assert copy.copy(graph)._client is client
        assert graph.ro_query(
            "RETURN $value AS first, $other AS second",
            {"value": 2**63 - 1, "other": -(2**63)},
        ).rows == [[2**63 - 1, -(2**63)]]
        assert graph.query("RETURN 1 AS first, 2 AS second").columns == [
            "first",
            "second",
        ]
        with pytest.raises(RequestError, match="same name"):
            graph.query("RETURN 1 AS same, 2 AS same")
        escaped = "'\\\n\t\r\b\f☺"
        assert graph.query("RETURN $value", {"value": escaped}).rows == [[escaped]]
        assert graph.query(
            "RETURN $value", {"value": {"space key": [False, None, 1]}}
        ).rows == [[GraphMap([("space key", [False, None, 1])])]]
        with pytest.raises(RequestError):
            graph.ro_query("CREATE (:Forbidden)")
        with pytest.raises(RequestError):
            graph.query("THIS IS NOT CYPHER")
        # Schema dictionaries are not cached across deletion or replacement.
        client.delete([key])
        replaced = graph.query("CREATE (n:Replacement {fresh:7}) RETURN n")
        assert replaced.rows[0][0].labels == ["Replacement"]
        assert replaced.rows[0][0].properties == GraphMap([("fresh", 7)])
        if topology[0] == "standalone":
            with pytest.raises(RequestError):
                client.select(123456)
            with pytest.raises(RequestError, match="SELECT|unsettled"):
                client.select_graph(key)
            client.select(0)
            client.select(1)
            client.select(0)
            with pytest.raises(RequestError, match="database|SELECT"):
                graph.query("RETURN 1")
    finally:
        client.delete([key])
        client.close()
    with pytest.raises(ClosingError):
        graph.query("RETURN 1")
    with pytest.raises(ClosingError):
        client.select_graph(key)


@pytest.mark.anyio
async def test_async_graph_conformance(config, topology):
    from glide import GlideClient, GlideClusterClient

    cls = GlideClusterClient if topology[0] == "cluster" else GlideClient
    client = await cls.create(config)
    key = graph_key()
    graph = client.select_graph(key)
    try:
        assert await client.exists([key]) == 0
        assert_all_tags(await graph.query(ALL_TAGS))
        completed = {}

        async def read(index):
            completed[index] = (
                await graph.ro_query("RETURN $index", {"index": index})
            ).rows

        async with anyio.create_task_group() as tasks:
            for index in range(8):
                tasks.start_soon(read, index)
        assert completed == {index: [[index]] for index in range(8)}
        with anyio.move_on_after(0):
            await graph.ro_query("RETURN 1")
        assert (await graph.ro_query("RETURN 2")).rows == [[2]]
        assert not client._pending_futures
        assert copy.copy(graph)._client is client
        assert (
            await graph.ro_query(
                "RETURN $value, $other", {"value": 2**63 - 1, "other": -(2**63)}
            )
        ).rows == [[2**63 - 1, -(2**63)]]
        distinct = await graph.query("RETURN 1 AS first, 2 AS second")
        assert distinct.columns == ["first", "second"]
        with pytest.raises(RequestError, match="same name"):
            await graph.query("RETURN 1 AS same, 2 AS same")
        escaped = "'\\\n\t\r\b\f☺"
        assert (await graph.query("RETURN $value", {"value": escaped})).rows == [
            [escaped]
        ]
        with pytest.raises(RequestError):
            await graph.ro_query("CREATE (:Forbidden)")
        with pytest.raises(RequestError):
            await graph.query("THIS IS NOT CYPHER")
        await client.delete([key])
        replaced = await graph.query("CREATE (n:Replacement {fresh:7}) RETURN n")
        assert replaced.rows[0][0].labels == ["Replacement"]
        assert replaced.rows[0][0].properties == GraphMap([("fresh", 7)])
        if topology[0] == "standalone":
            with pytest.raises(RequestError):
                await client.select(123456)
            with pytest.raises(RequestError, match="SELECT|unsettled"):
                client.select_graph(key)
            await client.select(0)
            await client.select(1)
            await client.select(0)
            with pytest.raises(RequestError, match="database|SELECT"):
                await graph.query("RETURN 1")
    finally:
        await client.delete([key])
        await client.close()
    with pytest.raises(ClosingError):
        await graph.query("RETURN 1")
    with pytest.raises(ClosingError):
        client.select_graph(key)


def test_sync_lazy_selection_retains_parent_without_network():
    from glide_sync import GlideClient

    client = GlideClient.create(
        GlideClientConfiguration(
            [NodeAddress("127.0.0.1", 1)], lazy_connect=True, database_id=5
        )
    )
    owner = weakref.ref(client)
    client._graph_pool_managed = True
    with pytest.raises(RequestError, match="pooled"):
        client.select_graph("pooled")
    client._graph_pool_managed = False
    graph = client.select_graph("not-created")
    clone = copy.copy(graph)
    native = weakref.ref(graph._native_context)
    assert clone._native_context is graph._native_context
    assert graph.database == 5
    del client
    gc.collect()
    assert owner() is graph._client
    del graph
    gc.collect()
    assert native() is clone._native_context
    clone._client.close()
    with pytest.raises(ClosingError):
        clone.query("RETURN 1")
    del clone
    gc.collect()
    assert native() is None


@pytest.mark.anyio
async def test_async_lazy_selection_retains_parent_without_network():
    from glide import GlideClient

    client = await GlideClient.create(
        GlideClientConfiguration(
            [NodeAddress("127.0.0.1", 1)], lazy_connect=True, database_id=5
        )
    )
    owner = weakref.ref(client)
    client._graph_pool_managed = True
    with pytest.raises(RequestError, match="pooled"):
        client.select_graph("pooled")
    client._graph_pool_managed = False
    graph = client.select_graph("not-created")
    clone = copy.copy(graph)
    native = weakref.ref(graph._native_context)
    assert clone._native_context is graph._native_context
    assert graph.database == 5
    del client
    gc.collect()
    assert owner() is graph._client
    del graph
    gc.collect()
    assert native() is clone._native_context
    await clone._client.close()
    with pytest.raises(ClosingError):
        await clone.query("RETURN 1")
    del clone
    gc.collect()
    assert native() is None


def test_sync_pooled_selection_rejected_even_after_pool_destruction(config):
    from glide_sync import ClientPool, Graph, PoolConfig

    pool = ClientPool.create(config, PoolConfig(max_size=1, min_idle=0))
    try:
        with pool.borrow() as borrowed, pytest.raises(RequestError, match="pooled"):
            borrowed.select_graph("must-not-retain-lease")
    finally:
        pool.close()
    # The wrapper deliberately survives its native adapter. Rejection must occur
    # before any getter/submission can dereference that now-dangling pointer.
    with pytest.raises(RequestError, match="pooled"):
        borrowed.select_graph("must-not-retain-lease")
    with pytest.raises(RequestError, match="pooled"):
        Graph(borrowed, "must-not-retain-lease", 0, 0).query("RETURN 1")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend", ["asyncio", ("asyncio", {"use_uvloop": True})]
)
async def test_async_pooled_selection_rejected_even_after_pool_destruction(config):
    from glide import AsyncClientPool, Graph, PoolConfig

    pool = await AsyncClientPool.create(config, PoolConfig(max_size=1, min_idle=0))
    try:
        async with pool.borrow() as borrowed:
            with pytest.raises(RequestError, match="pooled"):
                borrowed.select_graph("must-not-retain-lease")
    finally:
        pool.close()
    with pytest.raises(RequestError, match="pooled"):
        borrowed.select_graph("must-not-retain-lease")
    with pytest.raises(RequestError, match="pooled"):
        await Graph(borrowed, "must-not-retain-lease", 0, 0).query("RETURN 1")


@pytest.mark.parametrize(
    "module_name,class_name",
    [
        ("glide.isolated_scope", "AsyncIsolatedScope"),
        ("glide_sync.isolated_scope", "IsolatedScope"),
    ],
)
def test_isolated_scope_selection_is_local_and_never_escapes(module_name, class_name):
    scope_type = getattr(importlib.import_module(module_name), class_name)
    scope = object.__new__(scope_type)
    scope._released = True
    try:
        for released in (False, True):
            scope._released = released
            with pytest.raises(RequestError, match="isolated-scope lease"):
                scope.select_graph("must-not-retain-lease")
    finally:
        scope._released = True


def test_sync_entity_metadata_with_single_inflight_permit(config, topology):
    from glide_sync import GlideClient, GlideClusterClient

    config.inflight_requests_limit = 1
    cls = GlideClusterClient if topology[0] == "cluster" else GlideClient
    client = cls.create(config)
    key = graph_key()
    try:
        graph = client.select_graph(key)
        node = graph.query("CREATE (n:SinglePermit {value:7}) RETURN n").rows[0][0]
        assert node.labels == ["SinglePermit"]
        assert node.properties == GraphMap([("value", 7)])
        assert graph.ro_query("MATCH (n) RETURN n").rows == [[node]]
    finally:
        client.delete([key])
        client.close()


@pytest.mark.anyio
async def test_async_entity_metadata_with_single_inflight_permit(config, topology):
    from glide import GlideClient, GlideClusterClient

    config.inflight_requests_limit = 1
    cls = GlideClusterClient if topology[0] == "cluster" else GlideClient
    client = await cls.create(config)
    key = graph_key()
    try:
        graph = client.select_graph(key)
        node = (await graph.query("CREATE (n:SinglePermit {value:7}) RETURN n")).rows[
            0
        ][0]
        assert node.labels == ["SinglePermit"]
        assert node.properties == GraphMap([("value", 7)])
        assert (await graph.ro_query("MATCH (n) RETURN n")).rows == [[node]]
    finally:
        await client.delete([key])
        await client.close()


def test_sync_configured_replica_query_and_metadata(replica_config):
    from glide_sync import GlideClient

    client = GlideClient.create(replica_config)
    key = graph_key()
    try:
        graph = client.select_graph(key)
        created = graph.query(REPLICA_CREATE, REPLICA_PARAMS)
        assert len(created.rows) == 1
        assert isinstance(created.rows[0][0], GraphPath)
        assert client.wait(1, 5000) >= 1
        with _monitor_replica(key):
            result = graph.ro_query(REPLICA_READ)
            assert result.columns == ["p", "a", "r", "b"]
            assert result.rows == created.rows
    finally:
        client.delete([key])
        client.close()


@pytest.mark.anyio
async def test_async_configured_replica_query_and_metadata(replica_config):
    from glide import GlideClient

    client = await GlideClient.create(replica_config)
    key = graph_key()
    try:
        graph = client.select_graph(key)
        created = await graph.query(REPLICA_CREATE, REPLICA_PARAMS)
        assert len(created.rows) == 1
        assert isinstance(created.rows[0][0], GraphPath)
        assert await client.wait(1, 5000) >= 1
        with _monitor_replica(key):
            result = await graph.ro_query(REPLICA_READ)
            assert result.columns == ["p", "a", "r", "b"]
            assert result.rows == created.rows
    finally:
        await client.delete([key])
        await client.close()
