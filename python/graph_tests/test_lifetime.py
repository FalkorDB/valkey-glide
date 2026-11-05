# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Deterministic graph ownership gates, independent of server endpoints."""

import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from glide_shared import ClosingError, GlideClientConfiguration, NodeAddress
from glide_shared.graph import _NativeGraphContext
from glide_sync import GlideClient


def offline_client(query):
    client = GlideClient(
        GlideClientConfiguration([NodeAddress("127.0.0.1", 1)], lazy_connect=True)
    )
    pointer = object()
    closed = []
    client._core_client = pointer
    client._lib = SimpleNamespace(
        graph_query_with_context=query,
        close_client=closed.append,
    )
    client._handle_cmd_result = lambda result: result
    context = _NativeGraphContext(object(), lambda pointer: None)
    return client, pointer, context, closed


def test_sync_graph_calls_overlap_and_close_defers_free():
    entered = threading.Barrier(3)
    release = threading.Event()
    observed = []

    def query(pointer, context, request_id, request, size):
        observed.append(pointer)
        entered.wait(timeout=5)
        assert release.wait(5)
        assert closed == []
        return request

    client, pointer, context, closed = offline_client(query)
    with ThreadPoolExecutor(max_workers=2) as pool:
        calls = [
            pool.submit(client._execute_graph_request, request, context)
            for request in (b"first", b"second")
        ]
        try:
            entered.wait(timeout=5)
            client.close()
            client.close()
            assert client.closed
            assert closed == []
            with pytest.raises(ClosingError):
                client._execute_graph_request(b"late", context)
        finally:
            release.set()
        assert [call.result(timeout=5) for call in calls] == [b"first", b"second"]
    assert observed == [pointer, pointer]
    assert closed == [pointer]
    assert client._deferred_core_client == client._ffi.NULL


def test_sync_graph_ffi_exception_releases_deferred_client():
    entered = threading.Event()
    release = threading.Event()

    def query(*args):
        entered.set()
        assert release.wait(5)
        raise RuntimeError("FFI submission failed")

    client, pointer, context, closed = offline_client(query)
    with ThreadPoolExecutor(max_workers=1) as pool:
        call = pool.submit(client._execute_graph_request, b"request", context)
        assert entered.wait(5)
        try:
            client.close()
            assert closed == []
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="FFI submission failed"):
            call.result(timeout=5)
    assert closed == [pointer]


@pytest.mark.parametrize("graph_finishes_first", [False, True])
def test_sync_graph_and_rdma_share_deferred_close(graph_finishes_first):
    entered = threading.Event()
    release = threading.Event()

    def query(*args):
        entered.set()
        assert release.wait(5)

    client, pointer, context, closed = offline_client(query)
    cancelled = []
    client._lib.rdma_close_regions = lambda native: (
        cancelled.append(native) or client._ffi.NULL
    )
    callbacks = (object(), object())
    client._pubsub_callback_ref, client._credential_provider_callback_ref = callbacks
    owner_closed = []
    client._credential_provider_callback_owner = SimpleNamespace(
        close=lambda: owner_closed.append(True)
    )
    assert client._begin_rdma_call() is pointer
    with ThreadPoolExecutor(max_workers=1) as pool:
        call = pool.submit(client._execute_graph_request, b"request", context)
        assert entered.wait(5)
        try:
            client.close()
            assert client._deferred_callback_refs == callbacks
            assert cancelled == [pointer]
            assert closed == owner_closed == []
            if graph_finishes_first:
                release.set()
                call.result(timeout=5)
            else:
                client._end_rdma_call()
            assert closed == owner_closed == []
        finally:
            release.set()
        call.result(timeout=5)
    if graph_finishes_first:
        client._end_rdma_call()
    assert closed == [pointer]
    assert owner_closed == [True]
    assert client._deferred_callback_refs == ()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize("query_in_child", [False, True], ids=["cleanup", "query"])
def test_inherited_sync_graph_rejects_queries_and_skips_native_cleanup(
    query_in_child,
):
    script = """
import copy
import gc
import os
import signal
import sys
import time
from glide_shared import ClosingError, GlideClientConfiguration, NodeAddress
from glide_sync import GlideClient

client = GlideClient.create(
    GlideClientConfiguration([NodeAddress("127.0.0.1", 1)], lazy_connect=True)
)
original_lib = client._lib
parent_pid = os.getpid()
released = []
def release(pointer):
    if os.getpid() != parent_pid:
        os._exit(91)
    released.append(pointer)
    original_lib.graph_context_drop(pointer)
class Library:
    def __getattr__(self, name):
        return getattr(original_lib, name)
    graph_context_drop = staticmethod(release)
client._lib = Library()
graph = client.select_graph("inherited")
clone = copy.copy(graph)
exit_graph = client.select_graph("inherited-until-exit")
# An unrelated parent thread can own this short-lived guard at fork.
client._client_lock.acquire()
pid = os.fork()
if pid == 0:
    if os.environ["GRAPH_QUERY_AFTER_FORK"] == "1":
        for handle in (graph, clone):
            for operation in (handle.query, handle.ro_query):
                try:
                    operation("RETURN 1")
                except ClosingError as error:
                    assert "fork" in str(error)
                else:
                    os._exit(92)
        del operation, handle
    del graph, clone
    gc.collect()
    client._lib = original_lib
    fresh = client.select_graph("child-local")
    del fresh
    gc.collect()
    client.close()
    # Keep one inherited context until interpreter shutdown as well.
    sys.exit(0)
client._client_lock.release()
deadline = time.monotonic() + 8
while True:
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        break
    if time.monotonic() > deadline:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        raise AssertionError("child query or cleanup hung")
    time.sleep(0.01)
assert os.waitstatus_to_exitcode(status) == 0, status
client.close()
del graph, clone, exit_graph
gc.collect()
assert len(released) == 2, released
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(sys.path)
    environment["GRAPH_QUERY_AFTER_FORK"] = "1" if query_in_child else "0"
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=20,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
