#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
"""Replay captured wire fixtures and assert isolated live topology semantics."""

from __future__ import annotations

import argparse
import io
import json
import select
import signal
import socket
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import falkordb_monitor as monitor
from falkordb_cases import FIXTURES, Recorder, array, rows, scalar
from falkordb_harness import IMAGE, LABEL, OWNER, Harness, wait_for
from falkordb_protocol import Frame, read_frame


class MonitorTests(unittest.TestCase):
    """Test monitor evidence and the subprocess protocol without Docker or bindings."""

    def test_monitor_quoted_bytes_and_physical_connection(self) -> None:
        """Redis byte escapes, not shell escaping, define MONITOR arguments."""
        event = monitor.parse_monitor_line(
            r'123.456 [2 127.0.0.1:1234] "graph.ro_query" "g:{abc}" '
            r'"RETURN \"x\\y\n\x41\xc3\xa9\"" "--compact"'
        )
        self.assertEqual(event.command, "GRAPH.RO_QUERY")
        self.assertEqual(event.db, 2)
        self.assertEqual(event.client, "127.0.0.1:1234")
        self.assertEqual(event.args, ("g:{abc}", 'RETURN "x\\y\nAé"', "--compact"))

    def test_monitor_rejects_malformed_lines_and_escapes(self) -> None:
        """Bad observation data cannot silently count as valid affinity proof."""
        for line in (
            'not-a-header "PING"',
            '123.456 [0 peer] "GRAPH.RO_QUERY" "unterminated',
            '123.456 [0 peer] unquoted "GRAPH.RO_QUERY"',
            r'123.456 [0 peer] "GRAPH.RO_QUERY" "\q"',
        ):
            with self.subTest(line=line), self.assertRaises(ValueError):
                monitor.parse_monitor_line(line)
        for token in ("\\", r"\x0", r"\xgg"):
            with self.subTest(token=token), self.assertRaises(ValueError):
                monitor.unescape_token(token)

    def test_evidence_filters_exact_key_and_classifies_dictionaries(self) -> None:
        """Other jobs and similar key prefixes never contaminate the selected graph."""
        evidence = monitor.Evidence("g:{abc}")
        for query in (
            "MATCH p=()-->() RETURN p",
            "CALL db.labels()",
            "CALL db.relationshipTypes()",
            "CALL db.propertyKeys()",
        ):
            evidence.record(
                monitor.MonitorEvent(0, "peer:1", "GRAPH.RO_QUERY", ("g:{abc}", query))
            )
            evidence.record(
                monitor.MonitorEvent(
                    0, "peer:2", "GRAPH.QUERY", ("g:{abc}:other", query)
                )
            )
        report = evidence.report("127.0.0.1:6379")
        self.assertTrue(report["complete"])
        self.assertTrue(report["same_connection"])
        self.assertEqual(
            report["counts"],
            {
                "query": 1,
                "labels": 1,
                "relationship_types": 1,
                "property_keys": 1,
                "write": 0,
            },
        )
        self.assertEqual(len(evidence.commands), 4)
        evidence.record(
            monitor.MonitorEvent(0, "peer:2", "GRAPH.RO_QUERY", ("g:{abc}", "RETURN 1"))
        )
        self.assertFalse(evidence.report("endpoint")["same_connection"])
        evidence.record(
            monitor.MonitorEvent(0, "peer:1", "GRAPH.QUERY", ("g:{abc}", "CREATE ()"))
        )
        self.assertEqual(evidence.counts["write"], 1)

    def test_incomplete_capture_is_not_false_positive(self) -> None:
        """Successful observation alone does not prove that the API used the replica."""
        report = monitor.Evidence("g:{abc}").report("endpoint")
        self.assertFalse(report["complete"])
        self.assertFalse(report["same_connection"])

    def test_partial_frames_and_rejected_acknowledgment(self) -> None:
        """No readiness before the complete +OK; errors and truncated EOF fail."""
        output = io.StringIO()
        stream = monitor.MonitorStream(monitor.Evidence("g"), output)
        self.assertFalse(stream.feed(b"+O"))
        self.assertEqual(output.getvalue(), "")
        self.assertFalse(stream.feed(b"K\r\n"))
        self.assertEqual(output.getvalue(), "READY\n")
        with self.assertRaises(ConnectionError):
            stream.feed(b"")
        for wire in (b"-NOPERM no monitor\r\n", b"+NOT_OK\r\n"):
            output = io.StringIO()
            stream = monitor.MonitorStream(monitor.Evidence("g"), output)
            with self.assertRaises(ValueError):
                stream.feed(wire)
            self.assertEqual(output.getvalue(), "")

    def test_control_partial_stop_eof_and_invalid_input(self) -> None:
        """Control input is bounded and invalid shutdown text cannot be ignored."""
        control = monitor.ControlInput()
        self.assertFalse(control.feed(b"ST"))
        self.assertTrue(control.feed(b"OP\n"))
        self.assertTrue(monitor.ControlInput().feed(b""))
        for chunk in (b"QUIT\n", b"x" * 1025):
            with self.subTest(chunk=chunk[:8]), self.assertRaises(ValueError):
                monitor.ControlInput().feed(chunk)
        control = monitor.ControlInput()
        self.assertFalse(control.feed(b"QUIT"))
        with self.assertRaises(ValueError):
            control.feed(b"")

    def test_capture_timeout_closes_owned_socket_without_readiness(self) -> None:
        """A silent server cannot keep an orphaned helper or socket alive."""
        output = io.StringIO()
        with patch("falkordb_monitor.Connection") as connection:
            instance = connection.return_value.__enter__.return_value
            instance.command.return_value = Frame("array", [Frame("bulk", "slave")])
            with patch("falkordb_monitor.select.select", return_value=([], [], [])):
                with self.assertRaises(TimeoutError):
                    monitor.capture("127.0.0.1:6379", "g", 1, io.BytesIO(), output)
            self.assertEqual(output.getvalue(), "")
            connection.return_value.__exit__.assert_called_once()

    def test_role_validation_before_monitor(self) -> None:
        """A primary endpoint must fail before readiness or MONITOR side effects."""
        monitor.require_replica(Frame("array", [Frame("bulk", "slave")]))
        for role in (
            Frame("array", [Frame("bulk", "master")]),
            Frame("array", []),
            Frame("error", "NOPERM"),
        ):
            with self.subTest(role=role), self.assertRaises(ValueError):
                monitor.require_replica(role)
        output = io.StringIO()
        with patch("falkordb_monitor.Connection") as connection:
            instance = connection.return_value.__enter__.return_value
            instance.command.return_value = Frame("array", [Frame("bulk", "master")])
            with self.assertRaises(ValueError):
                monitor.capture("127.0.0.1:6379", "g:{abc}", 1, io.BytesIO(), output)
            self.assertEqual(output.getvalue(), "")
            instance.socket.sendall.assert_not_called()
            connection.return_value.__exit__.assert_called_once()

    def test_endpoint_and_capture_limits(self) -> None:
        """Input validation happens before allocating a network connection."""
        self.assertEqual(monitor.parse_endpoint("[::1]:6379"), ("::1", 6379))
        for endpoint in ("host", ":6379", "host:0", "host:65536"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                monitor.parse_endpoint(endpoint)
        for graph, duration in (
            ("bad key", 1.0),
            ("g", 0.0),
            ("g", float("nan")),
            ("g", 601.0),
        ):
            with self.subTest(graph=graph, duration=duration):
                with patch("falkordb_monitor.Connection") as connection:
                    with self.assertRaises(ValueError):
                        monitor.capture(
                            "host:6379", graph, duration, io.BytesIO(), io.StringIO()
                        )
                    connection.assert_not_called()

    def test_cli_ready_stop_fence_and_clean_eof(self) -> None:
        """Both shutdown forms drain through a real PING fence and exit boundedly."""
        graph = "monitor-test:{01234567-89ab-cdef-0123-456789abcdef}"
        script = Path(__file__).with_name("falkordb_monitor.py")
        for stop in (b"STOP\n", b""):
            with self.subTest(stop=stop), socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen(2)
                listener.settimeout(4)
                failures: list[BaseException] = []

                def serve() -> None:
                    try:
                        monitor, _ = listener.accept()
                        with monitor, monitor.makefile("rb") as requests:
                            monitor.settimeout(4)
                            assert read_frame(requests).plain() == ["ROLE"]
                            monitor.sendall(b"*1\r\n$5\r\nslave\r\n")
                            assert read_frame(requests).plain() == ["MONITOR"]
                            monitor.sendall(b"+OK\r\n")
                            for query in (
                                "MATCH p=()-->() RETURN p",
                                "CALL db.labels()",
                                "CALL db.relationshipTypes()",
                            ):
                                monitor.sendall(
                                    f'+123.456 [0 peer:1] "GRAPH.RO_QUERY" '
                                    f'"{graph}" "{query}" "--compact"\r\n'.encode()
                                )
                            barrier, _ = listener.accept()
                            with barrier, barrier.makefile("rb") as ping:
                                barrier.settimeout(4)
                                request = read_frame(ping).plain()
                                assert isinstance(request, list)
                                assert len(request) == 2 and request[0] == "PING"
                                fence = str(request[1])
                                barrier.sendall(
                                    f"${len(fence)}\r\n{fence}\r\n".encode()
                                )
                                # Deliberately deliver the final dictionary late;
                                # the helper must consume it before the fence.
                                monitor.sendall(
                                    f'+123.457 [0 peer:1] "GRAPH.RO_QUERY" '
                                    f'"{graph}" "CALL db.propertyKeys()" "--compact"\r\n'
                                    f'+123.458 [0 barrier:1] "PING" "{fence}"\r\n'.encode()
                                )
                    except BaseException as error:
                        failures.append(error)

                worker = threading.Thread(target=serve)
                worker.start()
                process = subprocess.Popen(
                    [
                        sys.executable,
                        str(script),
                        "--endpoint",
                        f"127.0.0.1:{listener.getsockname()[1]}",
                        "--graph",
                        graph,
                        "--timeout",
                        "3",
                    ],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    assert process.stdout is not None
                    ready, _, _ = select.select([process.stdout], [], [], 4)
                    self.assertTrue(ready, "helper did not become ready")
                    self.assertEqual(process.stdout.readline(), b"READY\n")
                    output, error = process.communicate(stop, timeout=5)
                    self.assertEqual(process.returncode, 0, error.decode())
                    report = json.loads(output)
                    self.assertTrue(report["complete"])
                    self.assertTrue(report["same_connection"])
                    self.assertEqual(report["role"], "replica")
                    self.assertEqual(report["graph"], graph)
                    self.assertEqual(
                        report["counts"],
                        {
                            "query": 1,
                            "labels": 1,
                            "relationship_types": 1,
                            "property_keys": 1,
                            "write": 0,
                        },
                    )
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.communicate(timeout=5)
                    worker.join(timeout=5)
                self.assertFalse(worker.is_alive(), "fake server did not stop")
                self.assertEqual(failures, [])


class ProtocolTests(unittest.TestCase):
    """Fast dependency-free checks for the recorder and fixture contract."""

    def test_nested_error_is_not_a_top_level_exception(self) -> None:
        """A schema rejection is an array containing an error, not a result."""
        reply = read_frame(io.BytesIO(b"*2\r\n-ERR invalid graph version\r\n:7\r\n"))
        self.assertEqual(reply.plain(), [{"error": "ERR invalid graph version"}, 7])
        self.assertEqual(array(reply)[0].kind, "error")

    def test_resp2_and_resp3_double_types(self) -> None:
        """RESP3 doubles and RESP2 bulk strings remain distinguishable."""
        resp2 = read_frame(io.BytesIO(b"$3\r\n1.5\r\n"))
        resp3 = read_frame(io.BytesIO(b",1.5\r\n"))
        self.assertEqual(resp2.value, resp3.value)
        self.assertEqual(resp2.kind, "bulk")
        self.assertEqual(resp3.kind, "double")

    def test_resp_nulls(self) -> None:
        """Both protocol null encodings normalize to typed null."""
        for wire in (b"$-1\r\n", b"_\r\n", b"*-1\r\n"):
            self.assertEqual(
                read_frame(io.BytesIO(wire)).json(), {"type": "null", "value": None}
            )

    def test_truncated_and_invalid_frames(self) -> None:
        """A broken fixture transport cannot silently produce a valid frame."""
        for wire in (b"", b"$3\r\nab", b"*1\r\n"):
            with self.assertRaises(ConnectionError):
                read_frame(io.BytesIO(wire))
        with self.assertRaises(ValueError):
            read_frame(io.BytesIO(b"*-2\r\n"))

    def test_only_execution_time_is_normalized(self) -> None:
        """Never mask cached execution, versions, stats, strings or numeric payloads."""
        # Construct length from the payload rather than depending on its presentation.
        payload = b"Query internal execution time: 0.123456 milliseconds"
        wire = f"${len(payload)}\r\n".encode() + payload + b"\r\n"
        reply = read_frame(io.BytesIO(wire))
        envelope = Frame("array", [Frame("array", [reply])])
        self.assertEqual(
            envelope.json(True),
            {
                "type": "array",
                "value": [
                    {
                        "type": "array",
                        "value": [
                            {
                                "type": "bulk",
                                "value": "Query internal execution time: <milliseconds> milliseconds",
                            }
                        ],
                    }
                ],
            },
        )
        self.assertEqual(reply.json(True), reply.json())
        self.assertEqual(reply.json()["value"], payload.decode())

    def test_returned_string_resembling_statistic_is_not_normalized(self) -> None:
        """Only statistics, never user result strings, can receive normalization."""
        reply = Frame("bulk", "Query internal execution time: 0.123456 milliseconds")
        envelope = Frame(
            "array", [Frame("array", []), Frame("array", [reply]), Frame("array", [])]
        )
        self.assertEqual(envelope.json(True), envelope.json())

    def test_manifest_matches_harness_pin(self) -> None:
        """A changed harness image requires an explicit fixture provenance update."""
        manifest = json.loads((FIXTURES / "manifest.json").read_text())
        self.assertEqual(manifest["server"]["image"], IMAGE)
        self.assertEqual(manifest["server"]["module_version"], 60003)
        for protocol in (2, 3):
            fixture = json.loads((FIXTURES / f"resp{protocol}.json").read_text())
            self.assertEqual(
                sum(len(case["steps"]) for case in fixture["cases"]),
                manifest["recording"]["commands_per_protocol"],
            )

    def test_cleanup_refuses_unowned_manifest(self) -> None:
        """An arbitrary state file cannot authorize removal of unrelated resources."""
        harness = Harness(Path("unused-falkordb-state.json"))
        harness.state = {"owner": "someone-else", "run": "x", "nodes": []}
        with self.assertRaises(ValueError):
            harness.stop()

    def test_cleanup_refuses_mismatching_live_label(self) -> None:
        """A recorded ID is not sufficient: both live labels must match."""
        harness = Harness(Path("unused-falkordb-state.json"))
        harness.state = {
            "owner": OWNER,
            "run": "expected",
            "nodes": [{"id": "container-id"}],
        }
        details = [{"Config": {"Labels": {LABEL: OWNER, f"{LABEL}.run": "other"}}}]
        with patch("falkordb_harness.subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = json.dumps(details)
            with self.assertRaisesRegex(RuntimeError, "Ownership mismatch"):
                harness.stop()
            self.assertEqual(run.call_count, 1)

    def test_committed_fixtures_cover_every_tag_and_protocol(self) -> None:
        """Golden files include an actual sample for all 16 tags in each protocol."""
        for protocol in (2, 3):
            fixture = json.loads((FIXTURES / f"resp{protocol}.json").read_text())
            self.assertEqual(fixture["protocol"], protocol)
            result = fixture["cases"][0]["steps"][0]["response"]
            cells = result["value"][1]["value"][0]["value"]
            tags = [cell["value"][0]["value"] for cell in cells]
            self.assertEqual(tags, list(range(1, 17)))
            vector = cells[11]["value"][1]["value"]
            self.assertEqual(
                [component["type"] for component in vector],
                ["bulk" if protocol == 2 else "double"] * 3,
            )


def replica_affinity(harness: Harness, protocol: int) -> dict[str, object]:
    """Expose mismatched dictionaries if metadata escapes the chosen replica."""
    primary = next(node for node in harness.nodes() if node["role"] == "standalone")
    replica = next(node for node in harness.nodes() if node["role"] == "replica")
    key = f"glide-graph-affinity:{uuid.uuid4().hex}"
    with Harness.connect(primary, protocol) as writer, Harness.connect(
        replica, protocol
    ) as reader:
        try:
            created = writer.command(
                "GRAPH.QUERY", key, "CREATE (:Before {old:1})", "--compact"
            )
            assert created.kind == "array", created
            assert writer.command("WAIT", 1, 10000).plain() == 1
            assert (
                scalar(
                    reader.command(
                        "GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"
                    )
                )
                == "Before"
            )
            original_client = reader.command("CLIENT", "ID").plain()
            assert reader.command("CLIENT", "PAUSE", 60000, "WRITE").plain() == "OK"
            try:
                writer.command("GRAPH.DELETE", key)
                replacement = writer.command(
                    "GRAPH.QUERY", key, "CREATE (:After {new:2})", "--compact"
                )
                assert replacement.kind == "array", replacement
                old_result = reader.command(
                    "GRAPH.RO_QUERY", key, "MATCH(n) RETURN n", "--compact"
                )
                assert array(array(rows(old_result)[0])[0])[1].plain() == [
                    0,
                    [0],
                    [[0, 3, 1]],
                ]
                old_label = scalar(
                    reader.command(
                        "GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"
                    )
                )
                new_label = scalar(
                    writer.command(
                        "GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"
                    )
                )
                assert (old_label, new_label) == ("Before", "After")
                assert reader.command("CLIENT", "ID").plain() == original_client
            finally:
                assert reader.command("CLIENT", "UNPAUSE").plain() == "OK"
            assert writer.command("WAIT", 1, 10000).plain() == 1

            def caught_up() -> bool:
                return (
                    scalar(
                        reader.command(
                            "GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"
                        )
                    )
                    == "After"
                )

            wait_for(caught_up, "replica graph recreation")
            refused = reader.command("GRAPH.QUERY", key, "CREATE ()", "--compact")
            assert refused.kind == "error" and "READONLY" in str(refused.value)
            return {
                "replication_barrier": True,
                "paused_replica_result_label": "Before",
                "same_replica_dictionary_label": "Before",
                "different_primary_dictionary_label": "After",
                "same_physical_replica_connection": True,
                "read_only_replica_rejects_writes": True,
            }
        finally:
            writer.command("DEL", key)
            writer.command("WAIT", 1, 10000)


def cluster_affinity(harness: Harness, protocol: int) -> dict[str, object]:
    """Check graph-key MOVED redirects and metadata on the exact owning node."""
    nodes = [
        node for node in harness.nodes() if str(node["role"]).startswith("cluster-")
    ]
    key = f"glide-graph-cluster:{{{uuid.uuid4().hex}}}"
    with Harness.connect(nodes[0], protocol) as seed:
        slot = seed.command("CLUSTER", "KEYSLOT", key).plain()
        slots = seed.command("CLUSTER", "SLOTS").plain()
    assert isinstance(slot, int) and isinstance(slots, list)
    owner_ip = next(entry[2][0] for entry in slots if entry[0] <= slot <= entry[1])
    owner = next(node for node in nodes if node["ip"] == owner_ip)
    wrong = next(node for node in nodes if node["ip"] != owner_ip)
    with Harness.connect(wrong, protocol) as conn:
        moved = conn.command("GRAPH.QUERY", key, "CREATE (:Forbidden)", "--compact")
        assert moved.kind == "error", moved
        assert moved.value == f"MOVED {slot} {owner_ip}:6379", moved
    with Harness.connect(owner, protocol) as conn:
        try:
            assert conn.command("EXISTS", key).plain() == 0
            client_id = conn.command("CLIENT", "ID").plain()
            created = conn.command(
                "GRAPH.QUERY",
                key,
                "CREATE (:ClusterLabel {value:7}) RETURN 7",
                "--compact",
            )
            assert scalar(created) == 7
            assert (
                scalar(
                    conn.command("GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact")
                )
                == "ClusterLabel"
            )
            assert (
                scalar(
                    conn.command(
                        "GRAPH.RO_QUERY", key, "CALL db.propertyKeys()", "--compact"
                    )
                )
                == "value"
            )
            assert conn.command("CLIENT", "ID").plain() == client_id
            assert conn.command("WAIT", 1, 10000).plain() == 1
            return {
                "moved_is_preexecution": True,
                "graph_key_selects_owner": True,
                "query_and_metadata_same_physical_connection": True,
                "cluster_replica_synchronized": True,
            }
        finally:
            conn.command("DEL", key)
            conn.command("WAIT", 1, 10000)


def cluster_ask(harness: Harness, protocol: int) -> dict[str, object]:
    """Prove ASK is pre-execution and ASKING is needed again for metadata commands."""
    nodes = [
        node for node in harness.nodes() if str(node["role"]).startswith("cluster-")
    ]
    key = f"glide-graph-ask:{{{uuid.uuid4().hex}}}"
    with Harness.connect(nodes[0], protocol) as seed:
        slot = seed.command("CLUSTER", "KEYSLOT", key).plain()
        slots = seed.command("CLUSTER", "SLOTS").plain()
    assert isinstance(slot, int) and isinstance(slots, list)
    owner_entry = next(entry for entry in slots if entry[0] <= slot <= entry[1])
    target_entry = next(entry for entry in slots if entry[2][0] != owner_entry[2][0])
    owner_id, target_id = owner_entry[2][2], target_entry[2][2]
    owner = next(node for node in nodes if node["ip"] == owner_entry[2][0])
    target = next(node for node in nodes if node["ip"] == target_entry[2][0])
    masters = [
        next(node for node in nodes if node["ip"] == entry[2][0]) for entry in slots
    ]
    with Harness.connect(owner, protocol) as source, Harness.connect(
        target, protocol
    ) as destination:
        try:
            assert source.command("EXISTS", key).plain() == 0
            assert (
                destination.command(
                    "CLUSTER", "SETSLOT", slot, "IMPORTING", owner_id
                ).plain()
                == "OK"
            )
            assert (
                source.command(
                    "CLUSTER", "SETSLOT", slot, "MIGRATING", target_id
                ).plain()
                == "OK"
            )
            asked = source.command(
                "GRAPH.QUERY", key, "CREATE (:Forbidden)", "--compact"
            )
            assert asked.plain() == {"error": f"ASK {slot} {target['ip']}:6379"}
            moved = destination.command(
                "GRAPH.QUERY", key, "CREATE (:Forbidden)", "--compact"
            )
            assert moved.plain() == {"error": f"MOVED {slot} {owner['ip']}:6379"}
            existence = destination.pipeline([("ASKING",), ("EXISTS", key)])
            assert [reply.plain() for reply in existence] == ["OK", 0]
            created = destination.pipeline(
                [
                    ("ASKING",),
                    (
                        "GRAPH.QUERY",
                        key,
                        "CREATE (:Imported {value:9}) RETURN 9",
                        "--compact",
                    ),
                ]
            )
            assert created[0].plain() == "OK" and scalar(created[1]) == 9
            without_asking = destination.command(
                "GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"
            )
            assert without_asking.plain() == {
                "error": f"MOVED {slot} {owner['ip']}:6379"
            }
            metadata = destination.pipeline(
                [
                    ("ASKING",),
                    ("GRAPH.RO_QUERY", key, "CALL db.labels()", "--compact"),
                ]
            )
            assert metadata[0].plain() == "OK" and scalar(metadata[1]) == "Imported"
            return {
                "ask_is_preexecution": True,
                "asking_plus_query_single_pipeline": True,
                "asking_permission_is_one_command_only": True,
                "asking_plus_metadata_same_target": True,
            }
        finally:
            for master in masters:
                with Harness.connect(master, protocol) as conn:
                    promoted = conn.command(
                        "CLUSTER", "SETSLOT", slot, "NODE", target_id
                    )
                    assert promoted.plain() == "OK", promoted
            destination.command("DEL", key)
            destination.command("WAIT", 1, 10000)


def live(state: Path, topology: str, record: bool) -> dict[str, object]:
    """Own a topology for this invocation and always perform exact targeted cleanup."""
    harness = Harness(state)
    try:
        harness.start(topology)
        endpoint = next(
            (node for node in harness.nodes() if node["role"] == "standalone"), None
        )
        if endpoint is None:
            raise ValueError("Fixture replay requires standalone or all topology")
        for protocol in (2, 3):
            with Harness.connect(endpoint, protocol) as conn:
                captured = Recorder(conn, protocol).run()
            path = FIXTURES / f"resp{protocol}.json"
            if record:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    json.dumps(captured, indent=2, ensure_ascii=False) + "\n"
                )
            else:
                expected = json.loads(path.read_text())
                assert captured == expected, f"RESP{protocol} wire fixture changed"
        result: dict[str, object] = {"fixture_protocols": [2, 3]}
        if topology in ("all", "replica"):
            result["replica"] = {
                f"resp{protocol}": replica_affinity(harness, protocol)
                for protocol in (2, 3)
            }
        if topology == "all":
            result["cluster"] = {
                f"resp{protocol}": {
                    **cluster_affinity(harness, protocol),
                    **cluster_ask(harness, protocol),
                }
                for protocol in (2, 3)
            }
        if topology == "all":
            path = FIXTURES / "topology.json"
            if record:
                path.write_text(json.dumps(result, indent=2) + "\n")
            else:
                assert result == json.loads(
                    path.read_text()
                ), "Topology fixture changed"
        return result
    finally:
        harness.stop()


def main() -> None:
    """Run fast unit tests and the dedicated live server gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--record", action="store_true", help="Replace fixtures after assertions pass"
    )
    parser.add_argument(
        "--topology", choices=("all", "standalone", "replica"), default="all"
    )
    parser.add_argument("--state", type=Path)
    args = parser.parse_args()

    def interrupted(signum: int, _: object) -> None:
        raise InterruptedError(f"Received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    state = args.state or Path(f".build/falkordb-{uuid.uuid4().hex}.json")
    result = live(state, args.topology, args.record)
    suite = unittest.TestSuite(
        unittest.defaultTestLoader.loadTestsFromTestCase(case)
        for case in (ProtocolTests, MonitorTests)
    )
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        sys.exit(1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
