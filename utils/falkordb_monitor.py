#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
"""Bounded replica MONITOR evidence for dedicated cross-language graph tests."""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import select
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import BinaryIO, TextIO

from falkordb_protocol import Connection, Frame, read_frame

MONITOR_COMMAND = b"*1\r\n$7\r\nMONITOR\r\n"
MAX_LINE_BYTES = 2 * 1024 * 1024
MAX_MATCHED_COMMANDS = 256
METADATA = {
    "call db.labels()": "labels",
    "call db.relationshiptypes()": "relationship_types",
    "call db.propertykeys()": "property_keys",
}
ESCAPES = {
    ord("n"): b"\n",
    ord("r"): b"\r",
    ord("t"): b"\t",
    ord("b"): b"\b",
    ord("a"): b"\a",
    ord("f"): b"\f",
    ord("v"): b"\v",
    ord("\\"): b"\\",
    ord('"'): b'"',
}
HEADER = re.compile(r"^\d+\.\d+ \[(\d+) ([^\]]+)\] (.*)$")
TOKEN = re.compile(r'"((?:[^"\\]|\\.)*)"')
GRAPH_KEY = re.compile(r"[A-Za-z0-9_:{}.-]{1,512}", re.ASCII)


def unescape_token(token: str) -> str:
    """Decode Redis MONITOR's quoted-byte escapes, not Cypher or graph values."""
    source = token.encode("utf-8")
    output = bytearray()
    index = 0
    while index < len(source):
        byte = source[index]
        index += 1
        if byte != ord("\\"):
            output.append(byte)
            continue
        if index == len(source):
            raise ValueError("Truncated MONITOR escape")
        escaped = source[index]
        index += 1
        if escaped == ord("x"):
            digits = source[index : index + 2]
            if len(digits) != 2 or not re.fullmatch(b"[0-9a-fA-F]{2}", digits):
                raise ValueError("Invalid MONITOR hex escape")
            output.append(int(digits, 16))
            index += 2
        elif escaped in ESCAPES:
            output.extend(ESCAPES[escaped])
        else:
            raise ValueError("Unknown MONITOR escape")
    return output.decode("utf-8", errors="backslashreplace")


@dataclass(frozen=True)
class MonitorEvent:
    """One server MONITOR line with its physical client address."""

    db: int
    client: str
    command: str
    args: tuple[str, ...]


def parse_monitor_line(line: str) -> MonitorEvent:
    """Parse only the MONITOR text envelope, preserving decoded command arguments."""
    header = HEADER.fullmatch(line)
    if header is None:
        raise ValueError("Malformed MONITOR header")
    arguments = header[3]
    tokens = []
    end = 0
    for token in TOKEN.finditer(arguments):
        if arguments[end : token.start()].strip():
            raise ValueError("Malformed MONITOR argument separator")
        tokens.append(unescape_token(token[1]))
        end = token.end()
    if not tokens or arguments[end:].strip():
        raise ValueError("Malformed MONITOR quoted arguments")
    return MonitorEvent(
        db=int(header[1]),
        client=header[2],
        command=tokens[0].upper(),
        args=tuple(tokens[1:]),
    )


@dataclass
class Evidence:
    """Retain only exact-key graph commands; callers assert their query results."""

    graph: str
    counts: dict[str, int] = field(
        default_factory=lambda: {
            "query": 0,
            "labels": 0,
            "relationship_types": 0,
            "property_keys": 0,
            "write": 0,
        }
    )
    commands: list[dict[str, object]] = field(default_factory=list)
    connections: set[tuple[int, str]] = field(default_factory=set)

    def record(self, event: MonitorEvent) -> None:
        """Classify a selected graph query without decoding its result."""
        if (
            event.command not in ("GRAPH.RO_QUERY", "GRAPH.QUERY")
            or len(event.args) < 2
            or event.args[0] != self.graph
        ):
            return
        if len(self.commands) >= MAX_MATCHED_COMMANDS:
            raise ValueError("Too many matching graph commands in one capture")
        query = event.args[1]
        kind = (
            "write"
            if event.command == "GRAPH.QUERY"
            else METADATA.get(query.strip().casefold(), "query")
        )
        self.counts[kind] += 1
        self.commands.append(
            {
                "command": event.command,
                "query": query,
                "client": event.client,
                "db": event.db,
                "kind": kind,
            }
        )
        if event.command == "GRAPH.RO_QUERY":
            self.connections.add((event.db, event.client))

    def report(self, endpoint: str) -> dict[str, object]:
        """Report successful observation separately from complete affinity evidence."""
        return {
            "endpoint": endpoint,
            "graph": self.graph,
            "role": "replica",
            "counts": self.counts,
            "commands": self.commands,
            "complete": all(
                self.counts[kind] > 0
                for kind in ("query", "labels", "relationship_types", "property_keys")
            ),
            "same_connection": len(self.connections) == 1,
        }


def parse_endpoint(endpoint: str) -> tuple[str, int]:
    """Accept an explicit host:port, including bracketed IPv6 endpoints."""
    host, separator, port_text = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError("Endpoint must be host:port")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("Endpoint port must be between 1 and 65535")
    return host.removeprefix("[").removesuffix("]"), port


def require_replica(role: Frame) -> None:
    """Reject primary or malformed ROLE replies before issuing MONITOR."""
    if (
        role.kind != "array"
        or not isinstance(role.value, list)
        or not role.value
        or role.value[0].value != "slave"
    ):
        raise ValueError("Target ROLE must be replica (server role 'slave')")


@dataclass
class ControlInput:
    """Consume bounded STOP lines or a clean stdin EOF without buffered read-ahead."""

    pending: bytearray = field(default_factory=bytearray)

    def feed(self, chunk: bytes) -> bool:
        """Return whether the caller requested a fenced shutdown."""
        self.pending.extend(chunk)
        if len(self.pending) > 1024:
            raise ValueError("Monitor control input exceeds capture limit")
        stop = not chunk
        while b"\n" in self.pending:
            line, _, tail = self.pending.partition(b"\n")
            self.pending = bytearray(tail)
            if line.strip() == b"STOP":
                stop = True
            elif line.strip():
                raise ValueError("Monitor stdin accepts only STOP or clean EOF")
        if not chunk and bytes(self.pending.strip()) not in (b"", b"STOP"):
            raise ValueError("Monitor stdin accepts only STOP or clean EOF")
        return stop


@dataclass
class MonitorStream:
    """Frame a bounded RESP2 MONITOR stream using the existing RESP recorder."""

    evidence: Evidence
    output: TextIO
    ready: bool = False
    fence: str | None = None
    pending: bytearray = field(default_factory=bytearray)

    def accept(self, frame: Frame) -> bool:
        """Acknowledge readiness or record one event, returning true at the stop fence."""
        if frame.kind != "simple" or not isinstance(frame.value, str):
            raise ValueError("MONITOR did not return a simple-string frame")
        if not self.ready:
            if frame.value != "OK":
                raise ValueError("MONITOR did not acknowledge with +OK")
            self.ready = True
            print("READY", file=self.output, flush=True)
            return False
        event = parse_monitor_line(frame.value)
        if (
            self.fence is not None
            and event.command == "PING"
            and event.args == (self.fence,)
        ):
            return True
        self.evidence.record(event)
        return False

    def feed(self, chunk: bytes) -> bool:
        """Handle partial or coalesced frames without losing the last metadata event."""
        if not chunk:
            raise ConnectionError("MONITOR socket closed before the stop fence")
        self.pending.extend(chunk)
        while b"\r\n" in self.pending:
            line, _, tail = self.pending.partition(b"\r\n")
            self.pending = bytearray(tail)
            if len(line) > MAX_LINE_BYTES:
                raise ValueError("MONITOR line exceeds capture limit")
            if self.accept(read_frame(io.BytesIO(line + b"\r\n"))):
                return True
        if len(self.pending) > MAX_LINE_BYTES:
            raise ValueError("MONITOR line exceeds capture limit")
        return False


def validate_inputs(endpoint: str, graph: str, duration: float) -> tuple[str, int]:
    """Reject invalid capture configuration before opening owned sockets."""
    if GRAPH_KEY.fullmatch(graph) is None:
        raise ValueError("Graph key must be 1..512 ASCII letters/digits/_ : { } . -")
    if not math.isfinite(duration) or not 0 < duration <= 600:
        raise ValueError(
            "Timeout must be finite and greater than 0 through 600 seconds"
        )
    return parse_endpoint(endpoint)


def capture(
    endpoint: str,
    graph: str,
    duration: float,
    input_stream: BinaryIO,
    output_stream: TextIO,
) -> dict[str, object]:
    """Observe until STOP/EOF, then drain through a unique same-server PING fence."""
    host, port = validate_inputs(endpoint, graph, duration)
    deadline = time.monotonic() + duration

    def remaining() -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("Replica MONITOR capture exceeded its deadline")
        return left

    evidence = Evidence(graph)
    monitor = MonitorStream(evidence, output_stream)
    control = ControlInput()
    with Connection(host, port, timeout=min(10, remaining())) as connection:
        connection.socket.settimeout(remaining())
        require_replica(connection.command("ROLE"))
        # ROLE consumed the only requested reply. Read MONITOR directly from the
        # socket so BufferedReader read-ahead cannot hide frames from select().
        connection.socket.settimeout(remaining())
        connection.socket.sendall(MONITOR_COMMAND)
        while True:
            readers = [connection.socket.fileno()]
            if monitor.ready and monitor.fence is None:
                readers.append(input_stream.fileno())
            readable, _, _ = select.select(readers, [], [], remaining())
            if not readable:
                raise TimeoutError("Replica MONITOR capture exceeded its deadline")
            if connection.socket.fileno() in readable:
                if monitor.feed(connection.socket.recv(65536)):
                    return evidence.report(endpoint)
            if (
                monitor.ready
                and monitor.fence is None
                and input_stream.fileno() in readable
            ):
                if control.feed(os.read(input_stream.fileno(), 4096)):
                    monitor.fence = f"glide-falkordb-monitor-stop:{uuid.uuid4().hex}"
                    with Connection(
                        host, port, timeout=min(10, remaining())
                    ) as barrier:
                        barrier.socket.settimeout(remaining())
                        reply = barrier.command("PING", monitor.fence)
                        if reply.value != monitor.fence:
                            raise ValueError("MONITOR stop fence PING did not echo")


def main() -> int:
    """Run the line-oriented subprocess protocol used by binding conformance tests."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="replica host:port")
    parser.add_argument("--graph", required=True, help="exact ASCII UUID graph key")
    parser.add_argument(
        "--timeout", type=float, default=30, help="total seconds, max 600"
    )
    arguments = parser.parse_args()

    def terminated(_signal: int, _frame: object) -> None:
        raise InterruptedError("Replica MONITOR process terminated")

    signal.signal(signal.SIGTERM, terminated)
    try:
        report = capture(
            arguments.endpoint,
            arguments.graph,
            arguments.timeout,
            sys.stdin.buffer,
            sys.stdout,
        )
        print(json.dumps(report, separators=(",", ":")), flush=True)
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"ERROR {error}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("ERROR Replica MONITOR interrupted", file=sys.stderr, flush=True)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
