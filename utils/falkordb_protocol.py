#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
"""Small, dependency-free RESP recorder; deliberately not a GLIDE implementation."""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from typing import BinaryIO


@dataclass(frozen=True)
class Frame:
    """Preserve the RESP wire type, including errors nested inside arrays."""

    kind: str
    value: str | int | None | list[Frame]

    def plain(self) -> object:
        """Return convenient values without discarding nested error identity."""
        if isinstance(self.value, list):
            return [item.plain() for item in self.value]
        if self.kind == "error":
            return {"error": self.value}
        return self.value

    def json(self, normalize_time: bool = False) -> dict[str, object]:
        """Produce portable JSON while retaining every RESP type."""
        value: object = self.value
        if isinstance(self.value, list):
            children = self.value
            if (
                normalize_time
                and self.kind == "array"
                and len(children) in (1, 3)
                and children[-1].kind == "array"
                and isinstance(children[-1].value, list)
            ):
                statistics = [
                    (
                        Frame(
                            item.kind,
                            re.sub(
                                r"^Query internal execution time: \d+\.\d+ milliseconds$",
                                "Query internal execution time: <milliseconds> milliseconds",
                                item.value,
                            ),
                        )
                        if item.kind == "bulk" and isinstance(item.value, str)
                        else item
                    )
                    for item in children[-1].value
                ]
                children = [*children[:-1], Frame("array", statistics)]
            value = [item.json() for item in children]
        return {"type": self.kind, "value": value}


def read_frame(stream: BinaryIO) -> Frame:
    """Read the fixed-size RESP2/RESP3 types emitted by the pinned server."""
    prefix = stream.read(1)
    line = stream.readline()
    if not prefix or not line.endswith(b"\r\n"):
        raise ConnectionError("Truncated RESP frame")
    text = line[:-2].decode("utf-8")
    if prefix in (b"*", b"%", b"~"):
        count = int(text)
        if count == -1:
            return Frame("null", None)
        if count < 0:
            raise ValueError("Invalid RESP aggregate length")
        kind = {b"*": "array", b"%": "map", b"~": "set"}[prefix]
        return Frame(
            kind,
            [read_frame(stream) for _ in range(count * (2 if prefix == b"%" else 1))],
        )
    if prefix in (b"$", b"!", b"="):
        size = int(text)
        if size == -1:
            return Frame("null", None)
        if size < 0:
            raise ValueError("Invalid RESP string length")
        data = stream.read(size)
        if len(data) != size or stream.read(2) != b"\r\n":
            raise ConnectionError("Truncated RESP string")
        kind = {b"$": "bulk", b"!": "error", b"=": "verbatim"}[prefix]
        return Frame(kind, data.decode("utf-8"))
    if prefix == b":":
        return Frame("integer", int(text))
    if prefix in (b"+", b"-", b",", b"#", b"("):
        return Frame(
            {
                b"+": "simple",
                b"-": "error",
                b",": "double",
                b"#": "boolean",
                b"(": "bigint",
            }[prefix],
            text,
        )
    if prefix == b"_" and not text:
        return Frame("null", None)
    raise ValueError(f"Unsupported RESP prefix: {prefix!r}")


class Connection:
    """One physical connection, so metadata and results can share node affinity."""

    def __init__(
        self, host: str, port: int, protocol: int = 2, *, timeout: float = 10
    ) -> None:
        """Connect and optionally negotiate RESP3."""
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.stream = self.socket.makefile("rb")
        if protocol not in (2, 3):
            self.close()
            raise ValueError("Only RESP2 and RESP3 are supported")
        if protocol == 3:
            reply = self.command("HELLO", "3")
            if reply.kind != "map":
                self.close()
                raise RuntimeError(f"HELLO 3 failed: {reply}")

    def command(self, *args: str | int) -> Frame:
        """Send one command and return its typed frame, without raising Redis errors."""
        return self.pipeline([args])[0]

    def pipeline(self, commands: list[tuple[str | int, ...]]) -> list[Frame]:
        """Send an atomic-on-this-connection command batch, e.g. ASKING plus query."""
        wire = b""
        for args in commands:
            values = [str(arg).encode("utf-8") for arg in args]
            wire += f"*{len(values)}\r\n".encode("ascii") + b"".join(
                f"${len(value)}\r\n".encode("ascii") + value + b"\r\n"
                for value in values
            )
        self.socket.sendall(wire)
        return [read_frame(self.stream) for _ in commands]

    def close(self) -> None:
        """Close both file wrapper and socket."""
        self.stream.close()
        self.socket.close()

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
