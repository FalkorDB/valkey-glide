#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
"""Executable compatibility cases, recorded directly from FalkorDB 6.0.3."""

from __future__ import annotations

import re
import uuid
from pathlib import Path

from falkordb_protocol import Connection, Frame

if not __debug__:
    raise RuntimeError(
        "FalkorDB recorder safety checks require assertions; optimized Python "
        "(-O, -OO or PYTHONOPTIMIZE) is unsupported"
    )

FIXTURES = Path(__file__).resolve().parents[1] / "glide-core/tests/fixtures/graph"
ALL_TAGS_QUERY = (
    "CREATE p=(a:Person:Researcher {name:'Ada',age:42})"
    "-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN "
    "null AS null_value, 'text' AS string_value, "
    "-9223372036854775808 AS integer_value, true AS boolean_value, "
    "1.5 AS double_value, [1,null,{nested:[false,'x']}] AS list_value, "
    "r AS relationship_value, a AS node_value, p AS path_value, "
    "{first:1,second:'two'} AS map_value, "
    "point({latitude:1.5,longitude:2.5}) AS point_value, "
    "vecf32([1,2.5,-3]) AS vector_value, "
    "localdatetime('2020-01-02T03:04:05') AS datetime_value, "
    "date('2020-01-02') AS date_value, "
    "localtime('03:04:05') AS time_value, "
    "duration('P1Y2M3DT4H5M6S') AS duration_value"
)


def array(frame: Frame) -> list[Frame]:
    """Assert a non-null RESP array before traversing it."""
    assert frame.kind == "array" and isinstance(frame.value, list), frame
    return frame.value


def rows(frame: Frame) -> list[Frame]:
    """Read the rows of a three-part result envelope."""
    envelope = array(frame)
    assert len(envelope) == 3, frame
    return array(envelope[1])


def stats(frame: Frame) -> dict[str, str]:
    """Read stats-only or full results without confusing schema and write versions."""
    result: dict[str, str] = {}
    for item in array(array(frame)[-1]):
        assert item.kind == "bulk" and isinstance(item.value, str), item
        key, value = item.value.split(": ", 1)
        result[key] = value
    return result


def scalar(frame: Frame) -> object:
    """Read one compact scalar from a one-row, one-column result."""
    return array(array(rows(frame)[0])[0])[1].plain()


class Recorder:
    """Record commands with replaceable graph keys and independently check semantics."""

    def __init__(self, connection: Connection, protocol: int) -> None:
        """Allocate a private key prefix; never FLUSH a server."""
        self.connection = connection
        self.protocol = protocol
        self.prefix = f"glide-graph-fixture:{uuid.uuid4().hex}"
        self.cases: list[dict[str, object]] = []
        self.keys: list[str] = []
        self.key = ""
        self.steps: list[dict[str, object]] = []

    def case(self, name: str) -> None:
        """Start a new case with a distinct graph and deterministic entity IDs."""
        self.key = f"{self.prefix}:{name}"
        self.keys.append(self.key)
        self.steps = []
        self.cases.append({"name": name, "steps": self.steps})

    def command(self, *args: str | int) -> Frame:
        """Record one actual wire response; only execution duration is normalized."""
        result = self.connection.command(*args)
        self.steps.append(
            {
                "command": [
                    "{graph}" if str(arg) == self.key else str(arg) for arg in args
                ],
                "response": result.json(normalize_time=True),
            }
        )
        return result

    def query(self, query: str, *flags: str | int, readonly: bool = False) -> Frame:
        """Issue a compact graph command, using the same physical connection."""
        return self.command(
            "GRAPH.RO_QUERY" if readonly else "GRAPH.QUERY",
            self.key,
            query,
            "--compact",
            *flags,
        )

    def cleanup(self) -> None:
        """Delete only the exact graph keys created by this recorder."""
        for key in self.keys:
            self.connection.command("DEL", key)

    def run(self) -> dict[str, object]:
        """Execute all assertions before allowing a fixture to be recorded."""
        try:
            self.all_tags()
            self.envelopes_and_schema()
            self.recreation()
            self.parameters()
            self.flag_bounds()
            self.temporal_and_raw()
            return {"protocol": self.protocol, "cases": self.cases}
        finally:
            self.cleanup()

    def all_tags(self) -> None:
        """Prove every compact tag, entity/property shape and metadata dictionary."""
        self.case("all_tags")
        result = self.query(ALL_TAGS_QUERY)
        cells = array(rows(result)[0])
        assert [array(cell)[0].plain() for cell in cells] == list(range(1, 17))
        assert [array(cell)[1].plain() for cell in cells[:5]] == [
            None,
            "text",
            -9223372036854775808,
            "true",
            "1.5",
        ]
        node = array(cells[7])[1].plain()
        relationship = array(cells[6])[1].plain()
        assert node == [0, [0, 1], [[0, 2, "Ada"], [1, 3, 42]]]
        assert relationship == [0, 0, 0, 1, [[2, 3, 2020]]]
        path = array(array(cells[8])[1])
        assert [array(part)[0].plain() for part in path] == [6, 6]
        assert len(array(array(path[0])[1])) == 2
        assert len(array(array(path[1])[1])) == 1
        assert array(cells[9])[1].plain() == ["first", [3, 1], "second", [2, "two"]]
        assert array(cells[10])[1].plain() == ["1.5", "2.5"]
        vector = array(array(cells[11])[1])
        assert [part.kind for part in vector] == [
            "bulk" if self.protocol == 2 else "double"
        ] * 3
        assert [part.plain() for part in vector] == ["1", "2.5", "-3"]
        assert [array(cell)[1].plain() for cell in cells[12:]] == [
            1577934245,
            1577923200,
            11045,
            36907506,
        ]
        assert stats(result)["Graph version"] == "1"
        for procedure, expected in (
            ("db.labels", ["Person", "Researcher"]),
            ("db.relationshipTypes", ["KNOWS"]),
            ("db.propertyKeys", ["name", "age", "since"]),
        ):
            metadata = self.query(f"CALL {procedure}()", readonly=True)
            assert [
                array(array(row)[0])[1].plain() for row in rows(metadata)
            ] == expected

    def envelopes_and_schema(self) -> None:
        """Prove pre-execution schema rejection and independent write versions."""
        self.case("envelopes_schema_and_write_version")
        missing = self.query("CREATE (:Wrong)", "version", 1)
        assert missing.plain() == [{"error": "ERR invalid graph version"}, 0]
        assert self.command("EXISTS", self.key).plain() == 0
        created = self.query("CREATE (n:Alpha {name:'first'})")
        assert len(array(created)) == 1
        assert stats(created)["Graph version"] == "1"
        updated = self.query("MATCH(n) SET n.name='second'")
        assert stats(updated)["Graph version"] == "2"
        rejected = self.query("MATCH(n) SET n.name='must-not-execute'", "version", 0)
        assert rejected.plain() == [{"error": "ERR invalid graph version"}, 1]
        accepted = self.query("MATCH(n) RETURN n.name", "version", 1)
        assert scalar(accepted) == "second"
        assert stats(accepted)["Graph version"] == "2"
        assert rows(self.query("MATCH(n:Missing) RETURN n")) == []
        refused = self.query("CREATE ()", readonly=True)
        assert refused.plain() == {
            "error": "graph.RO_QUERY is to be executed only on read-only queries"
        }
        self.query("MATCH(n) SET n:Beta")
        assert self.query("RETURN 1", "version", 1).plain() == [
            {"error": "ERR invalid graph version"},
            2,
        ]
        accepted = self.query("RETURN 1", "version", 2)
        assert stats(accepted)["Graph version"] == "3"
        assert stats(self.query("RETURN 1", "version", 2))["Cached execution"] == "1"

    def recreation(self) -> None:
        """Show equal schema guards/IDs can name different dictionaries after deletion."""
        self.case("recreation_requires_operation_scoped_dictionaries")
        self.query("CREATE (:Before {old:1})")
        before = self.query("CALL db.labels()", readonly=True)
        assert scalar(before) == "Before"
        self.command("GRAPH.DELETE", self.key)
        self.query("CREATE (:After {new:2})")
        after = self.query("MATCH(n) RETURN n", "version", 1)
        assert array(array(rows(after)[0])[0])[1].plain() == [
            0,
            [0],
            [[0, 3, 2]],
        ]
        assert scalar(self.query("CALL db.labels()", readonly=True)) == "After"
        assert scalar(self.query("CALL db.propertyKeys()", readonly=True)) == "new"

    def parameters(self) -> None:
        """Exercise pinned identifier quoting, nested typed values and string escapes."""
        self.case("cypher_parameters")
        text = "a'b\\c\n\t\r\b\f☺"
        query = (
            "CYPHER `x y`='a\\'b\\\\c\\n\\t\\r\\b\\f\\u263a' "
            "`map`={`a b`:[null,true,-9223372036854775808,1.5,'x']} "
            "RETURN $`x y` AS s, $map AS m"
        )
        result = self.query(query)
        assert array(array(rows(result)[0])[0])[1].plain() == text
        assert array(array(rows(result)[0])[1])[1].plain() == [
            "a b",
            [
                6,
                [
                    [1, None],
                    [4, "true"],
                    [3, -9223372036854775808],
                    [5, "1.5"],
                    [2, "x"],
                ],
            ],
        ]
        rejected = self.query('CYPHER `m`={"key":1} RETURN $m')
        assert rejected.kind == "error"
        assert rejected.value == "Failed to parse the value of parameter 'm'"
        rejected = self.query("CYPHER `x``y`=1 RETURN 1")
        assert rejected.kind == "error"
        assert "Invalid input" in str(rejected.value)

    def flag_bounds(self) -> None:
        """Prove canonical decimal, signed timeout bounds and unsigned schema bounds."""
        self.case("query_flag_bounds")
        for timeout in ("0", "1", "9223372036854775807"):
            assert scalar(self.query("RETURN 1", "TIMEOUT", timeout)) == 1
        for timeout in ("-1", "9223372036854775808", "01", "+1", " 1"):
            assert self.query("RETURN 1", "TIMEOUT", timeout).plain() == {
                "error": "Failed to parse query timeout value"
            }
        for version in ("-1", "4294967296", "01", "+1", " 1"):
            assert self.query("RETURN 1", "version", version).plain() == {
                "error": "Failed to parse graph version value"
            }
        assert self.query("RETURN 1", "version", "4294967295").plain() == [
            {"error": "ERR invalid graph version"},
            0,
        ]
        self.command("GRAPH.CONFIG", "SET", "TIMEOUT_MAX", "25")
        try:
            assert scalar(self.query("RETURN 1", "TIMEOUT", 25)) == 1
            too_long = self.query("RETURN 1", "TIMEOUT", 26)
            assert too_long.plain() == {
                "error": (
                    "The query TIMEOUT parameter value cannot exceed the TIMEOUT_MAX "
                    "configuration parameter value"
                )
            }
        finally:
            self.command("GRAPH.CONFIG", "SET", "TIMEOUT_MAX", "0")

    def temporal_and_raw(self) -> None:
        """Prove second precision, duration offset encoding, and untouched verbose output."""
        self.case("temporal_seconds_and_verbose")
        temporal = self.query(
            "RETURN localdatetime('2020-01-02T03:04:05.123') AS fractional, "
            "duration('PT1S') AS second, duration('P1M') AS month, duration('P0D') AS zero"
        )
        assert [array(cell)[1].plain() for cell in array(rows(temporal)[0])] == [
            1577934245,
            1,
            2678400,
            0,
        ]
        verbose = self.command(
            "GRAPH.QUERY", self.key, "RETURN 1 AS i, true AS b, [1,'x'] AS l"
        )
        assert array(verbose)[0].plain() == ["i", "b", "l"]
        assert array(verbose)[1].plain() == [[1, "true", "[1, x]"]]
        duration_line = stats(verbose)["Query internal execution time"]
        assert re.fullmatch(r"\d+\.\d+ milliseconds", duration_line)
