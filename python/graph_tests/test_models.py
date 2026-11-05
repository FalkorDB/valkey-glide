# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import copy
import datetime
import gc
from decimal import Decimal

import pytest
from glide_shared.exceptions import RequestError
from glide_shared.graph import (
    GraphMap,
    GraphNode,
    GraphPath,
    GraphPoint,
    GraphRelationship,
    GraphTemporal,
    GraphVector,
    _NativeGraphContext,
    decode_graph_result,
    encode_graph_request,
    validate_graph_name,
)
from glide_shared.protobuf.graph_request_pb2 import QueryRequest


def test_native_context_copies_share_one_release():
    pointer = object()
    released = []
    context = _NativeGraphContext(pointer, released.append)
    shallow = copy.copy(context)
    deep = copy.deepcopy(context)
    assert shallow is context
    assert deep is context
    del context, shallow
    gc.collect()
    assert released == []
    del deep
    gc.collect()
    assert released == [pointer]
    gc.collect()
    assert released == [pointer]


def encode(params=None, timeout=None, query="RETURN $value"):
    return QueryRequest.FromString(
        encode_graph_request("graph", 7, query, params, timeout, True, 12)
    )


def cell(kind, value):
    return {b"kind": kind.encode(), b"value": value}


def result(cells, columns=None, statistics=None):
    return decode_graph_result(
        {
            b"columns": columns if columns is not None else [b"x"] * len(cells),
            b"rows": [cells],
            b"statistics": statistics or {b"raw": []},
        }
    )


@pytest.mark.parametrize(
    "value,kind",
    [
        (None, "null_value"),
        (False, "boolean_value"),
        (True, "boolean_value"),
        (-(2**63), "integer_value"),
        (2**63 - 1, "integer_value"),
        (1.25, "double_value"),
        ("'\\\n\t\0☺", "string_value"),
        ([], "list_value"),
        ({}, "map_value"),
    ],
)
def test_typed_parameters_are_lossless(value, kind):
    request = encode({"value": value})
    parameter = request.parameters.entries[0].value
    assert parameter.WhichOneof("value") == kind
    if kind not in ("list_value", "map_value", "null_value"):
        assert getattr(parameter, kind) == value
    assert request.database == 7
    assert request.database_epoch == 12
    assert request.readonly
    assert not request.HasField("timeout_ms")


def test_nested_parameters_and_repeated_container_are_supported():
    shared = [False, -(2**63), 2**63 - 1]
    request = encode({"first": {"space key": shared}, "second": shared}, timeout=0)
    first = request.parameters.entries[0].value.map_value.entries[0]
    assert first.key == "space key"
    assert first.value.list_value.values[1].integer_value == -(2**63)
    assert request.parameters.entries[1].value.list_value.values[2].integer_value == (
        2**63 - 1
    )
    assert request.HasField("timeout_ms")
    assert request.timeout_ms == 0


@pytest.mark.parametrize(
    "value",
    [
        2**63,
        -(2**63) - 1,
        float("nan"),
        float("inf"),
        float("-inf"),
        datetime.date(2020, 1, 1),
        datetime.datetime(2020, 1, 1),
        Decimal("1.2"),
        b"bytes",
        (1, 2),
        object(),
        GraphNode(1, ["Label"], GraphMap([])),
    ],
)
def test_unsupported_parameters_include_the_path(value):
    with pytest.raises((TypeError, ValueError), match=r"params\['outer'\]\[0\]"):
        encode({"outer": [value]})


def test_cycle_is_rejected_without_rejecting_repeated_containers():
    items = []
    items.append({"cycle": items})
    with pytest.raises(ValueError, match=r"params\['value'\]\[0\]\['cycle'\].*cyclic"):
        encode({"value": items})
    mapping = {}
    mapping["self"] = mapping
    with pytest.raises(ValueError, match="cyclic"):
        encode(mapping)


@pytest.mark.parametrize("timeout", [-1, True, 1.5, 2**63, "1"])
def test_invalid_timeout_is_local(timeout):
    with pytest.raises(ValueError, match="timeout_ms"):
        encode(timeout=timeout)


@pytest.mark.parametrize("name", ["", "a\0b", "a\nb", "a\x7fb", "\ud800", 1])
def test_invalid_graph_selection_is_local(name):
    with pytest.raises((TypeError, ValueError)):
        validate_graph_name(name)


@pytest.mark.parametrize("key", ["", "tick`key", "a\0b", "a\nb", "é" * 257, 1])
def test_invalid_parameter_identifiers(key):
    with pytest.raises((TypeError, ValueError), match="params"):
        encode({key: 1})


def test_query_nul_and_deep_nesting_are_local():
    with pytest.raises(ValueError, match="query"):
        encode(query="RETURN 'a\0b'")
    value = 1
    for _ in range(34):
        value = [value]
    with pytest.raises(ValueError, match="nesting"):
        encode({"value": value})


def test_canonical_scalars_maps_and_statistics_are_exact():
    decoded = result(
        [
            cell("null", None),
            cell("integer", -(2**63)),
            cell("integer", 2**63 - 1),
            cell("string", b"hello"),
            cell("boolean", False),
            cell("double", -0.0),
            cell("list", [cell("integer", 1)]),
            cell(
                "map",
                [
                    [b"duplicate", cell("integer", 1)],
                    [b"duplicate", cell("integer", 2)],
                ],
            ),
        ],
        statistics={
            b"raw": [b"Unknown future statistic: 8"],
            b"nodes_created": 2,
            b"cached_execution": False,
            b"execution_time_ms": 1.25,
            b"graph_version": str(2**64 - 1).encode(),
        },
    )
    assert decoded.columns == ["x"] * 8
    assert decoded.rows[0][:7] == [None, -(2**63), 2**63 - 1, "hello", False, -0.0, [1]]
    assert decoded.rows[0][7] == GraphMap([("duplicate", 1), ("duplicate", 2)])
    assert decoded.statistics.raw == ["Unknown future statistic: 8"]
    assert decoded.statistics.nodes_created == 2
    assert decoded.statistics.nodes_deleted is None
    assert decoded.statistics.cached_execution is False
    assert decoded.statistics.graph_version == 2**64 - 1


def test_entities_paths_points_vectors_temporal_are_materialized():
    node = cell(
        "node",
        {
            b"id": 2**63 - 1,
            b"labels": [b"Person"],
            b"properties": [[b"age", cell("integer", 42)]],
        },
    )
    relationship = cell(
        "relationship",
        {
            b"id": 8,
            b"type": b"KNOWS",
            b"source_id": 2**63 - 1,
            b"destination_id": 4,
            b"properties": [],
        },
    )
    row = result(
        [
            node,
            relationship,
            cell("path", {b"nodes": [node], b"relationships": [relationship]}),
            cell("point", {b"latitude": 1.25, b"longitude": -2.5}),
            cell("vector", [1.0, 2.5]),
            *[
                cell(kind, -(2**63))
                for kind in ("datetime", "date", "time", "duration")
            ],
        ]
    ).rows[0]
    assert row[0] == GraphNode(2**63 - 1, ["Person"], GraphMap([("age", 42)]))
    assert row[1] == GraphRelationship(8, "KNOWS", 2**63 - 1, 4, GraphMap([]))
    assert row[2] == GraphPath([row[0]], [row[1]])
    assert row[3] == GraphPoint(1.25, -2.5)
    assert row[4] == GraphVector([1.0, 2.5])
    assert row[5:] == [
        GraphTemporal(kind, -(2**63))
        for kind in ("datetime", "date", "time", "duration")
    ]


@pytest.mark.parametrize(
    "bad",
    [
        [[], [], []],
        {b"columns": [b"x"], b"rows": [[]], b"statistics": {b"raw": []}},
        {b"columns": [], b"rows": [], b"statistics": {}},
    ],
)
def test_raw_or_malformed_envelopes_fail_closed(bad):
    with pytest.raises(RequestError, match="canonical graph"):
        decode_graph_result(bad)


@pytest.mark.parametrize(
    "bad",
    [
        cell("unknown", None),
        cell("integer", True),
        cell("boolean", 1),
        cell("double", 1),
        cell("map", [[b"key"]]),
        cell("path", {b"nodes": [cell("integer", 1)], b"relationships": []}),
    ],
)
def test_malformed_canonical_values_fail_closed(bad):
    with pytest.raises(RequestError, match="canonical graph"):
        result([bad])
