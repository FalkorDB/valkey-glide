# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Shared, lossless FalkorDB result models and typed query parameters."""

from __future__ import annotations

import math
import os
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from typing import Union, cast

from .exceptions import ClosingError, RequestError
from .protobuf.graph_request_pb2 import Parameter, ParameterMap, QueryRequest

GraphParameter = Union[
    None, bool, int, float, str, list["GraphParameter"], dict[str, "GraphParameter"]
]


class _NativeGraphContext:
    """One owned Rust context, shared by copies of a language graph handle."""

    def __init__(self, pointer: object, release: Callable[[object], object]) -> None:
        self._pointer = pointer
        self._process_id = os.getpid()
        self._finalizer = weakref.finalize(
            self, _release_native_context, pointer, release, self._process_id
        )

    @property
    def pointer(self) -> object:
        if self._process_id != os.getpid():
            raise ClosingError(
                "Cannot use a graph handle created before fork(). "
                "Select a new graph in the child process."
            )
        return self._pointer

    def __copy__(self) -> _NativeGraphContext:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> _NativeGraphContext:
        return self


def _release_native_context(
    pointer: object, release: Callable[[object], object], process_id: int
) -> None:
    # The inherited Rust client's threads and locks did not survive fork.
    # Neither a query nor its destructor may enter that runtime in the child.
    if process_id == os.getpid():
        release(pointer)


@dataclass(frozen=True)
class GraphMap:
    """Ordered graph map entries, preserving duplicate keys."""

    entries: list[tuple[str, GraphValue]]


@dataclass(frozen=True)
class GraphNode:
    """A graph node with resolved labels and property names."""

    id: int
    labels: list[str]
    properties: GraphMap


@dataclass(frozen=True)
class GraphRelationship:
    """A directed relationship with resolved type and property names."""

    id: int
    type: str
    source_id: int
    destination_id: int
    properties: GraphMap


@dataclass(frozen=True)
class GraphPath:
    """An ordered path; relationship endpoints retain their stored direction."""

    nodes: list[GraphNode]
    relationships: list[GraphRelationship]


@dataclass(frozen=True)
class GraphPoint:
    """A geographical point in degrees."""

    latitude: float
    longitude: float


@dataclass(frozen=True)
class GraphVector:
    """A vector whose components preserve the server's float32 values."""

    values: list[float]


@dataclass(frozen=True)
class GraphTemporal:
    """A lossless server temporal value, not a Python date or timedelta.

    ``kind`` is datetime, date, time, or duration. ``value`` contains the
    server's signed epoch seconds (or its calendar-offset duration encoding).
    """

    kind: str
    value: int


GraphValue = Union[
    None,
    bool,
    int,
    float,
    str,
    list["GraphValue"],
    GraphMap,
    GraphNode,
    GraphRelationship,
    GraphPath,
    GraphPoint,
    GraphVector,
    GraphTemporal,
]


@dataclass(frozen=True)
class GraphStatistics:
    """Query statistics, retaining all original lines, including unknown ones."""

    raw: list[str]
    labels_added: int | None = None
    labels_removed: int | None = None
    nodes_created: int | None = None
    nodes_deleted: int | None = None
    properties_set: int | None = None
    properties_removed: int | None = None
    relationships_created: int | None = None
    relationships_deleted: int | None = None
    indices_created: int | None = None
    indices_deleted: int | None = None
    cached_execution: bool | None = None
    execution_time_ms: float | None = None
    graph_version: int | None = None


@dataclass(frozen=True)
class GraphQueryResult:
    """Column names (including duplicates), typed rows, and query statistics."""

    columns: list[str]
    rows: list[list[GraphValue]]
    statistics: GraphStatistics


def _validate_text(value: str, path: str, allow_nul: bool = False) -> None:
    if type(value) is not str:
        raise TypeError(f"{path}: expected a string")
    if not allow_nul and "\0" in value:
        raise ValueError(f"{path}: NUL characters are not supported")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{path}: invalid Unicode string") from error


def validate_graph_name(name: str) -> None:
    """Validate a graph name locally without opening a connection."""
    _validate_text(name, "graph_name")
    if not name or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in name):
        raise ValueError("graph_name: must not be empty or contain control characters")


def _validate_key(key: str, path: str) -> None:
    _validate_text(key, path)
    if (
        not key
        or "`" in key
        or len(key.encode("utf-8")) > 512
        or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in key)
    ):
        raise ValueError(f"{path}: invalid Cypher parameter/map identifier")


def _scalar_parameter(value: GraphParameter, path: str) -> Parameter | None:
    result = Parameter()
    if value is None:
        result.null_value = True
    elif type(value) is bool:
        result.boolean_value = value
    elif type(value) is int:
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError(f"{path}: integer outside signed 64-bit range")
        result.integer_value = value
    elif type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{path}: non-finite float is not supported")
        result.double_value = value
    elif type(value) is str:
        _validate_text(value, path, allow_nul=True)
        result.string_value = value
    else:
        return None
    return result


def _parameter(
    value: GraphParameter, path: str, active: set[int], depth: int
) -> Parameter:
    if depth > 32:
        raise ValueError(f"{path}: parameter nesting exceeds 32 levels")
    result = _scalar_parameter(value, path)
    if result is not None:
        return result
    if type(value) not in (list, dict):
        raise TypeError(f"{path}: unsupported parameter type {type(value).__name__}")
    identity = id(value)
    if identity in active:
        raise ValueError(f"{path}: cyclic parameter container")
    active.add(identity)
    result = Parameter()
    try:
        if isinstance(value, list):
            result.list_value.SetInParent()
            for index, item in enumerate(value):
                result.list_value.values.add().CopyFrom(
                    _parameter(item, f"{path}[{index}]", active, depth + 1)
                )
        else:
            result.map_value.CopyFrom(
                _parameters(
                    cast(dict[str, GraphParameter], value), path, active, depth + 1
                )
            )
    finally:
        active.remove(identity)
    return result


def _parameters(
    params: dict[str, GraphParameter], path: str, active: set[int], depth: int
) -> ParameterMap:
    result = ParameterMap()
    for key, value in params.items():
        _validate_key(key, f"{path} key")
        entry = result.entries.add(key=key)
        entry.value.CopyFrom(_parameter(value, f"{path}[{key!r}]", active, depth))
    return result


def encode_graph_request(
    name: str,
    database: int,
    query: str,
    params: dict[str, GraphParameter] | None,
    timeout_ms: int | None,
    readonly: bool,
    database_epoch: int = 0,
) -> bytes:
    """Encode graph parameters without JSON or floating-point integer coercion."""
    validate_graph_name(name)
    _validate_text(query, "query")
    if timeout_ms is not None and (
        type(timeout_ms) is not int or not 0 <= timeout_ms < (1 << 63)
    ):
        raise ValueError("timeout_ms: expected an integer in [0, 2**63 - 1]")
    if params is not None and type(params) is not dict:
        raise TypeError("params: expected a dictionary with string keys")
    request = QueryRequest(
        graph_name=name,
        query=query,
        readonly=readonly,
        database=database,
        database_epoch=database_epoch,
    )
    if params is not None:
        request.parameters.CopyFrom(_parameters(params, "params", {id(params)}, 0))
    if timeout_ms is not None:
        request.timeout_ms = timeout_ms
    return request.SerializeToString()


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise ValueError("expected a string in the canonical graph result")


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected a map in the canonical graph result")
    return {_text(key): item for key, item in value.items()}


def _array(value: object) -> list:
    if not isinstance(value, list):
        raise TypeError("expected an array in the canonical graph result")
    return value


def _integer(value: object) -> int:
    if type(value) is not int:
        raise ValueError("expected an integer in the canonical graph result")
    return value


def _entries(value: object) -> GraphMap:
    entries = []
    for pair in _array(value):
        pair = _array(pair)
        if len(pair) != 2:
            raise ValueError("invalid canonical graph map entry")
        entries.append((_text(pair[0]), _decode_value(pair[1])))
    return GraphMap(entries)


def _null(value: object) -> None:
    if value is not None:
        raise ValueError("invalid canonical graph null")


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise ValueError("invalid canonical graph boolean")
    return value


def _double(value: object) -> float:
    if type(value) is not float:
        raise ValueError("invalid canonical graph double")
    return value


def _decode_list(value: object) -> list[GraphValue]:
    return [_decode_value(entry) for entry in _array(value)]


def _decode_path(fields: dict[str, object]) -> GraphPath:
    nodes = [_decode_value(node) for node in _array(fields["nodes"])]
    relationships = [_decode_value(edge) for edge in _array(fields["relationships"])]
    if any(not isinstance(node, GraphNode) for node in nodes) or any(
        not isinstance(edge, GraphRelationship) for edge in relationships
    ):
        raise ValueError("invalid canonical graph path")
    typed_nodes = cast(list[GraphNode], nodes)
    typed_relationships = cast(list[GraphRelationship], relationships)
    if len(typed_nodes) != len(typed_relationships) + 1:
        raise ValueError("invalid canonical graph path length")
    for index, edge in enumerate(typed_relationships):
        adjacent = (typed_nodes[index].id, typed_nodes[index + 1].id)
        if adjacent not in (
            (edge.source_id, edge.destination_id),
            (edge.destination_id, edge.source_id),
        ):
            raise ValueError("invalid canonical graph path endpoints")
    return GraphPath(typed_nodes, typed_relationships)


_SIMPLE_DECODERS: dict[str, Callable[[object], GraphValue]] = {
    "null": _null,
    "string": _text,
    "integer": _integer,
    "boolean": _boolean,
    "double": _double,
    "list": _decode_list,
    "map": _entries,
}


def _decode_value(envelope: object) -> GraphValue:
    item = _mapping(envelope)
    kind, value = _text(item["kind"]), item["value"]
    decoder = _SIMPLE_DECODERS.get(kind)
    if decoder is not None:
        return decoder(value)
    if kind == "vector":
        components = _array(value)
        if any(type(component) is not float for component in components):
            raise ValueError("invalid canonical graph vector")
        return GraphVector(components)
    if kind in ("datetime", "date", "time", "duration"):
        return GraphTemporal(kind, _integer(value))
    fields = _mapping(value)
    if kind == "node":
        return GraphNode(
            _integer(fields["id"]),
            [_text(label) for label in _array(fields["labels"])],
            _entries(fields["properties"]),
        )
    if kind == "relationship":
        return GraphRelationship(
            _integer(fields["id"]),
            _text(fields["type"]),
            _integer(fields["source_id"]),
            _integer(fields["destination_id"]),
            _entries(fields["properties"]),
        )
    if kind == "path":
        return _decode_path(fields)
    if kind == "point":
        latitude, longitude = fields["latitude"], fields["longitude"]
        if type(latitude) is not float or type(longitude) is not float:
            raise ValueError("invalid canonical graph point")
        return GraphPoint(latitude, longitude)
    raise ValueError(f"unknown canonical graph value kind: {kind}")


def _statistics(stats: dict[str, object]) -> GraphStatistics:
    def integer(key: str) -> int | None:
        value = stats.get(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            if not value.isdigit() or len(value) > 20:
                raise ValueError("invalid canonical unsigned graph statistic")
            value = int(value)
        parsed = _integer(value)
        if not 0 <= parsed < (1 << 64):
            raise ValueError("canonical graph statistic outside unsigned 64-bit range")
        return parsed

    cached = stats.get("cached_execution")
    duration = stats.get("execution_time_ms")
    if cached is not None and type(cached) is not bool:
        raise ValueError("invalid canonical cached_execution statistic")
    if duration is not None and type(duration) is not float:
        raise ValueError("invalid canonical execution_time_ms statistic")
    return GraphStatistics(
        raw=[_text(line) for line in _array(stats["raw"])],
        labels_added=integer("labels_added"),
        labels_removed=integer("labels_removed"),
        nodes_created=integer("nodes_created"),
        nodes_deleted=integer("nodes_deleted"),
        properties_set=integer("properties_set"),
        properties_removed=integer("properties_removed"),
        relationships_created=integer("relationships_created"),
        relationships_deleted=integer("relationships_deleted"),
        indices_created=integer("indices_created"),
        indices_deleted=integer("indices_deleted"),
        cached_execution=cast("bool | None", cached),
        execution_time_ms=cast("float | None", duration),
        graph_version=integer("graph_version"),
    )


def decode_graph_result(value: object) -> GraphQueryResult:
    """Materialize the Rust core's canonical envelope, never a compact response."""
    try:
        envelope = _mapping(value)
        columns = [_text(column) for column in _array(envelope["columns"])]
        rows = [
            [_decode_value(cell) for cell in _array(row)]
            for row in _array(envelope["rows"])
        ]
        if any(len(row) != len(columns) for row in rows):
            raise ValueError("canonical graph row width does not match columns")
        statistics = _statistics(_mapping(envelope["statistics"]))
        return GraphQueryResult(columns, rows, statistics)
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        raise RequestError(f"Invalid canonical graph response: {error}") from error
