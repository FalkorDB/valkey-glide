# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Synchronous graph handles backed by the owning GLIDE client."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from glide_shared.graph import (
    GraphParameter,
    GraphQueryResult,
    _NativeGraphContext,
    decode_graph_result,
    encode_graph_request,
)

if TYPE_CHECKING:
    from .glide_client import BaseClient


@dataclass(frozen=True)
class Graph:
    """A local graph selection retaining its client and selected database."""

    _client: BaseClient = field(repr=False)
    graph_name: str
    database: int
    _database_epoch: int = field(repr=False)
    _native_context: _NativeGraphContext = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_native_context",
            self._client._create_graph_context(
                self.graph_name, self.database, self._database_epoch
            ),
        )

    def query(
        self,
        query: str,
        params: dict[str, GraphParameter] | None = None,
        timeout_ms: int | None = None,
    ) -> GraphQueryResult:
        """Execute a parameterized query on the primary; timeout is milliseconds."""
        request = encode_graph_request(
            self.graph_name,
            self.database,
            query,
            params,
            timeout_ms,
            False,
            self._database_epoch,
        )
        return decode_graph_result(
            self._client._execute_graph_request(request, self._native_context)
        )

    def ro_query(
        self,
        query: str,
        params: dict[str, GraphParameter] | None = None,
        timeout_ms: int | None = None,
    ) -> GraphQueryResult:
        """Execute a read-only query using the client's configured read strategy."""
        request = encode_graph_request(
            self.graph_name,
            self.database,
            query,
            params,
            timeout_ms,
            True,
            self._database_epoch,
        )
        return decode_graph_result(
            self._client._execute_graph_request(request, self._native_context)
        )
