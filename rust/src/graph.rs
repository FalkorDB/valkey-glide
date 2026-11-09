// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Typed, query-first FalkorDB graph operations.
//!
//! Graph selection is local. Queries share the existing client's connections,
//! selected database and request deadline. Parameters are serialized by the Rust
//! core, not interpolated into Cypher. FalkorDB v6.0.3 is the initial baseline.

use crate::{GlideError, ValkeyResult};
use glide_core::graph::GraphContext;

pub use glide_core::graph::{
    GraphNode, GraphParameter, GraphPath, GraphPoint, GraphQueryOptions, GraphQueryResult,
    GraphRelationship, GraphStatistics, GraphValue,
};

/// A lightweight graph selected from a standalone or cluster client.
///
/// Clones share their graph context. Dictionaries are resolved per operation,
/// since the server does not expose a graph-incarnation token. Concurrent
/// external graph replacement and concurrent `SELECT` are not supported.
#[derive(Clone)]
pub struct Graph {
    inner: GraphContext,
}

impl Graph {
    pub(crate) fn new(client: glide_core::client::Client, name: String) -> ValkeyResult<Self> {
        let database = client.current_database();
        GraphContext::new(client, name, database)
            .map(|inner| Self { inner })
            .map_err(GlideError::from_redis_error)
    }

    /// Return the locally selected name.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Execute `GRAPH.QUERY` on the graph's primary.
    ///
    /// A timeout or disconnect is not retried: execution may have occurred.
    pub async fn query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> ValkeyResult<GraphQueryResult> {
        self.inner
            .query(query, options)
            .await
            .map_err(GlideError::from_redis_error)
    }

    /// Execute `GRAPH.RO_QUERY`, honoring the client's replica-read strategy.
    pub async fn ro_query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> ValkeyResult<GraphQueryResult> {
        self.inner
            .ro_query(query, options)
            .await
            .map_err(GlideError::from_redis_error)
    }
}

/// Blocking graph operations, using the same context and codec as [`Graph`].
#[cfg(feature = "sync")]
#[derive(Clone)]
pub struct SyncGraph {
    inner: Graph,
}

#[cfg(feature = "sync")]
impl SyncGraph {
    pub(crate) fn new(inner: Graph) -> Self {
        Self { inner }
    }

    /// Return the locally selected graph name.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Execute a query and block until its result is decoded.
    pub fn query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> ValkeyResult<GraphQueryResult> {
        crate::sync::block_on(self.inner.query(query, options))
    }

    /// Execute a read-only query and block until its result is decoded.
    pub fn ro_query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> ValkeyResult<GraphQueryResult> {
        crate::sync::block_on(self.inner.ro_query(query, options))
    }
}
