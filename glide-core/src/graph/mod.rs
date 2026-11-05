// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Typed FalkorDB queries over the existing GLIDE client.
//!
//! Dictionaries are operation-scoped: v6.0.3 does not expose a graph-incarnation
//! token that makes reuse safe after another client replaces a graph.

pub mod bridge;
pub mod decode;
pub mod params;
#[cfg(feature = "proto")]
mod request;
mod value;

pub use value::*;

use crate::client::Client;
use redis::{Cmd, ErrorKind, RedisError, RedisResult};
use std::sync::Arc;

#[cfg(feature = "proto")]
pub use request::{execute_request, execute_request_with_context};

/// A locally selected graph. Clones retain the same client and selected database.
#[derive(Clone)]
pub struct GraphContext {
    inner: Arc<Context>,
}

struct Context {
    client: Client,
    name: String,
    database: u32,
    database_epoch: u64,
}

impl GraphContext {
    /// Select a graph without opening a connection or creating server state.
    ///
    /// `database` is captured at selection and checked before every operation.
    pub fn new(client: Client, name: String, database: u32) -> RedisResult<Self> {
        params::validate_graph_name(&name)?;
        if client.is_pool_managed() {
            return Err(RedisError::from((
                ErrorKind::ClientError,
                "Graph handles cannot retain a pooled client lease",
            )));
        }
        let database_epoch = client.graph_database_epoch();
        if database_epoch & 1 != 0 {
            return Err(database_changed());
        }
        Ok(Self {
            inner: Arc::new(Context {
                client,
                name,
                database,
                database_epoch,
            }),
        })
    }

    /// The graph's name, as selected locally.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Execute a query on the primary, without ambiguous transport retries.
    pub async fn query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> RedisResult<GraphQueryResult> {
        self.execute(query, options, false).await
    }

    /// Execute a read-only query using the parent's configured read strategy.
    pub async fn ro_query(
        &self,
        query: &str,
        options: &GraphQueryOptions,
    ) -> RedisResult<GraphQueryResult> {
        self.execute(query, options, true).await
    }

    async fn execute(
        &self,
        query: &str,
        options: &GraphQueryOptions,
        readonly: bool,
    ) -> RedisResult<GraphQueryResult> {
        let deadline = tokio::time::Instant::now() + self.inner.client.get_request_timeout();
        let query = params::serialize_query(query, options)?;
        if !self.database_matches() {
            return Err(database_changed());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
        }
        match tokio::time::timeout_at(deadline, self.execute_inner(&query, options, readonly)).await
        {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into()),
        }
    }

    async fn execute_inner(
        &self,
        query: &str,
        options: &GraphQueryOptions,
        readonly: bool,
    ) -> RedisResult<GraphQueryResult> {
        let mut client = self.inner.client.clone();
        let mut command = self.command(query, readonly, options.timeout_ms);
        let mut connection = client.graph_connection(&command, None).await?;
        // A MOVED/ASK reply proves that the query did not execute. No other
        // transport or server failure is eligible for replay.
        let mut redirects = 0;
        let response = loop {
            match client.send_graph_command(&mut command, &connection).await {
                Err(error)
                    if client.is_cluster_mode()
                        && matches!(error.kind(), ErrorKind::Moved | ErrorKind::Ask)
                        && redirects < 5 =>
                {
                    redirects += 1;
                    connection = client.graph_connection(&command, Some(&error)).await?;
                }
                result => break result?,
            }
        };
        // Dispatch attaches an inflight tracker; release it before metadata admission.
        drop(command);
        let needed = decode::required_dictionaries(&response)?;
        let mut schema = GraphSchema::default();
        for (required, procedure, target) in [
            (needed.labels, "db.labels", &mut schema.labels),
            (
                needed.relationship_types,
                "db.relationshipTypes",
                &mut schema.relationship_types,
            ),
            (
                needed.property_keys,
                "db.propertyKeys",
                &mut schema.property_keys,
            ),
        ] {
            if required {
                let mut metadata =
                    self.command(&format!("CALL {procedure}()"), true, options.timeout_ms);
                let response = client
                    .send_graph_command(&mut metadata, &connection)
                    .await?;
                *target = decode::decode_dictionary(&response)?;
            }
        }
        if !self.database_matches() {
            return Err(database_changed());
        }
        decode::decode_result(&response, &schema)
    }

    fn command(&self, query: &str, readonly: bool, timeout_ms: Option<u64>) -> Cmd {
        let mut command = redis::cmd(if readonly {
            "GRAPH.RO_QUERY"
        } else {
            "GRAPH.QUERY"
        });
        command.arg(&self.inner.name).arg(query).arg("--compact");
        if let Some(timeout) = timeout_ms {
            command.arg("timeout").arg(timeout);
        }
        command
    }

    fn database_matches(&self) -> bool {
        self.inner.client.current_database() == self.inner.database
            && self.inner.client.graph_database_epoch() == self.inner.database_epoch
    }
}

fn database_changed() -> RedisError {
    RedisError::from((
        ErrorKind::ClientError,
        "Graph handle's selected database changed; select a new graph handle",
    ))
}
