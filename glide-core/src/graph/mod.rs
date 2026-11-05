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
        let database_epoch = client.graph_selection_epoch()?;
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
        self.execute_with_deadline(query, options, readonly, deadline)
            .await
    }

    async fn execute_with_deadline(
        &self,
        query: &str,
        options: &GraphQueryOptions,
        readonly: bool,
        deadline: tokio::time::Instant,
    ) -> RedisResult<GraphQueryResult> {
        let query = params::serialize_query(query, options)?;
        if !self.database_matches() {
            return Err(database_changed());
        }
        within_deadline(deadline, self.execute_inner(&query, options, readonly)).await
    }

    async fn execute_inner(
        &self,
        query: &str,
        options: &GraphQueryOptions,
        readonly: bool,
    ) -> RedisResult<GraphQueryResult> {
        let _state = self.inner.client.graph_operation_guard().await;
        if !self.database_matches() {
            return Err(database_changed());
        }
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
                    // Release the completed attempt's tracker before re-admission.
                    command = self.command(query, readonly, options.timeout_ms);
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

async fn within_deadline<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = RedisResult<T>>,
) -> RedisResult<T> {
    if tokio::time::Instant::now() >= deadline {
        return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
    }
    let result = tokio::time::timeout_at(deadline, operation).await;
    // Tokio cannot preempt synchronous decoding or canonical-result conversion.
    if tokio::time::Instant::now() >= deadline {
        return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
    }
    match result {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into()),
    }
}

fn database_changed() -> RedisError {
    RedisError::from((
        ErrorKind::ClientError,
        "Graph handle's selected database changed; select a new graph handle",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ConnectionRequest, NodeAddress};
    use std::time::Duration;

    async fn lazy_graph() -> GraphContext {
        let client = Client::new(
            ConnectionRequest {
                addresses: vec![NodeAddress {
                    host: "127.0.0.1".into(),
                    port: 1,
                    unix_socket_path: None,
                }],
                lazy_connect: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        GraphContext::new(client, "local".into(), 0).unwrap()
    }

    #[tokio::test]
    async fn expired_adaptation_budget_is_not_reset_before_query() {
        let graph = lazy_graph().await;
        let error = graph
            .execute_with_deadline(
                "RETURN 42",
                &GraphQueryOptions::default(),
                false,
                tokio::time::Instant::now() - Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(error.is_timeout(), "{error}");
    }

    #[tokio::test]
    async fn concurrent_graph_operations_share_the_state_lease() {
        let graph = lazy_graph().await;
        let _first = graph.inner.client.graph_operation_guard().await;
        let _second = tokio::time::timeout(
            Duration::from_secs(1),
            graph.inner.client.graph_operation_guard(),
        )
        .await
        .expect("independent graph operations must not serialize");
        assert!(GraphContext::new(graph.inner.client.clone(), "other".into(), 0).is_ok());
    }

    #[tokio::test]
    async fn state_admission_wait_uses_the_graph_deadline_and_is_cancel_safe() {
        let graph = lazy_graph().await;
        let first = graph.inner.client.graph_operation_guard().await;
        let mut changing_client = graph.inner.client.clone();
        let change = tokio::spawn(async move {
            changing_client
                .send_command(redis::cmd("SELECT").arg(0), None)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while graph.inner.client.graph_selection_epoch().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(GraphContext::new(graph.inner.client.clone(), "other".into(), 0).is_err());
        let error = graph
            .execute_with_deadline(
                "RETURN 42",
                &GraphQueryOptions::default(),
                false,
                tokio::time::Instant::now() + Duration::from_millis(20),
            )
            .await
            .unwrap_err();
        assert!(error.is_timeout(), "{error}");
        change.abort();
        assert!(change.await.unwrap_err().is_cancelled());
        assert_eq!(graph.inner.client.graph_database_epoch(), 0);
        drop(first);
        assert!(GraphContext::new(graph.inner.client.clone(), "other".into(), 0).is_ok());
    }

    #[tokio::test]
    async fn synchronous_completion_cannot_outlive_the_operation_budget() {
        for result in [
            Ok(42),
            Err(RedisError::from((
                ErrorKind::ResponseError,
                "server failure",
            ))),
        ] {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
            let mut executions = 0;
            let error = within_deadline(deadline, async {
                executions += 1;
                std::thread::sleep(Duration::from_millis(200));
                result
            })
            .await
            .unwrap_err();
            assert!(error.is_timeout(), "{error}");
            assert_eq!(executions, 1);
        }
    }

    #[tokio::test]
    async fn exhausted_deadline_does_not_poll_the_operation() {
        let mut polled = false;
        let error = within_deadline(tokio::time::Instant::now(), async {
            polled = true;
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(error.is_timeout(), "{error}");
        assert!(!polled);
    }

    #[tokio::test]
    async fn on_time_completion_preserves_results_and_error_categories() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(
            within_deadline(deadline, async { Ok(42) }).await.unwrap(),
            42
        );
        let error = within_deadline(deadline, async {
            Err::<(), _>(RedisError::from((
                ErrorKind::ResponseError,
                "server failure",
            )))
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ResponseError);
    }

    #[tokio::test]
    async fn clones_share_context_until_the_final_owner_is_dropped() {
        let graph = lazy_graph().await;
        let clone = graph.clone();
        assert!(Arc::ptr_eq(&graph.inner, &clone.inner));
        let released = Arc::downgrade(&graph.inner);
        drop(graph);
        assert_eq!(released.strong_count(), 1);
        drop(clone);
        assert!(released.upgrade().is_none());
    }
}
