// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use super::{Admission, Client, ClientWrapper, GlideConnection};
use redis::cluster_routing::{RoutingInfo, SingleNodeRoutingInfo};
use redis::{Cmd, ErrorKind, RedisError, RedisResult, Value};
use std::sync::atomic::Ordering;

#[derive(Clone)]
pub(crate) struct GraphConnection {
    pub(super) connection: GlideConnection,
    pub(super) asking: bool,
}

impl Client {
    fn ensure_graph_outside_transaction(&self) -> RedisResult<()> {
        if self.shared.multi_active.load(Ordering::Acquire) {
            return Err(RedisError::from((
                ErrorKind::ClientError,
                "Graph queries cannot execute inside raw MULTI; use custom commands for transactions",
            )));
        }
        Ok(())
    }

    /// Graph-handle database generation; an odd value means SELECT is unsettled.
    pub fn graph_database_epoch(&self) -> u64 {
        self.shared.graph_database_epoch.load(Ordering::Acquire)
    }

    pub(super) fn mark_graph_database_dirty(&self) {
        self.shared
            .graph_database_epoch
            .fetch_add(2, Ordering::AcqRel);
        self.shared
            .graph_database_epoch
            .fetch_or(1, Ordering::AcqRel);
    }

    /// Select an existing connection for one graph query and its metadata reads.
    pub(crate) async fn graph_connection(
        &mut self,
        command: &Cmd,
        redirect: Option<&RedisError>,
    ) -> RedisResult<GraphConnection> {
        self.ensure_graph_outside_transaction()?;
        self.reconcile_iam_before_dispatch().await?;
        let wrapper = self.get_or_initialize_client().await?;
        match wrapper {
            ClientWrapper::Standalone(client) => {
                if redirect.is_some() {
                    return Err(RedisError::from((
                        ErrorKind::ClientError,
                        "A standalone graph operation cannot follow a cluster redirect",
                    )));
                }
                let connection = client
                    .graph_connection(
                        command
                            .arg_idx(0)
                            .is_some_and(|name| name.eq_ignore_ascii_case(b"GRAPH.RO_QUERY")),
                    )
                    .await?;
                Ok(GraphConnection {
                    connection,
                    asking: false,
                })
            }
            ClientWrapper::Cluster { client, .. } => {
                if let Some(error) = redirect {
                    return Ok(GraphConnection {
                        connection: client.connection_for_redirect(error).await?,
                        asking: error.kind() == ErrorKind::Ask,
                    });
                }
                let Some(RoutingInfo::SingleNode(route)) = RoutingInfo::for_routable(command)
                else {
                    return Err(RedisError::from((
                        ErrorKind::ClientError,
                        "Graph commands require a single graph-key route",
                    )));
                };
                if !matches!(route, SingleNodeRoutingInfo::SpecificNode(_)) {
                    return Err(RedisError::from((
                        ErrorKind::ClientError,
                        "Graph command is missing its graph routing key",
                    )));
                }
                Ok(GraphConnection {
                    connection: client.connection_for_route(route).await?,
                    asking: false,
                })
            }
            ClientWrapper::Lazy(_) => unreachable!("lazy client was initialized"),
        }
    }

    /// Dispatch without transport-level replay on a connection pinned to this operation.
    pub(crate) async fn send_graph_command(
        &mut self,
        command: &mut Cmd,
        connection: &GraphConnection,
    ) -> RedisResult<Value> {
        self.ensure_graph_outside_transaction()?;
        self.reconcile_iam_before_dispatch().await?;
        let permit = Admission::check(self).await?.reserve(self)?;
        self.dispatch(permit, command, None, Some(connection.clone()))
            .await
    }
}
