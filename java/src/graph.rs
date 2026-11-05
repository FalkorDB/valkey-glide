// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use crate::jni_client::{complete_callback, complete_error_sync, get_handle_table, get_runtime};
use dashmap::DashMap;
use glide_core::graph::GraphContext;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jlong, jlongArray};
use std::sync::OnceLock;

struct SelectedGraph {
    parent: u64,
    context: GraphContext,
}

fn contexts() -> &'static DashMap<u64, SelectedGraph> {
    static CONTEXTS: OnceLock<DashMap<u64, SelectedGraph>> = OnceLock::new();
    CONTEXTS.get_or_init(DashMap::new)
}

fn retain(parent: u64, context: GraphContext) -> u64 {
    let id = glide_core::pool::allocate_client_id();
    contexts().insert(id, SelectedGraph { parent, context });
    id
}

fn release(graph: u64) {
    contexts().remove(&graph);
}

pub(crate) fn clear_contexts() {
    contexts().clear();
}

pub(crate) fn release_parent(parent: u64) {
    contexts().retain(|_, selected| selected.parent != parent);
}

fn throw_selection_error(env: &mut JNIEnv, message: &str) -> jlongArray {
    let _ = env.throw_new("glide/api/models/exceptions/RequestException", message);
    std::ptr::null_mut()
}

/// Retain a selected context locally. No connection is opened and no server command is issued.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_internal_NativeGraphContext_selectNative(
    mut env: JNIEnv,
    _class: JClass,
    parent: jlong,
    name: JString,
) -> jlongArray {
    let parent = parent as u64;
    // Keep the parent entry locked until retention, so close cannot miss a concurrent selection.
    let Some(client) = get_handle_table().get(&parent) else {
        let _ = env.throw_new(
            "glide/api/models/exceptions/ClosingException",
            "Client is closed",
        );
        return std::ptr::null_mut();
    };
    let name: String = match env.get_string(&name) {
        Ok(name) => name.into(),
        Err(error) => return throw_selection_error(&mut env, &error.to_string()),
    };
    let epoch = client.graph_database_epoch();
    let database = client.current_database();
    let context = match GraphContext::new(client.value().clone(), name, database) {
        Ok(context) => context,
        Err(error) => return throw_selection_error(&mut env, &error.to_string()),
    };
    if epoch & 1 != 0 || client.graph_database_epoch() != epoch {
        return throw_selection_error(
            &mut env,
            "Cannot select a graph while database selection is unsettled",
        );
    }
    let selected = match env.new_long_array(3) {
        Ok(selected) => selected,
        Err(error) => return throw_selection_error(&mut env, &error.to_string()),
    };
    let id = retain(parent, context);
    if let Err(error) = env.set_long_array_region(
        &selected,
        0,
        &[id as jlong, database.into(), epoch as jlong],
    ) {
        release(id);
        return throw_selection_error(&mut env, &error.to_string());
    }
    drop(client);
    selected.into_raw()
}

/// Called by the existing Java phantom-reference cleanup worker. Removal is idempotent.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_internal_NativeGraphContext_releaseNative(
    _env: JNIEnv,
    _class: JClass,
    graph: jlong,
) {
    release(graph as u64);
}

/// Clone the retained context before returning to Java or spawning any asynchronous work.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_internal_NativeGraphContext_executeNative(
    mut env: JNIEnv,
    _class: JClass,
    parent: jlong,
    graph: jlong,
    callback_id: jlong,
    request: JByteArray,
) {
    let Some(jvm) = crate::get_jvm_or_complete_error(&mut env, callback_id, "graphQuery") else {
        return;
    };
    let parent = parent as u64;
    if !get_handle_table().contains_key(&parent) {
        let _ = env.throw_new(
            "glide/api/models/exceptions/ClosingException",
            "Client is closed",
        );
        return;
    }
    let context = match contexts().get(&(graph as u64)) {
        Some(selected) if selected.parent == parent => selected.context.clone(),
        _ => {
            complete_error_sync(&mut env, callback_id, "Graph context is released", 0);
            return;
        }
    };
    let bytes = match env.convert_byte_array(request) {
        Ok(bytes) => bytes,
        Err(error) => {
            complete_error_sync(&mut env, callback_id, &error.to_string(), 0);
            return;
        }
    };
    get_runtime().spawn(async move {
        let result = glide_core::graph::execute_request_with_context(context, &bytes).await;
        complete_callback(jvm, callback_id, result, false);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use glide_core::client::{Client, ConnectionRequest, NodeAddress};
    use std::time::Duration;

    fn local_context() -> GraphContext {
        let client = get_runtime()
            .block_on(Client::new(
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
            ))
            .unwrap();
        GraphContext::new(client, "local-only".into(), 0).unwrap()
    }

    #[test]
    fn releasing_selection_is_idempotent_and_drops_registry_ownership() {
        let id = retain(0, local_context());
        assert!(contexts().contains_key(&id));
        release(id);
        assert!(!contexts().contains_key(&id));
        release(id);
        assert!(!contexts().contains_key(&id));
    }

    #[test]
    fn parent_close_releases_only_its_selected_contexts() {
        let parent = glide_core::pool::allocate_client_id();
        let other_parent = glide_core::pool::allocate_client_id();
        let first = retain(parent, local_context());
        let second = retain(parent, local_context());
        let other = retain(other_parent, local_context());
        let operation = contexts().get(&first).unwrap().context.clone();
        release_parent(parent);
        assert!(!contexts().contains_key(&first));
        assert!(!contexts().contains_key(&second));
        assert!(contexts().contains_key(&other));
        assert_eq!(operation.name(), "local-only");
        release_parent(parent);
        release_parent(other_parent);
        assert!(!contexts().contains_key(&other));
    }

    #[test]
    fn operation_clone_completes_after_selection_is_released() {
        let id = retain(0, local_context());
        let operation = contexts().get(&id).unwrap().context.clone();
        let (resume, wait) = tokio::sync::oneshot::channel::<()>();
        let (completed, result) = std::sync::mpsc::channel();
        get_runtime().spawn(async move {
            wait.await.unwrap();
            completed.send(operation.name().to_owned()).unwrap();
        });
        release(id);
        assert!(!contexts().contains_key(&id));
        resume.send(()).unwrap();
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)).unwrap(),
            "local-only"
        );
    }
}
