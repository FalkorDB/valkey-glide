// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use glide_core::connection_request::{ConnectionRequest, NodeAddress};
use glide_core::errors::RequestErrorType;
use glide_ffi::{
    ClientType, CommandResponse, close_client, create_client, free_command_result,
    free_connection_response, graph_context_create, graph_context_drop, graph_current_database,
    graph_database_epoch, graph_query, graph_query_with_context,
};
use protobuf::Message;
use std::ffi::{CStr, c_char, c_void};
use std::sync::{Mutex, mpsc};
use std::time::Duration;

fn lazy_client(client_type: &ClientType) -> *const c_void {
    let mut request = ConnectionRequest::new();
    request.lazy_connect = true;
    request.database_id = 5;
    let mut address = NodeAddress::new();
    address.host = "127.0.0.1".into();
    address.port = 1;
    request.addresses.push(address);
    let bytes = request.write_to_bytes().unwrap();
    // SAFETY: inputs remain valid for the synchronous create call; the returned
    // adapter is transferred to the caller, which must close it once.
    unsafe {
        let response = create_client(bytes.as_ptr(), bytes.len(), client_type, None, None, 0);
        assert!(!response.is_null());
        assert!((*response).connection_error_message.is_null());
        let adapter = (*response).conn_ptr;
        free_connection_response(response.cast_mut());
        adapter
    }
}

#[test]
fn graph_selection_and_invalid_requests_do_not_connect() {
    let adapter = lazy_client(&ClientType::SyncClient);
    // SAFETY: the adapter stays live until close and every result is freed once.
    unsafe {
        assert_eq!(graph_current_database(adapter), 5);
        assert_eq!(graph_database_epoch(adapter), 0);
        for bytes in [&[0xff][..], &[0x0a, 0xff][..]] {
            let result = graph_query(adapter, 1, bytes.as_ptr(), bytes.len());
            assert!(!result.is_null());
            assert!((*result).response.is_null());
            assert!(!(*result).command_error.is_null());
            assert!(
                CStr::from_ptr((*(*result).command_error).command_error_message)
                    .to_string_lossy()
                    .contains("graph request protobuf")
            );
            free_command_result(result);
        }
        close_client(adapter);
    }
}

#[test]
fn retained_graph_context_validates_identity_and_drops_after_client_close() {
    let adapter = lazy_client(&ClientType::SyncClient);
    // SAFETY: adapter, name, output slot, and requests remain live through each
    // synchronous call. Every native context and result is released exactly once.
    unsafe {
        let mut context = std::ptr::null_mut();
        let name = b"selected";
        assert!(
            graph_context_create(adapter, name.as_ptr(), name.len(), 5, 0, &mut context).is_null()
        );
        assert!(!context.is_null());
        for (request_name, database, epoch) in
            [("different", 5, 0), ("selected", 4, 0), ("selected", 5, 2)]
        {
            let mut request = glide_core::graph_request::QueryRequest::new();
            request.graph_name = request_name.into();
            request.database = database;
            request.database_epoch = epoch;
            request.query = "RETURN 1".into();
            let bytes = request.write_to_bytes().unwrap();
            let result = graph_query_with_context(adapter, context, 1, bytes.as_ptr(), bytes.len());
            assert!(!(*result).command_error.is_null());
            assert!(
                CStr::from_ptr((*(*result).command_error).command_error_message)
                    .to_string_lossy()
                    .contains("selected graph context")
            );
            free_command_result(result);
        }
        for (name, database, epoch) in [
            (&b"selected"[..], 4, 0),
            (&b"selected"[..], 5, 1),
            (&b"selected"[..], 5, 2),
            (&b"\xff"[..], 5, 0),
            (&b"nul\0name"[..], 5, 0),
        ] {
            let mut rejected = context;
            let result = graph_context_create(
                adapter,
                name.as_ptr(),
                name.len(),
                database,
                epoch,
                &mut rejected,
            );
            assert!(rejected.is_null());
            assert!(!(*result).command_error.is_null());
            free_command_result(result);
        }
        close_client(adapter);
        graph_context_drop(context);
        graph_context_drop(std::ptr::null_mut());
    }
}

static CALLBACK: Mutex<Option<mpsc::Sender<usize>>> = Mutex::new(None);

unsafe extern "C-unwind" fn unexpected_success(_id: usize, _response: *const CommandResponse) {
    panic!("malformed graph protobuf cannot succeed");
}

unsafe extern "C-unwind" fn failure(id: usize, _message: *const c_char, _kind: RequestErrorType) {
    CALLBACK.lock().unwrap().as_ref().unwrap().send(id).unwrap();
}

#[test]
fn malformed_async_graph_request_uses_original_callback_id() {
    let (sender, receiver) = mpsc::channel();
    *CALLBACK.lock().unwrap() = Some(sender);
    let adapter = lazy_client(&ClientType::AsyncClient {
        success_callback: unexpected_success,
        failure_callback: failure,
        allow_stack_response: false,
    });
    let bytes = vec![0xff];
    // SAFETY: the adapter and request allocation remain live until submission
    // returns. No caller buffer is retained by the asynchronous request.
    unsafe {
        assert!(graph_query(adapter, 7341, bytes.as_ptr(), bytes.len()).is_null());
    }
    drop(bytes);
    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), 7341);
    // SAFETY: no submission races close; the callback already finished.
    unsafe { close_client(adapter) };
    *CALLBACK.lock().unwrap() = None;
}
