## Note for developers - 
When building a new wrapper using glide-ffi, be sure to specify the appropriate GLIDE_NAME and GLIDE_VERSION environment variables during the build process. This is especially important for production or deployment workflows. These values determine how the client identifies itself to the server and will appear in the `CLIENT INFO` output. If not set, the defaults are `"GlideFFI"` for the client name and `"unknown"` for the version.

For example:
```
GLIDE_NAME=GlideGo GLIDE_VERSION=2.0.0 cargo build
```

## Additive graph bridge

`graph_context_create(adapter, name, name_len, database, epoch, context_out)`
selects and retains an owned `Box<GraphContext>` without I/O. It validates the
database/epoch snapshot. Success returns null and sets `context_out`; failure
sets it to null and returns a synchronous `CommandResult` error even for async
clients. Free errors with `free_command_result`.

`graph_context_drop(context)` releases that Box exactly once; null is a no-op.
Clones of language handles share ownership of the same selected Rust context.
No registry, extra connection, alternate allocator, or response layout is added.

`graph_query_with_context(adapter, context, request_id, bytes, len)` accepts the core's
`graph_request.QueryRequest` protobuf. It copies and decodes the caller's bytes
and clones the retained context before returning, then uses the existing `ClientAdapter::execute_request`
transport. Async clients keep their callback/pipe request IDs and return null;
sync clients return the existing `CommandResult`, released with
`free_command_result`. Context/name/database/epoch must match the request.
The caller can release its context immediately after submission returns; pending
requests retain their own Rust clone. Prevent context drop from racing submission.
The older `graph_query(adapter, request_id, bytes, len)` remains compatible.

`graph_current_database(adapter)` and `graph_database_epoch(adapter)` are local
atomic reads, including for lazy clients. Read epoch/database/epoch when selecting
a handle and reject odd or inconsistent epochs. Include both database and epoch
in every graph request. The caller must protect every FFI submission/getter
against concurrent `close_client`; completed response arenas are independent of
the adapter's lifetime.

Focused gates:

```bash
cd ffi
cargo test --test test_graph_bridge -- --test-threads=1
cargo test --lib graph_large_version_uses_lossless_decimal_string
cd miri-tests
cargo +nightly miri test --test ffi_miri_tests graph_
cargo +nightly miri test --test ffi_miri_tests malformed_graph_request_
cargo +nightly miri test --test ffi_miri_tests retained_graph_context_
```

The Miri mock executes graph requests locally, allowing request-buffer and
response-arena ownership to be checked without a server. Real Python graph
conformance runs separately against the digest-pinned harness (see
`../python/README.md`).
