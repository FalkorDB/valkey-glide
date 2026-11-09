// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package glide

// #include "lib.h"
import "C"

import (
	"context"
	"errors"
	"runtime"
	"unicode/utf8"
	"unsafe"

	"github.com/valkey-io/valkey-glide/go/v2/interfaces"
	graphtransport "github.com/valkey-io/valkey-glide/go/v2/internal/graph"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
	"google.golang.org/protobuf/proto"
)

// Graph is a local handle to a named FalkorDB graph. See [interfaces.Graph].
type Graph = interfaces.Graph

type graphHandle struct {
	parent        *baseClient
	name          string
	database      uint32
	databaseEpoch uint64
	nativeContext unsafe.Pointer
}

var _ Graph = (*graphHandle)(nil)

// SelectGraph creates a local graph handle without network I/O. It retains its
// parent client and owns a shared Rust GraphContext with the selected database
// and selection epoch. The context is automatically released by Go's garbage
// collector. Queries reject closed parents, subsequent SELECT attempts, and
// pooled borrowed clients.
func (client *baseClient) SelectGraph(name string) (Graph, error) {
	if name == "" || !utf8.ValidString(name) {
		return nil, errors.New("graph name must be a nonempty UTF-8 string")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.graphUnsupported {
		return nil, errors.New("graph handles are not supported on pool-borrowed clients")
	}
	if client.coreClient == nil {
		return nil, NewClosingError("SelectGraph failed: the client is closed")
	}
	epoch := uint64(C.graph_database_epoch(client.coreClient))
	database := uint32(C.graph_current_database(client.coreClient))
	if epoch%2 != 0 || uint64(C.graph_database_epoch(client.coreClient)) != epoch {
		return nil, errors.New("cannot select a graph while database selection is unsettled")
	}
	nameBytes := C.CBytes([]byte(name))
	// This stack slot contains only a native pointer, never a Go pointer. The
	// FFI copies the name and creates an owned Box<GraphContext> synchronously.
	var nativeContext unsafe.Pointer
	result := C.graph_context_create(
		client.coreClient,
		(*C.uint8_t)(nameBytes),
		C.uintptr_t(len(name)),
		C.uint32_t(database),
		C.uint64_t(epoch),
		&nativeContext,
	)
	C.free(nameBytes)
	if result != nil {
		defer C.free_command_result(result)
		if nativeContext != nil {
			C.graph_context_drop(nativeContext)
		}
		if result.command_error == nil {
			return nil, errors.New("graph context creation returned an invalid result")
		}
		return nil, GoError(
			uint32(result.command_error.command_error_type),
			C.GoString(result.command_error.command_error_message),
		)
	}
	if nativeContext == nil {
		return nil, errors.New("graph context creation returned no context")
	}
	graph := &graphHandle{
		parent: client, name: name, database: database, databaseEpoch: epoch, nativeContext: nativeContext,
	}
	runtime.SetFinalizer(graph, finalizeGraph)
	return graph, nil
}

func finalizeGraph(graph *graphHandle) {
	// No public release races this finalizer. Query keeps graph reachable until
	// the FFI has synchronously cloned its shared context for asynchronous work.
	if graph.nativeContext != nil {
		C.graph_context_drop(graph.nativeContext)
		graph.nativeContext = nil
	}
}

func (graph *graphHandle) Name() string {
	return graph.name
}

func (graph *graphHandle) Database() uint32 {
	return graph.database
}

func (graph *graphHandle) Query(
	ctx context.Context,
	query string,
	queryOptions *options.GraphQueryOptions,
) (*models.GraphResult, error) {
	return graph.execute(ctx, query, queryOptions, false)
}

func (graph *graphHandle) ROQuery(
	ctx context.Context,
	query string,
	queryOptions *options.GraphQueryOptions,
) (*models.GraphResult, error) {
	return graph.execute(ctx, query, queryOptions, true)
}

func (graph *graphHandle) execute(
	ctx context.Context,
	query string,
	queryOptions *options.GraphQueryOptions,
	readonly bool,
) (*models.GraphResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params map[string]any
	var timeout *int64
	if queryOptions != nil {
		params = queryOptions.Params
		timeout = queryOptions.Timeout
	}
	request, err := graphtransport.NewRequest(graph.name, query, graph.database, readonly, params, timeout)
	if err != nil {
		return nil, err
	}
	request.DatabaseEpoch = graph.databaseEpoch
	message, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	// The FFI copies the protobuf before scheduling async work and never retains
	// a Go pointer. Its pending future owns a clone of the shared GraphContext.
	messageBytes := C.CBytes(message)
	defer C.free(messageBytes)
	client := graph.parent
	resultChannel := make(chan payload, 1)
	client.mu.Lock()
	if client.coreClient == nil {
		client.mu.Unlock()
		return nil, NewClosingError("graph query failed: the client is closed")
	}
	if uint64(C.graph_database_epoch(client.coreClient)) != graph.databaseEpoch ||
		uint32(C.graph_current_database(client.coreClient)) != graph.database {
		client.mu.Unlock()
		return nil, errors.New("graph handle is stale after database selection; call SelectGraph again")
	}
	requestID := client.beginRequest(resultChannel)
	immediate := C.graph_query_with_context(
		client.coreClient,
		graph.nativeContext,
		C.uintptr_t(requestID),
		(*C.uint8_t)(messageBytes),
		C.uintptr_t(len(message)),
	)
	runtime.KeepAlive(graph)
	client.mu.Unlock()
	// Async clients normally return nil. Copy any immediate error before
	// freeing its CommandResult, without transferring its owned response.
	if immediate != nil {
		defer C.free_command_result(immediate)
		client.removePendingRequest(requestID)
		takeRequest(requestID)
		if immediate.command_error != nil {
			return nil, GoError(
				uint32(immediate.command_error.command_error_type),
				C.GoString(immediate.command_error.command_error_message),
			)
		}
		canonical, err := parseInterface(immediate.response)
		if err != nil {
			return nil, err
		}
		return graphtransport.DecodeResult(canonical)
	}
	result, err := client.waitForResponse(ctx, requestID, resultChannel)
	if err != nil {
		return nil, err
	}
	if result.error != nil {
		return nil, result.error
	}
	canonical, err := handleInterfaceResponse(result.value)
	if err != nil {
		return nil, err
	}
	return graphtransport.DecodeResult(canonical)
}
