// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package interfaces

import (
	"context"

	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

// Graph owns a shared Rust graph context, automatically released by Go's garbage
// collector. It retains its parent client but does not own a connection and needs
// no Close. Queries fail after the parent closes or changes databases. Select a
// new handle after a successful database change.
type Graph interface {
	Name() string
	Database() uint32
	// Query executes GRAPH.QUERY; params are not interpolated by the binding.
	Query(ctx context.Context, query string, options *options.GraphQueryOptions) (*models.GraphResult, error)
	// ROQuery executes GRAPH.RO_QUERY using the core's graph-aware read routing.
	ROQuery(ctx context.Context, query string, options *options.GraphQueryOptions) (*models.GraphResult, error)
}

// GraphCommands provides local graph selection without network I/O.
type GraphCommands interface {
	// SelectGraph retains the client, graph name, and selected-database snapshot.
	// Pool-borrowed clients are unsupported because a handle can outlive a lease.
	SelectGraph(name string) (Graph, error)
}
