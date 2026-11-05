// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package options

// GraphQueryOptions configures a FalkorDB graph query.
type GraphQueryOptions struct {
	// Params contains typed Cypher parameters, referenced as $name in the query.
	// Supported values are nil, booleans, signed 64-bit integers, finite floats,
	// UTF-8 strings, lists, and string-keyed maps of supported values.
	Params map[string]any
	// Timeout is the server-side timeout in milliseconds. Nil uses the server
	// default; a pointer to zero explicitly sends TIMEOUT 0. Negative values
	// are invalid. The server validates its supported timeout range.
	Timeout *int64
}

// NewGraphQueryOptions creates graph query options using the server defaults.
func NewGraphQueryOptions() *GraphQueryOptions {
	return &GraphQueryOptions{}
}

// SetParams sets typed Cypher parameters.
func (o *GraphQueryOptions) SetParams(params map[string]any) *GraphQueryOptions {
	o.Params = params
	return o
}

// SetTimeout sets the server-side timeout in milliseconds, including zero.
func (o *GraphQueryOptions) SetTimeout(milliseconds int64) *GraphQueryOptions {
	o.Timeout = &milliseconds
	return o
}
