// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package glide

import (
	"fmt"

	"github.com/valkey-io/valkey-glide/go/v2/config"
)

func ExampleClient_SelectGraph() {
	// Lazy connection demonstrates that selecting a graph performs no I/O.
	client, err := NewClient(config.NewClientConfiguration().
		WithAddress(&config.NodeAddress{Host: "localhost", Port: 6379}).
		WithLazyConnect(true))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()
	graph, err := client.SelectGraph("people")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(graph.Name(), graph.Database())
	// Output: people 0
}

func ExampleClusterClient_SelectGraph() {
	client, err := NewClusterClient(config.NewClusterClientConfiguration().
		WithAddress(&config.NodeAddress{Host: "localhost", Port: 7000}).
		WithLazyConnect(true))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()
	graph, err := client.SelectGraph("people{tenant}")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(graph.Name(), graph.Database())
	// Output: people{tenant} 0
}
