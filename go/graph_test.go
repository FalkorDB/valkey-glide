// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package glide

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/valkey-io/valkey-glide/go/v2/interfaces"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

func TestGraphSelectionUnavailableOnIsolatedScopes(t *testing.T) {
	var scope any = &IsolatedScope{}
	if _, supported := scope.(interfaces.GraphCommands); supported {
		t.Fatal("isolated scopes must not expose graph handles that can outlive their release")
	}
}

func TestGraphSelectionRejectsInvalidParent(t *testing.T) {
	client := &baseClient{mu: &sync.Mutex{}}
	for _, name := range []string{"", "\xff", "closed"} {
		if _, err := client.SelectGraph(name); err == nil {
			t.Fatalf("SelectGraph(%q) accepted invalid or closed parent", name)
		}
	}
	client.graphUnsupported = true
	if _, err := client.SelectGraph("pooled"); err == nil || !strings.Contains(err.Error(), "pool-borrowed") {
		t.Fatalf("expected pool rejection, got %v", err)
	}
}

func TestGraphQueryCancellationAndClosedParent(t *testing.T) {
	client := &baseClient{mu: &sync.Mutex{}}
	graph := &graphHandle{parent: client, name: "g", database: 3, databaseEpoch: 4}
	if graph.Name() != "g" || graph.Database() != 3 {
		t.Fatal("local graph identity changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := graph.Query(ctx, "RETURN 1", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := graph.ROQuery(context.Background(), "RETURN 1", nil); err == nil {
		t.Fatal("query accepted a closed parent")
	}
	if _, err := graph.Query(context.Background(), "RETURN $bad", options.NewGraphQueryOptions().SetParams(
		map[string]any{"bad": struct{}{}},
	)); err == nil || !strings.Contains(err.Error(), `$["bad"]`) {
		t.Fatalf("expected validation with a parameter path, got %v", err)
	}
}
