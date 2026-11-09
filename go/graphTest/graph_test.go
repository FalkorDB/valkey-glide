// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//go:build falkordb

package graphtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

func address(t *testing.T, variable string) *config.NodeAddress {
	t.Helper()
	endpoint := os.Getenv(variable)
	if endpoint == "" {
		t.Skip(variable + " is not configured; use the isolated FalkorDB harness")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return &config.NodeAddress{Host: host, Port: port}
}

// The explicit RESP2/RESP3 standalone+cluster all16 matrix lives in the tagged
// root graph_conformance_test.go so it can configure the normal protobuf client
// transport without adding a public protocol-selection API solely for tests.

func TestGraphParameterNesting(t *testing.T) {
	client, err := glide.NewClient(config.NewClientConfiguration().
		WithAddress(address(t, "FALKORDB_STANDALONE_ENDPOINT")).
		WithRequestTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	name := fmt.Sprintf("glide-go-parameter-depth-%d", time.Now().UnixNano())
	defer func() {
		if _, err := client.CustomCommand(context.Background(), []string{"GRAPH.DELETE", name}); err != nil {
			t.Logf("graph cleanup: %v", err)
		}
	}()
	graph, err := client.SelectGraph(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"list", "map"} {
		for _, depth := range []int{60, 100, 101} {
			t.Run(fmt.Sprintf("%s/depth%d", kind, depth), func(t *testing.T) {
				var parameter, expected any = int64(7), int64(7)
				step := "[0]"
				// The parameter root map is depth zero; the leaf starts at one.
				for level := 1; level < depth; level++ {
					if kind == "list" {
						parameter, expected = []any{parameter}, []any{expected}
					} else {
						parameter = map[string]any{"value": parameter}
						expected = models.GraphMap{{Key: "value", Value: expected}}
						step = `["value"]`
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				result, err := graph.Query(ctx, "RETURN $nested AS nested",
					options.NewGraphQueryOptions().SetParams(map[string]any{"nested": parameter}))
				if depth == 101 {
					path := `$["nested"]` + strings.Repeat(step, depth-1)
					if err == nil || !strings.Contains(err.Error(), path) ||
						!strings.Contains(err.Error(), "maximum nesting depth exceeded") {
						t.Fatalf("depth 101 must fail Go validation at %s: %v", path, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Go-accepted depth %d failed through the native Graph API: %v", depth, err)
				}
				if !reflect.DeepEqual(result.Columns, []string{"nested"}) ||
					!reflect.DeepEqual(result.Rows, [][]any{{expected}}) {
					t.Fatalf("depth %d changed the nested parameter value: %+v", depth, result)
				}
			})
		}
	}
}

func TestGraphDatabaseEpochAndPoolRejection(t *testing.T) {
	cfg := config.NewClientConfiguration().WithAddress(address(t, "FALKORDB_STANDALONE_ENDPOINT"))
	client, err := glide.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	graph, err := client.SelectGraph(fmt.Sprintf("glide-go-epoch-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.Select(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Select(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Query(ctx, "RETURN 1", nil); err == nil {
		t.Fatal("old graph accepted a database change away and back")
	}
	pool, err := glide.NewClientPool(cfg, glide.DefaultPoolConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	id, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Release(id)
	borrowed, err := pool.GetClient(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := borrowed.SelectGraph("lease"); err == nil {
		t.Fatal("graph selection from a pool-borrowed handle was accepted")
	}
	if _, err := borrowed.Client.SelectGraph("lease"); err == nil {
		t.Fatal("accessing the embedded client bypassed pooled graph rejection")
	}
}

func TestGraphInFlightCancellationAndServerTimeout(t *testing.T) {
	cfg := config.NewClientConfiguration().
		WithAddress(address(t, "FALKORDB_STANDALONE_ENDPOINT")).
		WithRequestTimeout(2 * time.Second)
	client, err := glide.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	name := fmt.Sprintf("glide-go-cancel-%d", time.Now().UnixNano())
	defer func() {
		if _, err := client.CustomCommand(context.Background(), []string{"GRAPH.DELETE", name}); err != nil {
			t.Logf("graph cleanup: %v", err)
		}
	}()
	graph, err := client.SelectGraph(name)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if _, err := graph.Query(ctx, "RETURN 1", nil); err != nil {
		t.Fatal(err)
	}
	const expensiveQuery = "UNWIND range(1,1000000000) AS x RETURN sum(x)"
	observed := make(chan struct{}, 4)
	monitor, err := glide.NewMonitorClient(cfg, func(line glide.MonitorLine) {
		if strings.EqualFold(line.Command, "GRAPH.QUERY") && len(line.Args) >= 2 &&
			line.Args[0] == name && strings.Contains(line.Args[1], expensiveQuery) {
			select {
			case observed <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()
	awaitDispatch := func(t *testing.T) {
		t.Helper()
		select {
		case <-observed:
		case <-ctx.Done():
			t.Fatal("server did not observe the bounded graph query")
		}
	}
	assertHealthy := func(t *testing.T) {
		t.Helper()
		result, err := graph.ROQuery(ctx, "RETURN 7 AS value", nil)
		if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{int64(7)}}) {
			t.Fatalf("graph did not recover after abandoned or timed-out query: %+v %v", result, err)
		}
	}
	awaitBoundedCompletion := func(t *testing.T) {
		t.Helper()
		// Cancellation abandons the Go wait, not native execution. A successful
		// read is the recovery barrier, including delayed timeout callbacks.
		recoveryContext, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			result, err := graph.ROQuery(recoveryContext, "RETURN 7 AS value", nil)
			if err == nil {
				if !reflect.DeepEqual(result.Rows, [][]any{{int64(7)}}) {
					t.Fatalf("recovery read returned unexpected rows: %+v", result)
				}
				return
			}
			select {
			case <-ticker.C:
			case <-recoveryContext.Done():
				t.Fatalf("graph did not recover after abandoned query: %v (last read: %v)", recoveryContext.Err(), err)
			}
		}
	}
	t.Run("cancel_after_server_dispatch", func(t *testing.T) {
		queryContext, cancel := context.WithCancel(ctx)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := graph.Query(queryContext, expensiveQuery, options.NewGraphQueryOptions().SetTimeout(100))
			result <- err
		}()
		// Unlike a pre-cancelled context, MONITOR proves this query reached the
		// server before cancellation. It is still bounded if the assertion fails.
		awaitDispatch(t)
		time.Sleep(time.Millisecond)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight cancellation returned %v", err)
		}
		awaitBoundedCompletion(t)
	})
	t.Run("one_millisecond_context_deadline", func(t *testing.T) {
		queryContext, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		_, err := graph.Query(queryContext, expensiveQuery, options.NewGraphQueryOptions().SetTimeout(100))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("in-flight context deadline returned %v", err)
		}
		awaitDispatch(t)
		awaitBoundedCompletion(t)
	})
	t.Run("positive_server_timeout", func(t *testing.T) {
		_, err := graph.Query(ctx, expensiveQuery, options.NewGraphQueryOptions().SetTimeout(1))
		if err == nil || errors.Is(err, context.DeadlineExceeded) ||
			!strings.Contains(strings.ToLower(err.Error()), "timed out") {
			t.Fatalf("expected a surfaced server timeout, got %v", err)
		}
		assertHealthy(t)
	})
}
