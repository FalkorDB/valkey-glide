// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//go:build falkordb

package glide

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

func trackGraphFinalization(graph Graph) <-chan struct{} {
	finalized := make(chan struct{}, 1)
	handle := graph.(*graphHandle)
	runtime.SetFinalizer(handle, nil)
	runtime.SetFinalizer(handle, func(handle *graphHandle) {
		finalizeGraph(handle)
		finalized <- struct{}{}
	})
	return finalized
}

func collectGraphContext(t *testing.T, finalized <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-finalized:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("owned native graph context was not finalized before the bounded query completed")
}

func abandonNativeGraphQuery(
	ctx context.Context,
	client *Client,
	name, query string,
	observed <-chan struct{},
) (<-chan struct{}, error) {
	graph, err := client.SelectGraph(name)
	if err != nil {
		return nil, err
	}
	finalized := trackGraphFinalization(graph)
	queryContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func(graph Graph) {
		_, err := graph.Query(queryContext, query, options.NewGraphQueryOptions().SetTimeout(1000))
		result <- err
	}(graph)
	select {
	case <-observed:
	case <-ctx.Done():
		return nil, fmt.Errorf("bounded graph query did not reach the server: %w", ctx.Err())
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("expected in-flight cancellation, got %v", err)
	}
	return finalized, nil
}

func closeNativeGraphParent(client *Client, name string) (<-chan struct{}, error) {
	graph, err := client.SelectGraph(name)
	if err != nil {
		return nil, err
	}
	finalized := trackGraphFinalization(graph)
	client.Close()
	if _, err := graph.Query(context.Background(), "RETURN 1", nil); err == nil {
		return nil, errors.New("native graph context bypassed its closed Go parent")
	}
	return finalized, nil
}

func TestGraphLocalNativeContextSelection(t *testing.T) {
	client, err := NewClient(config.NewClientConfiguration().
		WithAddress(&config.NodeAddress{Host: "127.0.0.1", Port: 1}).
		WithLazyConnect(true))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	graph, err := client.SelectGraph("local-without-connection")
	if err != nil {
		t.Fatalf("local graph selection tried to connect: %v", err)
	}
	handle := graph.(*graphHandle)
	if handle.nativeContext == nil {
		t.Fatal("selected graph does not own a native context")
	}
	for _, invalidName := range []string{"control\x00", "control\n"} {
		if _, err := client.SelectGraph(invalidName); err == nil {
			t.Fatalf("native graph selection accepted invalid key %q", invalidName)
		}
	}
	runtime.SetFinalizer(handle, nil)
	finalizeGraph(handle)
	finalizeGraph(handle)
	if handle.nativeContext != nil {
		t.Fatal("native graph context was not released")
	}
}

func TestGraphNativeContextFinalization(t *testing.T) {
	endpoint := os.Getenv("FALKORDB_STANDALONE_ENDPOINT")
	if endpoint == "" {
		t.Skip("FALKORDB_STANDALONE_ENDPOINT is not configured")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.NewClientConfiguration().
		WithAddress(&config.NodeAddress{Host: host, Port: port}).
		WithRequestTimeout(2 * time.Second)
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	name := fmt.Sprintf("glide-go-finalize-%d", time.Now().UnixNano())
	// This separate client owns cleanup even after the graph's parent closes.
	control, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	defer func() {
		if _, err := control.CustomCommand(context.Background(), []string{"GRAPH.DELETE", name}); err != nil {
			t.Logf("graph cleanup: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seed, err := control.SelectGraph(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Query(ctx, "RETURN 1", nil); err != nil {
		t.Fatal(err)
	}
	const expensiveQuery = "UNWIND range(1,1000000000) AS x RETURN sum(x)"
	observed := make(chan struct{}, 1)
	monitor, err := NewMonitorClient(cfg, func(line MonitorLine) {
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
	started := time.Now()
	finalized, err := abandonNativeGraphQuery(ctx, client, name, expensiveQuery, observed)
	if err != nil {
		t.Fatal(err)
	}
	// The Go handle drops its native Box while the asynchronous request still
	// owns a cloned Rust GraphContext. ASan exercises the eventual late callback.
	collectGraphContext(t, finalized)
	if time.Since(started) >= time.Second {
		t.Fatal("native graph context was not dropped while its bounded query was in flight")
	}
	timer := time.NewTimer(time.Until(started.Add(1100 * time.Millisecond)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	next, err := client.SelectGraph(name)
	if err != nil {
		t.Fatal(err)
	}
	result, err := next.ROQuery(ctx, "RETURN 9 AS value", nil)
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{int64(9)}}) {
		t.Fatalf("parent did not recover after finalization and late callback: %+v %v", result, err)
	}
	closed, err := closeNativeGraphParent(client, name)
	if err != nil {
		t.Fatal(err)
	}
	collectGraphContext(t, closed)
}
