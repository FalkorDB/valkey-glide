// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//go:build falkordb

package glide

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/interfaces"
	"github.com/valkey-io/valkey-glide/go/v2/internal/protobuf"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

// Go's public configuration has no protocol selector. This test-only wrapper
// drives the normal client creation transport without adding a production API.
type graphProtocolConfiguration struct {
	clientConfiguration
	protocol protobuf.ProtocolVersion
}

func (cfg graphProtocolConfiguration) ToProtobuf() (*protobuf.ConnectionRequest, error) {
	request, err := cfg.clientConfiguration.ToProtobuf()
	if err != nil {
		return nil, err
	}
	request.Protocol = cfg.protocol
	return request, nil
}

func graphConformanceAddress(t *testing.T, variable string) *config.NodeAddress {
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

func newGraphProtocolClient(
	t *testing.T,
	cfg *config.ClientConfiguration,
	protocol protobuf.ProtocolVersion,
) *Client {
	t.Helper()
	base, err := createClient(graphProtocolConfiguration{clientConfiguration: cfg, protocol: protocol})
	if err != nil {
		t.Fatal(err)
	}
	base.setMessageHandler(NewMessageHandler(nil, nil))
	return &Client{baseClient: *base, clientConfig: cfg}
}

func newGraphProtocolClusterClient(
	t *testing.T,
	cfg *config.ClusterClientConfiguration,
	protocol protobuf.ProtocolVersion,
) *ClusterClient {
	t.Helper()
	base, err := createClient(graphProtocolConfiguration{clientConfiguration: cfg, protocol: protocol})
	if err != nil {
		t.Fatal(err)
	}
	base.setMessageHandler(NewMessageHandler(nil, nil))
	return &ClusterClient{baseClient: *base, clientConfig: cfg}
}

func assertGraphProtocol(t *testing.T, client interfaces.BaseClientCommands, protocol protobuf.ProtocolVersion) {
	t.Helper()
	var info any
	var err error
	switch client := client.(type) {
	case *Client:
		info, err = client.CustomCommand(context.Background(), []string{"CLIENT", "INFO"})
	case *ClusterClient:
		value, commandErr := client.CustomCommandWithRoute(
			context.Background(), []string{"CLIENT", "INFO"}, config.RandomRoute,
		)
		info, err = value.SingleValue(), commandErr
	default:
		t.Fatalf("unexpected graph conformance client %T", client)
	}
	if err != nil {
		t.Fatal(err)
	}
	text, ok := info.(string)
	if !ok {
		t.Fatalf("CLIENT INFO returned %T, not protocol evidence", info)
	}
	expected := "resp=3"
	if protocol == protobuf.ProtocolVersion_RESP2 {
		expected = "resp=2"
	}
	for _, field := range strings.Fields(text) {
		if field == expected {
			t.Logf("server negotiated %s (%s)", protocol, field)
			return
		}
	}
	t.Fatalf("server did not confirm %s: %s", protocol, text)
}

func TestGraphConformance(t *testing.T) {
	for _, topology := range []string{"standalone", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			for _, protocol := range []protobuf.ProtocolVersion{protobuf.ProtocolVersion_RESP2, protobuf.ProtocolVersion_RESP3} {
				t.Run(protocol.String(), func(t *testing.T) {
					var client interfaces.BaseClientCommands
					var custom func(context.Context, []string) (any, error)
					if topology == "standalone" {
						standalone := newGraphProtocolClient(t, config.NewClientConfiguration().
							WithAddress(graphConformanceAddress(t, "FALKORDB_STANDALONE_ENDPOINT")), protocol)
						client, custom = standalone, standalone.CustomCommand
					} else {
						cluster := newGraphProtocolClusterClient(t, config.NewClusterClientConfiguration().
							WithAddress(graphConformanceAddress(t, "FALKORDB_CLUSTER_ENDPOINT")), protocol)
						client = cluster
						custom = func(ctx context.Context, args []string) (any, error) {
							return cluster.CustomCommand(ctx, args)
						}
					}
					defer client.Close()
					assertGraphProtocol(t, client, protocol)
					name := fmt.Sprintf("glide-go-graph-%s-%d", protocol, time.Now().UnixNano())
					ctx := context.Background()
					cleaned := false
					cleanup := func() {
						if cleaned {
							return
						}
						cleaned = true
						if _, err := custom(ctx, []string{"GRAPH.DELETE", name}); err != nil {
							t.Logf("graph cleanup: %v", err)
						}
					}
					defer cleanup()
					graph, err := client.SelectGraph(name)
					if err != nil {
						t.Fatal(err)
					}
					params := map[string]any{"min": int64(math.MinInt64), "max": int64(math.MaxInt64), "text": "世界 ' \\ \x00"}
					result, err := graph.Query(ctx, "RETURN $min AS minimum, $max AS maximum, $text AS text",
						options.NewGraphQueryOptions().SetParams(params).SetTimeout(0))
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(result.Columns, []string{"minimum", "maximum", "text"}) ||
						!reflect.DeepEqual(
							result.Rows,
							[][]any{{int64(math.MinInt64), int64(math.MaxInt64), params["text"]}},
						) {
						t.Fatalf("typed parameter or column loss: %+v", result)
					}
					allTypes := "CREATE p=(a:Person:Researcher {name:'Ada',age:42})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) " +
						"RETURN null, 'text', -9223372036854775808, true, 1.5, [1,null,{nested:[false,'x']}], r, a, p, " +
						"{first:1,second:'two'}, point({latitude:1.5,longitude:2.5}), vecf32([1,2.5,-3]), " +
						"localdatetime('2020-01-02T03:04:05'), date('2020-01-02'), localtime('03:04:05'), duration('P1Y2M3DT4H5M6S')"
					result, err = graph.Query(ctx, allTypes, nil)
					if err != nil {
						t.Fatal(err)
					}
					wantTypes := []any{
						nil, "", int64(0), false, float64(0),
						[]any{},
						models.GraphRelationship{},
						models.GraphNode{},
						models.GraphPath{},
						models.GraphMap{},
						models.GraphPoint{},
						models.GraphVector{},
						models.GraphDateTime(0), models.GraphDate(0), models.GraphTime(0), models.GraphDuration(0),
					}
					if len(result.Rows) != 1 || len(result.Rows[0]) != len(wantTypes) {
						t.Fatalf("unexpected all-types result: %+v", result)
					}
					for index, want := range wantTypes {
						if reflect.TypeOf(result.Rows[0][index]) != reflect.TypeOf(want) {
							t.Errorf("column %d: got %T, want %T", index, result.Rows[0][index], want)
						}
					}
					node := result.Rows[0][7].(models.GraphNode)
					if !reflect.DeepEqual(node.Labels, []string{"Person", "Researcher"}) || len(node.Properties) != 2 ||
						result.Statistics.NodesCreated == nil || *result.Statistics.NodesCreated != 2 {
						t.Fatalf("node schema or statistics lost: %+v", result)
					}
					if _, err := graph.ROQuery(ctx, "MATCH (a:Person) RETURN a", nil); err != nil {
						t.Fatal(err)
					}
					if _, err := graph.ROQuery(ctx, "CREATE (:Forbidden)", nil); err == nil {
						t.Fatal("ROQuery accepted a mutation")
					}
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if _, err := graph.Query(cancelled, "RETURN 1", nil); !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled graph query returned %v", err)
					}
					if _, err := graph.Query(ctx, "this is not valid Cypher", nil); err == nil {
						t.Fatal("syntax error was accepted")
					}
					if _, err := graph.ROQuery(ctx, "MATCH (a:Researcher) RETURN a", nil); err != nil {
						t.Fatal(err)
					}
					cleanup()
					client.Close()
					if _, err := graph.Query(ctx, "RETURN 1", nil); err == nil {
						t.Fatal("graph outlived its closed parent")
					}
				})
			}
		})
	}
}
