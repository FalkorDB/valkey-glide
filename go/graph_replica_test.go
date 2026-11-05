// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//go:build falkordb

package glide

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/internal/protobuf"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

type graphReplicaEvidence struct {
	Endpoint       string         `json:"endpoint"`
	Graph          string         `json:"graph"`
	Role           string         `json:"role"`
	Complete       bool           `json:"complete"`
	SameConnection bool           `json:"same_connection"`
	Counts         map[string]int `json:"counts"`
	Commands       []struct {
		Command string `json:"command"`
		Client  string `json:"client"`
		Kind    string `json:"kind"`
	} `json:"commands"`
}

func startGraphReplicaMonitor(t *testing.T, endpoint, graph string) func() graphReplicaEvidence {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	command := exec.CommandContext(ctx, "python3", "../utils/falkordb_monitor.py",
		"--endpoint", endpoint, "--graph", graph, "--timeout", "15")
	command.Stderr = os.Stderr
	input, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		input.Close()
		cancel()
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		input.Close()
		output.Close()
		cancel()
		t.Fatal(err)
	}
	finished := false
	t.Cleanup(func() {
		if !finished {
			input.Close()
			command.Process.Kill()
			command.Wait()
		}
		cancel()
	})
	reader := bufio.NewReader(output)
	ready, err := reader.ReadString('\n')
	if err != nil || ready != "READY\n" {
		t.Fatalf("replica monitor was not ready: %q %v", ready, err)
	}
	return func() graphReplicaEvidence {
		t.Helper()
		_, stopErr := fmt.Fprintln(input, "STOP")
		input.Close()
		var evidence graphReplicaEvidence
		decodeErr := json.NewDecoder(reader).Decode(&evidence)
		waitErr := command.Wait()
		finished = true
		cancel()
		if stopErr != nil || decodeErr != nil || waitErr != nil {
			t.Fatalf("replica monitor failed: stop=%v decode=%v wait=%v", stopErr, decodeErr, waitErr)
		}
		return evidence
	}
}

func assertReplicaGraphEvidence(t *testing.T, evidence graphReplicaEvidence, endpoint, graph string) string {
	t.Helper()
	if evidence.Endpoint != endpoint || evidence.Graph != graph || evidence.Role != "replica" ||
		!evidence.Complete || !evidence.SameConnection || evidence.Counts["write"] != 0 || len(evidence.Commands) != 4 {
		t.Fatalf("missing actual replica connection evidence: %+v", evidence)
	}
	for _, kind := range []string{"query", "labels", "relationship_types", "property_keys"} {
		if evidence.Counts[kind] != 1 {
			t.Fatalf("expected exactly one %s on the replica, got %+v", kind, evidence)
		}
	}
	client := evidence.Commands[0].Client
	for _, command := range evidence.Commands {
		if command.Command != "GRAPH.RO_QUERY" || command.Client != client {
			t.Fatalf("graph query/dictionaries did not share the replica connection: %+v", evidence)
		}
	}
	t.Logf("replica %s: query=1 labels=1 relationship_types=1 property_keys=1 write=0 same_connection=true client=%s",
		endpoint, client)
	return client
}

func assertReplicaConnectionProtocol(t *testing.T, listing, name, address string, protocol protobuf.ProtocolVersion) {
	t.Helper()
	expected := "resp=3"
	if protocol == protobuf.ProtocolVersion_RESP2 {
		expected = "resp=2"
	}
	for _, line := range strings.Split(listing, "\n") {
		fields := make(map[string]bool)
		for _, field := range strings.Fields(line) {
			fields[field] = true
		}
		if fields["name="+name] && fields["addr="+address] && fields[expected] {
			t.Logf("replica negotiated %s (%s) on monitored connection %s", protocol, expected, address)
			return
		}
	}
	t.Fatalf("replica CLIENT LIST did not confirm %s for monitored client %s (%s): %s", protocol, name, address, listing)
}

func assertGraphProperty(t *testing.T, properties models.GraphMap, key string, expected any) {
	t.Helper()
	for _, property := range properties {
		if property.Key == key && reflect.DeepEqual(property.Value, expected) {
			return
		}
	}
	t.Fatalf("missing lossless property %s=%#v in %#v", key, expected, properties)
}

func TestGraphConfiguredReplica(t *testing.T) {
	primary := graphConformanceAddress(t, "FALKORDB_STANDALONE_ENDPOINT")
	replica := graphConformanceAddress(t, "FALKORDB_REPLICA_ENDPOINT")
	for _, protocol := range []protobuf.ProtocolVersion{protobuf.ProtocolVersion_RESP2, protobuf.ProtocolVersion_RESP3} {
		t.Run(protocol.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			name := fmt.Sprintf("glide-go-replica-%s-%d", protocol, time.Now().UnixNano())
			writer := newGraphProtocolClient(t, config.NewClientConfiguration().
				WithAddress(primary).WithRequestTimeout(6*time.Second), protocol)
			defer writer.Close()
			defer func() {
				if _, err := writer.CustomCommand(context.Background(), []string{"GRAPH.DELETE", name}); err != nil {
					t.Logf("graph cleanup: %v", err)
				}
			}()
			seed, err := writer.SelectGraph(name)
			if err != nil {
				t.Fatal(err)
			}
			_, err = seed.Query(ctx,
				"CREATE p=(a:ReplicaPerson:Verified {name:$name,large:$large})"+
					"-[r:ReplicaKnows {weight:1}]->(b:ReplicaPerson {name:'Bob'}) RETURN p",
				options.NewGraphQueryOptions().SetParams(map[string]any{"name": "Ada 世界", "large": int64(math.MaxInt64)}))
			if err != nil {
				t.Fatal(err)
			}
			ack, err := writer.CustomCommand(ctx, []string{"WAIT", "1", "5000"})
			if err != nil || ack != int64(1) {
				t.Fatalf("replica did not acknowledge the graph seed: %v %v", ack, err)
			}
			reader := newGraphProtocolClient(t, config.NewClientConfiguration().
				WithAddress(primary).WithAddress(replica).
				WithReadFrom(config.PreferReplica).WithClientName(name), protocol)
			defer reader.Close()
			assertGraphProtocol(t, reader, protocol)
			graph, err := reader.SelectGraph(name)
			if err != nil {
				t.Fatal(err)
			}
			finish := startGraphReplicaMonitor(t, os.Getenv("FALKORDB_REPLICA_ENDPOINT"), name)
			result, err := graph.ROQuery(ctx,
				"MATCH p=(a:ReplicaPerson)-[r:ReplicaKnows]->(b:ReplicaPerson) RETURN p, a, r", nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 3 {
				t.Fatalf("unexpected replica graph result: %+v", result)
			}
			path, pathOK := result.Rows[0][0].(models.GraphPath)
			node, nodeOK := result.Rows[0][1].(models.GraphNode)
			edge, edgeOK := result.Rows[0][2].(models.GraphRelationship)
			if !pathOK || !nodeOK || !edgeOK || len(path.Nodes) != 2 || len(path.Relationships) != 1 ||
				!reflect.DeepEqual(node.Labels, []string{"ReplicaPerson", "Verified"}) ||
				edge.Type != "ReplicaKnows" || edge.SourceID != node.ID || edge.DestinationID != path.Nodes[1].ID {
				t.Fatalf("replica entity/path schema was not resolved: %+v", result)
			}
			assertGraphProperty(t, node.Properties, "name", "Ada 世界")
			assertGraphProperty(t, node.Properties, "large", int64(math.MaxInt64))
			assertGraphProperty(t, edge.Properties, "weight", int64(1))
			connection := assertReplicaGraphEvidence(t, finish(), os.Getenv("FALKORDB_REPLICA_ENDPOINT"), name)
			// Inspect only CLIENT LIST on the exact replica endpoint. Graph reads
			// above use both addresses and PreferReplica, never a forced route.
			inspector := newGraphProtocolClient(t, config.NewClientConfiguration().
				WithAddress(replica).WithNodeDiscoveryMode(config.NodeDiscoveryModeStatic), protocol)
			defer inspector.Close()
			listing, err := inspector.CustomCommand(ctx, []string{"CLIENT", "LIST"})
			if err != nil {
				t.Fatal(err)
			}
			text, ok := listing.(string)
			if !ok {
				t.Fatalf("replica CLIENT LIST returned %T", listing)
			}
			assertReplicaConnectionProtocol(t, text, name, connection, protocol)
		})
	}
}
