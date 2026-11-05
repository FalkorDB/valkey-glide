// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package graph

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestGraphTestBuildTags(t *testing.T) {
	for _, libc := range []string{"gnu", "musl"} {
		t.Run(libc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "make", "--no-print-directory", "-s", "-n", "-C", "../..",
				"graph-test", "UNAME=Linux", "ARCH=x86_64", "IS_ALPINE=false",
				"TARGET_TRIPLET=x86_64-unknown-linux-"+libc)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("make graph-test dry run: %s: %v", output, err)
			}
			match := regexp.MustCompile(`go test -tags=(?:'([^']+)'|([^ ]+))`).FindStringSubmatch(string(output))
			if match == nil {
				t.Fatalf("missing graph test build tags: %s", output)
			}
			tags := match[1] + match[2]
			command = exec.CommandContext(ctx, "go", "list", "-tags="+tags, "-f", "{{join .CgoLDFLAGS \" \"}}", ".")
			command.Dir = "../.."
			command.Env = append(os.Environ(), "CGO_ENABLED=1", "GOOS=linux", "GOARCH=amd64", "GOFLAGS=")
			output, err = command.CombinedOutput()
			if err != nil {
				t.Fatalf("go list with graph test tags: %s: %v", output, err)
			}
			if !strings.Contains(string(output), "rustbin/x86_64-unknown-linux-"+libc) {
				t.Fatalf("graph-test selected the wrong native library: %s", output)
			}
			if !strings.Contains(tags, "falkordb") {
				t.Fatalf("graph-test omitted the FalkorDB build tag: %q", tags)
			}
		})
	}
}
