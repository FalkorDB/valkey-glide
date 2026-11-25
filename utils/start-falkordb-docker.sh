#!/usr/bin/env bash
# Starts FalkorDB servers in Docker for the graph routing tests
# (glide-core/tests/test_graph_routing.rs):
#   - a standalone primary and one replica
#   - a cluster of 3 primaries with 1 replica each
# Every node runs the image's own valkey-server with the image's FalkorDB
# module. Only FalkorDB 6.x images (on Valkey) are supported. The image's entrypoint is
# bypassed: it would also start the FalkorDB browser on port 3000 on every node.
#
# Usage:
#   IMAGE=falkordb/falkordb:6.2.1 utils/start-falkordb-docker.sh
#   IMAGE=falkordb/falkordb:6.2.1 VALKEY_IMAGE=valkey/valkey:7.2 utils/start-falkordb-docker.sh
#   utils/start-falkordb-docker.sh stop
#
# Environment:
#   IMAGE               Image to start (required unless stopping).
#   VALKEY_IMAGE        Optional Valkey image whose valkey-server runs instead of
#                       the FalkorDB image's own, with the same FalkorDB module,
#                       to test other Valkey versions.
#   STANDALONE_PORT     Standalone primary port (default 6479); its replica uses the next port.
#   CLUSTER_PORT_START  First of 6 consecutive cluster ports (default 7479).
#   ENV_FILE            Where to write the endpoints outside GitHub Actions
#                       (default utils/.falkordb-endpoints.env), for `source`.
#
# Exports GRAPH_STANDALONE_ENDPOINTS (primary,replica) and
# GRAPH_CLUSTER_ENDPOINTS, to GITHUB_ENV when set, else to ENV_FILE.
#
# Containers use --network host and bind 127.0.0.1. On macOS, Docker Desktop
# needs Settings > Resources > Network > "Enable host networking"; without it
# the servers start but the host cannot reach them.

set -euo pipefail

STANDALONE_PORT="${STANDALONE_PORT:-6479}"
REPLICA_PORT=$((STANDALONE_PORT + 1))
CLUSTER_PORT_START="${CLUSTER_PORT_START:-7479}"
CLUSTER_PORTS=$(seq "$CLUSTER_PORT_START" $((CLUSTER_PORT_START + 5)))
ENV_FILE="${ENV_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.falkordb-endpoints.env}"
PREFIX="glide-falkordb"
MODULE="/var/lib/falkordb/bin/falkordb.so"

stop_all() {
    local names
    names=$(docker ps -aq --filter "name=^${PREFIX}-")
    if [ -n "$names" ]; then
        # shellcheck disable=SC2086 # one id per word
        docker rm -f $names >/dev/null
    fi
}

if [ "${1:-}" = "stop" ]; then
    stop_all
    echo "Removed ${PREFIX}-* containers"
    exit 0
fi

: "${IMAGE:?IMAGE must be set, e.g. falkordb/falkordb:6.2.1}"

docker pull "$IMAGE"

# The image's server and CLI. FalkorDB 4.x images (a Redis module) have neither.
read -r SERVER_BIN CLI_BIN < <(docker run --rm --entrypoint sh "$IMAGE" -c \
    'echo "$(command -v valkey-server)" "$(command -v valkey-cli)"')
if [ -z "$SERVER_BIN" ] || [ -z "$CLI_BIN" ]; then
    echo "ERROR: ${IMAGE} has no valkey-server or valkey-cli; use a FalkorDB 6.x image" >&2
    exit 1
fi
echo "Using ${SERVER_BIN} and ${CLI_BIN} from ${IMAGE}"

# Optionally replace the server binary with another Valkey version's, mounted
# into every node container.
SERVER_MOUNT=()
if [ -n "${VALKEY_IMAGE:-}" ]; then
    docker pull "$VALKEY_IMAGE"
    VALKEY_BIN_DIR=$(mktemp -d)
    VALKEY_CID=$(docker create "$VALKEY_IMAGE")
    docker cp "${VALKEY_CID}:/usr/local/bin/valkey-server" "${VALKEY_BIN_DIR}/valkey-server" >/dev/null
    docker rm "$VALKEY_CID" >/dev/null
    SERVER_BIN="/opt/glide-valkey/valkey-server"
    SERVER_MOUNT=(-v "${VALKEY_BIN_DIR}/valkey-server:${SERVER_BIN}:ro")
    echo "Using valkey-server from ${VALKEY_IMAGE}"
fi

stop_all

# cli <container> <port> <args...>: runs the image's CLI inside a container.
cli() {
    local name="$1" port="$2"
    shift 2
    docker exec "$name" "$CLI_BIN" -p "$port" "$@"
}

fail() {
    echo "ERROR: $1" >&2
    docker logs "$2" 2>&1 | tail -20 >&2 || true
    if [[ "$(uname -s)" == "Darwin" ]]; then
        echo "HINT: on macOS, Docker Desktop needs 'Enable host networking'" \
            "(Settings > Resources > Network) for --network host." >&2
    fi
    exit 1
}

# start_node <container> <port> <extra server args...>
start_node() {
    local name="$1" port="$2"
    shift 2
    # The ${arr[@]+...} form keeps an empty array valid under `set -u` in bash 3.2.
    docker run -d --name "$name" --network host \
        ${SERVER_MOUNT[@]+"${SERVER_MOUNT[@]}"} --entrypoint "$SERVER_BIN" "$IMAGE" \
        --port "$port" --bind 127.0.0.1 --protected-mode no --dir /tmp \
        --save "" --appendonly no --enable-debug-command yes \
        --loadmodule "$MODULE" "$@" >/dev/null
}

# wait_for <container> <port> <description> <command that succeeds when ready>
wait_for() {
    local name="$1" port="$2" what="$3"
    shift 3
    for _ in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    fail "${what} on port ${port} not ready after 30 s" "$name"
}

has_graph_module() { cli "$1" "$2" MODULE LIST | grep -qx graph; }
replica_linked() { cli "$1" "$2" INFO replication | grep -q "^master_link_status:up"; }
cluster_ok() { cli "$1" "$2" CLUSTER INFO | grep -q "^cluster_state:ok"; }
replica_ready() {
    replica_linked "$1" "$2" && ! cli "$1" "$2" INFO replication | grep -q "^master_sync_in_progress:1"
}

echo "=== Standalone primary ${STANDALONE_PORT}, replica ${REPLICA_PORT} ==="
start_node "${PREFIX}-standalone" "$STANDALONE_PORT"
wait_for "${PREFIX}-standalone" "$STANDALONE_PORT" "standalone primary" \
    has_graph_module "${PREFIX}-standalone" "$STANDALONE_PORT"
start_node "${PREFIX}-replica" "$REPLICA_PORT" --replicaof 127.0.0.1 "$STANDALONE_PORT"
wait_for "${PREFIX}-replica" "$REPLICA_PORT" "standalone replica" \
    replica_ready "${PREFIX}-replica" "$REPLICA_PORT"

echo "=== Cluster on ports ${CLUSTER_PORT_START}-$((CLUSTER_PORT_START + 5)) ==="
CLUSTER_ADDRESSES=()
for port in $CLUSTER_PORTS; do
    start_node "${PREFIX}-cluster-${port}" "$port" \
        --cluster-enabled yes --cluster-config-file "nodes-${port}.conf" --cluster-node-timeout 5000
    CLUSTER_ADDRESSES+=("127.0.0.1:${port}")
done
for port in $CLUSTER_PORTS; do
    wait_for "${PREFIX}-cluster-${port}" "$port" "cluster node" \
        has_graph_module "${PREFIX}-cluster-${port}" "$port"
done
docker exec "${PREFIX}-cluster-${CLUSTER_PORT_START}" "$CLI_BIN" --cluster create \
    "${CLUSTER_ADDRESSES[@]}" --cluster-replicas 1 --cluster-yes
for port in $CLUSTER_PORTS; do
    wait_for "${PREFIX}-cluster-${port}" "$port" "cluster state" \
        cluster_ok "${PREFIX}-cluster-${port}" "$port"
    if cli "${PREFIX}-cluster-${port}" "$port" ROLE | head -1 | grep -qE "^(slave|replica)$"; then
        wait_for "${PREFIX}-cluster-${port}" "$port" "cluster replica" \
            replica_ready "${PREFIX}-cluster-${port}" "$port"
    fi
done

STANDALONE_ENDPOINTS="127.0.0.1:${STANDALONE_PORT},127.0.0.1:${REPLICA_PORT}"
CLUSTER_ENDPOINTS=$(IFS=,; echo "${CLUSTER_ADDRESSES[*]}")
if [ -n "${GITHUB_ENV:-}" ]; then
    {
        echo "GRAPH_STANDALONE_ENDPOINTS=${STANDALONE_ENDPOINTS}"
        echo "GRAPH_CLUSTER_ENDPOINTS=${CLUSTER_ENDPOINTS}"
    } >>"$GITHUB_ENV"
else
    {
        echo "export GRAPH_STANDALONE_ENDPOINTS=${STANDALONE_ENDPOINTS}"
        echo "export GRAPH_CLUSTER_ENDPOINTS=${CLUSTER_ENDPOINTS}"
    } >"$ENV_FILE"
    echo "Endpoints written to ${ENV_FILE}; run: source ${ENV_FILE}"
fi
echo "FalkorDB servers ready ($(cli "${PREFIX}-standalone" "$STANDALONE_PORT" INFO server | grep -E '^valkey_version:' | tr -d '\r')):" \
    "standalone ${STANDALONE_ENDPOINTS}; cluster ${CLUSTER_ENDPOINTS}"
