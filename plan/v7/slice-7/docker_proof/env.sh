# v7-S7 proof environment: a private scratch stack ($PFX-*, default p0rh-*), never the project .env.
# Source it; it exports every connection variable explicitly and prints the endpoints.
# Overrides: V7S7_PREFIX (container/network names), V7S7_KAFKA_PORT / V7S7_NEO4J_PORT (host ports),
# V7S7_IMAGE_TAG (graph-loader-*:<tag>), V7S7_STATE, V7S7_*_CPUSET.
PROOF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PROOF_DIR/../../../.." && pwd)"
STATE=${V7S7_STATE:-/mnt/graph-loader/bench/p0/v7s7}
PFX=${V7S7_PREFIX:-p0rh}
KAFKA_PORT=${V7S7_KAFKA_PORT:-29992} NEO4J_PORT=${V7S7_NEO4J_PORT:-28387}
export PATH="/mnt/graph-loader/graph-loader/.venv/bin:$PATH" PYTHONPATH="$ROOT"
[ -f "$STATE/.pw" ] && . "$STATE/.pw"   # NEO4J_PASSWORD (written by stack.sh, mode 600)
export KAFKA_BOOTSTRAP_SERVERS=localhost:$KAFKA_PORT NEO4J_URI=bolt://localhost:$NEO4J_PORT
export LOADER_KAFKA_BOOTSTRAP_SERVERS=$PFX-kafka:29092 LOADER_NEO4J_URI=bolt://$PFX-neo4j:7687
export NEO4J_USERNAME=neo4j NEO4J_DATABASE=v7s7 LOADER_CPUSET=${V7S7_LOADER_CPUSET:-31}
# cpusets of the scratch stack (the CI runner overrides them)
V7S7_KAFKA_CPUSET=${V7S7_KAFKA_CPUSET:-30} V7S7_NEO4J_CPUSET=${V7S7_NEO4J_CPUSET:-26-29}
export IMAGE_TAG=${V7S7_IMAGE_TAG:-v7s7}
export GRAPH_LOADER_NODE_IMAGE=graph-loader-node:$IMAGE_TAG GRAPH_LOADER_EDGE_IMAGE=graph-loader-edge:$IMAGE_TAG \
  GRAPH_LOADER_CLOCK_IMAGE=graph-loader-clock:$IMAGE_TAG
QLOG_DIR="$STATE/neo4j/logs"
kx() { docker exec "$PFX-kafka" "$@"; }
cyp() { docker exec -e P="$NEO4J_PASSWORD" "$PFX-neo4j" bash -c "cypher-shell -u neo4j -p \"\$P\" -d ${2:-v7s7} \"$1\""; }
endpoints() {
  echo "endpoints: KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS NEO4J_URI=$NEO4J_URI NEO4J_DATABASE=$NEO4J_DATABASE" \
    "LOADER_KAFKA=$LOADER_KAFKA_BOOTSTRAP_SERVERS LOADER_NEO4J=$LOADER_NEO4J_URI images=:$IMAGE_TAG cpuset=$LOADER_CPUSET" \
    "stack=$PFX-*"
}
