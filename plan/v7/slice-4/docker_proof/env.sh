# v7-S4 proof environment: a private scratch stack (p0rg-*), never the project .env.
# Source it; it exports every connection variable explicitly and prints the endpoints.
PROOF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PROOF_DIR/../../../.." && pwd)"
STATE=${V7S4_STATE:-/mnt/graph-loader/bench/p0/v7s4}
export PATH="/mnt/graph-loader/graph-loader/.venv/bin:$PATH" PYTHONPATH="$ROOT"
[ -f "$STATE/.pw" ] && . "$STATE/.pw"   # NEO4J_PASSWORD (written by stack.sh, mode 600)
export KAFKA_BOOTSTRAP_SERVERS=localhost:29892 NEO4J_URI=bolt://localhost:28287
export LOADER_KAFKA_BOOTSTRAP_SERVERS=p0rg-kafka:29092 LOADER_NEO4J_URI=bolt://p0rg-neo4j:7687
export NEO4J_USERNAME=neo4j NEO4J_DATABASE=v7s4 LOADER_CPUSET=${V7S4_LOADER_CPUSET:-31}
# cpusets of the scratch stack (the CI runner overrides them)
V7S4_KAFKA_CPUSET=${V7S4_KAFKA_CPUSET:-30} V7S4_NEO4J_CPUSET=${V7S4_NEO4J_CPUSET:-26-29}
export IMAGE_TAG=v7s4
export GRAPH_LOADER_NODE_IMAGE=graph-loader-node:v7s4 GRAPH_LOADER_EDGE_IMAGE=graph-loader-edge:v7s4 \
  GRAPH_LOADER_CLOCK_IMAGE=graph-loader-clock:v7s4
QLOG_DIR="$STATE/neo4j/logs"
kx() { docker exec p0rg-kafka "$@"; }
cyp() { docker exec -e P="$NEO4J_PASSWORD" p0rg-neo4j bash -c "cypher-shell -u neo4j -p \"\$P\" -d ${2:-v7s4} \"$1\""; }
endpoints() {
  echo "endpoints: KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS NEO4J_URI=$NEO4J_URI NEO4J_DATABASE=$NEO4J_DATABASE" \
    "LOADER_KAFKA=$LOADER_KAFKA_BOOTSTRAP_SERVERS LOADER_NEO4J=$LOADER_NEO4J_URI images=:$IMAGE_TAG cpuset=$LOADER_CPUSET"
}
