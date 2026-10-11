#!/bin/bash
# v7-S2 Docker proof: a two-topic WORKS_AT (order_by, replicas 3) in stream, --edge-leases bulk and classic bulk.
# usage: run.sh stream|lease|classic      (run from anywhere; needs the shared graph-loader-kafka/-neo4j stack)
# Images: IMAGE_TAG=v7s2 make build-images first. Everything is scoped to DB $PROOF_DB and to v7s2-* topics/groups.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
MODE=${1:?stream|lease|classic}
PROOF_DB=v7s2
export PATH="$ROOT/.venv/bin:/mnt/graph-loader/graph-loader/.venv/bin:$PATH"
export GRAPH_LOADER_NODE_IMAGE=graph-loader-node:v7s2 GRAPH_LOADER_EDGE_IMAGE=graph-loader-edge:v7s2 GRAPH_LOADER_CLOCK_IMAGE=graph-loader-clock:v7s2
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://graph-loader-neo4j:7687
export NEO4J_USERNAME=neo4j NEO4J_DATABASE=${NEO4J_DATABASE:-$PROOF_DB} LOADER_CPUSET=${LOADER_CPUSET:-12-17}
[ "$NEO4J_DATABASE" = "$PROOF_DB" ] || { echo "refusing: NEO4J_DATABASE=$NEO4J_DATABASE is not the proof database $PROOF_DB"; exit 2; }
export NEO4J_PASSWORD="$(docker exec graph-loader-neo4j printenv NEO4J_AUTH | cut -d/ -f2)"
OUT=${OUT:-/tmp/v7s2-proof}/$MODE; mkdir -p "$OUT"
# unique per execution: bulk run ids are derived from it and leftover control-topic events of a reused id conflict
RUN=v7s2-$MODE-$(date +%s)
KX="docker exec graph-loader-kafka"
fail=0; check() { if [ "$1" = 0 ]; then echo "PASS $2"; else echo "FAIL $2"; fail=1; fi; }
cyp() { docker exec -e P="$NEO4J_PASSWORD" graph-loader-neo4j bash -c "cypher-shell -u neo4j -p \"\$P\" -d $PROOF_DB \"$1\""; }

# --- clean slate (proof topics/groups/database only) ---
docker exec -e P="$NEO4J_PASSWORD" graph-loader-neo4j bash -c "cypher-shell -u neo4j -p \"\$P\" -d system \"CREATE DATABASE $PROOF_DB IF NOT EXISTS WAIT\"" >/dev/null
for t in $($KX kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -E '^v7s2-'); do $KX kafka-topics --bootstrap-server kafka:29092 --delete --topic "$t" >/dev/null 2>&1; done
for g in $($KX kafka-consumer-groups --bootstrap-server kafka:29092 --list 2>/dev/null | grep v7s2); do $KX kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group "$g" >/dev/null 2>&1; done
for i in $(seq 1 20); do $KX kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -qE '^v7s2-' || break; sleep 1; done
[ "$NEO4J_DATABASE" = "$PROOF_DB" ] && cyp "MATCH (n) DETACH DELETE n" >/dev/null 2>&1
python "$HERE/proof.py" schema "$OUT/schema.yaml"
python "$HERE/proof.py" publish | tail -1
date -u '+%F %T' > "$OUT/start"

cd "$ROOT"
case $MODE in
  stream)  ARGS="--mode stream" ;;
  lease)   ARGS="--mode bulk --edge-leases" ;;
  classic) ARGS="--mode bulk --edge-classic" ;;
esac
python -m src.cli start $ARGS --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build --network graph-loader-net > "$OUT/cli.log" 2>&1 &
PID=$!
if [ "$MODE" = stream ]; then
  for i in $(seq 1 100); do sleep 3; python "$HERE/proof.py" verify > "$OUT/verify.txt" 2>&1 && break; done
  sleep 5; kill -TERM $PID
fi
wait $PID; RC=$?
check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC"
date -u '+%F %T' > "$OUT/end"

python "$HERE/proof.py" verify | tee "$OUT/verify.txt"
check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "exact graph"

# --- zero lag on both topics, every partition committed ---
rows=0; lag=0
for g in $($KX kafka-consumer-groups --bootstrap-server kafka:29092 --list 2>/dev/null | grep -E "^v7s2-WORKS_AT-$RUN"); do
  while read -r topic part cur end l; do
    rows=$((rows+1)); [ "$l" = 0 ] || lag=$((lag+1))
  done < <($KX kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$g" 2>/dev/null | awk '$2 ~ /^v7s2-works-at-/ {print $2,$3,$4,$5,$6}')
done
echo "partitions described=$rows nonzero-lag=$lag"
check $([ "$rows" = 7 ] && [ "$lag" = 0 ] && echo 0 || echo 1) "zero lag on all 7 partitions of both topics"

# --- Neo4j query log: every completed entry for the proof db in the run window has waiting: 0 ---
for f in $(docker exec graph-loader-neo4j bash -c 'ls -tr /logs/query.log.[0-9]* 2>/dev/null | tac; echo /logs/query.log'); do docker exec graph-loader-neo4j cat "$f"; done > "$OUT/qall.txt"
s=$(cat "$OUT/start"); e=$(cat "$OUT/end")
awk -v s="$s" -v e="$e" '{t=substr($0,1,19); gsub("T"," ",t)} t>=s && t<=e' "$OUT/qall.txt" | grep -E "[[:space:]]$PROOF_DB - " | grep -v 'Query started' > "$OUT/qwin.txt"
n=$(wc -l < "$OUT/qwin.txt"); nz=$(grep -o 'waiting: [0-9]*' "$OUT/qwin.txt" | grep -vc 'waiting: 0$')
echo "query log entries db=$PROOF_DB window=[$s .. $e]: $n, with waiting>0: $nz"
if [ "$MODE" = classic ]; then
  # classic mode serialises replicas with APOC node locks by design; waits are expected, reported only
  check $([ "$n" -gt 0 ] && echo 0 || echo 1) "query log present (classic: lock waits are informational, $nz entries waited)"
else
  check $([ "$n" -gt 0 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "query log: all waiting: 0"
fi

echo "containers left for $RUN:"; docker ps -a --format '{{.Names}}' | grep -i "$(printf $RUN | xxd -p)" || true
exit $fail
