#!/bin/bash
# v7-S5 Docker proof: a two-topic WORKS_AT (topic_conflict: precedence, replicas 3), --edge-leases bulk and classic bulk.
# usage: [PROOF_TOPICS=a,b|c,b,a] run.sh lease|classic   (needs the shared graph-loader-kafka/-neo4j stack)
# PROOF_TOPICS is the schema's topic list in precedence order; reversing it must reverse the winner.
# Images: IMAGE_TAG=v7s5 make build-images first. Everything is scoped to DB $PROOF_DB and to v7s5-* topics/groups.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
MODE=${1:?lease|classic}
PROOF_DB=v7s5
export PATH="$ROOT/.venv/bin:/mnt/graph-loader/graph-loader/.venv/bin:$PATH"
export GRAPH_LOADER_NODE_IMAGE=graph-loader-node:v7s5 GRAPH_LOADER_EDGE_IMAGE=graph-loader-edge:v7s5 GRAPH_LOADER_CLOCK_IMAGE=graph-loader-clock:v7s5
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://graph-loader-neo4j:7687
export NEO4J_USERNAME=neo4j NEO4J_DATABASE=${NEO4J_DATABASE:-$PROOF_DB} LOADER_CPUSET=${LOADER_CPUSET:-12-17}
[ "$NEO4J_DATABASE" = "$PROOF_DB" ] || { echo "refusing: NEO4J_DATABASE=$NEO4J_DATABASE is not the proof database $PROOF_DB"; exit 2; }
export NEO4J_PASSWORD="$(docker exec graph-loader-neo4j printenv NEO4J_AUTH | cut -d/ -f2)"
export PROOF_TOPICS=${PROOF_TOPICS:-a,b}
OUT=${OUT:-/tmp/v7s5-proof}/$MODE-${PROOF_TOPICS//,/}; mkdir -p "$OUT"
# unique per execution: bulk run ids are derived from it and leftover control-topic events of a reused id conflict
RUN=v7s5-$MODE-$(date +%s)
KX="docker exec graph-loader-kafka"
fail=0; check() { if [ "$1" = 0 ]; then echo "PASS $2"; else echo "FAIL $2"; fail=1; fi; }
cyp() { docker exec -e P="$NEO4J_PASSWORD" graph-loader-neo4j bash -c "cypher-shell -u neo4j -p \"\$P\" -d $PROOF_DB \"$1\""; }

# --- clean slate (proof topics/groups/database only) ---
docker exec -e P="$NEO4J_PASSWORD" graph-loader-neo4j bash -c "cypher-shell -u neo4j -p \"\$P\" -d system \"CREATE DATABASE $PROOF_DB IF NOT EXISTS WAIT\"" >/dev/null
for t in $($KX kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -E '^v7s5-'); do $KX kafka-topics --bootstrap-server kafka:29092 --delete --topic "$t" >/dev/null 2>&1; done
for g in $($KX kafka-consumer-groups --bootstrap-server kafka:29092 --list 2>/dev/null | grep v7s5); do $KX kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group "$g" >/dev/null 2>&1; done
for i in $(seq 1 20); do $KX kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -qE '^v7s5-' || break; sleep 1; done
[ "$NEO4J_DATABASE" = "$PROOF_DB" ] && cyp "MATCH (n) DETACH DELETE n" >/dev/null 2>&1
python "$HERE/proof.py" schema "$OUT/schema.yaml"
python "$HERE/proof.py" publish | tail -1
date -u '+%F %T' > "$OUT/start"

cd "$ROOT"
case $MODE in
  lease)   ARGS="--mode bulk --edge-leases" ;;
  classic) ARGS="--mode bulk --edge-classic" ;;
esac
python -m src.cli start $ARGS --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build --network graph-loader-net > "$OUT/cli.log" 2>&1 &
PID=$!
wait $PID; RC=$?
check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC"
date -u '+%F %T' > "$OUT/end"

python "$HERE/proof.py" verify | tee "$OUT/verify.txt"
check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "exact graph"
# sub-stages ran strictly one after the other, in the listed (precedence) order
grep -E 'stage=bulk-substage(-complete)? ' "$OUT/cli.log" | sed -E 's/^.*(stage=bulk-substage[-a-z]*) edge=[A-Z_]+ topic=v7s5-works-at-([abc]).*/\1 \2/' > "$OUT/substages.txt"
want=""; for x in ${PROOF_TOPICS//,/ }; do want="${want}stage=bulk-substage $x|stage=bulk-substage-complete $x|"; done
check $([ "$(tr '\n' '|' < "$OUT/substages.txt")" = "$want" ] && echo 0 || echo 1) "sub-stage order $PROOF_TOPICS (start, complete each)"

# --- one consumer group per topic; zero lag on every partition of every topic ---
NT=$(python "$HERE/proof.py" topics); NP=$(python "$HERE/proof.py" partitions)
groups=0; rows=0; lag=0
for g in $($KX kafka-consumer-groups --bootstrap-server kafka:29092 --list 2>/dev/null | grep -E "^v7s5-WORKS_AT-$RUN"); do
  groups=$((groups+1))
  while read -r topic part cur end l; do
    rows=$((rows+1)); [ "$l" = 0 ] || lag=$((lag+1))
  done < <($KX kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$g" 2>/dev/null | awk '$2 ~ /^v7s5-works-at-/ {print $2,$3,$4,$5,$6}')
done
echo "groups=$groups (topics=$NT) partitions described=$rows (want $NP) nonzero-lag=$lag"
check $([ "$groups" = "$NT" ] && echo 0 || echo 1) "one consumer group per topic"
check $([ "$rows" = "$NP" ] && [ "$lag" = 0 ] && echo 0 || echo 1) "zero lag on all $NP partitions of all topics"

# --- Neo4j query log: every completed entry for the proof db in the run window has waiting: 0 ---
for f in $(docker exec graph-loader-neo4j bash -c 'ls -tr /logs/query.log.[0-9]* 2>/dev/null | tac; echo /logs/query.log'); do docker exec graph-loader-neo4j cat "$f"; done > "$OUT/qall.txt"
s=$(cat "$OUT/start"); e=$(cat "$OUT/end")
awk -v s="$s" -v e="$e" '{t=substr($0,1,19); gsub("T"," ",t)} t>=s && t<=e' "$OUT/qall.txt" | grep -E "[[:space:]]$PROOF_DB - " | grep -v 'Query started' > "$OUT/qwin.txt"
n=$(wc -l < "$OUT/qwin.txt"); nw=$(grep -o 'waiting: [0-9]*' "$OUT/qwin.txt" | wc -l); nz=$(grep -o 'waiting: [0-9]*' "$OUT/qwin.txt" | grep -vc 'waiting: 0$')
echo "query log entries db=$PROOF_DB window=[$s .. $e]: $n, with a waiting field: $nw, with waiting>0: $nz"
# an entry without a waiting field must not pass vacuously
check $([ "$n" -gt 0 ] && [ "$n" = "$nw" ] && echo 0 || echo 1) "every query-log entry carries a waiting field ($nw of $n)"
if [ "$MODE" = classic ]; then
  # classic mode serialises replicas with APOC node locks by design; waits are expected, reported only
  check $([ "$n" -gt 0 ] && echo 0 || echo 1) "query log present (classic: lock waits are informational, $nz entries waited)"
else
  check $([ "$n" -gt 0 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "query log: all waiting: 0"
fi

left=$(docker ps -a --format '{{.Names}} {{.Labels}}' | grep -c -e "$RUN" -e "$(printf $RUN | xxd -p)")
echo "containers left for $RUN: $left"
check $([ "$left" = 0 ] && echo 0 || echo 1) "zero containers left"
exit $fail
