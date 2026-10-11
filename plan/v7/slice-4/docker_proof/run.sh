#!/bin/bash
# v7-S4 Docker proof on the private p0rg-* stack (stack.sh up first; images :v7s4).
# usage: run.sh orderby|arrival|bulk
#   orderby: stream, WORKS_AT on 2 topics with order_by, edges published before nodes -> every row of both
#            topics goes to WORKS_AT.dlq; the retry consumer drains it; an orphan pair parks (one row per
#            topic); replay-dlq --source-topic / --parked behave as documented; order_by protects the newer value.
#   arrival: stream, topic_conflict: arrival; topic b is DLQ'd before topic a (names sort the other way):
#            the parked-pair index sequence must replay each pair b-then-a, so a's last record wins.
#   bulk:    plain `start --mode bulk` with dead_letter -> lease default; only the orphan rows go to WORKS_AT.dlq.
# NOTE: stream groups are stable. A still-live earlier fleet with the same consumer_group_id makes `start`
# wait about 50 s (session timeout + 5 s) for its members and then exit 1; stop it first.
# Everything is scoped to database v7s4, v7s4-* / WORKS_AT.dlq* topics, and this run's run id.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/env.sh"
KIND=${1:?orderby|arrival|bulk}
RUN=v7s4-$KIND-$(date +%s)
OUT=$STATE/$RUN; mkdir -p "$OUT"
export GRAPH_LOADER_TIMING_DIR=$OUT/timing; mkdir -p "$GRAPH_LOADER_TIMING_DIR"
TA=v7s4-works-at-a TB=v7s4-works-at-b DLQ=WORKS_AT.dlq PARKED=WORKS_AT.dlq.parked
PY="python $HERE/proof.py"
fail=0; check() { if [ "$1" = 0 ]; then echo "PASS $2"; else echo "FAIL $2"; fail=1; fi; }
now() { date -u '+%F %T'; }
endpoints | tee "$OUT/endpoints.txt"
[ "$NEO4J_DATABASE" = v7s4 ] && [ "$KAFKA_BOOTSTRAP_SERVERS" = localhost:29892 ] || { echo "refusing: not the scratch stack"; exit 2; }

# group lag summary "<rows> <nonzero>" for a group restricted to topics matching $2
lag() { kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --describe --group "$1" 2>/dev/null |
  awk -v re="$2" '$2 ~ re {n++; if ($6 != "0") z++} END {print n+0, z+0}'; }
replay() { python -m src.cli replay-dlq --config "$OUT/schema.yaml" --edge-type WORKS_AT "$@"; }
summary() { grep -o 'matched=[0-9]* replayed=[0-9]*' "$1" | tail -1; }

# --- clean slate: proof database, v7s4 topics and groups only ---
cyp "CREATE OR REPLACE DATABASE v7s4 WAIT" system >/dev/null
for t in $(kx kafka-topics --bootstrap-server p0rg-kafka:29092 --list 2>/dev/null | grep -E '^(v7s4-|WORKS_AT\.dlq)'); do
  kx kafka-topics --bootstrap-server p0rg-kafka:29092 --delete --topic "$t" >/dev/null 2>&1; done
for i in $(seq 1 30); do kx kafka-topics --bootstrap-server p0rg-kafka:29092 --list 2>/dev/null | grep -qE '^(v7s4-|WORKS_AT\.dlq)' || break; sleep 1; done
$PY schema "$OUT/schema.yaml" "$KIND"
$PY topics
cd "$ROOT"
date -u '+%F %T' > "$OUT/start"

if [ "$KIND" = bulk ]; then
  $PY publish-nodes; $PY publish-edges
  python -m src.cli start --mode bulk --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build \
    --network p0rg-net > "$OUT/cli.log" 2>&1; RC=$?
  check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC"
  check $(grep -q 'edge_mode=lease reason=dead_letter' "$OUT/cli.log" && echo 0 || echo 1) "plain bulk start chose the lease fleet because of dead_letter"
  $PY verify bulk | tee "$OUT/verify.txt"
  check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "exact graph (orphan pair absent)"
  $PY dlq $DLQ | tee "$OUT/dlq.json"
  check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if d['partitions']==6 and d['envelopes']==2 and d['by_original_topic']=={'$TA':1,'$TB':1} else 1)"; echo $?) \
    "one WORKS_AT.dlq, 6 partitions (max of 4 and 6), exactly the 2 orphan rows, one per original topic"
  read -r rows nz < <(for g in $(kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --list 2>/dev/null | grep -F "WORKS_AT-$RUN"); do lag "$g" '^v7s4-works-at-'; done | awk '{r+=$1; z+=$2} END {print r+0, z+0}')
  check $([ "$rows" = 10 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "edge groups: $rows partitions described, $nz with lag"
else
  python -m src.cli start --mode stream --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build \
    --network p0rg-net --retry-consumer-replicas 1 > "$OUT/cli.log" 2>&1 &
  PID=$!
  timeout 180 bash -c "until [ \$(docker ps --filter label=run_id=$RUN --format '{{.Names}}' | grep -c edge) -ge 2 ] && docker ps --filter label=run_id=$RUN --filter label=component=retry-consumer -q | grep -q .; do sleep 1; done"
  sleep 8
  check $([ "$($PY partitions $DLQ)" = 6 ] && echo 0 || echo 1) "WORKS_AT.dlq provisioned with 6 partitions (max of 4 and 6)"
  EG=v7s4-$KIND-WORKS_AT  # stable stream group: <consumer_group_id>-<type>, no run id
  if [ "$KIND" = orderby ]; then
    $PY publish-edges | tee "$OUT/publish.txt"
    timeout 120 bash -c "until [ \$($PY hw $DLQ) -ge 4002 ]; do sleep 1; done"; echo "dlq records after edges: $($PY hw $DLQ)"
    $PY publish-nodes
    for i in $(seq 1 100); do
      $PY verify orderby > "$OUT/verify.txt" 2>&1 && [ "$($PY hw $PARKED)" -ge 2 ] && break; sleep 3; done
    cat "$OUT/verify.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "retry consumer drained WORKS_AT.dlq: exact graph"
    $PY dlq $DLQ | tee "$OUT/dlq.json"; $PY dlq $PARKED | tee "$OUT/parked.json"
    # replay-dlq counts distinct records (one per original identity, its latest DLQ copy; review S3)
    na=$(python -c "import json;print(json.load(open('$OUT/dlq.json'))['records_by_original_topic'].get('$TA',0))")
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if set(d['by_original_topic'])=={'$TA','$TB'} and d['pairs_split_over_dlq_partitions']==0 else 1)"; echo $?) \
      "DLQ holds rows of both topics, each pair in one DLQ partition"
    check $(python -c "import json,sys;d=json.load(open('$OUT/parked.json'));sys.exit(0 if d['by_original_topic']=={'$TA':1,'$TB':1} else 1)"; echo $?) "parked: the orphan pair's row from each topic"
    replay --source-topic $TA --dry-run > "$OUT/r1.log" 2>&1
    check $([ "$(summary "$OUT/r1.log")" = "matched=$na replayed=0" ] && echo 0 || echo 1) "replay --source-topic a --dry-run matches exactly topic a's $na distinct records ($(summary "$OUT/r1.log"))"
    replay --source-topic not-a-topic --dry-run > "$OUT/r2.log" 2>&1; r2=$?
    check $([ $r2 = 1 ] && grep -q 'is not a topic of edge WORKS_AT' "$OUT/r2.log" && echo 0 || echo 1) "replay --source-topic of a foreign topic exits 1"
    replay --parked --dry-run > "$OUT/r3.log" 2>&1
    check $([ "$(summary "$OUT/r3.log")" = "matched=2 replayed=0" ] && echo 0 || echo 1) "replay --parked --dry-run: 2 parked envelopes"
    $PY publish-nodes --orphan; sleep 5
    replay --parked --source-topic $TB > "$OUT/r4.log" 2>&1
    check $([ "$(summary "$OUT/r4.log")" = "matched=1 replayed=1" ] && grep -q "target_topic=$TB replayed=1" "$OUT/r4.log" && echo 0 || echo 1) "replay --parked --source-topic b republishes 1 envelope to topic b"
    for i in $(seq 1 40); do [ "$(cyp "MATCH (:Person {person_id: 999999})-[r:WORKS_AT]->() RETURN r.role" | tail -1)" = '"orphan-b"' ] && break; sleep 2; done
    replay --parked --source-topic $TA > "$OUT/r5.log" 2>&1
    check $([ "$(summary "$OUT/r5.log")" = "matched=1 replayed=1" ] && grep -q "target_topic=$TA replayed=1" "$OUT/r5.log" && echo 0 || echo 1) "replay --parked --source-topic a republishes the older row to topic a"
    sleep 15  # the live loader reads it (or DLQs it) and writes it through the conditional SET
    $PY verify orderby --with-orphan | tee "$OUT/verify2.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify2.txt" && echo 0 || echo 1) "exact graph incl. the orphan pair: b's newer value kept after a's older replay (order_by)"
  else
    $PY publish-edges-topic arrival b
    timeout 120 bash -c "until [ \"\$(kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --describe --group $EG 2>/dev/null | awk '\$2==\"$TB\" && \$6!=\"0\"' | wc -l)\" = 0 ] && [ \"\$(kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --describe --group $EG 2>/dev/null | awk '\$2==\"$TB\"' | wc -l)\" = 6 ]; do sleep 1; done"
    sleep 3; echo "topic b consumed and DLQ'd; dlq records: $($PY hw $DLQ)"
    $PY publish-edges-topic arrival a
    timeout 120 bash -c "until [ \"\$(kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --describe --group $EG 2>/dev/null | awk '\$2==\"$TA\" && \$6!=\"0\"' | wc -l)\" = 0 ] && [ \"\$(kx kafka-consumer-groups --bootstrap-server p0rg-kafka:29092 --describe --group $EG 2>/dev/null | awk '\$2==\"$TA\"' | wc -l)\" = 4 ]; do sleep 1; done"
    sleep 2; echo "topic a consumed and DLQ'd; dlq records: $($PY hw $DLQ)"
    $PY publish-nodes
    for i in $(seq 1 100); do $PY verify arrival > "$OUT/verify.txt" 2>&1 && break; sleep 3; done
    cat "$OUT/verify.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "arrival: every pair ends with its last-indexed record (b DLQ'd first, then a)"
    $PY dlq $DLQ | tee "$OUT/dlq.json"
    check $([ "$($PY hw $PARKED)" = 0 ] && echo 0 || echo 1) "arrival: nothing parked (WORKS_AT.dlq.parked empty)"
  fi
  # v7-S4 review M1: DLQ growth is O(roots x cycles): every envelope is a record's first DLQ copy,
  # one of its own failure cycles (attempt_count), or at most about one blocked copy per such cycle.
  python - "$OUT/dlq.json" "$KIND" <<'EOF' | tee "$OUT/growth.txt"
import json, sys
d = json.load(open(sys.argv[1])); n_in = 4002 if sys.argv[2] == "orderby" else 3000
blocked_retry = d["by_publisher"].get("retry:blocked_by_earlier_unresolved_row", 0)
cycles = d["own_failure_cycles"]
print(f"growth: envelopes={d['envelopes']} input={n_in} ratio={d['envelopes'] / n_in:.2f} records={d['records']} "
      f"own_failure_cycles={cycles} retry_blocked_copies={blocked_retry} by_publisher={d['by_publisher']}")
# every envelope is a record's first copy, one of its own failure cycles, or a blocked copy; blocked
# copies re-published by the retry consumer must be rare (a follower whose root's copy is not fetched yet)
sys.exit(0 if d["envelopes"] <= d["records"] + cycles + blocked_retry and blocked_retry <= max(50, cycles // 10) else 1)
EOF
  check ${PIPESTATUS[0]} "DLQ growth O(roots x cycles): $(cat "$OUT/growth.txt")"
  sleep 5
  read -r rows nz < <(lag "v7s4-$KIND-retry-consumer" '^WORKS_AT\.dlq$')
  check $([ "$rows" = 6 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "retry consumer group: $rows DLQ partitions, $nz with lag"
  read -r rows nz < <(lag "$EG" '^v7s4-works-at-')
  check $([ "$rows" = 10 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "edge group: $rows partitions, $nz with lag"
  for n in $(docker ps --filter label=run_id=$RUN --filter label=component=retry-consumer --format '{{.Names}}'); do docker logs "$n" > "$OUT/retry-consumer.log" 2>&1; done
  kill -TERM $PID; wait $PID; RC=$?
  check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC after SIGTERM"
fi
date -u '+%F %T' > "$OUT/end"
sleep 2
left=$(docker ps -a --filter label=run_id=$RUN -q | wc -l)
check $([ "$left" = 0 ] && echo 0 || echo 1) "containers left for $RUN: $left"

# --- Neo4j query log: every completed entry for db v7s4 in the run window has waiting: 0 ---
{ ls -tr "$QLOG_DIR"/query.log.[0-9]* 2>/dev/null | tac | xargs -r cat; cat "$QLOG_DIR/query.log"; } > "$OUT/qall.txt"
s=$(cat "$OUT/start"); e=$(cat "$OUT/end")
awk -v s="$s" -v e="$e" '{t=substr($0,1,19); gsub("T"," ",t)} t>=s && t<=e' "$OUT/qall.txt" | grep -E '[[:space:]]v7s4 - ' | grep -v 'Query started' > "$OUT/qwin.txt"
n=$(wc -l < "$OUT/qwin.txt"); w=$(grep -c 'waiting: ' "$OUT/qwin.txt"); nz=$(grep -o 'waiting: [0-9]*' "$OUT/qwin.txt" | grep -vc 'waiting: 0$')
echo "query log entries db=v7s4 window=[$s .. $e]: $n, with a waiting field: $w, waiting>0: $nz"
check $([ "$n" -gt 0 ] && [ "$w" = "$n" ] && [ "$nz" = 0 ] && echo 0 || echo 1) "query log: all waiting: 0"
echo "run=$RUN out=$OUT"
exit $fail
