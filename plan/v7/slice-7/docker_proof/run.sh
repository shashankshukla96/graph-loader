#!/bin/bash
# v7-S7 Docker proof on the private $V7S7_PREFIX-* stack (default p0rh-*; stack.sh up first; images :$V7S7_IMAGE_TAG, default v7s7).
# usage: run.sh orderby|arrival|scoping|bulk|planned|classic
#   LIKES reads v7s7-likes-posts-a (4 partitions) and -b (6), Person->Post, and v7s7-likes-comments (3),
#   Person->Comment; Post and Comment ids collide. replicas: 2.
#   orderby: stream, order_by, dead_letter. Post rows are written; Comment rows arrive before the Comment nodes,
#            so they all go to the type's ONE LIKES.dlq (6 partitions) with labelled keys; then the Comment nodes;
#            the retry consumer drains it. replay-dlq: a --source-topic dry run counts; a --target-topic of the
#            other pair is refused with 0 publishes. One orphan row per topic parks.
#   arrival: stream, topic_conflict: arrival, dead_letter. Post topic b is published (and DLQ'd) first, then a,
#            then the nodes: each Post pair ends with its last-indexed record; Comment ids collide.
#   scoping: stream, arrival, dead_letter. Person and Post nodes present, Comment nodes late: the Comment rows are
#            DLQ'd and indexed, the colliding Post rows are written live (no false cross-pair blocking); then the
#            Comment nodes and the retry consumer drains the DLQ.
#   bulk:    plain `start --mode bulk` with route_missing_endpoints_in_bulk -> lease default; one orphan row per
#            topic in LIKES.dlq with its pair's labelled key.
#   planned: plain `start --mode bulk` without dead_letter -> planned default; the plan lists both pairs.
#   classic: --edge-classic; waits reported, not asserted.
# Everything is scoped to database v7s7, v7s7-* / LIKES.dlq* topics, and this run's run id.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/env.sh"
KIND=${1:?orderby|arrival|scoping|bulk|planned|classic}
RUN=v7s7-$KIND-$(date +%s)
OUT=$STATE/$RUN; mkdir -p "$OUT"
export GRAPH_LOADER_TIMING_DIR=$OUT/timing; mkdir -p "$GRAPH_LOADER_TIMING_DIR"
TA=v7s7-likes-posts-a TB=v7s7-likes-posts-b TC=v7s7-likes-comments DLQ=LIKES.dlq PARKED=LIKES.dlq.parked
PY="python $HERE/proof.py"
fail=0; check() { if [ "$1" = 0 ]; then echo "PASS $2"; else echo "FAIL $2"; fail=1; fi; }
endpoints | tee "$OUT/endpoints.txt"
[ "$NEO4J_DATABASE" = v7s7 ] && [ "$KAFKA_BOOTSTRAP_SERVERS" = "localhost:$KAFKA_PORT" ] || { echo "refusing: not the scratch stack"; exit 2; }

# group lag summary "<rows> <nonzero>" for a group restricted to topics matching $2
lag() { kx kafka-consumer-groups --bootstrap-server $PFX-kafka:29092 --describe --group "$1" 2>/dev/null |
  awk -v re="$2" '$2 ~ re {n++; if ($6 != "0") z++} END {print n+0, z+0}'; }
replay() { python -m src.cli replay-dlq --config "$OUT/schema.yaml" --edge-type LIKES "$@"; }
summary() { grep -o 'matched=[0-9]* replayed=[0-9]*' "$1" | tail -1; }
count() { cyp "$1" | tail -1; }
wait_lag_zero() {  # group, topic regex, partitions, timeout
  local i; for i in $(seq 1 "$4"); do read -r r z < <(lag "$1" "$2"); [ "$r" = "$3" ] && [ "$z" = 0 ] && return 0; sleep 1; done; return 1; }

# --- clean slate: proof database, v7s7 topics and groups only ---
cyp "CREATE OR REPLACE DATABASE v7s7 WAIT" system >/dev/null
for t in $(kx kafka-topics --bootstrap-server $PFX-kafka:29092 --list 2>/dev/null | grep -E '^(v7s7-|LIKES\.dlq)'); do
  kx kafka-topics --bootstrap-server $PFX-kafka:29092 --delete --topic "$t" >/dev/null 2>&1; done
for i in $(seq 1 30); do kx kafka-topics --bootstrap-server $PFX-kafka:29092 --list 2>/dev/null | grep -qE '^(v7s7-|LIKES\.dlq)' || break; sleep 1; done
$PY schema "$OUT/schema.yaml" "$KIND"
$PY topics
cd "$ROOT"
date -u '+%F %T' > "$OUT/start"

bulk_groups_lag() {  # every bulk edge group of this run over the 13 LIKES partitions
  for g in $(kx kafka-consumer-groups --bootstrap-server $PFX-kafka:29092 --list 2>/dev/null | grep -F "LIKES-$RUN"); do
    lag "$g" '^v7s7-likes-'; done | awk '{r+=$1; z+=$2} END {print r+0, z+0}'; }

if [ "$KIND" = bulk ] || [ "$KIND" = planned ] || [ "$KIND" = classic ]; then
  $PY publish-nodes; $PY publish-edges "$KIND"
  flag=(); [ "$KIND" = classic ] && flag=(--edge-classic)
  python -m src.cli start --mode bulk --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build \
    --network "$PFX-net" "${flag[@]}" > "$OUT/cli.log" 2>&1; RC=$?
  check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC"
  case $KIND in
    bulk) want='edge_mode=lease reason=dead_letter' ;; planned) want='edge_mode=planned reason=default' ;;
    classic) want='edge_mode=classic reason=flag' ;; esac
  check $(grep -q "$want" "$OUT/cli.log" && echo 0 || echo 1) "the CLI logged $want"
  $PY verify "$KIND" | tee "$OUT/verify.txt"
  check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "exact graph per labelled pair, 0 cross-pair edges"
  if [ "$KIND" = bulk ]; then
    $PY dlq $DLQ | tee "$OUT/dlq.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if d['partitions']==6 and d['envelopes']==3 and d['by_original_topic']=={'$TA':1,'$TB':1,'$TC':1} and d['key_shapes']=={'labelled_matching_topic':3} else 1)"; echo $?) \
      "one LIKES.dlq, 6 partitions, exactly the 3 orphan rows (one per topic), keys [S,T,sk,tk] of each topic's pair"
  fi
  if [ "$KIND" = planned ]; then
    $PY plan "$GRAPH_LOADER_TIMING_DIR" | tee "$OUT/plan.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/plan.json'));sys.exit(0 if [p[:2] for p in d['pairs']]==[['Person','Post'],['Person','Comment']] and d['chunk_fields']==[11] and d['chunk_pairs']==[0,1] and not d['top_level_source'] else 1)"; echo $?) \
      "plan: types.LIKES.pairs lists both pairs, every chunk has 11 fields with its pair"
  else
    read -r rows nz < <(bulk_groups_lag)
    check $([ "$rows" = 13 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "edge groups: $rows partitions described, $nz with lag"
  fi
else
  python -m src.cli start --mode stream --config "$OUT/schema.yaml" --run-id "$RUN" --skip-image-build \
    --network "$PFX-net" --retry-consumer-replicas 1 > "$OUT/cli.log" 2>&1 &
  PID=$!
  timeout 180 bash -c "until [ \$(docker ps --filter label=run_id=$RUN --format '{{.Names}}' | grep -c edge) -ge 2 ] && docker ps --filter label=run_id=$RUN --filter label=component=retry-consumer -q | grep -q .; do sleep 1; done"
  sleep 8
  check $([ "$($PY partitions $DLQ)" = 6 ] && echo 0 || echo 1) "LIKES.dlq provisioned with 6 partitions (max of 4, 6 and 3)"
  EG=v7s7-$KIND-LIKES-$RUN
  if [ "$KIND" = orderby ]; then
    $PY publish-nodes Person,Post
    timeout 120 bash -c "until [ \"\$(docker exec -e P=\$NEO4J_PASSWORD $PFX-neo4j cypher-shell -u neo4j -p \$NEO4J_PASSWORD -d v7s7 'MATCH (n) WHERE n:Person OR n:Post RETURN count(n)' | tail -1)\" = 1540 ]; do sleep 1; done"
    $PY publish-edges orderby | tee "$OUT/publish.txt"
    wait_lag_zero "$EG" '^v7s7-likes-' 13 180; check $? "edge group consumed all 13 partitions before the Comment nodes exist"
    posts=$(count "MATCH (:Person)-[r:LIKES]->(:Post) RETURN count(r)"); comments=$(count "MATCH (:Person)-[r:LIKES]->(:Comment) RETURN count(r)")
    want_posts=$(python -c "import sys;sys.path.insert(0,'$HERE');import proof;print(len(proof.expected('orderby')[('Person','Post')]))")
    check $([ "$posts" = "$want_posts" ] && [ "$comments" = 0 ] && echo 0 || echo 1) "before the Comment nodes: $posts/$want_posts Post likes written, $comments Comment likes"
    $PY dlq $DLQ | tee "$OUT/dlq-before.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq-before.json'));sys.exit(0 if d['records_by_original_topic'].get('$TC',0)==2001 and set(d['key_shapes'])=={'labelled_matching_topic'} else 1)"; echo $?) \
      "every Comment row (2001) is in LIKES.dlq; every envelope key is [S,T,sk,tk] of its original_topic's pair"
    $PY publish-nodes Comment
    for i in $(seq 1 100); do $PY verify orderby > "$OUT/verify.txt" 2>&1 && [ "$($PY hw $PARKED)" -ge 3 ] && break; sleep 3; done
    cat "$OUT/verify.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "retry consumer drained LIKES.dlq: exact graph per labelled pair, 0 cross-pair edges"
    $PY dlq $DLQ | tee "$OUT/dlq.json"; $PY dlq $PARKED | tee "$OUT/parked.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if set(d['key_shapes'])=={'labelled_matching_topic'} and d['pairs_split_over_dlq_partitions']==0 else 1)"; echo $?) \
      "DLQ: every key labelled with its topic's pair, 0 labelled pairs split over DLQ partitions"
    check $(python -c "import json,sys;d=json.load(open('$OUT/parked.json'));sys.exit(0 if d['by_original_topic']=={'$TA':1,'$TB':1,'$TC':1} else 1)"; echo $?) "parked: the orphan's row from each topic"
    nc=$(python -c "import json;print(json.load(open('$OUT/dlq.json'))['records_by_original_topic'].get('$TC',0))")
    replay --source-topic $TC --dry-run > "$OUT/r1.log" 2>&1
    check $([ "$(summary "$OUT/r1.log")" = "matched=$nc replayed=0" ] && echo 0 || echo 1) "replay --source-topic comments --dry-run: $nc distinct records ($(summary "$OUT/r1.log"))"
    before=$($PY hw $TA)
    replay --target-topic $TA --source-topic $TC > "$OUT/r2.log" 2>&1; r2=$?
    after=$($PY hw $TA)
    check $([ $r2 = 1 ] && grep -q "replaying $TC records into $TA would write them as Person->Post" "$OUT/r2.log" && [ "$before" = "$after" ] && echo 0 || echo 1) \
      "replay --target-topic posts-a --source-topic comments exits 1 with 0 publishes ($before -> $after)"
  elif [ "$KIND" = scoping ]; then
    # Final follow-ups S3: Person and Post nodes present, Comment nodes late. Every Comment row goes to the DLQ
    # and the parked-pair index as a Person->Comment entry; the Post rows, most on the same (person, id) keys,
    # must be written live -- a pair-blind index would block them behind the parked Comment rows.
    $PY publish-nodes Person,Post
    timeout 120 bash -c "until [ \"\$(docker exec -e P=\$NEO4J_PASSWORD $PFX-neo4j cypher-shell -u neo4j -p \$NEO4J_PASSWORD -d v7s7 'MATCH (n) WHERE n:Person OR n:Post RETURN count(n)' | tail -1)\" = 1540 ]; do sleep 1; done"
    $PY publish-edges scoping c
    wait_lag_zero "$EG" "^$TC\$" 3 180; check $? "comments consumed and dead-lettered (no Comment nodes yet)"
    sleep 3; echo "dlq records: $($PY hw $DLQ)"
    $PY publish-edges scoping b
    wait_lag_zero "$EG" "^($TB|$TC)\$" 9 180; check $? "Post topic b consumed"
    $PY publish-edges scoping a
    wait_lag_zero "$EG" '^v7s7-likes-' 13 180; check $? "Post topic a consumed"
    sleep 3
    $PY verify scoping --only Post > "$OUT/verify-posts.txt" 2>&1; cat "$OUT/verify-posts.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify-posts.txt" && echo 0 || echo 1) \
      "before the Comment nodes: every Post like written live and exact, 0 Comment likes"
    $PY dlq $DLQ | tee "$OUT/dlq-before.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq-before.json'));sys.exit(0 if d['records_by_original_topic']=={'$TC':1500} and set(d['key_shapes'])=={'labelled_matching_topic'} else 1)"; echo $?) \
      "the DLQ holds the 1500 Comment rows and no Post row: no Post row was blocked behind a Comment row of its keys"
    $PY publish-nodes Comment
    for i in $(seq 1 100); do $PY verify scoping > "$OUT/verify.txt" 2>&1 && break; sleep 3; done
    cat "$OUT/verify.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "after the Comment nodes: exact graph per labelled pair, 0 cross-pair edges"
    $PY dlq $DLQ | tee "$OUT/dlq.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if set(d['by_original_topic'])=={'$TC'} and set(d['key_shapes'])=={'labelled_matching_topic'} else 1)"; echo $?) \
      "DLQ: Comment rows only, every key labelled with its topic's pair"
    check $([ "$($PY hw $PARKED)" = 0 ] && echo 0 || echo 1) "scoping: nothing parked"
  else
    $PY publish-edges arrival b,c
    wait_lag_zero "$EG" "^($TB|$TC)\$" 9 180; check $? "topics b and comments consumed and DLQ'd (no nodes yet)"
    sleep 3; echo "dlq records: $($PY hw $DLQ)"
    $PY publish-edges arrival a
    wait_lag_zero "$EG" '^v7s7-likes-' 13 180; check $? "topic a consumed and DLQ'd behind b's rows"
    sleep 2; echo "dlq records: $($PY hw $DLQ)"
    $PY publish-nodes
    for i in $(seq 1 100); do $PY verify arrival > "$OUT/verify.txt" 2>&1 && break; sleep 3; done
    cat "$OUT/verify.txt"
    check $(grep -q 'exact_match: True' "$OUT/verify.txt" && echo 0 || echo 1) "arrival: every labelled pair ends with its last-indexed record (b DLQ'd first, then a)"
    $PY dlq $DLQ | tee "$OUT/dlq.json"
    check $(python -c "import json,sys;d=json.load(open('$OUT/dlq.json'));sys.exit(0 if set(d['key_shapes'])=={'labelled_matching_topic'} else 1)"; echo $?) "DLQ keys labelled with their topic's pair"
    check $([ "$($PY hw $PARKED)" = 0 ] && echo 0 || echo 1) "arrival: nothing parked"
  fi
  sleep 5
  read -r rows nz < <(lag "graph-loader-retry-consumer-$RUN" '^LIKES\.dlq$')
  check $([ "$rows" = 6 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "retry consumer group: $rows DLQ partitions, $nz with lag"
  read -r rows nz < <(lag "$EG" '^v7s7-likes-')
  check $([ "$rows" = 13 ] && [ "$nz" = 0 ] && echo 0 || echo 1) "edge group: $rows partitions, $nz with lag"
  for n in $(docker ps --filter label=run_id=$RUN --filter label=component=retry-consumer --format '{{.Names}}'); do docker logs "$n" > "$OUT/retry-consumer.log" 2>&1; done
  for n in $(docker ps --filter label=run_id=$RUN --format '{{.Names}}' | grep edge | head -1); do docker logs "$n" > "$OUT/edge-loader.log" 2>&1; done
  check $(grep -q 'stage=edge-multi-pair edge=LIKES mode=lease pairs=Person>Post,Person>Comment' "$OUT/edge-loader.log" && echo 0 || echo 1) "edge loader runs one lease coordinator per pair"
  kill -TERM $PID; wait $PID; RC=$?
  check $([ $RC = 0 ] && echo 0 || echo 1) "cli rc=$RC after SIGTERM"
fi
date -u '+%F %T' > "$OUT/end"
sleep 2
left=$(docker ps -a --filter label=run_id=$RUN -q | wc -l)
check $([ "$left" = 0 ] && echo 0 || echo 1) "containers left for $RUN: $left"

# --- Neo4j query log: every completed entry for db v7s7 in the run window has waiting: 0 (classic: reported) ---
{ ls -tr "$QLOG_DIR"/query.log.[0-9]* 2>/dev/null | tac | xargs -r cat; cat "$QLOG_DIR/query.log"; } > "$OUT/qall.txt"
s=$(cat "$OUT/start"); e=$(cat "$OUT/end")
awk -v s="$s" -v e="$e" '{t=substr($0,1,19); gsub("T"," ",t)} t>=s && t<=e' "$OUT/qall.txt" | grep -E '[[:space:]]v7s7 - ' | grep -v 'Query started' > "$OUT/qwin.txt"
n=$(wc -l < "$OUT/qwin.txt"); w=$(grep -c 'waiting: ' "$OUT/qwin.txt"); nz=$(grep -o 'waiting: [0-9]*' "$OUT/qwin.txt" | grep -vc 'waiting: 0$')
echo "query log entries db=v7s7 window=[$s .. $e]: $n, with a waiting field: $w, waiting>0: $nz"
if [ "$KIND" = classic ]; then
  echo "classic: lock waits reported, not asserted ($nz entries with waiting > 0)"
  check $([ "$n" -gt 0 ] && [ "$w" = "$n" ] && echo 0 || echo 1) "query log: entries present"
else
  check $([ "$n" -gt 0 ] && [ "$w" = "$n" ] && [ "$nz" = 0 ] && echo 0 || echo 1) "query log: all waiting: 0"
fi
echo "run=$RUN out=$OUT"
exit $fail
