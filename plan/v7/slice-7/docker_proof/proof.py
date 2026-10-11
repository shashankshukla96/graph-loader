"""v7-S7 Docker proof helper: ONE relationship type, LIKES, over two label pairs with colliding ids.

LIKES reads v7s7-likes-posts-a (4 partitions) and v7s7-likes-posts-b (6), both Person->Post, and
v7s7-likes-comments (3), Person->Comment. Post and Comment ids are the same (0..ITEMS-1) and the Comment likes
reuse most of the Post likes' (person, id) keys, so any cross-talk between the pairs is visible.

usage: proof.py schema <path> orderby|arrival|scoping|bulk|planned|classic
       proof.py topics
       proof.py publish-nodes [Person,Post,Comment] [--orphan]
       proof.py publish-edges <kind> [a,b,c]     (topic letters; default all)
       proof.py verify <kind> [--with-orphan] [--only Post|Comment]
       proof.py dlq <topic>                       envelope counts, key shapes, pairs over partitions
       proof.py plan <timing dir>                 the plan file's pairs and chunk field counts
       proof.py partitions <topic> | hw <topic>
Environment: KAFKA_BOOTSTRAP_SERVERS, NEO4J_URI, NEO4J_DATABASE, NEO4J_USERNAME, NEO4J_PASSWORD.
"""
import glob
import json
import os
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

P, ITEMS = 1500, 40
TA, TB, TC = "v7s7-likes-posts-a", "v7s7-likes-posts-b", "v7s7-likes-comments"
PARTS = {TA: 4, TB: 6, TC: 3}  # the type-wide DLQ must get 6
LETTER = {"a": TA, "b": TB, "c": TC}
NODE_TOPICS = {"Person": ("v7s7-person", "person_id", 4), "Post": ("v7s7-post", "post_id", 1),
               "Comment": ("v7s7-comment", "comment_id", 1)}
PAIR_OF = {TA: ("Person", "Post"), TB: ("Person", "Post"), TC: ("Person", "Comment")}
TARGET_KEY = {TA: "post_id", TB: "post_id", TC: "comment_id"}
ORPHAN = 999999  # a Person never published: one row per topic stays unresolved (bulk: the DLQ rows)
EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _pool(n_pairs, seed):
    rnd = random.Random(seed)
    return rnd, list(dict.fromkeys((rnd.randint(1, P), rnd.randrange(ITEMS)) for _ in range(n_pairs)))


def dataset(kind):
    """``{topic: [(person, item, liked_at, role)]}``: unique liked_at per record, colliding (person, id) keys.

    orderby/bulk/planned/classic: 2,000 records per Post topic, shuffled, plus 2,000 Comment records drawn
    mostly from the same key pool. arrival: 1,500 per Post topic (b's published and dead-lettered before a's),
    1,500 Comment records."""
    rnd, pool = _pool(1500, 17)
    ordered = kind in ("arrival", "scoping")  # arrival-rule runs: published in order, not shuffled
    n = 1500 if ordered else 2000
    out = {TA: [], TB: [], TC: []}
    idx = 0
    for topic, tag in ((TB, "b"), (TA, "a"), (TC, "c")):
        for _ in range(n):
            if topic == TC and rnd.random() < 0.3:
                person, item = rnd.randint(1, P), rnd.randrange(ITEMS)
            else:
                person, item = rnd.choice(pool)
            when = EPOCH + timedelta(seconds=idx)
            if topic == TC and idx % 2 == 0:
                # half of the Comment values are older than every Post value: a leak in either direction on a
                # colliding key leaves a wrong value under order_by
                when = EPOCH - timedelta(days=1, seconds=idx)
            out[topic].append((person, item, when, f"{tag}{idx}"))
            idx += 1
    if not ordered:
        for topic in out:
            rnd.shuffle(out[topic])
    if kind in ("orderby", "bulk"):  # the DLQ runs: one unresolvable row per topic (planned/classic fail closed)
        for topic in out:
            out[topic].append((ORPHAN, 1, EPOCH - timedelta(days=3), f"orphan-{topic[-1]}"))
    return out


def expected(kind, with_orphan=False):
    """``{(S, T): {(person, item): (liked_at, role)}}``."""
    best = {("Person", "Post"): {}, ("Person", "Comment"): {}}
    data = dataset(kind)
    if kind in ("arrival", "scoping"):
        # Not order_by: a pair ends with its last-indexed record. b's rows entered the index first, then a's;
        # within one topic a pair is on one partition (keyed by person), so in publish order. (scoping: the
        # Post rows are written live, b fully consumed before a is published; the same last-wins order.)
        for topic in (TB, TA, TC):
            for person, item, liked_at, role in data[topic]:
                best[PAIR_OF[topic]][(person, item)] = (liked_at, role)
        return best
    for topic, rows in data.items():
        for person, item, liked_at, role in rows:
            if person == ORPHAN and not with_orphan:
                continue
            key = (person, item)
            current = best[PAIR_OF[topic]].get(key)
            if current is None or liked_at > current[0]:
                best[PAIR_OF[topic]][key] = (liked_at, role)
    return best


def _producer(bootstrap):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})


def topics(bootstrap):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = {topic: parts for topic, _key, parts in NODE_TOPICS.values()} | PARTS
    existing = admin.list_topics(timeout=30).topics
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in wanted.items() if t not in existing]
    for f in (admin.create_topics(new).values() if new else []):
        f.result(30)
    print("topics", wanted)


def publish_nodes(bootstrap, labels, orphan):
    producer = _producer(bootstrap)
    if orphan:
        producer.produce("v7s7-person", key=str(ORPHAN), value=json.dumps({"person_id": ORPHAN, "name": "orphan"}))
    else:
        for label in labels:
            topic, key, _parts = NODE_TOPICS[label]
            ids = range(1, P + 1) if label == "Person" else range(ITEMS)
            for i in ids:
                producer.produce(topic, key=str(i), value=json.dumps({key: i, "name": f"{label}-{i}"}))
    assert producer.flush(60) == 0
    print("published nodes", "orphan" if orphan else ",".join(labels))


def publish_edges(bootstrap, kind, letters):
    data = dataset(kind)
    producer = _producer(bootstrap)
    counts = {}
    for letter in letters:
        topic = LETTER[letter]
        for person, item, liked_at, role in data[topic]:
            producer.produce(topic, key=str(person), value=json.dumps({
                "source": {"person_id": person}, "target": {TARGET_KEY[topic]: item},
                "properties": {"liked_at": liked_at.isoformat(), "role": role},
            }))
        counts[topic] = len(data[topic])
    assert producer.flush(60) == 0
    print("published edges", counts)


def verify(uri, db, kind, with_orphan, only=None):
    """``only`` (a target label): check that pair alone and that the other pair has no relationship yet."""
    from neo4j import GraphDatabase

    want = expected(kind, with_orphan)
    if only is not None:
        want = {pair: (rows if pair[1] == only else {}) for pair, rows in want.items()}
    driver = GraphDatabase.driver(uri, auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]))
    ok = True
    with driver.session(database=db) as s:
        for (source, target), rows in want.items():
            key = "post_id" if target == "Post" else "comment_id"
            got = {}
            for r in s.run(f"MATCH (p:{source})-[r:LIKES]->(t:{target}) "
                           f"RETURN p.person_id AS p, t.{key} AS t, r.liked_at AS at, r.role AS role"):
                k = (r["p"], r["t"])
                assert k not in got, f"duplicate relationship {k}"
                got[k] = (r["at"].to_native().astimezone(timezone.utc), r["role"])
            missing, extra = set(rows) - set(got), set(got) - set(rows)
            wrong = [k for k in rows if k in got and got[k] != rows[k]]
            print(f"{source}->{target}: got={len(got)} want={len(rows)} missing={len(missing)} extra={len(extra)} "
                  f"wrong={len(wrong)}")
            for k in wrong[:3]:
                print("  wrong", k, "got", got[k], "want", rows[k])
            ok = ok and not missing and not extra and not wrong
        cross = s.run("MATCH (a)-[r:LIKES]->(b) WHERE NOT (a:Person AND (b:Post OR b:Comment)) "
                      "RETURN count(r) AS c").single()["c"]
        total = s.run("MATCH ()-[r:LIKES]->() RETURN count(r) AS c").single()["c"]
    driver.close()
    full = expected(kind, with_orphan)
    colliding = len(set(full[("Person", "Post")]) & set(full[("Person", "Comment")]))
    print(f"cross_pair_edges={cross} total={total} want_total={sum(len(r) for r in want.values())} "
          f"colliding_keys={colliding}")
    ok = ok and cross == 0 and total == sum(len(r) for r in want.values())
    print("exact_match:", ok)
    return 0 if ok else 1


def partitions(bootstrap, topic):
    from confluent_kafka.admin import AdminClient

    meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(topic=topic, timeout=30).topics.get(topic)
    return len(meta.partitions) if meta is not None and meta.error is None else 0


def dlq(bootstrap, topic, stall_seconds=30.0):
    """Read every envelope of a DLQ-family topic up to its captured high watermarks (never commits)."""
    import time

    from confluent_kafka import Consumer, TopicPartition

    from src.loader.dead_letter import decode_envelope

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "v7s7-proof-reader", "enable.auto.commit": False})
    n = partitions(bootstrap, topic)
    ends, assignment = {}, []
    for p in range(n):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
        if high > low:
            ends[p] = high
            assignment.append(TopicPartition(topic, p, low))
    envelopes, per_key_partitions = [], {}
    if assignment:
        consumer.assign(assignment)
        remaining, last = dict(ends), time.monotonic()
        while remaining:
            m = consumer.poll(1)
            if m is None:
                if time.monotonic() - last > stall_seconds:
                    raise SystemExit(f"dlq read of {topic} stalled; partitions left {sorted(remaining)}")
                continue
            last = time.monotonic()
            if m.error() or m.partition() not in remaining:
                continue
            if m.offset() >= remaining[m.partition()]:
                remaining.pop(m.partition())
                continue
            if m.offset() + 1 >= remaining[m.partition()]:
                remaining.pop(m.partition())
            e = decode_envelope(m.value())
            envelopes.append(e)
            per_key_partitions.setdefault(e.key_bytes, set()).add(m.partition())
    consumer.close()
    shapes = Counter()
    for e in envelopes:
        key = json.loads(e.key_bytes)
        labelled = len(key) == 4 and tuple(key[:2]) == PAIR_OF.get(e.original_topic)
        shapes["labelled_matching_topic" if labelled else f"other:{len(key)}"] += 1
    records = {(e.original_topic, e.original_partition, e.original_offset) for e in envelopes}
    print(json.dumps({
        "topic": topic, "partitions": n, "envelopes": len(envelopes),
        "by_original_topic": dict(Counter(e.original_topic for e in envelopes)),
        "records_by_original_topic": dict(Counter(ident[0] for ident in records)),
        "key_shapes": dict(shapes), "labelled_pairs": len(per_key_partitions),
        "pairs_split_over_dlq_partitions": sum(1 for parts in per_key_partitions.values() if len(parts) > 1),
        "reason_codes": dict(Counter(e.reason_code for e in envelopes)),
    }))


def plan(timing_dir):
    paths = sorted(glob.glob(os.path.join(timing_dir, "**", "*.json"), recursive=True))
    plans = [p for p in paths if '"graph-loader-edge-plan/1"' in open(p).read(200)]
    if not plans:
        raise SystemExit(f"no plan file under {timing_dir}")
    data = json.load(open(plans[-1]))
    item = data["types"]["LIKES"]
    print(json.dumps({
        "plan": plans[-1], "pairs": [[p["source"], p["target"], p["topics"], p["rows"], p["chunks"]]
                                     for p in item.get("pairs", [])],
        "top_level_source": "source" in item, "chunk_fields": sorted({len(c) for c in data["chunks"]}),
        "chunk_pairs": sorted({c[10] for c in data["chunks"] if len(c) == 11}), "rows": item["rows"],
    }))


def schema(path, kind):
    arrival = kind in ("arrival", "scoping")
    rule = "    topic_conflict: arrival\n" if arrival else "    order_by: liked_at\n"
    attempts, max_delay = (200, 5.0) if arrival else (30, 3.0)
    if kind in ("orderby", "arrival", "scoping", "bulk"):
        bulk = "      route_missing_endpoints_in_bulk: true\n" if kind == "bulk" else ""
        dead_letter = f"""    dead_letter:
      enabled: true
      max_retry_attempts: {attempts}
{bulk}"""
    else:
        dead_letter = ""

    def entry(topic):
        target = PAIR_OF[topic][1]
        return (f"      - {{name: {topic}, nodes: {{source: Person, target: {target}}}, "
                f"source_key_property: person_id, target_key_property: {TARGET_KEY[topic]}}}\n")

    nodes = "".join(
        f"""  - label: {label}
    topic: {topic}
    key_property: {key}
    properties:
      {key}: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
""" for label, (topic, key, _parts) in NODE_TOPICS.items())
    text = f"""loading:
  consumer_group_id: v7s7-{kind}
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/v7s7.jsonl

nodes:
{nodes}
edges:
  - type: LIKES
    topics:
{entry(TA)}{entry(TB)}{entry(TC)}{rule}    replicas: 2
    retry: {{base_delay_seconds: 1.0, max_delay_seconds: {max_delay}}}
{dead_letter}    properties:
      liked_at: {{type: datetime, required: true}}
      role: {{type: string}}
"""
    open(path, "w").write(text)


if __name__ == "__main__":
    cmd, bootstrap = sys.argv[1], os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if cmd == "schema":
        schema(sys.argv[2], sys.argv[3])
    elif cmd == "topics":
        topics(bootstrap)
    elif cmd == "publish-nodes":
        args = [a for a in sys.argv[2:] if not a.startswith("--")]
        publish_nodes(bootstrap, (args[0] if args else "Person,Post,Comment").split(","), "--orphan" in sys.argv)
    elif cmd == "publish-edges":
        publish_edges(bootstrap, sys.argv[2], (sys.argv[3] if len(sys.argv) > 3 else "a,b,c").split(","))
    elif cmd == "verify":
        only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
        sys.exit(verify(os.environ["NEO4J_URI"], os.environ["NEO4J_DATABASE"], sys.argv[2], "--with-orphan" in sys.argv,
                        only))
    elif cmd == "dlq":
        dlq(bootstrap, sys.argv[2])
    elif cmd == "plan":
        plan(sys.argv[2])
    elif cmd == "partitions":
        print(partitions(bootstrap, sys.argv[2]))
    elif cmd == "hw":
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": bootstrap, "group.id": "v7s7-proof-hw"})
        print(sum(c.get_watermark_offsets(TopicPartition(sys.argv[2], p), timeout=30)[1]
                  for p in range(partitions(bootstrap, sys.argv[2]))))
        c.close()
