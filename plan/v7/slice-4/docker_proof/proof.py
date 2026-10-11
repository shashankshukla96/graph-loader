"""v7-S4 Docker proof helper: a social variant with WORKS_AT fed by two topics and the DLQ enabled.

usage: proof.py schema <path> orderby|arrival|bulk
       proof.py topics                     create every proof topic (empty)
       proof.py publish-edges orderby      both topics, shuffled, incl. the orphan pair
       proof.py publish-edges-topic arrival <a|b>
       proof.py publish-nodes [--orphan]   persons/companies (the orphan person only with --orphan)
       proof.py verify orderby|arrival|bulk [--with-orphan]
       proof.py dlq <topic>                envelope counts per original_topic, partitions
       proof.py partitions <topic> | hw <topic>
Environment: KAFKA_BOOTSTRAP_SERVERS, NEO4J_URI, NEO4J_DATABASE, NEO4J_USERNAME, NEO4J_PASSWORD.
"""
import json
import os
import random
import sys
from collections import Counter
from datetime import date, timedelta

P, C = 1500, 30
TA, TB = "v7s4-works-at-a", "v7s4-works-at-b"
PARTS = {TA: 4, TB: 6}  # the type-wide DLQ must get 6
TPERSON, TCOMPANY = "v7s4-person", "v7s4-company"
ORPHAN = (999999, "co-001")  # its Person is published only for the parked-replay step
ORPHAN_A = (date(2001, 1, 1), "orphan-a")  # older
ORPHAN_B = (date(2002, 1, 1), "orphan-b")  # newer: order_by keeps it whatever replays later


def _pool(n_pairs, seed):
    rnd = random.Random(seed)
    pool = list(dict.fromkeys((rnd.randint(1, P), f"co-{rnd.randint(1, C):03d}") for _ in range(n_pairs)))
    return rnd, pool


def orderby_dataset():
    """(topic, person, company, since, role); shuffled across topics; unique since per record."""
    rnd, pool = _pool(1500, 7)
    base = date(2000, 1, 1)
    records, idx = [], 0
    for topic, tag in ((TA, "a"), (TB, "b")):
        for _ in range(2000):
            person, company = rnd.choice(pool)
            records.append((topic, person, company, base + timedelta(days=idx), f"{tag}{idx}"))
            idx += 1
    rnd.shuffle(records)
    records.append((TA, *ORPHAN, *ORPHAN_A))
    records.append((TB, *ORPHAN, *ORPHAN_B))
    return records


def arrival_dataset():
    """Topic b is published (and DLQ'd) first, topic a only afterwards. `since` is NOT an
    order_by here: it is just a payload, so the expected value of a pair is the last record
    that entered the parked-pair index, i.e. a's last record if the pair has one, else b's."""
    rnd, pool = _pool(800, 11)
    base = date(2000, 1, 1)
    out = {TA: [], TB: []}
    idx = 0
    for topic, tag in ((TB, "b"), (TA, "a")):
        for _ in range(1500):
            person, company = rnd.choice(pool)
            out[topic].append((topic, person, company, base + timedelta(days=idx), f"{tag}{idx}"))
            idx += 1
    return out


def expected(kind, with_orphan=False):
    best = {}
    if kind in ("orderby", "bulk"):
        for _t, person, company, since, role in orderby_dataset():
            if (person, company) == ORPHAN and not with_orphan:
                continue
            key = (person, company)
            if key not in best or since > best[key][0]:
                best[key] = (since, role)
    else:  # arrival: b first, then a; within a topic a pair is on one partition (keyed by person)
        data = arrival_dataset()
        for topic in (TB, TA):
            for _t, person, company, since, role in data[topic]:
                best[(person, company)] = (since, role)
    return best


def _producer(bootstrap):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})


def _produce_edges(producer, records):
    for topic, person, company, since, role in records:
        producer.produce(topic, key=str(person), value=json.dumps({
            "source": {"person_id": person}, "target": {"company_id": company},
            "properties": {"since": since.isoformat(), "role": role},
        }))
    assert producer.flush(60) == 0


def topics(bootstrap):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = {TPERSON: 4, TCOMPANY: 1, **PARTS}
    existing = admin.list_topics(timeout=30).topics
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in wanted.items() if t not in existing]
    for f in (admin.create_topics(new).values() if new else []):
        f.result(30)
    print("topics", wanted)


def publish_nodes(bootstrap, orphan):
    producer = _producer(bootstrap)
    if orphan:
        producer.produce(TPERSON, key=str(ORPHAN[0]), value=json.dumps({"person_id": ORPHAN[0], "name": "orphan"}))
    else:
        for p in range(1, P + 1):
            producer.produce(TPERSON, key=str(p), value=json.dumps({"person_id": p, "name": f"P{p}"}))
        for c in range(1, C + 1):
            producer.produce(TCOMPANY, key=f"co-{c:03d}", value=json.dumps({"company_id": f"co-{c:03d}", "name": f"C{c}"}))
    assert producer.flush(60) == 0
    print("published nodes", "orphan" if orphan else f"persons={P} companies={C}")


def verify(uri, db, kind, with_orphan):
    from neo4j import GraphDatabase

    want = expected(kind, with_orphan)
    driver = GraphDatabase.driver(uri, auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]))
    got = {}
    with driver.session(database=db) as s:
        for r in s.run("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
                       "RETURN p.person_id AS p, c.company_id AS c, r.since AS since, r.role AS role"):
            key = (r["p"], r["c"])
            assert key not in got, f"duplicate relationship {key}"
            got[key] = (date.fromisoformat(str(r["since"])), r["role"])
    driver.close()
    missing, extra = set(want) - set(got), set(got) - set(want)
    wrong = [k for k in want if k in got and got[k] != want[k]]
    print(f"rels got={len(got)} want={len(want)} missing={len(missing)} extra={len(extra)} wrong={len(wrong)}")
    for k in wrong[:5]:
        print("  wrong", k, "got", got[k], "want", want[k])
    if kind == "arrival":
        both = sum(1 for k in want if any(r[1:3] == k for r in arrival_dataset()[TA])
                   and any(r[1:3] == k for r in arrival_dataset()[TB]))
        print(f"pairs with records in both topics: {both}")
    ok = not missing and not extra and not wrong
    print("exact_match:", ok)
    return 0 if ok else 1


def partitions(bootstrap, topic):
    from confluent_kafka.admin import AdminClient

    meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(topic=topic, timeout=30).topics.get(topic)
    return len(meta.partitions) if meta is not None and meta.error is None else 0


def dlq(bootstrap, topic, stall_seconds=30.0):
    """Read every envelope of a DLQ-family topic up to the high watermarks captured once (never
    commits). Tracks each partition's position, so an offset gap cannot make it spin: a partition
    ends at its captured high watermark, and the whole read fails after ``stall_seconds`` without
    a message."""
    import time

    from confluent_kafka import Consumer, TopicPartition

    from src.loader.dead_letter import decode_envelope

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "v7s4-proof-reader", "enable.auto.commit": False})
    n = partitions(bootstrap, topic)
    ends, assignment = {}, []
    for p in range(n):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
        if high > low:
            ends[p] = high
            assignment.append(TopicPartition(topic, p, low))
    by_topic, per_partition_pairs, envelopes = Counter(), {}, []
    if assignment:
        consumer.assign(assignment)
        remaining = dict(ends)
        last = time.monotonic()
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
            by_topic[e.original_topic] += 1
            per_partition_pairs.setdefault(e.key_bytes, set()).add(m.partition())
            envelopes.append(e)
    consumer.close()
    split = sum(1 for parts in per_partition_pairs.values() if len(parts) > 1)
    # A re-DLQ keeps loader_identity and first_failure_at_ms but refreshes dlq_routed_at_ms; the live
    # writer's first copy has dlq_routed_at_ms == first_failure_at_ms.
    by_publisher = Counter(
        f"{'live' if e.dlq_routed_at_ms == e.first_failure_at_ms else 'retry'}:{e.reason_code}" for e in envelopes
    )
    # root cycles: per record, the highest attempt_count seen = re-DLQs of its own failure
    attempts = {}
    for e in envelopes:
        ident = (e.original_topic, e.original_partition, e.original_offset)
        attempts[ident] = max(attempts.get(ident, 0), e.attempt_count)
    print(json.dumps({"topic": topic, "partitions": n, "envelopes": len(envelopes),
                      "by_original_topic": dict(by_topic), "pairs": len(per_partition_pairs),
                      "records": len(attempts), "own_failure_cycles": sum(attempts.values()),
                      "records_by_original_topic": dict(Counter(ident[0] for ident in attempts)),
                      "pairs_split_over_dlq_partitions": split,
                      "reason_codes": dict(Counter(e.reason_code for e in envelopes)),
                      "by_publisher": dict(by_publisher)}))


def schema(path, kind):
    edge_rule = "    order_by: since\n" if kind in ("orderby", "bulk") else "    topic_conflict: arrival\n"
    # orderby: the orphan pair must park quickly; arrival: nothing may park while topic b, then a,
    # wait in the DLQ for their nodes (a park would unblock the pair's later rows by design).
    attempts, max_delay = (6, 3.0) if kind == "orderby" else (200, 5.0)
    bulk = "      route_missing_endpoints_in_bulk: true\n" if kind == "bulk" else ""
    text = f"""loading:
  consumer_group_id: v7s4-{kind}
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/v7s4.jsonl

nodes:
  - label: Person
    topic: {TPERSON}
    key_property: person_id
    properties:
      person_id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
  - label: Company
    topic: {TCOMPANY}
    key_property: company_id
    properties:
      company_id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}

edges:
  - type: WORKS_AT
    topics: [{TA}, {TB}]
{edge_rule}    replicas: 2
    nodes: {{source: Person, target: Company}}
    source_key_property: person_id
    target_key_property: company_id
    retry: {{base_delay_seconds: 1.0, max_delay_seconds: {max_delay}}}
    dead_letter:
      enabled: true
      max_retry_attempts: {attempts}
{bulk}    properties:
      since: {{type: date, required: true}}
      role: {{type: string}}
"""
    open(path, "w").write(text)


if __name__ == "__main__":
    cmd, bootstrap = sys.argv[1], os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if cmd == "schema":
        schema(sys.argv[2], sys.argv[3])
    elif cmd == "topics":
        topics(bootstrap)
    elif cmd == "publish-edges":
        _produce_edges(_producer(bootstrap), orderby_dataset())
        print(f"published edges={len(orderby_dataset())} expected_pairs={len(expected('orderby'))}")
    elif cmd == "publish-edges-topic":
        letter = sys.argv[3]
        records = arrival_dataset()[TA if letter == "a" else TB]
        _produce_edges(_producer(bootstrap), records)
        print(f"published {letter} edges={len(records)}")
    elif cmd == "publish-nodes":
        publish_nodes(bootstrap, "--orphan" in sys.argv)
    elif cmd == "verify":
        sys.exit(verify(os.environ["NEO4J_URI"], os.environ["NEO4J_DATABASE"], sys.argv[2], "--with-orphan" in sys.argv))
    elif cmd == "dlq":
        dlq(bootstrap, sys.argv[2])
    elif cmd == "partitions":
        print(partitions(bootstrap, sys.argv[2]))
    elif cmd == "hw":  # sum of high watermarks (records ever written) of a topic
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": bootstrap, "group.id": "v7s4-proof-hw"})
        print(sum(c.get_watermark_offsets(TopicPartition(sys.argv[2], p), timeout=30)[1]
                  for p in range(partitions(bootstrap, sys.argv[2]))))
        c.close()
