"""v7-S5 Docker proof helper: publish / verify a two-topic WORKS_AT social variant.

usage: proof.py publish | verify | schema
"""
import json
import os
import random
import sys
from datetime import date, timedelta

P, C, POOL, N = 3000, 40, 4000, 3000
# PROOF_TOPICS is the schema's topic list in precedence order (e.g. "a,b" or the reversed "c,b,a");
# each letter has its own fixed data, so reversing the list must reverse the winner.
LETTERS = os.environ.get("PROOF_TOPICS", "a,b").split(",")
PARTS = {"a": 4, "b": 3, "c": 2}
TOPICS = [f"v7s5-works-at-{x}" for x in LETTERS]
TPERSON, TCOMPANY = "v7s5-person", "v7s5-company"
HERE = os.path.dirname(os.path.abspath(__file__))


def dataset():
    """Unique pairs per topic (within-topic order never matters); topics overlap heavily.

    Returns (topic, person, company, since, role_or_None), published in REVERSE precedence order
    (last-listed topic first), so arrival order is the opposite of precedence order. The first-listed
    topic carries since+role; later topics carry no role on every other record, so the property merge
    (``r += props``: a later topic overwrites only what it carries) is checked.
    """
    rnd = random.Random(7)
    pool = list(dict.fromkeys((rnd.randint(1, P), f"co-{rnd.randint(1, C):03d}") for _ in range(POOL)))
    base = date(2000, 1, 1)
    per_letter = {}
    for letter in sorted(PARTS):
        sample = rnd.sample(pool, N)
        per_letter[letter] = [
            (f"v7s5-works-at-{letter}", p, c, base + timedelta(days=5000 * (ord(letter) - 97) + i),
             f"{letter}{i}" if (letter == LETTERS[0] or i % 2 == 0) else None)
            for i, (p, c) in enumerate(sample)
        ]
    records = []
    for letter in reversed(LETTERS):
        records.extend(per_letter[letter])
    return records


def expected():
    """Apply topics in precedence (list) order with property-merge semantics."""
    out = {}
    data = dataset()
    for topic in TOPICS:
        for t, person, company, since, role in data:
            if t != topic:
                continue
            cur = dict(out.get((person, company), {}))
            cur["since"] = since
            if role is not None:
                cur["role"] = role
            out[(person, company)] = cur
    return {k: (v["since"], v.get("role")) for k, v in out.items()}


def publish(bootstrap):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [(TPERSON, 4), (TCOMPANY, 1)] + [(f"v7s5-works-at-{x}", PARTS[x]) for x in LETTERS]
    for f in admin.create_topics([NewTopic(t, num_partitions=n, replication_factor=1) for t, n in topics]).values():
        f.result(30)
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
    data = dataset()
    persons = {p for _t, p, *_ in data}
    companies = {c for _t, _p, c, *_ in data}
    for p in sorted(persons):
        producer.produce(TPERSON, key=str(p), value=json.dumps({"person_id": p, "name": f"P{p}"}))
    for c in sorted(companies):
        producer.produce(TCOMPANY, key=c, value=json.dumps({"company_id": c, "name": c}))
    for topic, person, company, since, role in data:
        props = {"since": since.isoformat()}
        if role is not None:
            props["role"] = role
        producer.produce(topic, key=str(person), value=json.dumps({
            "source": {"person_id": person}, "target": {"company_id": company}, "properties": props,
        }))
    assert producer.flush(60) == 0
    sets = [{(p, c) for t, p, c, *_ in data if t == topic} for topic in TOPICS]
    print(f"published persons={len(persons)} companies={len(companies)} rel_records={len(TOPICS) * N} "
          f"in_all_topics={len(set.intersection(*sets))} expected_pairs={len(expected())} "
          f"partitions={sum(PARTS[x] for x in LETTERS)} topics={len(TOPICS)}")


def verify(uri, db):
    from neo4j import GraphDatabase

    want = expected()
    driver = GraphDatabase.driver(uri, auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]))
    got = {}
    with driver.session(database=db) as s:
        for r in s.run("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
                       "RETURN p.person_id AS p, c.company_id AS c, r.since AS since, r.role AS role"):
            key = (r["p"], r["c"])
            assert key not in got, f"duplicate relationship {key}"
            got[key] = (date.fromisoformat(str(r["since"])), r["role"])
        persons = s.run("MATCH (p:Person) RETURN count(p) AS n").single()["n"]
        companies = s.run("MATCH (c:Company) RETURN count(c) AS n").single()["n"]
    driver.close()
    missing = set(want) - set(got)
    extra = set(got) - set(want)
    wrong = [k for k in want if k in got and got[k] != want[k]]
    print(f"rels got={len(got)} want={len(want)} missing={len(missing)} extra={len(extra)} wrong={len(wrong)} "
          f"persons={persons} companies={companies}")
    for k in wrong[:5]:
        print("  wrong", k, "got", got[k], "want", want[k])
    print("exact_match:", not missing and not extra and not wrong)
    return 0 if not missing and not extra and not wrong else 1


def schema(path):
    text = f"""loading:
  mode: bulk
  consumer_group_id: v7s5
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/v7s5.jsonl

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
    topics: [{", ".join(TOPICS)}]
    topic_conflict: precedence
    replicas: 3
    nodes: {{source: Person, target: Company}}
    source_key_property: person_id
    target_key_property: company_id
    properties:
      since: {{type: date, required: true}}
      role: {{type: string}}
"""
    open(path, "w").write(text)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "publish":
        publish(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    elif cmd == "verify":
        sys.exit(verify(os.environ["NEO4J_URI"], os.environ["NEO4J_DATABASE"]))
    elif cmd == "schema":
        schema(sys.argv[2])
    elif cmd == "partitions":
        print(sum(PARTS[x] for x in LETTERS))
    elif cmd == "topics":
        print(len(TOPICS))
