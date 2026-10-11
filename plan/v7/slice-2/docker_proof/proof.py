"""v7-S2 Docker proof helper: publish / verify a two-topic WORKS_AT social variant.

usage: proof.py publish | verify | schema
"""
import json
import os
import random
import sys
from datetime import date, timedelta

P, C, POOL, N = 3000, 40, 4000, 6000
TA, TB = "v7s2-works-at-a", "v7s2-works-at-b"
TPERSON, TCOMPANY = "v7s2-person", "v7s2-company"
HERE = os.path.dirname(os.path.abspath(__file__))


def dataset():
    rnd = random.Random(7)
    pool = [(rnd.randint(1, P), f"co-{rnd.randint(1, C):03d}") for _ in range(POOL)]
    pool = list(dict.fromkeys(pool))
    base = date(2000, 1, 1)
    records = []  # (topic, person, company, since, role)
    idx = 0
    for topic, tag in ((TA, "a"), (TB, "b")):
        for _ in range(N):
            person, company = rnd.choice(pool)
            records.append((topic, person, company, base + timedelta(days=idx), f"{tag}{idx}"))
            idx += 1
    # publish order: shuffled across the two topics so neither topic is systematically "later"
    rnd.shuffle(records)
    return records


def expected():
    best = {}
    for _t, person, company, since, role in dataset():
        key = (person, company)
        if key not in best or since > best[key][0]:
            best[key] = (since, role)
    return best


def publish(bootstrap):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    for f in admin.create_topics([NewTopic(t, num_partitions=n, replication_factor=1) for t, n in
                                  ((TPERSON, 4), (TCOMPANY, 1), (TA, 4), (TB, 3))]).values():
        f.result(30)
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
    persons = {p for _t, p, *_ in dataset()}
    companies = {c for _t, _p, c, *_ in dataset()}
    for p in sorted(persons):
        producer.produce(TPERSON, key=str(p), value=json.dumps({"person_id": p, "name": f"P{p}"}))
    for c in sorted(companies):
        producer.produce(TCOMPANY, key=c, value=json.dumps({"company_id": c, "name": c}))
    for topic, person, company, since, role in dataset():
        producer.produce(topic, key=str(person), value=json.dumps({
            "source": {"person_id": person}, "target": {"company_id": company},
            "properties": {"since": since.isoformat(), "role": role},
        }))
    assert producer.flush(60) == 0
    print(f"published persons={len(persons)} companies={len(companies)} rel_records={2 * N} "
          f"expected_pairs={len(expected())}")


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
  consumer_group_id: v7s2
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/v7s2.jsonl

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
    order_by: since
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
