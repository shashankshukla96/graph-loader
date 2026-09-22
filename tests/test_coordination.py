import pytest
import json
from types import SimpleNamespace

from src.orchestrator.coordination import ClockHealthError, ClockLease, ClockProtocolError, ClockPublishError, GlobalBatchClock, decode_clock_lease, encode_clock_lease
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.rotation import build_rotation_plan
from src.models.schema import CoordinationConfig


def _lease(**changes):
    data = dict(run_id="run", epoch=1, slot_id=0, issued_at_ms=10, expires_at_ms=20, active_edge_types=("BOUGHT", "WORKS_AT"), bucket_owners={0: "BOUGHT", 1: "WORKS_AT"})
    data.update(changes); return ClockLease(**data)


def test_clock_lease_round_trip_is_canonical_and_immutable():
    encoded = encode_clock_lease(_lease())
    assert encoded == encode_clock_lease(_lease())
    lease = decode_clock_lease(encoded, expected_run_id="run", now_ms=19)
    assert lease and dict(lease.bucket_owners) == {0: "BOUGHT", 1: "WORKS_AT"}
    with pytest.raises(TypeError): lease.bucket_owners[0] = "BOUGHT"
    assert encoded == b'{"active_edge_types":["BOUGHT","WORKS_AT"],"bucket_owners":{"0":"BOUGHT","1":"WORKS_AT"},"epoch":1,"expires_at_ms":20,"issued_at_ms":10,"run_id":"run","shared_bucket_owners":{},"slot_id":0}'


def test_direct_lease_normalizes_mutable_active_types():
    source = ["BOUGHT", "WORKS_AT"]
    lease = _lease(active_edge_types=source)
    source.append("OTHER")
    assert lease.active_edge_types == ("BOUGHT", "WORKS_AT")


def test_direct_lease_rejects_invalid_container_types():
    with pytest.raises(ClockProtocolError): _lease(active_edge_types="BOUGHT")
    with pytest.raises(ClockProtocolError): _lease(bucket_owners=[(0, "BOUGHT")])


def test_foreign_malformed_and_expired_leases_fail_closed():
    assert decode_clock_lease(encode_clock_lease(_lease(run_id="other")), expected_run_id="run", now_ms=11) is None
    with pytest.raises(ClockProtocolError): decode_clock_lease(b"bad", expected_run_id="run", now_ms=1)
    with pytest.raises(ClockProtocolError): decode_clock_lease(encode_clock_lease(_lease()), expected_run_id="run", now_ms=20)
    with pytest.raises(ClockProtocolError): decode_clock_lease(b'{"run_id":"other","run_id":"other"}', expected_run_id="run", now_ms=1)


@pytest.mark.parametrize("change", [{"bucket_owners": {1: "BOUGHT"}}, {"active_edge_types": ("WORKS_AT", "BOUGHT")}, {"epoch": True}])
def test_invalid_lease_cannot_encode(change):
    with pytest.raises(ClockProtocolError): encode_clock_lease(_lease(**change))


@pytest.mark.parametrize("payload", [
    b'{"run_id":"run","epoch":1,"slot_id":0,"issued_at_ms":1,"expires_at_ms":2,"active_edge_types":["BAD-TYPE"],"bucket_owners":{"0":"BAD-TYPE"}}',
    b'{"run_id":"run","epoch":1,"slot_id":0,"issued_at_ms":1,"expires_at_ms":2,"active_edge_types":["A"],"bucket_owners":{"00":"A"}}',
])
def test_decode_rejects_unsafe_fields(payload):
    with pytest.raises(ClockProtocolError): decode_clock_lease(payload, expected_run_id="run", now_ms=1)


@pytest.mark.parametrize("payload", [
    b'{"run_id":"run","epoch":1,"slot_id":0,"issued_at_ms":1,"expires_at_ms":2,"active_edge_types":["A"],"bucket_owners":{}}',
    b'{"run_id":"run","epoch":1,"slot_id":0,"issued_at_ms":1,"expires_at_ms":2,"active_edge_types":["A"],"bucket_owners":{"x":"A"}}',
    b'{"run_id":"run","epoch":1,"slot_id":0,"issued_at_ms":1,"expires_at_ms":2,"active_edge_types":["A"],"bucket_owners":{"0":"B"}}',
])
def test_decode_rejects_bad_bucket_ownership(payload):
    with pytest.raises(ClockProtocolError): decode_clock_lease(payload, expected_run_id="run", now_ms=1)


@pytest.mark.parametrize("run,now", [("", 1), ("run", True), ("run", -1)])
def test_decode_rejects_bad_expected_context(run, now):
    with pytest.raises(ClockProtocolError): decode_clock_lease(encode_clock_lease(_lease()), expected_run_id=run, now_ms=now)


@pytest.mark.parametrize("change", [{"issued_at_ms": True}, {"expires_at_ms": True}, {"issued_at_ms": -1}, {"expires_at_ms": -1}, {"issued_at_ms": 20, "expires_at_ms": 20}])
def test_encode_rejects_unsafe_timing(change):
    with pytest.raises(ClockProtocolError): encode_clock_lease(_lease(**change))


def test_decode_error_does_not_echo_payload():
    sentinel = "secret-clock-payload"
    with pytest.raises(ClockProtocolError) as error:
        decode_clock_lease(("{bad:" + sentinel).encode(), expected_run_id="run", now_ms=1)
    assert sentinel not in str(error.value)


def test_decode_rejects_unknown_protocol_fields() -> None:
    payload = json.loads(encode_clock_lease(_lease()).decode("utf-8"))
    payload["unexpected"] = "ignored-by-older-parser"
    with pytest.raises(ClockProtocolError, match="unexpected"):
        decode_clock_lease(json.dumps(payload).encode("utf-8"), expected_run_id="run", now_ms=11)


def _clock(*, now=lambda: 100, producer=None):
    producer = producer or SimpleNamespace(produce=lambda *_args, **kwargs: kwargs["on_delivery"](None, object()), flush=lambda: 0)
    edge = SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(is_self_referencing=False))
    return GlobalBatchClock(run_id="run", edges=[edge], config=CoordinationConfig(bucket_count=2, slot_duration_ms=10, lease_timeout_ms=20), producer=producer, health_probe=lambda: None, wall_clock_ms=now, sleeper=lambda _seconds: None)


def test_global_clock_publishes_initial_idempotently_and_advances():
    clock = _clock()
    initial = clock.publish_initial()
    assert clock.publish_initial() is initial
    advanced = clock.advance()
    assert (advanced.epoch, advanced.slot_id) == (1, 1)
    assert dict(initial.bucket_owners) == {0: "WORKS_AT", 1: "WORKS_AT"}


def test_clock_rejects_preinitial_and_unacknowledged_publish():
    with pytest.raises(ClockPublishError, match="stage=clock run_id=run.*cannot advance"):
        _clock().advance()
    producer = SimpleNamespace(produce=lambda *_args, **_kwargs: None, flush=lambda: 0)
    with pytest.raises(ClockPublishError): _clock(producer=producer).publish_initial()


@pytest.mark.parametrize("flush", [1, None, True])
def test_clock_rejects_nonzero_or_noninteger_flush(flush):
    producer = SimpleNamespace(produce=lambda *_args, **kwargs: kwargs["on_delivery"](None, object()), flush=lambda: flush)
    with pytest.raises(ClockPublishError): _clock(producer=producer).publish_initial()


def test_clock_rejects_regressing_wall_time_without_replacing_lease():
    times = iter([100, 90])
    clock = _clock(now=lambda: next(times))
    initial = clock.publish_initial()
    with pytest.raises(ClockPublishError): clock.advance()
    assert clock._lease is initial


def test_clock_run_returns_one_for_invalid_wall_clock():
    clock = _clock(now=lambda: True)
    assert clock.run(SimpleNamespace(is_set=lambda: False)) == 1


def _rotation_plan():
    edges = [
        SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False)),
        SimpleNamespace(type="BOUGHT", nodes=SimpleNamespace(source="Person", target="Product", is_self_referencing=False)),
    ]
    return edges, build_rotation_plan(build_conflict_families(edges), bucket_count=2)


def test_current_run_decode_requires_exact_label_scoped_rotation_plan():
    _edges, plan = _rotation_plan()
    lease = _lease(shared_bucket_owners=plan.owners_for_epoch(1))
    payload = encode_clock_lease(lease, rotation_plan=plan)
    assert decode_clock_lease(payload, expected_run_id="run", now_ms=11, rotation_plan=plan) == lease
    decoded = json.loads(payload)
    decoded["shared_bucket_owners"] = {}
    with pytest.raises(ClockProtocolError, match="does not match rotation"):
        decode_clock_lease(json.dumps(decoded).encode(), expected_run_id="run", now_ms=11, rotation_plan=plan)
    decoded["shared_bucket_owners"] = {"Person": {"0": "BOUGHT", "1": "BOUGHT"}}
    with pytest.raises(ClockProtocolError, match="does not match rotation"):
        decode_clock_lease(json.dumps(decoded).encode(), expected_run_id="run", now_ms=11, rotation_plan=plan)


def test_expected_plan_rejects_active_owner_from_another_family_and_filters_valid_foreign():
    edges = [
        SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False)),
        SimpleNamespace(type="BOUGHT", nodes=SimpleNamespace(source="Person", target="Product", is_self_referencing=False)),
        SimpleNamespace(type="SELLS", nodes=SimpleNamespace(source="Vendor", target="Store", is_self_referencing=False)),
        SimpleNamespace(type="SUPPLIES", nodes=SimpleNamespace(source="Vendor", target="Warehouse", is_self_referencing=False)),
    ]
    plan = build_rotation_plan(build_conflict_families(edges), bucket_count=2)
    lease = ClockLease("run", 0, 0, 10, 20, tuple(sorted(edge.type for edge in edges)), {0: "BOUGHT", 1: "BOUGHT"}, plan.owners_for_epoch(0))
    payload = json.loads(encode_clock_lease(lease, rotation_plan=plan))
    payload["shared_bucket_owners"]["Person"] = {"0": "SELLS", "1": "SELLS"}
    with pytest.raises(ClockProtocolError, match="does not match rotation"):
        decode_clock_lease(json.dumps(payload).encode(), expected_run_id="run", now_ms=11, rotation_plan=plan)
    foreign = ClockLease("other", 0, 0, 10, 20, tuple(sorted(edge.type for edge in edges)), {0: "BOUGHT", 1: "BOUGHT"}, {})
    assert decode_clock_lease(encode_clock_lease(foreign), expected_run_id="run", now_ms=11, rotation_plan=plan) is None


def test_global_clock_emits_exact_rotation_map():
    edges, plan = _rotation_plan()
    producer = SimpleNamespace(produce=lambda *_args, **kwargs: kwargs["on_delivery"](None, object()), flush=lambda: 0)
    clock = GlobalBatchClock(run_id="run", edges=edges, config=CoordinationConfig(bucket_count=2, slot_duration_ms=10, lease_timeout_ms=20), producer=producer, health_probe=lambda: None, wall_clock_ms=lambda: 10, sleeper=lambda _seconds: None, rotation_plan=plan)
    assert clock.publish_initial().shared_bucket_owners == plan.owners_for_epoch(0)


@pytest.mark.parametrize("failure", ["health", "produce", "callback", "flush", "expiry", "allocation"])
def test_clock_failures_are_attributed_and_preserve_prior_lease(failure):
    times = iter([10, 11] if failure != "expiry" else [10, 30])
    producer = SimpleNamespace(
        produce=lambda *_args, **kwargs: kwargs["on_delivery"](None, object()), flush=lambda: 0,
    )
    clock = _clock(now=lambda: next(times), producer=producer)
    initial = clock.publish_initial()
    if failure == "health":
        clock._health = lambda: (_ for _ in ()).throw(RuntimeError("unhealthy"))
    elif failure == "produce":
        clock._producer = SimpleNamespace(produce=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("down")), flush=lambda: 0)
    elif failure == "callback":
        clock._producer = SimpleNamespace(produce=lambda *_args, **kwargs: kwargs["on_delivery"](RuntimeError("delivery"), object()), flush=lambda: 0)
    elif failure == "flush":
        clock._producer = SimpleNamespace(produce=lambda *_args, **kwargs: kwargs["on_delivery"](None, object()), flush=lambda: 1)
    elif failure == "allocation":
        clock._rotation_plan = object()
    with pytest.raises((ClockPublishError, ClockHealthError), match=r"stage=clock run_id=run epoch=1 slot=1"):
        clock.advance()
    assert clock._lease is initial


def test_clock_rejects_rotation_plan_mismatch_before_publish():
    edges, plan = _rotation_plan()
    with pytest.raises(ClockPublishError, match="stage=clock run_id=run.*rotation plan"):
        GlobalBatchClock(
            run_id="run", edges=edges[:1], config=CoordinationConfig(bucket_count=2, slot_duration_ms=10, lease_timeout_ms=20),
            producer=SimpleNamespace(), health_probe=lambda: None, wall_clock_ms=lambda: 10,
            sleeper=lambda _seconds: None, rotation_plan=plan,
        )
    with pytest.raises(ClockPublishError, match="stage=clock run_id=run.*rotation plan"):
        GlobalBatchClock(
            run_id="run", edges=edges, config=CoordinationConfig(bucket_count=3, slot_duration_ms=10, lease_timeout_ms=20),
            producer=SimpleNamespace(), health_probe=lambda: None, wall_clock_ms=lambda: 10,
            sleeper=lambda _seconds: None, rotation_plan=plan,
        )
