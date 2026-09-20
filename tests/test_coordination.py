import pytest
import json
from types import SimpleNamespace

from src.orchestrator.coordination import ClockLease, ClockProtocolError, ClockPublishError, GlobalBatchClock, decode_clock_lease, encode_clock_lease
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
    assert encoded == b'{"active_edge_types":["BOUGHT","WORKS_AT"],"bucket_owners":{"0":"BOUGHT","1":"WORKS_AT"},"epoch":1,"expires_at_ms":20,"issued_at_ms":10,"run_id":"run","slot_id":0}'


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
    with pytest.raises(ClockPublishError): _clock().advance()
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
