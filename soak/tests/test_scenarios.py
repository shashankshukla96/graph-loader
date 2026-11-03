"""Pure parts of the S2 scenarios: calibration arithmetic and the run-level red conditions."""
import json

import pytest

from soak import run_soak, scenarios


def test_slope_is_the_least_squares_trend_per_second():
    assert scenarios.slope([(0, 10), (10, 20), (20, 30)]) == pytest.approx(1.0)
    assert scenarios.slope([(0, 5), (60, 5), (120, 5)]) == 0.0
    assert scenarios.slope([(0, 1)]) == 0.0


def test_a_flat_lag_series_passes_and_a_growing_one_fails():
    flat = [(t, 40 + (t % 30)) for t in range(0, 600, 15)]
    assert scenarios.judge_flat(flat, records_per_s=100)['flat']
    growing = [(t, 5 * t) for t in range(0, 600, 15)]            # 5 records/s of backlog growth at 100 records/s offered
    verdict = scenarios.judge_flat(growing, records_per_s=100)
    assert not verdict['flat'] and verdict['slope_per_s'] == pytest.approx(5.0, rel=0.01)
    high_but_flat = [(t, 5000) for t in range(0, 600, 15)]       # a constant backlog above 10 s of traffic is not "caught up"
    assert not scenarios.judge_flat(high_but_flat, records_per_s=100)['flat']


def test_the_fault_plan_covers_every_fault_of_the_epic_in_order():
    class Faults:
        soak = type('S', (), {'replicas': {'PURCHASED': 2}})()

        def __getattr__(self, name):
            return lambda *a, **k: name
    names = [name for name, _ in scenarios.fault_plan(Faults())]
    assert names == ['neo4j-outage-30s', 'sigterm-during-neo4j-outage', 'kafka-broker-restart', 'kill-edge-loader', 'kill-retry-consumer',
                     'kill-node-loader', 'kill-lease-server', 'graceful-stop-replica-change', 'kill-9-cli']


class FakeSoak:
    def __init__(self, **kw):
        self.tripped, self.supervisor, self.peaks, self.fault_log, self.controls = None, None, {}, [], []
        self.__dict__.update(kw)
        self.querylog = type('Q', (), {'coverage': [], 'loader_waits': lambda s: []})()
        self.sampler = type('S', (), {'gaps': []})()

    def oom_events(self):
        return []


def test_a_container_above_twice_its_s1_peak_is_red(monkeypatch):
    monkeypatch.setattr(run_soak, 's1_peaks', lambda: {'edge-loader:PURCHASED': 80, 'neo4j': 100})
    assert run_soak.judge(FakeSoak(peaks={'edge-loader:PURCHASED': 150, 'neo4j': 900})) == []
    problems = run_soak.judge(FakeSoak(peaks={'edge-loader:PURCHASED': 161}))
    assert len(problems) == 1 and 'edge-loader:PURCHASED' in problems[0]


def test_run_level_red_conditions():
    soak = FakeSoak(tripped={'reason': 'host_memory'}, fault_log=[{'name': 'x', 'ok': False}], controls=[{'detected': False}])
    soak.supervisor = type('S', (), {'unexplained': [{'incarnation': 2}]})()
    soak.querylog = type('Q', (), {'coverage': ['gap'], 'loader_waits': lambda s: [{'waiting_ms': 5}]})()
    soak.sampler = type('S', (), {'gaps': [1]})()
    text = ' | '.join(run_soak.judge(soak))
    for needle in ('guard tripped', 'unexplained fleet exit', 'coverage failure', 'sampling gap', 'lock wait', 'fault(s) without', 'positive control'):
        assert needle in text


def test_a_new_window_is_the_window_of_the_records_the_generator_makes_next(tmp_path):
    from soak.generator import Generator
    from soak.journal import Journal
    from soak.runtime import Soak

    class Stub:
        window = 0
        journal = Journal(tmp_path / 'j.db')
    stub = Stub()
    stub.gen = Generator(stub.journal, 'a' * 32)
    Soak.new_window(stub, 'calm')
    stub.journal.set_meta('g_baseline_customers', 50)
    records = stub.gen.null_patch_cohort(4, 'calm-1')
    assert {r.window for r in records} == {1}
