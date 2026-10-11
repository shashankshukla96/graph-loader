"""F1: a launch-phase ``docker stop`` must not lose its single SIGTERM.

``docker stop`` sends SIGTERM once and SIGKILLs after the grace period. A container started
moments earlier can lose that SIGTERM (its init has not yet blocked signals, so the kernel
discards it) and then runs on until the SIGKILL: exit 137 after the whole timeout (P0 shut03 bulk,
shut06 stream). The fakes below model exactly that window, deterministically, and drive the real
CLI stop path (``_stop_exact_containers``) and the launch rollback path.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src import cli
from src.orchestrator import docker_service
from src.orchestrator.docker_service import DockerService, wait_until_stop_signal_ready

DOCKER_STOP_TIMEOUT = 10.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class InitRaceContainer:
    """A container whose init discards a SIGTERM arriving within ``blind_for`` seconds of its start.

    ``stop()`` is one Docker stop: one SIGTERM, then a SIGKILL after ``timeout`` if the process
    did not exit. A loader that receives the SIGTERM drains in ``drain_seconds`` and exits 0.
    """

    def __init__(self, cid: str, clock: FakeClock, *, blind_for: float = 0.05, drain_seconds: float = 0.3) -> None:
        self.id = cid
        self.name = f"graph-loader-node-{cid}"
        self._clock = clock
        self._started = clock.now
        self._blind_for = blind_for
        self._drain = drain_seconds
        self.stop_calls: list[float | None] = []
        self.exit_code: int | None = None
        self.attrs = {"State": {"Status": "running"}}

    def stop(self, timeout: float | None = None) -> None:
        self.stop_calls.append(timeout)
        grace = DOCKER_STOP_TIMEOUT if timeout is None else timeout
        if self._clock.now - self._started < self._blind_for:
            self._clock.now += grace          # SIGTERM discarded: runs on until the SIGKILL
            self.exit_code = 137
        else:
            self._clock.now += self._drain    # graceful drain
            self.exit_code = 0
        self.attrs = {"State": {"Status": "exited", "ExitCode": self.exit_code}}

    def reload(self) -> None:
        pass

    def remove(self, force: bool = False) -> None:
        pass


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(docker_service, "_monotonic", lambda: fake.now)
    monkeypatch.setattr(docker_service, "_sleep", fake.sleep)
    return fake


def _launch(clock: FakeClock, cid: str = "c0", **kwargs) -> InitRaceContainer:
    client = MagicMock()
    client.containers.run.side_effect = lambda **_kw: InitRaceContainer(cid, clock, **kwargs)
    service = DockerService(client=client)
    return service._run_container(image="graph-loader-node:latest", name=cid)


def test_reproduction_an_immediate_stop_loses_the_sigterm_and_ends_in_sigkill(clock, monkeypatch) -> None:
    """Without the hold (the old behaviour) the launch-phase stop ends in exit 137 after the full timeout."""
    monkeypatch.setattr(docker_service, "STOP_SIGNAL_READY_SECONDS", 0.0)
    container = _launch(clock)
    started = clock.now
    assert cli._stop_exact_containers([container], stop_timeout=10) == []
    assert container.exit_code == 137
    assert clock.now - started == pytest.approx(DOCKER_STOP_TIMEOUT)


def test_a_launch_phase_stop_is_held_until_the_container_can_take_its_sigterm(clock) -> None:
    container = _launch(clock)
    clock.now += 0.2  # the CLI asks for the stop 0.2 s after the launch returned
    started = clock.now
    assert cli._stop_exact_containers([container], stop_timeout=10) == []
    assert container.exit_code == 0
    assert clock.slept == [pytest.approx(0.8)]
    assert container.stop_calls == [10]  # still exactly one docker stop (one SIGTERM)
    assert clock.now - started == pytest.approx(0.8 + 0.3)  # well inside the drain budget


def test_steady_state_stops_are_unchanged(clock) -> None:
    """A container that has been up longer than the window (every bulk label-readiness stop and
    every steady-state stream stop) is stopped at once with the same single docker stop."""
    container = _launch(clock)
    clock.now += 30.0
    assert cli._stop_exact_containers([container]) == []
    assert clock.slept == []
    assert container.stop_calls == [None]
    assert container.exit_code == 0


def test_containers_this_process_did_not_launch_are_never_held(clock) -> None:
    foreign = InitRaceContainer("not-ours", clock, blind_for=0.0)
    assert wait_until_stop_signal_ready(foreign) == 0.0
    cli._stop_exact_containers([foreign])
    assert clock.slept == [] and foreign.stop_calls == [None]


def test_only_real_container_ids_are_recorded(clock) -> None:
    mock = MagicMock()  # unit-test doubles: their .id is not a string
    assert docker_service._record_launch(mock) is mock
    assert wait_until_stop_signal_ready(mock) == 0.0
    assert docker_service._launched_at == {}


def test_launch_records_expire(clock) -> None:
    _launch(clock, "old")
    clock.now += docker_service._LAUNCH_RECORD_TTL_SECONDS + 1
    _launch(clock, "new")
    assert set(docker_service._launched_at) == {"new"}


def test_launch_rollback_stops_are_held_too(clock) -> None:
    """A sibling launch failure rolls back the replicas that did start: those stops are launch-phase stops."""
    container = _launch(clock)
    docker_service._stop_quietly(container)
    assert container.exit_code == 0
    assert clock.slept == [pytest.approx(docker_service.STOP_SIGNAL_READY_SECONDS)]


def test_every_launcher_records_its_containers_without_changing_the_docker_call(clock, tmp_path) -> None:
    client = MagicMock()
    launched = []

    def run(**kwargs):
        launched.append(kwargs)
        return InitRaceContainer(f"id-{len(launched)}", clock)

    client.containers.run.side_effect = run
    service = DockerService(client=client)
    containers = service.run_node_loader(
        node_label="Person", topic="person", mode="bulk", config_path="config.yaml",
        rejection_dir=str(tmp_path), replicas=2,
    )
    assert client.containers.run.call_count == 2
    assert all(kwargs["init"] is True and kwargs["detach"] is True for kwargs in launched)
    assert set(docker_service._launched_at) == {container.id for container in containers}


# -- deterministic readiness: ``docker top`` shows the init's child ---------------------------------

TITLES = ["UID", "PID", "PPID", "C", "STIME", "TTY", "TIME", "CMD"]
INIT_ONLY = {"Titles": TITLES, "Processes": [["root", "10", "1", "0", "12:00", "?", "00:00:00",
                                              "/sbin/docker-init -- python -m src.loader.node_loader"]]}
INIT_AND_CHILD = {"Titles": TITLES, "Processes": INIT_ONLY["Processes"] + [
    ["root", "11", "10", "0", "12:00", "?", "00:00:00", "python -m src.loader.node_loader"]]}
NO_INIT = {"Titles": TITLES, "Processes": [["root", "10", "1", "0", "12:00", "?", "00:00:00", "python -m x"]]}


class ListedContainer(InitRaceContainer):
    """A container whose ``top()`` returns the given listings in turn (the last one repeats)."""

    def __init__(self, cid, clock, listings, **kwargs) -> None:
        super().__init__(cid, clock, **kwargs)
        self._listings = list(listings)
        self.top_calls = 0

    def top(self):
        self.top_calls += 1
        listing = self._listings[min(self.top_calls, len(self._listings)) - 1]
        if isinstance(listing, Exception):
            raise listing
        return listing


def _launch_listed(clock: FakeClock, listings, cid: str = "c0") -> ListedContainer:
    container = ListedContainer(cid, clock, listings)
    docker_service._record_launch(container)
    return container


def test_a_visible_child_after_the_floor_releases_the_stop_at_the_floor(clock) -> None:
    container = _launch_listed(clock, [INIT_AND_CHILD])
    assert wait_until_stop_signal_ready(container) == pytest.approx(1.0)
    assert container.top_calls == 1


def test_the_stop_waits_for_the_init_to_fork_its_child(clock) -> None:
    """Init only (signals not yet forwarded) for two polls, then the child appears."""
    container = _launch_listed(clock, [INIT_ONLY, INIT_ONLY, INIT_AND_CHILD])
    held = wait_until_stop_signal_ready(container)
    assert container.top_calls == 3
    assert held == pytest.approx(1.0 + 2 * docker_service._STOP_READY_POLL_SECONDS)


def test_the_hold_is_capped(clock) -> None:
    container = _launch_listed(clock, [INIT_ONLY])
    held = wait_until_stop_signal_ready(container)
    assert held == pytest.approx(docker_service.STOP_SIGNAL_READY_MAX_SECONDS)
    cli._stop_exact_containers([container], stop_timeout=10)
    assert container.stop_calls == [10]  # a stop is always issued after the hold


def test_a_failing_top_falls_back_to_the_floor(clock) -> None:
    container = _launch_listed(clock, [RuntimeError("top not supported")])
    assert wait_until_stop_signal_ready(container) == pytest.approx(1.0)
    assert container.top_calls == 1


def test_a_container_without_an_init_is_ready_once_its_process_is_listed(clock) -> None:
    container = _launch_listed(clock, [NO_INIT])
    assert wait_until_stop_signal_ready(container) == pytest.approx(1.0)


def test_readiness_is_remembered_and_old_containers_need_no_api_call(clock) -> None:
    container = _launch_listed(clock, [INIT_AND_CHILD])
    wait_until_stop_signal_ready(container)
    assert wait_until_stop_signal_ready(container) == 0.0  # floor passed, already seen ready
    old = _launch_listed(clock, [INIT_ONLY], cid="old")
    clock.now += docker_service.STOP_SIGNAL_READY_MAX_SECONDS
    assert wait_until_stop_signal_ready(old) == 0.0
    assert old.top_calls == 0 and container.top_calls == 1


def test_containers_not_launched_here_are_never_listed(clock) -> None:
    foreign = ListedContainer("foreign", clock, [INIT_ONLY])
    assert wait_until_stop_signal_ready(foreign) == 0.0
    assert foreign.top_calls == 0
