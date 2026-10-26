from tests.fixtures.e07_child_bypass import test_multiprocessing_spawn_missing_guard as run_control


def test_reject_mp_bypass(tmp_path):
    run_control(tmp_path)
