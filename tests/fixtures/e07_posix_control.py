from tests.fixtures.e07_child_bypass import test_posix_spawn_missing_guard as run_control


def test_reject_posix_bypass(tmp_path):
    run_control(tmp_path)
