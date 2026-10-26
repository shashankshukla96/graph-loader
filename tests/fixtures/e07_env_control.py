from tests.fixtures.e07_child_bypass import test_env_wrapper_missing_guard as run_control


def test_reject_env_bypass(tmp_path):
    run_control(tmp_path)
