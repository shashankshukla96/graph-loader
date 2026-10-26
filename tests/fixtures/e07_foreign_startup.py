from tests.fixtures.e07_startup_faults import reject_startup_record


def test_foreign_startup_prefix_is_rejected():
    reject_startup_record('wrong-prefix')
