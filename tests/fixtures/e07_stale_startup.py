from tests.fixtures.e07_startup_faults import reject_startup_record


def test_stale_startup_receipt_is_rejected():
    reject_startup_record('stale')
