from tests.fixtures.e07_startup_faults import reject_startup_record


def test_malformed_receipt_retains_owned_cleanup():
    reject_startup_record('malformed')
