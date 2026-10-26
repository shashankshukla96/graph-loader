"""Keep the real detached-child control active until external hard supervision."""
import time

from tests.fixtures.e07_detached import test_detached_owned_child as _detached_control


def test_timeout_reclaims_the_registered_detached_child():
    _detached_control()
    time.sleep(60)
