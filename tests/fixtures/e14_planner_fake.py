"""Picklable fakes for planner-worker process tests: a consumer that never delivers a record, so a scan runs until stopped."""
import time


class BlockingConsumer:
    def assign(self, partitions):
        pass

    def consume(self, num_messages=1, timeout=1.0):
        time.sleep(min(timeout, 0.2))
        return []

    def pause(self, partitions):
        pass

    def close(self):
        pass


class BlockingConsumerFactory:
    def __call__(self, config):
        return BlockingConsumer()
