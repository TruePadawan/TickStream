"""Checks measurement math independently of operating-system scheduling."""
import asyncio
import socket
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))
from metrics import Histogram, cpu_summary, process_sample, server_interval
from run import wait_readable


class HistogramTests(unittest.TestCase):
    """Checks precision, bounded storage, and pooling without averaging percentiles."""

    def test_bucket_upper_bounds_and_storage(self):
        """Every bucket bounds its sample with the documented relative precision."""
        for value in (0, 1, 2, 15, 16, 17, 31, 32, 1000, 1_000_000, 2**64 - 1):
            histogram = Histogram()
            histogram.record(value)
            index = next(i for i, count in enumerate(histogram.counts) if count)
            if index:
                exponent, slot = divmod(index - 1, 16)
                upper = (1 << exponent) + (slot + 1) * (1 << max(0, exponent - 4)) - 1
                self.assertGreaterEqual(upper, value)
                self.assertLessEqual(upper - value, max(0, value // 16))
            self.assertEqual(histogram.percentile(1), value)
            self.assertEqual(len(histogram.counts), 1025)
        for invalid in (-1, 2**64):
            with self.assertRaises(ValueError):
                Histogram().record(invalid)

    def test_pooling_and_tail_sample_threshold(self):
        """Sparse samples omit p99.9; merged counts retain the actual distribution."""
        small, large = Histogram(), Histogram()
        for _ in range(99):
            small.record(100)
        large.record(10000)
        small.merge(large)
        self.assertEqual(small.count, 100)
        self.assertLess(small.percentile(.99), 10000)
        self.assertEqual(small.percentile(1), 10000)
        self.assertIsNone(small.summary()["p999_ns_upper_bound"])
        for _ in range(9900):
            small.record(100)
        self.assertIsNotNone(small.summary()["p999_ns_upper_bound"])
        self.assertEqual(sum(value for _, value in small.summary()["buckets"]), 10000)
        self.assertIsNone(Histogram().percentile(.99))


class IntervalTests(unittest.TestCase):
    """Keeps server rates and process resources tied to their actual time intervals."""

    def test_server_rates_exclude_lifetime_and_warmup_counts(self):
        """Endpoint deltas use telemetry time, not the requested client duration."""
        keys = ("scheduled_ticks", "generated_ticks", "accepted_tick_offers", "skipped_ticks",
                "tick_frames_written", "conflation_replacements", "tick_queue_overflows",
                "output_overflows", "protocol_errors", "blocked_writes")
        snapshots = []
        for ns, generated in ((0, 9000), (2_000_000_000, 10000), (4_000_000_000, 10200), (9_000_000_000, 99999)):
            value = dict.fromkeys(keys, 0)
            value.update(timestamp_ns=ns, generated_ticks=generated, pending_ticks=4, pending_bytes=192)
            snapshots.append(value)
        result = server_interval(snapshots, 1_000_000_000, 5_000_000_000)
        self.assertEqual(result["generated_ticks_per_second"], 100)
        self.assertEqual(result["duration_seconds"], 2)
        self.assertIsNone(server_interval(snapshots, 2_000_000_000, 3_000_000_000))
        snapshots[2]["generated_ticks"] = 1
        with self.assertRaises(ValueError):
            server_interval(snapshots, 1_000_000_000, 5_000_000_000)

    def test_cpu_and_memory_use_only_measurement_samples(self):
        """CPU percentages use one core as 100%, and RSS is reported in bytes."""
        samples = [{"timestamp_ns": ns, "server": {"cpu_seconds": cpu, "rss_bytes": rss}}
                   for ns, cpu, rss in ((0, 0, 99999), (1_000_000_000, 10, 1000), (3_000_000_000, 11, 2000))]
        result = cpu_summary(samples, "server", 1_000_000_000, 3_000_000_000)
        self.assertEqual(result["cpu_percent_one_core"], 50)
        self.assertEqual(result["rss_max_bytes"], 2000)
        current = process_sample(os.getpid())
        self.assertGreaterEqual(current["cpu_seconds"], 0)
        self.assertGreater(current["rss_bytes"], 0)


class ReadinessTests(unittest.IsolatedAsyncioTestCase):
    """Read deadlines may cancel notification waits but must never consume stream bytes."""

    async def test_timeout_and_cancellation_preserve_bytes(self):
        """Readiness callbacks leave data in the socket across a pause boundary."""
        writer, reader = socket.socketpair()
        writer.setblocking(False)
        reader.setblocking(False)
        try:
            await wait_readable(reader, .001)
            writer.sendall(b"before-pause")
            await wait_readable(reader, .1)
            self.assertEqual(reader.recv(100), b"before-pause")
            pending = asyncio.create_task(wait_readable(reader, 10))
            await asyncio.sleep(0)
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            writer.sendall(b"after-pause")
            await wait_readable(reader, .1)
            self.assertEqual(reader.recv(100), b"after-pause")
        finally:
            writer.close()
            reader.close()


if __name__ == "__main__":
    unittest.main()
