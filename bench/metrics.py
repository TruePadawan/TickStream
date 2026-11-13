"""Bounded latency histograms and Linux process sampling for the benchmark."""
import math
import os
from pathlib import Path
import time


def clock_ns():
    """Use the same explicit clock basis as C++ generation timestamps."""
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


class Histogram:
    """1025 fixed buckets: zero plus 16 subdivisions per power-of-two range."""

    def __init__(self):
        self.counts = [0] * 1025
        self.count = 0
        self.minimum = None
        self.maximum = None
        self.total = 0

    def record(self, value):
        """Record integer nanoseconds; bucket width is at most 6.25% above 16 ns."""
        if not isinstance(value, int) or not 0 <= value < 2**64:
            raise ValueError("Latency must fit unsigned 64-bit nanoseconds")
        if value == 0:
            index = 0
        else:
            exponent = value.bit_length() - 1
            width = 1 << max(0, exponent - 4)
            index = 1 + exponent * 16 + (value - (1 << exponent)) // width
        self.counts[index] += 1
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def merge(self, other):
        """Pool samples without averaging client percentiles or storing every latency."""
        if other.count == 0:
            return
        self.counts = [a + b for a, b in zip(self.counts, other.counts)]
        self.minimum = other.minimum if self.minimum is None else min(self.minimum, other.minimum)
        self.maximum = other.maximum if self.maximum is None else max(self.maximum, other.maximum)
        self.count += other.count
        self.total += other.total

    def percentile(self, fraction):
        """Return the selected bucket's inclusive upper bound, capped at observed max."""
        if not 0 < fraction <= 1:
            raise ValueError("Percentile fraction must be in (0, 1]")
        if not self.count:
            return None
        rank = math.ceil(fraction * self.count)
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= rank:
                if index == 0:
                    return 0
                exponent, slot = divmod(index - 1, 16)
                width = 1 << max(0, exponent - 4)
                return min(self.maximum, (1 << exponent) + (slot + 1) * width - 1)
        raise RuntimeError("Histogram counts are inconsistent")

    def summary(self):
        """Export statistics and sparse bucket counts so pooled results are reproducible."""
        return {"samples": self.count, "min_ns": self.minimum, "max_ns": self.maximum,
                "mean_ns": self.total / self.count if self.count else None,
                "p50_ns_upper_bound": self.percentile(.50),
                "p99_ns_upper_bound": self.percentile(.99),
                "p999_ns_upper_bound": self.percentile(.999) if self.count >= 10000 else None,
                "buckets": [[i, value] for i, value in enumerate(self.counts) if value]}


def process_sample(pid):
    """Read cumulative CPU seconds and resident bytes without a monitoring dependency."""
    stat = Path(f"/proc/{pid}/stat").read_text()
    # The process name in parentheses can contain spaces and parentheses itself.
    fields = stat[stat.rfind(")") + 2:].split()
    return {"cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
            "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE")}


def cpu_summary(samples, process, start_ns, end_ns):
    """Compute utilization from samples inside the measurement window, not warm-up."""
    selected = [s for s in samples if start_ns <= s["timestamp_ns"] <= end_ns and process in s]
    if len(selected) < 2:
        return None
    first, last = selected[0], selected[-1]
    elapsed = (last["timestamp_ns"] - first["timestamp_ns"]) / 1e9
    return {"interval_start_ns": first["timestamp_ns"], "interval_end_ns": last["timestamp_ns"],
            "cpu_percent_one_core": 100 * (last[process]["cpu_seconds"] - first[process]["cpu_seconds"]) / elapsed,
            "rss_min_bytes": min(s[process]["rss_bytes"] for s in selected),
            "rss_max_bytes": max(s[process]["rss_bytes"] for s in selected)}


def server_interval(snapshots, start_ns, end_ns):
    """Use actual telemetry endpoints; never divide lifetime counts by client window time."""
    selected = [s for s in snapshots if start_ns <= s["timestamp_ns"] <= end_ns]
    if len(selected) < 2:
        return None
    first, last = selected[0], selected[-1]
    seconds = (last["timestamp_ns"] - first["timestamp_ns"]) / 1e9
    counters = ("scheduled_ticks", "generated_ticks", "accepted_tick_offers", "skipped_ticks",
                "tick_frames_written", "conflation_replacements", "tick_queue_overflows",
                "output_overflows", "protocol_errors", "blocked_writes")
    delta = {key: last[key] - first[key] for key in counters}
    if seconds <= 0 or any(value < 0 for value in delta.values()):
        raise ValueError("Non-monotonic server telemetry")
    return {"start_ns": first["timestamp_ns"], "end_ns": last["timestamp_ns"],
            "duration_seconds": seconds, "counter_deltas": delta,
            "generated_ticks_per_second": delta["generated_ticks"] / seconds,
            "tick_frames_written_per_second": delta["tick_frames_written"] / seconds,
            "max_sampled_pending_ticks": max(s["pending_ticks"] for s in selected),
            "max_sampled_pending_bytes": max(s["pending_bytes"] for s in selected)}
