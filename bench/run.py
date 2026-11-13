"""Run a same-machine TickStream workload and save reproducible JSON evidence."""
import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import protocol as p
from metrics import Histogram, clock_ns, cpu_summary, process_sample, server_interval


@dataclass
class ClientState:
    """Owns one raw socket; no background StreamReader can hide a slow client's backlog."""
    index: int
    slow: bool
    sock: socket.socket
    histogram: Histogram = field(default_factory=Histogram)
    previous: dict = field(default_factory=dict)
    buffer: bytearray = field(default_factory=bytearray)
    ticks_total: int = 0
    gaps: int = 0
    disconnect_ns: int | None = None
    error: str | None = None
    pause_start_ns: int | None = None
    pause_end_ns: int | None = None
    receive_buffer_bytes: int = 0
    incomplete_tail_bytes: int = 0


async def connect_client(client, args):
    """Connect and collect every subscription Ack before any Start is sent."""
    loop = asyncio.get_running_loop()
    await loop.sock_connect(client.sock, ("127.0.0.1", args.port))
    client.receive_buffer_bytes = client.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    waiting = set(args.tickers)
    await loop.sock_sendall(client.sock, b"".join(p.encode(p.Subscribe(t)) for t in sorted(waiting)))
    while waiting:
        result = p.decode(client.buffer)
        if result.status == p.DecodeStatus.ERROR:
            raise ValueError(f"Invalid handshake frame: {result.error}")
        if result.status == p.DecodeStatus.OK:
            message = result.message
            if not isinstance(message, p.Ack) or message.request_type != p.MsgType.SUBSCRIBE or message.ticker_id not in waiting:
                raise ValueError("Unexpected subscription acknowledgement")
            waiting.remove(message.ticker_id)
            del client.buffer[:result.consumed]
        else:
            chunk = await loop.sock_recv(client.sock, 4096)
            if not chunk:
                raise ConnectionError("Disconnected before subscriptions were acknowledged")
            client.buffer.extend(chunk)
    if client.buffer:
        raise ValueError("Unexpected data before coordinated start")


async def wait_readable(sock, timeout):
    """Wait for readiness without consuming bytes, making timeout/cancellation races lossless."""
    loop = asyncio.get_running_loop()
    ready = loop.create_future()

    def notify():
        if not ready.done():
            ready.set_result(None)

    loop.add_reader(sock.fileno(), notify)
    try:
        await asyncio.wait_for(ready, max(0, timeout))
    except asyncio.TimeoutError:
        pass
    finally:
        loop.remove_reader(sock.fileno())


async def read_client(client, args, start_ns, end_ns, abort):
    """Decode bounded batches, pausing actual socket reads for the designated slow peer."""
    offset = 0
    processed = 0
    pause_until = min(end_ns, start_ns + int(args.slow_pause * 1e9))
    try:
        while True:
            now = clock_ns()
            if now >= end_ns:
                return
            if client.slow and start_ns <= now < pause_until:
                client.pause_start_ns = now
                await asyncio.sleep((pause_until - now) / 1e9)
                client.pause_end_ns = clock_ns()
                continue
            result = p.decode(memoryview(client.buffer)[offset:])
            if result.status == p.DecodeStatus.ERROR:
                raise ValueError(f"Malformed frame: {result.error}")
            if result.status == p.DecodeStatus.NEED_MORE_DATA:
                if offset:
                    del client.buffer[:offset]
                    offset = 0
                # Read synchronously once ready. Cancelling sock_recv at a boundary could lose
                # bytes already consumed by its callback but not yet returned to this task.
                boundary = start_ns if client.slow and now < start_ns else end_ns
                try:
                    chunk = client.sock.recv(65536)
                except BlockingIOError:
                    await wait_readable(client.sock, (boundary - clock_ns()) / 1e9)
                    continue
                if not chunk:
                    client.disconnect_ns = clock_ns()
                    expected_overflow = client.slow and args.mode == "full" and client.pause_start_ns is not None
                    if client.buffer:
                        client.incomplete_tail_bytes = len(client.buffer)
                        if not expected_overflow:
                            raise ConnectionError("Disconnected during a frame")
                    if not expected_overflow:
                        raise ConnectionError("Unexpected client disconnect")
                    return
                client.buffer.extend(chunk)
                continue
            message = result.message
            now = clock_ns()
            if now >= end_ns or (client.slow and start_ns <= now < pause_until):
                continue
            if not isinstance(message, p.Tick) or message.ticker_id not in args.tickers:
                raise ValueError("Unexpected message or unsubscribed ticker")
            last = client.previous.get(message.ticker_id)
            if last is not None:
                if message.seq <= last:
                    raise ValueError("Duplicate or decreasing sequence")
                gap = message.seq - last - 1
                if gap and args.mode == "full":
                    raise ValueError("Sequence gap in full-delivery mode")
                if start_ns <= now < end_ns:
                    client.gaps += gap
            client.previous[message.ticker_id] = message.seq
            client.ticks_total += 1
            if start_ns <= now < end_ns:
                client.histogram.record(now - message.timestamp_ns)
            offset += result.consumed
            processed += 1
            if processed % 256 == 0:
                await asyncio.sleep(0)  # Prevent a busy client from starving peers and measurement timers.
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, ConnectionError) as error:
        client.error = str(error)
        abort.set()


async def monitor(process, samples, abort, interval):
    """Sample server/harness CPU and RSS and record scheduler delay, without unbounded queues."""
    next_ns = clock_ns()
    while True:
        now = clock_ns()
        if process.returncode is not None:
            abort.set()
            return
        try:
            samples.append({"timestamp_ns": now, "scheduler_lag_ns": max(0, now - next_ns),
                            "server": process_sample(process.pid), "harness": process_sample(os.getpid())})
        except (OSError, ValueError, IndexError):
            abort.set()
            return
        next_ns = now + int(interval * 1e9)
        await asyncio.sleep(interval)


def client_summary(client, duration, complete):
    """Keep per-client counts and histograms so pooling cannot conceal unhealthy peers."""
    return {"id": client.index, "role": "slow" if client.slow else "healthy",
            "ticks_including_warmup": client.ticks_total, "decoded_ticks": client.histogram.count,
            "deliveries_per_second": client.histogram.count / duration if complete else None,
            "sequence_gaps": client.gaps, "disconnect_ns": client.disconnect_ns,
            "error": client.error, "incomplete_tail_bytes": client.incomplete_tail_bytes, "pause_start_ns": client.pause_start_ns,
            "pause_end_ns": client.pause_end_ns, "receive_buffer_bytes": client.receive_buffer_bytes,
            "latency": client.histogram.summary()}


async def run_once(args, repetition, telemetry_path):
    """Own the subprocess and all clients, preserving failed-run evidence and guaranteed cleanup."""
    result = {"repetition": repetition, "status": "failed", "errors": [], "warnings": []}
    clients, readers, samples, connectors = [], [], [], []
    process = None
    sampler = None
    start_ns = end_ns = 0
    abort = asyncio.Event()
    started_line = ""
    command = [args.server, "--port", "0", "--manual-start", "--mode", args.mode,
               "--rate", str(args.rate), "--seed", str(args.seed), "--max-clients", str(args.max_clients),
               "--queue-capacity", str(args.queue_capacity), "--output-capacity", str(args.output_capacity),
               "--send-buffer-bytes", str(args.send_buffer_bytes), "--stats-file", str(telemetry_path),
               "--stats-interval-ms", str(args.stats_interval_ms)]
    result["server_command"] = command
    try:
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                                                      stderr=asyncio.subprocess.PIPE)
        result["server_pid"] = process.pid
        started_line = (await asyncio.wait_for(process.stdout.readline(), args.startup_timeout)).decode().strip()
        if not started_line.startswith("Listening on 127.0.0.1:"):
            raise RuntimeError(f"Server did not report readiness: {started_line!r}")
        # Each repetition gets a fresh ephemeral listener; do not race a separately selected free port.
        args.port = int(started_line.rsplit(":", 1)[1])
        for index in range(args.clients):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            slow = args.slow_client and index == args.clients - 1
            client = ClientState(index, slow, sock)
            clients.append(client)
            capacity = args.slow_receive_buffer_bytes if slow else args.receive_buffer_bytes
            if capacity:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, capacity)
        connectors = [asyncio.create_task(connect_client(c, args)) for c in clients]
        await asyncio.wait_for(asyncio.gather(*connectors), args.startup_timeout)
        start_sent_ns = clock_ns()
        start_ns = start_sent_ns + int(args.warmup * 1e9)
        end_ns = start_ns + int(args.duration * 1e9)
        result["window"] = {"start_sent_ns": start_sent_ns, "measurement_start_ns": start_ns,
                            "measurement_end_ns": end_ns, "duration_seconds": args.duration}
        readers = [asyncio.create_task(read_client(c, args, start_ns, end_ns, abort)) for c in clients]
        sampler = asyncio.create_task(monitor(process, samples, abort, args.sample_interval))
        await asyncio.get_running_loop().sock_sendall(clients[0].sock, p.encode(p.Start()))
        try:
            await asyncio.wait_for(abort.wait(), max(0, (end_ns - clock_ns()) / 1e9))
            raise RuntimeError("Client validation, sampling, or server process failed before measurement end")
        except asyncio.TimeoutError:
            result["status"] = "complete"
    except (OSError, ValueError, RuntimeError, asyncio.TimeoutError) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        # Stop observation before teardown; shutdown traffic never becomes a measured delivery.
        for task in readers + connectors:
            task.cancel()
        if sampler is not None:
            sampler.cancel()
        await asyncio.gather(*readers, *connectors, *([sampler] if sampler else []), return_exceptions=True)
        if process is not None:
            # EOF can precede process reaping; preserve an exiting server's own failure code.
            if result["status"] != "complete" and process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), .1)
                except asyncio.TimeoutError:
                    pass
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), args.shutdown_timeout)
            except asyncio.TimeoutError:
                process.kill()
                stdout, stderr = await process.communicate()
                result["errors"].append("Server required forced termination")
            result.update(server_returncode=process.returncode,
                          server_stdout=started_line + "\n" + stdout.decode(errors="replace"),
                          server_stderr=stderr.decode(errors="replace"))
            if process.returncode != 0:
                result["errors"].append(f"Server exited with code {process.returncode}")
        for client in clients:
            client.sock.close()

    telemetry_text = None
    try:
        telemetry_text = telemetry_path.read_text()
        snapshots = [json.loads(line) for line in telemetry_text.splitlines()]
        if not snapshots or snapshots[-1]["kind"] != "final":
            raise ValueError("Missing final server telemetry")
        result["server_lifetime_final"] = snapshots[-1]
        if snapshots[-1]["protocol_errors"] or snapshots[-1]["rejected"]:
            result["errors"].append("Server recorded protocol errors or rejected connections")
        if any(c.disconnect_ns is not None and c.slow for c in clients):
            overflows = snapshots[-1]["tick_queue_overflows"] + snapshots[-1]["output_overflows"]
            if not overflows:
                result["errors"].append("Slow-client disconnect has no corresponding server overflow")
        result["server_measurement_interval"] = server_interval(snapshots, start_ns, end_ns)
        if result["server_measurement_interval"] is None:
            result["errors"].append("Not enough server telemetry snapshots inside the measurement window")
        result["server_snapshots"] = snapshots
    except (OSError, ValueError, KeyError) as error:
        result["errors"].append(f"Telemetry: {error}")
        result["invalid_telemetry_text"] = telemetry_text
    result["process_samples"] = samples
    if start_ns:
        result["server_resources"] = cpu_summary(samples, "server", start_ns, end_ns)
        result["harness_resources"] = cpu_summary(samples, "harness", start_ns, end_ns)
        if result["server_resources"] is None or result["harness_resources"] is None:
            result["errors"].append("Not enough process samples inside the measurement window")
    for client in clients:
        if client.error:
            result["errors"].append(f"Client {client.index}: {client.error}")
        if not client.slow and client.histogram.count == 0:
            result["errors"].append(f"Healthy client {client.index} received no measured ticks")
    if result["errors"] and result["status"] == "complete":
        result["status"] = "incomplete"
    complete = result["status"] == "complete" and not result["errors"]
    result["clients"] = [client_summary(c, args.duration, complete) for c in clients]
    groups = {}
    for role in ("healthy", "slow"):
        histogram = Histogram()
        selected = [c for c in clients if c.slow == (role == "slow")]
        for client in selected:
            histogram.merge(client.histogram)
        groups[role] = {"clients": len(selected), "decoded_ticks": histogram.count,
                        "deliveries_per_second": histogram.count / args.duration if complete else None,
                        "sequence_gaps": sum(c.gaps for c in selected), "latency": histogram.summary()}
    result["groups"] = groups
    harness = result.get("harness_resources")
    if harness and harness["cpu_percent_one_core"] >= 85:
        result["warnings"].append("Python harness used >=85% of one core; investigate client-side saturation")
    if samples:
        result["max_sampler_lag_ns"] = max(s["scheduler_lag_ns"] for s in samples)
    if result.get("server_lifetime_final", {}).get("skipped_ticks", 0):
        result["warnings"].append("Generation skipped scheduled opportunities; target rate was not fully achieved")
    if args.slow_client:
        final = result.get("server_lifetime_final", {})
        result["server_backpressure_observed"] = bool(final.get("tick_queue_overflows") or final.get("output_overflows")
            or final.get("blocked_writes") or any(s.get("nonwritable_clients_with_output")
                for s in result.get("server_snapshots", []) if start_ns <= s["timestamp_ns"] <= end_ns))
        result["scenario_status"] = "observed" if result["server_backpressure_observed"] else "inconclusive"
        if not result["server_backpressure_observed"]:
            result["warnings"].append("Slow-consumer experiment inconclusive: no explicit backpressure/overflow observed")
    return result


def metadata(server, working_tree_status):
    """Record machine, executable, build commands, and working-tree provenance before timing."""
    def git(*arguments):
        completed = subprocess.run(["git", *arguments], cwd=ROOT, capture_output=True, text=True, timeout=5)
        return completed.stdout.strip() if completed.returncode == 0 else None
    cpuinfo = Path("/proc/cpuinfo").read_text()
    model = next((line.split(":", 1)[1].strip() for line in cpuinfo.splitlines() if line.startswith("model name")), "unknown")
    memtotal = next(line for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemTotal:"))
    cache = Path(server).parent / "compile_commands.json"
    commands = json.loads(cache.read_text()) if cache.exists() else None
    return {"utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
            "kernel": platform.release(), "cpu_model": model, "logical_cpus": os.cpu_count(),
            "cpu_affinity": sorted(os.sched_getaffinity(0)), "mem_total": memtotal,
            "python": sys.version, "clock": "Linux CLOCK_MONOTONIC nanoseconds", "git_revision": git("rev-parse", "HEAD"),
            "working_tree_status": working_tree_status,
            "executable_sha256": hashlib.sha256(Path(server).read_bytes()).hexdigest(),
            "compile_commands": commands}


def arguments():
    """Keep workload limits explicit and reject configurations that cannot be measured."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--max-clients", type=int, default=1024)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--mode", choices=("full", "conflated"), default="full")
    parser.add_argument("--tickers", type=int, nargs="+", choices=list(p.TICKERS), default=list(p.TICKERS))
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--duration", type=float, default=15)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--slow-client", action="store_true")
    parser.add_argument("--slow-pause", type=float, default=10)
    parser.add_argument("--send-buffer-bytes", type=int, default=0)
    parser.add_argument("--receive-buffer-bytes", type=int, default=0)
    parser.add_argument("--slow-receive-buffer-bytes", type=int, default=4096)
    parser.add_argument("--queue-capacity", type=int, default=256)
    parser.add_argument("--output-capacity", type=int, default=65536)
    parser.add_argument("--stats-interval-ms", type=int, default=50)
    parser.add_argument("--sample-interval", type=float, default=.1)
    parser.add_argument("--startup-timeout", type=float, default=10)
    parser.add_argument("--shutdown-timeout", type=float, default=5)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    import math
    for name in ("warmup", "duration", "slow_pause", "sample_interval", "startup_timeout", "shutdown_timeout"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name != "warmup" and value == 0):
            parser.error(f"{name} must be finite and {'nonnegative' if name == 'warmup' else 'positive'}")
    if not 1 <= args.clients <= 1024 or args.max_clients <= 0 or args.repeats <= 0:
        parser.error("clients must be 1..1024; max-clients and repeats must be positive")
    if not 1 <= args.rate <= 1000000 or not 0 <= args.seed < 2**64:
        parser.error("rate must be 1..1000000 and seed must fit uint64")
    if args.queue_capacity <= 0 or args.output_capacity < p.MAX_FRAME_SIZE:
        parser.error("queue-capacity must be positive and output-capacity must be at least 48")
    if not 1 <= args.stats_interval_ms <= 10000 or args.duration < 3 * max(args.sample_interval, args.stats_interval_ms / 1000):
        parser.error("duration must allow at least three process/telemetry sampling intervals")
    for name in ("send_buffer_bytes", "receive_buffer_bytes", "slow_receive_buffer_bytes"):
        if not 0 <= getattr(args, name) <= 2**31 - 1:
            parser.error(f"{name} must fit a nonnegative int32")
    if args.slow_client and (args.clients < 2 or args.slow_pause >= args.duration):
        parser.error("slow-client needs at least two clients and slow-pause shorter than duration")
    args.server = str(Path(args.server).resolve())
    args.tickers = sorted(set(args.tickers))
    return args


def main():
    """Save successes and failures as JSON; never overwrite prior benchmark evidence."""
    args = arguments()
    try:
        source_status = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                                       capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        source_status = None
    path = Path(args.output) if args.output else ROOT / "bench" / "results" / (
        datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S.%fZ.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the result path before running so existing results cannot be silently replaced.
    try:
        output = path.open("x")
    except OSError as error:
        print(f"benchmark: {error}", file=sys.stderr)
        return 1
    report = {"schema_version": 1, "status": "failed", "configuration": vars(args).copy(), "runs": []}
    report["measurement"] = {"throughput": "tick frames decoded during the client measurement window",
                             "latency": "generation to Python decode completion, including client scheduling",
                             "percentiles": "bucket upper bounds; <=6.25% bucket width above 16ns",
                             "pooling": "healthy and slow clients separated; per-client histograms retained",
                             "server_rates": "actual inner telemetry interval, reported separately from client window",
                             "queue_high_water_scope": "server process lifetime including warmup",
                             "p999_minimum_samples": 10000}
    try:
        report["metadata"] = metadata(args.server, source_status)
        info = subprocess.run([args.server, "--build-info"], capture_output=True, text=True, timeout=args.startup_timeout)
        if info.returncode:
            raise RuntimeError(f"Build-info query failed: {info.stderr.strip()}")
        report["metadata"]["build_info"] = json.loads(info.stdout)
        if report["metadata"]["build_info"].get("build_type") != "Release" or report["metadata"]["build_info"].get("sanitizers"):
            report["warnings"] = ["Use an unsanitized Release build for publishable performance measurements"]
        with tempfile.TemporaryDirectory(prefix="tickstream-bench-") as temp:
            for repetition in range(1, args.repeats + 1):
                run = asyncio.run(run_once(args, repetition, Path(temp) / f"server-{repetition}.jsonl"))
                report["runs"].append(run)
                print(f"Run {repetition}: {run['status']}, healthy decoded={run['groups']['healthy']['decoded_ticks']}")
                if run["status"] != "complete":
                    break
        report["status"] = "complete" if len(report["runs"]) == args.repeats and all(
            run["status"] == "complete" for run in report["runs"]) else "incomplete"
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        with output:
            json.dump(report, output, indent=2, allow_nan=False)
            output.write("\n")
    print(f"Results: {path}")
    return 0 if report["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
