"""Short end-to-end runs exercise benchmark results, failure reporting, and cleanup."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

EXECUTABLE = sys.argv.pop(1)
ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "bench" / "run.py"


class BenchmarkTests(unittest.TestCase):
    """Uses disposable JSON paths so verification never becomes a published baseline."""

    def invoke(self, folder, *extra, server=EXECUTABLE):
        """Run one short workload and return both process status and saved evidence."""
        output = Path(folder) / "result.json"
        command = [sys.executable, str(RUNNER), "--server", server, "--clients", "2", "--warmup", ".1",
                   "--duration", ".5", "--sample-interval", ".05", "--stats-interval-ms", "20",
                   "--startup-timeout", "2", "--shutdown-timeout", "2", "--output", str(output), *extra]
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        self.assertTrue(output.exists(), result.stderr)
        return result, json.loads(output.read_text())

    def assert_cleaned_up(self, report):
        """Every spawned server must have exited, including after startup/validation failure."""
        for run in report["runs"]:
            self.assertFalse(Path(f"/proc/{run['server_pid']}").exists())
            for pid in run.get("worker_pids", []):
                self.assertFalse(Path(f"/proc/{pid}").exists())

    def test_healthy_repeated_full_delivery(self):
        """Two repetitions produce nonzero decoded rates and separate measured server intervals."""
        with tempfile.TemporaryDirectory() as folder:
            result, report = self.invoke(folder, "--repeats", "2")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr + str([r.get("errors") for r in report.get("runs", [])]))
            self.assertEqual(report["status"], "complete")
            self.assertEqual(len(report["runs"]), 2)
            for run in report["runs"]:
                self.assertGreater(run["groups"]["healthy"]["decoded_ticks"], 0)
                self.assertEqual(run["groups"]["healthy"]["sequence_gaps"], 0)
                self.assertGreater(run["server_measurement_interval"]["generated_ticks_per_second"], 0)
                self.assertEqual(run["server_lifetime_final"]["kind"], "final")
                self.assertEqual(sum(c["decoded_ticks"] for c in run["clients"]), run["groups"]["healthy"]["decoded_ticks"])
            self.assert_cleaned_up(report)

    def test_slow_consumer_in_both_modes(self):
        """A real read pause triggers overflow or sampled kernel backpressure, with healthy delivery continuing."""
        for mode in ("full", "conflated"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder:
                result, report = self.invoke(folder, "--clients", "3", "--rate", "5000", "--duration", ".8",
                                             "--mode", mode, "--slow-client", "--slow-pause", ".45",
                                             "--send-buffer-bytes", "4096", "--queue-capacity", "128", "--workers", "2")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr + str([r.get("errors") for r in report.get("runs", [])]))
                run = report["runs"][0]
                self.assertEqual(run["groups"]["healthy"]["clients"], 2)
                self.assertGreater(run["groups"]["healthy"]["decoded_ticks"], 0)
                slow = run["clients"][-1]
                self.assertIsNotNone(slow["pause_start_ns"])
                self.assertGreaterEqual(slow["pause_start_ns"], run["window"]["measurement_start_ns"])
                self.assertEqual(run["scenario_status"], "observed")
                if mode == "full":
                    self.assertIsNotNone(slow["disconnect_ns"])
                    self.assertGreater(run["server_lifetime_final"]["tick_queue_overflows"], 0)
                else:
                    self.assertIsNone(slow["disconnect_ns"])
                    self.assertGreater(run["server_lifetime_final"]["conflation_replacements"], 0)
                    self.assertGreater(run["groups"]["slow"]["sequence_gaps"], 0)
                self.assert_cleaned_up(report)

    def test_multiple_client_workers(self):
        """Worker processes preserve per-client histograms and exit after each repetition."""
        with tempfile.TemporaryDirectory() as folder:
            result, report = self.invoke(folder, "--clients", "4", "--workers", "2", "--repeats", "2")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr + str([r.get("errors") for r in report["runs"]]))
            for run in report["runs"]:
                self.assertEqual(len(run["worker_pids"]), 2)
                self.assertEqual(len(run["worker_resources"]), 2)
                self.assertGreater(run["groups"]["healthy"]["decoded_ticks"], 0)
                self.assertEqual(run["groups"]["healthy"]["sequence_gaps"], 0)
                self.assertIsNotNone(run["harness_total_resources"])
            self.assert_cleaned_up(report)

    def test_startup_disconnect_is_not_a_successful_zero_run(self):
        """Exceeding the server connection limit records an incomplete setup and null throughput."""
        with tempfile.TemporaryDirectory() as folder:
            result, report = self.invoke(folder, "--max-clients", "1")
            self.assertNotEqual(result.returncode, 0)
            self.assertNotEqual(report["status"], "complete")
            run = report["runs"][0]
            self.assertTrue(run["errors"])
            self.assertIsNone(run["groups"]["healthy"]["deliveries_per_second"])
            self.assert_cleaned_up(report)

    def test_missing_executable_and_existing_result(self):
        """Failed startup is saved, and rerunning cannot overwrite previously saved evidence."""
        with tempfile.TemporaryDirectory() as folder:
            result, report = self.invoke(folder, server=str(Path(folder) / "missing"))
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(report["status"], "failed")
            original = (Path(folder) / "result.json").read_bytes()
            result = subprocess.run([sys.executable, str(RUNNER), "--server", EXECUTABLE,
                                     "--output", str(Path(folder) / "result.json")], capture_output=True, timeout=3)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((Path(folder) / "result.json").read_bytes(), original)

    def test_startup_timeout_cleans_up_server(self):
        """A process that never reports readiness is terminated and produces a failed result."""
        with tempfile.TemporaryDirectory() as folder:
            fake = Path(folder) / "stalled-server"
            fake.write_text("#!/usr/bin/env python3\nimport sys, time\n"
                            "if '--build-info' in sys.argv:\n"
                            "    print('{\"build_type\":\"Release\",\"sanitizers\":false}')\n"
                            "else:\n    time.sleep(10)\n")
            fake.chmod(0o755)
            result, report = self.invoke(folder, "--startup-timeout", "1", server=str(fake))
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(any("TimeoutError" in error for error in report["runs"][0]["errors"]))
            self.assert_cleaned_up(report)

    def test_early_server_exit_preserves_failure(self):
        """A peer that acknowledges setup then exits during measurement cannot produce valid rates."""
        with tempfile.TemporaryDirectory() as folder:
            fake = Path(folder) / "early-server"
            source = '''\
                #!/usr/bin/env python3
                import socket, sys
                if '--build-info' in sys.argv:
                    print('{"build_type":"Release","sanitizers":false}')
                    sys.exit(0)
                listener = socket.socket()
                listener.bind(('127.0.0.1', 0))
                listener.listen()
                print('Listening on 127.0.0.1:' + str(listener.getsockname()[1]), flush=True)
                peer, _ = listener.accept()
                data = bytearray()
                while True:
                    chunk = peer.recv(4096)
                    if not chunk: sys.exit(6)
                    data.extend(chunk)
                    while len(data) >= 4:
                        length = int.from_bytes(data[2:4], 'big')
                        if len(data) < 4 + length: break
                        frame = bytes(data[:4 + length])
                        del data[:4 + length]
                        if frame[1] == 5: sys.exit(7)
                        peer.sendall(bytes.fromhex('0104000501') + frame[4:8])
            '''
            fake.write_text(textwrap.dedent(source))
            fake.chmod(0o755)
            result, report = self.invoke(folder, "--clients", "1", server=str(fake))
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(report["runs"][0]["server_returncode"], 7)
            self.assertTrue(report["runs"][0]["errors"])
            self.assertIsNone(report["runs"][0]["groups"]["healthy"]["deliveries_per_second"])
            self.assert_cleaned_up(report)


if __name__ == "__main__":
    unittest.main()
