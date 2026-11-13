"""Exercises the executable's arguments, startup reporting, and graceful shutdown."""
import json
import tempfile
import pathlib
import selectors
import socket
import subprocess
import sys
import unittest

EXECUTABLE = sys.argv.pop(1)


class ServerCliTests(unittest.TestCase):
    """Checks the real process boundary in addition to in-process socket tests."""

    def test_help(self):
        """Help works without opening a listener and describes available options."""
        result = subprocess.run([EXECUTABLE, "--help"], capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--port", result.stdout)

    def test_build_info(self):
        """Build provenance is machine-readable and available without starting the server."""
        result = subprocess.run([EXECUTABLE, "--build-info"], capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        info = json.loads(result.stdout)
        self.assertEqual(info["cxx_standard"], 20)
        self.assertIsInstance(info["sanitizers"], bool)
        self.assertTrue(info["compiler"])

    def test_bad_arguments(self):
        """Invalid configuration exits nonzero with a standard-error diagnostic."""
        for args in (["--port", "-1"], ["--port", "65536"], ["--port", "1x"],
                     ["--address", "bad"], ["--max-clients", "0"],
                     ["--send-buffer-bytes", "99999999999999999999"],
                     ["--unknown", "1"], ["--port"], ["--rate", "0"],
                     ["--rate", "1000001"], ["--queue-capacity", "0"], ["--mode", "invalid"], ["--stats-interval-ms", "0"], ["--stats-interval-ms", "10001"]):
            with self.subTest(args=args):
                result = subprocess.run([EXECUTABLE, *args], capture_output=True, text=True, timeout=3)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("tickstream_server:", result.stderr)

    def test_python_client_subscribes_and_receives_ticks(self):
        """The shipped client coordinates start, decodes prices, and checks sequences."""
        client_script = pathlib.Path(__file__).resolve().parents[1] / "tools" / "client.py"
        for mode, rate, client_options in (("full", 200, []), ("conflated", 10000, ["--allow-gaps"])):
            with self.subTest(mode=mode):
                with subprocess.Popen([EXECUTABLE, "--port", "0", "--manual-start", "--rate", str(rate), "--mode", mode],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
                    try:
                        with selectors.DefaultSelector() as ready:
                            ready.register(process.stdout, selectors.EVENT_READ)
                            self.assertTrue(ready.select(timeout=3), "No startup message")
                        port = int(process.stdout.readline().strip().rsplit(":", 1)[1])
                        result = subprocess.run([sys.executable, str(client_script), "--port", str(port),
                                                 "--tickers", "AAPL", "MSFT", "--start", "--count", "12", *client_options],
                                                capture_output=True, text=True, timeout=5)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        if mode == "conflated":
                            self.assertIn("Sequence gaps observed:", result.stderr)
                        lines = result.stdout.splitlines()
                        self.assertEqual(len(lines), 12)
                        self.assertEqual({line.split()[0] for line in lines}, {"AAPL", "MSFT"})
                        process.terminate()
                        _, stderr = process.communicate(timeout=3)
                        self.assertEqual(process.returncode, 0, stderr)
                    finally:
                        if process.poll() is None:
                            process.kill()
                            process.communicate(timeout=3)

    def test_startup_and_sigterm(self):
        """An ephemeral listener accepts connections and stops cleanly on SIGTERM."""
        with tempfile.TemporaryDirectory() as folder, subprocess.Popen(
                [EXECUTABLE, "--port", "0", "--stats-file", str(pathlib.Path(folder) / "stats.jsonl")],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(process.stdout, selectors.EVENT_READ)
                    self.assertTrue(ready.select(timeout=3), "No startup message")
                line = process.stdout.readline()
                self.assertTrue(line.startswith("Listening on 127.0.0.1:"), line)
                port = int(line.strip().rsplit(":", 1)[1])
                with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
                    client.sendall(bytes.fromhex("01ff0000"))
                    self.assertEqual(client.recv(1), b"")
                process.terminate()
                stdout, stderr = process.communicate(timeout=3)
                self.assertEqual(process.returncode, 0, stderr)
                self.assertIn("accepted=1", stdout)
                self.assertIn("protocol_errors=1", stdout)
                snapshots = [json.loads(line) for line in (pathlib.Path(folder) / "stats.jsonl").read_text().splitlines()]
                self.assertEqual(snapshots[0]["kind"], "initial")
                self.assertEqual(snapshots[-1]["kind"], "final")
                self.assertEqual(snapshots[-1]["protocol_errors"], 1)
                self.assertGreaterEqual(snapshots[-1]["timestamp_ns"], snapshots[0]["timestamp_ns"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
