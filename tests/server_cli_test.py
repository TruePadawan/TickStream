"""Exercises the executable's arguments, startup reporting, and graceful shutdown."""
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

    def test_bad_arguments(self):
        """Invalid configuration exits nonzero with a standard-error diagnostic."""
        for args in (["--port", "-1"], ["--port", "65536"], ["--port", "1x"],
                     ["--address", "bad"], ["--max-clients", "0"],
                     ["--send-buffer-bytes", "99999999999999999999"],
                     ["--unknown", "1"], ["--port"]):
            with self.subTest(args=args):
                result = subprocess.run([EXECUTABLE, *args], capture_output=True, text=True, timeout=3)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("tickstream_server:", result.stderr)

    def test_startup_and_sigterm(self):
        """An ephemeral listener accepts connections and stops cleanly on SIGTERM."""
        with subprocess.Popen([EXECUTABLE, "--port", "0"], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as process:
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
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
