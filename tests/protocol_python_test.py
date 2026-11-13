import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))
import protocol as p

MESSAGES = {
    "subscribe": p.Subscribe(1), "unsubscribe": p.Unsubscribe(4),
    "tick": p.Tick(0x0102030405060708, 1, 100, 102, 101, 0x0a0b0c0d0e0f1011),
    "ack_subscribe": p.Ack(p.MsgType.SUBSCRIBE, 1),
    "ack_unsubscribe": p.Ack(p.MsgType.UNSUBSCRIBE, 4), "start": p.Start(),
}


class ProtocolTests(unittest.TestCase):
    """Verifies Python codec compatibility and safe incremental parsing."""

    def test_shared_golden_frames(self):
        """Checks the same exact byte fixtures used by the C++ tests."""
        fixtures = pathlib.Path(__file__).parent / "fixtures" / "protocol_v1.txt"
        seen = set()
        for line in fixtures.read_text().splitlines():
            name, hex_data = line.split()
            with self.subTest(name=name):
                frame = bytes.fromhex(hex_data)
                self.assertEqual(p.encode(MESSAGES[name]), frame)
                result = p.decode(frame)
                self.assertEqual(result, p.DecodeResult(p.DecodeStatus.OK, len(frame), MESSAGES[name]))
                seen.add(name)
        self.assertEqual(seen, set(MESSAGES))

    def test_every_truncated_prefix(self):
        """Ensures every incomplete prefix is retained without producing a message."""
        for name, message in MESSAGES.items():
            frame = p.encode(message)
            for size in range(len(frame)):
                with self.subTest(name=name, size=size):
                    self.assertEqual(p.decode(frame[:size]), p.DecodeResult(p.DecodeStatus.NEED_MORE_DATA))

    def test_concatenated_frames_and_partial_tail(self):
        """Checks frame-by-frame consumption and preservation of an unfinished tail."""
        messages = [p.Subscribe(1), p.Unsubscribe(2), p.Start()]
        data = bytearray(b"".join(map(p.encode, messages)) + b"\x01")
        for expected in messages:
            result = p.decode(data)
            self.assertEqual(result.status, p.DecodeStatus.OK)
            self.assertEqual(result.message, expected)
            del data[:result.consumed]
        self.assertEqual(p.decode(data).status, p.DecodeStatus.NEED_MORE_DATA)

    def assert_invalid(self, data, error):
        """Checks the shared error contract: no message and no consumed bytes."""
        self.assertEqual(p.decode(data), p.DecodeResult(p.DecodeStatus.ERROR, error=error))

    def test_invalid_headers(self):
        """Rejects bad headers before waiting for their claimed payloads."""
        self.assert_invalid(bytes.fromhex("02010004"), "invalid_version")
        self.assert_invalid(bytes.fromhex("01ff0004"), "unknown_type")
        for length in (0, 3, 5, 256, 65535):
            self.assert_invalid(b"\x01\x01" + length.to_bytes(2, "big"), "invalid_length")

    def test_invalid_payloads(self):
        """Checks malformed ticker IDs, acknowledgement types, sequences, and prices."""
        for ticker in (0, 5, 2**32 - 1):
            self.assert_invalid(b"\x01\x01\x00\x04" + ticker.to_bytes(4, "big"), "invalid_payload")
        self.assert_invalid(bytes.fromhex("010400050300000001"), "invalid_payload")
        frame = p.encode(MESSAGES["tick"])
        for offset, replacement in ((4, b"\0" * 8), (16, b"\x80"),
                                    (31, b"\x63"), (32, b"\x80")):
            data = bytearray(frame)
            data[offset:offset + len(replacement)] = replacement
            self.assert_invalid(data, "invalid_payload")

    def test_encode_validation(self):
        """Rejects invalid values and Python types before packing wire bytes."""
        invalid = [p.Subscribe(0), p.Subscribe(5), p.Subscribe(True), p.Subscribe(1.0),
                   p.Ack(p.MsgType.TICK, 1), p.Ack(True, 1), p.Ack(1.0, 1),
                   p.Tick(0, 1, 100, 102, 101, 0), p.Tick(1, 1, -1, 102, 101, 0),
                   p.Tick(1, 1, 103, 102, 101, 0), p.Tick(1, 1, 100, 102, -1, 0),
                   p.Tick(2**64, 1, 100, 102, 101, 0),
                   p.Tick(1, 1, 100, 2**63, 101, 0),
                   p.Tick(1, 1, 100, 102, 101, -1), object()]
        for message in invalid:
            with self.subTest(message=message), self.assertRaises(ValueError):
                p.encode(message)

    def test_integer_boundaries(self):
        """Checks maximum-width integers and valid zero values without overflow."""
        for tick in (p.Tick(2**64 - 1, 4, 0, 2**63 - 1, 2**63 - 1, 2**64 - 1),
                     p.Tick(1, 1, 0, 0, 0, 0)):
            self.assertEqual(p.decode(p.encode(tick)).message, tick)


if __name__ == "__main__":
    unittest.main()
