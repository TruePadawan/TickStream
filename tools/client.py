"""Small synchronous inspection client; throughput measurement belongs in bench/."""
import argparse
import socket
import sys
import time

import protocol as p


class Client:
    """Owns a TCP socket and retains bytes left over after decoding one frame."""

    def __init__(self, host, port, timeout=5.0):
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.timeout = timeout
        self.buffer = bytearray()

    def close(self):
        """Releases the connection, including after protocol or timeout errors."""
        self.socket.close()

    def send(self, message):
        """Sends an entire encoded frame, handling short writes through sendall."""
        self.socket.sendall(p.encode(message))

    def receive(self):
        """Returns one frame with a finite deadline, retaining any following bytes."""
        deadline = time.monotonic() + self.timeout
        while True:
            result = p.decode(self.buffer)
            if result.status == p.DecodeStatus.OK:
                del self.buffer[:result.consumed]
                if isinstance(result.message, (p.Subscribe, p.Unsubscribe, p.Start)):
                    raise ValueError("Server sent a client-only message")
                return result.message
            if result.status == p.DecodeStatus.ERROR:
                raise ValueError(f"Invalid server frame: {result.error}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for a complete frame")
            self.socket.settimeout(remaining)
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Server disconnected" + (" during a frame" if self.buffer else ""))
            self.buffer.extend(chunk)


def price_text(value):
    """Displays fixed-point prices without converting them to floating point."""
    return f"{value // p.PRICE_SCALE}.{value % p.PRICE_SCALE:04d}"


def main():
    """Subscribes, optionally starts generation, and checks a finite tick sample."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--tickers", nargs="+", choices=list(p.TICKERS.values()), default=["AAPL"])
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--allow-gaps", action="store_true",
                        help="Permit increasing sequence gaps from conflation; still reject duplicates/reordering")
    parser.add_argument("--start", action="store_true", help="Trigger a --manual-start server once")
    args = parser.parse_args()
    if args.count <= 0 or not 0 < args.timeout < float("inf"):
        parser.error("count and timeout must be positive and finite")
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    requested = {ticker for ticker, symbol in p.TICKERS.items() if symbol in args.tickers}
    client = None
    try:
        client = Client(args.host, args.port, args.timeout)
        for ticker in sorted(requested):
            client.send(p.Subscribe(ticker))
        waiting = set(requested)
        sequences = {}
        received = 0
        sequence_gaps = 0
        while waiting or received < args.count:
            message = client.receive()
            if isinstance(message, p.Ack):
                if message.request_type != p.MsgType.SUBSCRIBE or message.ticker_id not in waiting:
                    raise ValueError("Unexpected acknowledgement")
                waiting.remove(message.ticker_id)
                if not waiting and args.start:
                    client.send(p.Start())
            elif isinstance(message, p.Tick):
                if message.ticker_id not in requested:
                    raise ValueError("Received an unsubscribed ticker")
                previous = sequences.get(message.ticker_id)
                if previous is not None:
                    if message.seq <= previous:
                        raise ValueError(f"Duplicate or decreasing sequence for {p.TICKERS[message.ticker_id]}")
                    gap = message.seq - previous - 1
                    if gap and not args.allow_gaps:
                        raise ValueError(f"Sequence gap for {p.TICKERS[message.ticker_id]}")
                    sequence_gaps += gap
                sequences[message.ticker_id] = message.seq
                received += 1
                print(f"{p.TICKERS[message.ticker_id]} seq={message.seq} "
                      f"bid={price_text(message.bid)} ask={price_text(message.ask)} "
                      f"last={price_text(message.last)}")
        if args.allow_gaps:
            print(f"Sequence gaps observed: {sequence_gaps} (permitted)", file=sys.stderr)
    except (OSError, ValueError, ConnectionError) as error:
        print(f"client: {error}", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
