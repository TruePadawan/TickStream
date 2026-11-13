# TickStream

TickStream is a C++ market-data pub/sub project for Linux. The server
generates simulated prices and routes updates to subscribed clients using
non-blocking TCP and epoll, with bounded full-delivery and conflated buffering.

The current checkpoint provides subscription routing, seeded price generation,
both delivery policies, and a Python inspection client. Benchmarks come next. See
[the protocol specification](docs/protocol.md) and
[transport design](docs/transport.md).

## Requirements

- Linux (or Linux under WSL2)
- A C++20-compatible compiler, such as GCC or Clang
- CMake 4.3 or newer
- Make or Ninja
- Git and internet access for the initial GoogleTest download
- Python 3.10+ for protocol tests and the planned client and benchmark scripts

GoogleTest is fetched automatically; no separate installation is required.
CLion and GDB are optional development tools.

## Build and run

Run from the repository root. Configure, build, and run Debug:

```sh
cmake -S . -B build/debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DTICKSTREAM_BUILD_TESTS=ON
cmake --build build/debug
./build/debug/tickstream_server
```

For Release:

```sh
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DTICKSTREAM_BUILD_TESTS=ON
cmake --build build/release
./build/release/tickstream_server
```

Use Debug for development and Release for future performance measurements.
CLion can manage equivalent profiles in `cmake-build-debug/` and
`cmake-build-release/`; substitute those paths when using its builds.

The server listens on `127.0.0.1:9000` by default and stops on Ctrl+C or SIGTERM.
Use `--help` for options, or choose a port and connection limit:

```sh
./build/debug/tickstream_server --port 9001 --max-clients 100
```

`--port 0` requests an available port, printed at startup. Invalid frames and
client-sent Tick/Ack messages close that connection.

## Subscribe to prices

With the server running, open another terminal:

```sh
python3 tools/client.py --tickers AAPL MSFT --count 10
```

The client waits for acknowledgements, prints prices, and checks per-ticker sequence
continuity after the first received tick. It exits after the requested sample or
reports a timeout/protocol error on standard error. It is an inspection tool,
not a throughput benchmark.

For coordinated generation, run the server with manual start:

```sh
./build/debug/tickstream_server --manual-start --rate 1000 --seed 42 --queue-capacity 256
```

Then connect a client that sends the one-shot start request after its acknowledgements:

```sh
python3 tools/client.py --tickers AAPL MSFT --start --count 10
```

Only one client should use `--start`. Without `--manual-start`, generation begins
automatically. Duplicate subscriptions/unsubscriptions are acknowledged without
duplicating routing entries; disconnects remove all the client's subscriptions.

`--rate` is the aggregate requested ticks/sec across four round-robin symbols
(1–1,000,000, default 1,000). It is a target, not a throughput guarantee. Generation
uses 1 ms batches, at most 256 ticks per event-loop pass; overdue work beyond that
cap is skipped and counted. A fixed seed reproduces prices for equal generation
counts, not timestamps or real-time scheduling. Prices use integer units of 0.0001.

In full mode, `--queue-capacity` limits pending tick frames per client (default 256), including
any partially transmitted tick. `--output-capacity` separately limits unsent bytes
(default 65,536), including acknowledgements. Reaching either limit disconnects
only that client; full delivery has no silent tick replacement. Queue-container
overhead and kernel buffers are additional memory. Unsubscribe leaves earlier
queued updates intact. Shutdown closes connections without promising a final drain.

The shutdown summary distinguishes generated ticks, scheduled opportunities,
skipped work, accepted tick offers, replacements, and tick frames fully written to sockets.
Accepted offers include updates replacing an existing pending value; they do not
measure received throughput. The summary calls this counter `accepted_tick_offers`.
Socket writes do not prove client consumption. Queue high-water marks describe
the maximum for an individual client; pending bytes/ticks are totals across current
connections. Latency and throughput benchmarking are still pending.

## Delivery modes

`--mode full` is the default: retain every queued tick in order and disconnect a
client when its tick or byte limit fills. `--mode conflated` replaces pending
updates for the same ticker with the latest value, keeping that ticker's queue
position so other tickers make progress. Acknowledgements remain distinct FIFO
messages. Once a tick's first byte is sent, the rest of that frame is immutable.

Conflated output holds at most one unstarted tick per catalog entry plus one
partially transmitted tick: at most five tick frames for this four-symbol catalog.
The full-mode tick limit does not apply in conflated mode; `--output-capacity`
continues to bound all unsent bytes, including control traffic. An explicitly small
byte limit can still disconnect a client. Metadata and kernel buffers remain
additional memory; this is not a claim of constant process RSS.

Try conflation in two terminals:

```sh
./build/debug/tickstream_server --mode conflated --manual-start --rate 10000
```

```sh
python3 tools/client.py --tickers AAPL MSFT --start --allow-gaps --count 20
```

Conflation intentionally creates sequence gaps. `--allow-gaps` reports those gaps
while still rejecting duplicates and decreasing sequences. Without this option,
the inspection client requires contiguous sequences after its first tick.

Replacements can happen even for healthy clients when several updates arrive in
one generation batch. Data already accepted by the kernel cannot be replaced, so
a slow client can still read older prices before reaching newer pending values.
Always compare replacements and decoded deliveries alongside any future latency
results. See [conflation details](docs/conflation.md).

## Tests

```sh
ctest --test-dir build/debug --output-on-failure --no-tests=error
ctest --test-dir build/release --output-on-failure --no-tests=error
```

CTest runs the C++ GoogleTest suite and Python standard-library tests. Both codecs
are checked against the same golden byte fixtures, including incomplete frames,
invalid inputs, concatenated frames, and integer boundaries. Tests require both
`BUILD_TESTING` and `TICKSTREAM_BUILD_TESTS` to be enabled. Set either to `OFF` for a
server-only build that does not fetch GoogleTest or require Python.

Transport tests require permission to create loopback sockets and configure socket
buffers. They cover fragmented and combined requests, half-close, reconnects,
connection limits, malformed peers, output overflow, and real socket backpressure.
Market tests check subscription cleanup, deterministic prices, routing, sequence
continuity, start coordination, generation bounds, and tick queue overflow. CLI
tests check arguments, shutdown, and the shipped Python client end to end.
Conflation tests verify ticker independence, FIFO controls, bounded pending state,
replacement at capacity, immutable partial frames, and isolation of an unread peer
in both delivery modes.

To run just the Python codec tests:

```sh
python3 tests/protocol_python_test.py
```

## Sanitizers

With GCC or Clang and its AddressSanitizer/UndefinedBehaviorSanitizer runtime and
development libraries installed, configure a separate Debug build to detect memory
errors and undefined behavior:

```sh
cmake -S . -B build/sanitize -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DTICKSTREAM_BUILD_TESTS=ON -DTICKSTREAM_ENABLE_SANITIZERS=ON
cmake --build build/sanitize
ctest --test-dir build/sanitize --output-on-failure --no-tests=error
./build/sanitize/tickstream_server
```

Sanitizers instrument project code and are disabled by default. Do not use the
sanitized build for benchmarks.
