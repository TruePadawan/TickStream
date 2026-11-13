# TickStream

A single-threaded C++ market-data pub/sub server for Linux. It generates simulated
prices and routes binary updates to subscribed TCP clients using non-blocking
sockets and level-triggered `epoll`.

```text
Price generator → ticker subscriptions → per-client buffers → TCP clients
```

Full delivery preserves queued updates and disconnects clients on overflow.
Conflation keeps the latest unsent update per ticker, bounding pending tick state
while allowing slow consumers to stay connected. Prices use integers; per-ticker
sequence numbers let clients detect gaps.

## Build and run

Requires Linux (or WSL2), a C++20 compiler, CMake 4.3+, Make or Ninja, and Python
3.10+. Initial test configuration downloads GoogleTest and requires Git and internet.

```sh
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON -DTICKSTREAM_BUILD_TESTS=ON
cmake --build build/release
./build/release/tickstream_server
```

In another terminal:

```sh
python3 tools/client.py --tickers AAPL MSFT --count 10
```

The server defaults to `127.0.0.1:9000`; Ctrl+C stops it. Use `--help` for options.
For conflation, start the server with `--mode conflated` and add `--allow-gaps` to
the client. For debugging, configure `build/debug` with `-DCMAKE_BUILD_TYPE=Debug`.

## Tests

```sh
ctest --test-dir build/release --output-on-failure --no-tests=error
```

GoogleTest and Python tests cover framing, partial I/O, subscription cleanup,
conflation, slow clients, and benchmark failures. GCC/Clang sanitizer builds are
available with `-DTICKSTREAM_ENABLE_SANITIZERS=ON` when their runtimes are installed.

## Measured results

Full delivery on an Intel Core i7-10750H Linux host, with eight Python reader
workers. Medians across three 15-second trials after five-second warm-ups:

| Clients | Generated ticks/sec | Decoded client updates/sec | Pooled p99 latency |
| ---: | ---: | ---: | ---: |
| 100 | 1,000 | 100,000 | 1.704 ms |
| 10 | 10,000 | 99,999 | 1.049 ms |

Healthy full-delivery clients had zero sequence gaps. With one reader paused for
ten seconds, the other 99 maintained about 99,000 updates/sec in both modes.
Higher full-delivery loads overflowed. Latency includes Python decoding and host
scheduling; conflation intentionally omits intermediate updates.

Reproduce the 100-client workload with a Release build:

```sh
python3 bench/run.py --server build/release/tickstream_server \
  --clients 100 --workers 8 --rate 1000 --mode full --seed 42 \
  --send-buffer-bytes 4096 --warmup 5 --duration 15 --repeats 3
```

Results are saved locally in ignored `bench/results/`. See the
[design and benchmark notes](docs/design.md) for methodology and limitations, and
the [protocol specification](docs/protocol.md) for wire layouts.
