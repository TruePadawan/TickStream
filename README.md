# TickStream

TickStream is a C++ market-data pub/sub project for Linux. The planned server
generates simulated prices and routes updates to subscribed clients using
non-blocking TCP and epoll, with bounded buffering and optional conflation.

The current checkpoint provides the build setup, a tested C++ binary protocol
codec, and matching Python helpers. Networking, interactive clients, and benchmarks
are not implemented yet. See [the protocol specification](docs/protocol.md) for
message layouts and semantics.

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
