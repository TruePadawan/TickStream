# Design and Benchmark Notes

## Architecture

`MarketServer` combines subscription routing and a seeded price generator with
`TcpServer`, a single-threaded, level-triggered `epoll` reactor. Protocol encoding
is separate from I/O. `UniqueFd` closes descriptors automatically; monotonically
increasing connection IDs prevent stale events from targeting reused descriptors.
Disconnect callbacks remove subscriptions.

Connections retain incomplete frames and partial-write offsets. `EPOLLOUT` is
active only with pending output, and per-pass read/write/accept budgets limit each
client's work. The generator cycles through four tickers in 1 ms batches, capped
at 256 ticks per pass; excess catch-up work is counted and skipped. Seeds reproduce
prices for equal generation counts, not timing.

## Buffering

| Resource | Default / bound |
| --- | --- |
| Connections | 1,024 |
| Receive storage | 4 KiB per client |
| Full-delivery queue | 256 ticks per client, including a partial tick |
| Unsent bytes | 64 KiB per client, including controls |
| Conflated ticks | Four unstarted ticker slots plus one partial tick |

Full mode disconnects the owner on tick/byte overflow. Conflation replaces matching
unstarted ticks without moving their slots, preserving fairness; controls are
never replaced. Its catalog-derived tick bound replaces the FIFO tick cap, while
the byte cap still applies.

Partially transmitted frames are immutable. Kernel-buffered bytes cannot be
conflated. Metadata and kernel buffers add memory beyond these bounds. Unsubscribe
leaves earlier queued updates intact; shutdown does not guarantee a final drain.

## Benchmarking

Use the README command with an unsanitized Release build. To compare slow consumers,
add `--slow-client --slow-pause 10` and run both delivery modes. The slow client is
included in the count and pauses actual socket reads.

The harness waits for all subscription acknowledgements before starting generation.
`--workers` distributes readers across processes; one Python event loop approached
full CPU utilization at 100 clients. Results remain local in ignored `bench/results/`.

- Throughput counts complete frames decoded during measurement. One generated tick
  reaching 100 clients counts as 100 deliveries.
- Latency spans generation to Python decode completion using Linux
  `CLOCK_MONOTONIC`. Warm-up deliveries are excluded; older buffered ticks decoded
  inside measurement are included.
- Histograms have 1,025 fixed buckets, width at most 6.25% above 16 ns. Percentiles
  are upper bounds; p99.9 needs 10,000 samples. Healthy/slow and per-client results
  are separate.
- Server rates use their own recorded telemetry interval. Queue high-water marks
  include warm-up. Socket writes and accepted offers are not decoded deliveries.
- `/proc` supplies CPU/RSS samples; one core is 100% CPU. Keep sampling settings
  fixed between comparisons. CPU warnings can identify a reader bottleneck.

Failed runs exit nonzero and retain diagnostics. A complete slow scenario may be
inconclusive if backpressure was not observed. Interpret latency alongside gaps,
replacements, disconnects, and achieved generation rate.

## Baseline context

Measured 13 November 2026 on an i7-10750H with 12 logical CPUs and approximately
15.4 GiB RAM: Linux `7.1.13-200.fc44.x86_64`, GCC 16.2.1 Release (`-O3 -DNDEBUG`),
Python 3.14.7, sanitizers disabled, server revision `baf2dd2`.

Settings: eight reader workers, seed 42, 4 KiB requested send buffer, default queue
limits, five-second warm-up, fifteen-second measurement, three repetitions,
50 ms telemetry, and 100 ms process sampling. No CPU pinning or frequency locking.

For the README's 100-client result, pooled p99 ranged from 1.573–2.228 ms, the worst
individual client's p99 was 2.62 ms, and sampled server RSS was 4.27–4.47 MiB.
Full mode disconnected the paused client; conflation retained it. The other 99
maintained about 99,000 updates/sec in both modes.

Eighteen of twenty configurations completed all trials. Full delivery at 100 clients
and a 10,000-tick/sec target overflowed during warm-up. A short 200,000-update/sec
probe failed longer validation and is excluded from sustained claims. Loopback
results include Python and shared-host scheduling effects.
