# TickStream Wire Protocol v1

TCP transports an ordered byte stream; reads do not delimit frames. All multibyte
integers use big-endian (network) byte order. Encode fields individually, without
C++ struct padding. This is the first implemented version; the earlier three-byte
header and heartbeat sketches are superseded.

## Header

| Frame offset | Width | Field |
| --- | --- | --- |
| 0 | 1 | Version, always 1 |
| 1 | 1 | Message type |
| 2 | 2 | Unsigned payload length, excluding the header |

Maximum payload is 44 bytes; maximum frame is 48 bytes. Each message type has one
exact payload size. Reject incorrect lengths immediately after reading a complete
header, without waiting for the claimed payload.

## Messages and direction

| Type | Value | Payload bytes | Direction | Payload fields, in order |
| --- | --- | --- | --- | --- |
| Subscribe | 1 | 4 | Client to server | ticker ID: u32 |
| Unsubscribe | 2 | 4 | Client to server | ticker ID: u32 |
| Tick | 3 | 44 | Server to client | sequence: u64, ticker ID: u32, bid: i64, ask: i64, last: i64, timestamp: u64 |
| Ack | 4 | 5 | Server to client | acknowledged request type: u8, ticker ID: u32 |
| Start | 5 | 0 | Client to server | none |

Tick fields begin at frame offsets 4, 12, 16, 24, 32, and 40 respectively. Signed
prices have a two's-complement wire representation. Ack may acknowledge only
Subscribe or Unsubscribe, and carries the original request's ticker ID.

## Values and validation

- Ticker IDs: 1 = AAPL, 2 = MSFT, 3 = GOOG, 4 = AMZN. Other IDs are invalid.
- Prices are nonnegative integers in units of 1/10,000 currency unit. Bid must be
  no greater than ask. Last need not lie inside the current spread.
- Sequence numbers start at 1 and advance independently per ticker, once per
  generated tick, before routing. All recipients share that tick's sequence.
- Timestamps are unsigned nanoseconds from Linux `CLOCK_MONOTONIC`, not Unix time.
  Zero is valid. Same-machine Python clients use
  `time.clock_gettime_ns(time.CLOCK_MONOTONIC)` for comparable receive timestamps.
- Invalid versions, unknown types, incorrect lengths, and invalid payload values
  are protocol errors. The receiving connection must close; no resynchronization
  or error response is attempted.

## Subscription and start semantics

Subscribe and Unsubscribe are idempotent. Ack is enqueued after the subscription
state changes, including duplicate requests. Unsubscribe stops new updates from
being queued, but earlier pending or transmitted updates may still arrive. A
client establishes its sequence baseline from the first tick after subscribing.

In the future server's `--manual-start` mode, the generator waits for the first
Start message. The benchmark waits for all subscription acknowledgements before
sending it. Start is a one-shot trigger; it has no acknowledgement. A repeated
Start, or Start outside manual-start mode, is a connection protocol error. Start
is a local benchmarking control, not an authenticated administration interface.

The codec validates frame shape and values in either direction. The connection
layer additionally rejects messages inappropriate for the receiving role and
enforces start state. The market server implements these behaviors. Conflation
changes which ticks are sent, not the wire format; sequence gaps are expected in
that mode, while duplicate or decreasing sequences remain invalid behavior.

## Incremental codec API

C++ `encode(message, span<byte>)` returns the encoded size. It throws
`std::invalid_argument` for invalid payloads and `std::length_error` for insufficient
output capacity. Validation precedes writes; failed calls leave output untouched.
Bytes beyond the encoded frame are unchanged. `encoded_size` reports the required
size but does not validate values.

C++ `decode(span<const byte>)` returns `Ok`, `NeedMoreData`, or `Error`. Success
includes one message and the exact number of consumed bytes, even when more frames
follow. Other results consume zero bytes and contain no message. Decode does not
allocate a buffer based on untrusted lengths. Callers retain incomplete input and
bound accumulated receive storage (planned server limit: 4 KiB).

`tools/protocol.py` provides matching immutable message records, `encode`, and
`decode`, using only the Python standard library. Invalid encoding raises
`ValueError`. Decode has the same status/consumption contract; it accepts contiguous
byte buffers. Both codecs use `tests/fixtures/protocol_v1.txt` as golden fixtures.

For example, subscribing to AAPL is `01 01 00 04 00 00 00 01`: version 1,
Subscribe, four-byte payload, ticker ID 1.
