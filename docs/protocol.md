# Wire Protocol v1

TCP is a byte stream: reads may contain partial or multiple frames. All multibyte
fields use big-endian byte order; encode fields individually without struct padding.

## Header

| Frame offset | Bytes | Field |
| --- | --- | --- |
| 0 | 1 | Version: 1 |
| 1 | 1 | Message type |
| 2 | 2 | Unsigned payload length, excluding header |

Maximum payload: 44 bytes. Maximum frame: 48 bytes. Reject lengths that do not
match the message type immediately after receiving a complete header.

## Messages

`uN` and `iN` mean unsigned and signed N-bit integers. Signed values use two's complement.

| Type | ID | Payload bytes | Direction | Payload fields in order |
| --- | --- | --- | --- | --- |
| Subscribe | 1 | 4 | Client → server | ticker ID: u32 |
| Unsubscribe | 2 | 4 | Client → server | ticker ID: u32 |
| Tick | 3 | 44 | Server → client | sequence: u64, ticker ID: u32, bid: i64, ask: i64, last: i64, timestamp: u64 |
| Ack | 4 | 5 | Server → client | request type: u8, ticker ID: u32 |
| Start | 5 | 0 | Client → server | none |

Tick fields begin at frame offsets 4, 12, 16, 24, 32, and 40. Ack identifies the
original Subscribe or Unsubscribe request and its ticker.

## Values and behavior

- Ticker IDs: 1 = AAPL, 2 = MSFT, 3 = GOOG, 4 = AMZN. Other IDs are invalid.
- Prices are nonnegative integers in units of 0.0001 currency unit. Bid ≤ ask;
  last need not lie within the spread.
- Sequences start at 1 and increase independently per ticker before routing.
  Recipients share the same sequence. The first received tick establishes a
  client's baseline; conflation permits increasing gaps, never reordering.
- Timestamps are unsigned Linux `CLOCK_MONOTONIC` nanoseconds, not Unix time;
  zero is valid. Same-machine Python receivers use
  `time.clock_gettime_ns(time.CLOCK_MONOTONIC)`.
- Subscribe/Unsubscribe are idempotent. Ack is queued after applying each request,
  including duplicates. Unsubscribe stops new routing; earlier pending ticks may arrive.
- In `--manual-start` mode, Start triggers generation once, without acknowledgement.
  The harness waits for all subscription acknowledgements first. Repeated Start,
  or Start outside manual mode, is a protocol error.
- Invalid versions, types, lengths, payload values, and wrong-direction messages
  close the connection. No error frame or resynchronization is attempted.

## Codec contract

C++ `encode(message, span<byte>)` returns the frame size. Invalid payloads throw
`std::invalid_argument`; short output throws `std::length_error`. Failure leaves
output untouched; success preserves bytes beyond the frame. `encoded_size`
reports capacity needed without validating payload values.

`decode(span<const byte>)` returns one message and its consumed byte count on
`Ok`. `NeedMoreData` and `Error` consume nothing and contain no message. Retain
incomplete input; the server caps receive storage at 4 KiB. Connection handling
checks message direction and Start state in addition to codec validation.

`tools/protocol.py` has matching records and encode/decode results; invalid encoding
raises `ValueError`. Shared golden fixtures are in `tests/fixtures/protocol_v1.txt`.
For example, subscribing to AAPL encodes as `01 01 00 04 00 00 00 01`.
