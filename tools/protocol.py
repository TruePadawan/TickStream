"""TickStream v1 codec"""

from dataclasses import dataclass
from enum import IntEnum, Enum
import struct

VERSION = 1
HEADER_SIZE = 4
MAX_PAYLOAD = 44
MAX_FRAME_SIZE = 48
PRICE_SCALE = 10_000
TICKERS = {1: "AAPL", 2: "MSFT", 3: "GOOG", 4: "AMZN"}


class MsgType(IntEnum):
    """Wire identifiers shared with the C++ codec."""

    SUBSCRIBE = 1
    UNSUBSCRIBE = 2
    TICK = 3
    ACK = 4
    START = 5


@dataclass(frozen=True)
class Subscribe:
    """Request future updates for one ticker."""

    ticker_id: int


@dataclass(frozen=True)
class Unsubscribe:
    """Stop new updates from being queued for one ticker."""

    ticker_id: int


@dataclass(frozen=True)
class Tick:
    """One price update with its per-ticker sequence and generation timestamp."""

    seq: int
    ticker_id: int
    bid: int
    ask: int
    last: int
    timestamp_ns: int


@dataclass(frozen=True)
class Ack:
    """Confirmation that a subscription request has been applied."""

    request_type: MsgType
    ticker_id: int


@dataclass(frozen=True)
class Start:
    """One-shot generation trigger for manual-start benchmarks."""

    pass


class DecodeStatus(Enum):
    """Distinguish a complete frame, incomplete input, and invalid input."""

    OK = "ok"
    NEED_MORE_DATA = "need_more_data"
    ERROR = "error"


@dataclass(frozen=True)
class DecodeResult:
    """One decoded frame or a non-consuming incomplete/error result."""

    status: DecodeStatus
    consumed: int = 0
    message: Subscribe | Unsubscribe | Tick | Ack | Start | None = None
    error: str | None = None


_LAYOUTS = {
    MsgType.SUBSCRIBE: struct.Struct("!I"),
    MsgType.UNSUBSCRIBE: struct.Struct("!I"),
    MsgType.TICK: struct.Struct("!QIqqqQ"),
    MsgType.ACK: struct.Struct("!BI"),
    MsgType.START: struct.Struct("!"),
}
_TYPES = {Subscribe: MsgType.SUBSCRIBE, Unsubscribe: MsgType.UNSUBSCRIBE,
          Tick: MsgType.TICK, Ack: MsgType.ACK, Start: MsgType.START}


def _integer(value, low, high):
    """Check integer range without accepting Python booleans as numbers."""
    return isinstance(value, int) and not isinstance(value, bool) and low <= value <= high


def _valid(message):
    """Enforce wire value ranges and message-specific payload rules."""
    if isinstance(message, Start):
        return True
    if not _integer(message.ticker_id, 1, len(TICKERS)):
        return False
    if isinstance(message, Tick):
        return (_integer(message.seq, 1, 2 ** 64 - 1)
                and _integer(message.bid, 0, 2 ** 63 - 1)
                and _integer(message.ask, 0, 2 ** 63 - 1)
                and message.bid <= message.ask
                and _integer(message.last, 0, 2 ** 63 - 1)
                and _integer(message.timestamp_ns, 0, 2 ** 64 - 1))
    if isinstance(message, Ack):
        return (not isinstance(message.request_type, bool)
                and isinstance(message.request_type, int)
                and message.request_type in (MsgType.SUBSCRIBE, MsgType.UNSUBSCRIBE))
    return True


def encode(message) -> bytes:
    """Encode one message, raising ValueError for invalid messages."""
    kind = _TYPES.get(type(message))
    if kind is None or not _valid(message):
        raise ValueError("Invalid protocol payload")
    if isinstance(message, Tick):
        values = (message.seq, message.ticker_id, message.bid, message.ask,
                  message.last, message.timestamp_ns)
    elif isinstance(message, Ack):
        values = (message.request_type, message.ticker_id)
    elif isinstance(message, Start):
        values = ()
    else:
        values = (message.ticker_id,)
    payload = _LAYOUTS[kind].pack(*values)
    return struct.pack("!BBH", VERSION, kind, len(payload)) + payload


def decode(data: bytes | bytearray | memoryview) -> DecodeResult:
    """Decode one frame. Retain all bytes on incomplete input; close on errors."""
    if len(data) < HEADER_SIZE:
        return DecodeResult(DecodeStatus.NEED_MORE_DATA)
    version, raw_kind, length = struct.unpack_from("!BBH", data)
    if version != VERSION:
        return DecodeResult(DecodeStatus.ERROR, error="invalid_version")
    try:
        kind = MsgType(raw_kind)
    except ValueError:
        return DecodeResult(DecodeStatus.ERROR, error="unknown_type")
    if length != _LAYOUTS[kind].size:
        return DecodeResult(DecodeStatus.ERROR, error="invalid_length")
    size = HEADER_SIZE + length
    if len(data) < size:
        return DecodeResult(DecodeStatus.NEED_MORE_DATA)
    values = _LAYOUTS[kind].unpack_from(data, HEADER_SIZE)
    constructor = {MsgType.SUBSCRIBE: Subscribe, MsgType.UNSUBSCRIBE: Unsubscribe,
                   MsgType.TICK: Tick, MsgType.ACK: Ack, MsgType.START: Start}[kind]
    message = constructor(*values)
    if not _valid(message):
        return DecodeResult(DecodeStatus.ERROR, error="invalid_payload")
    return DecodeResult(DecodeStatus.OK, size, message)
