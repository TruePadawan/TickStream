#ifndef TICKSTREAM_PROTOCOL_H
#define TICKSTREAM_PROTOCOL_H

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace protocol {
    // Constants
    constexpr size_t HEADER_SIZE = 3;
    constexpr size_t MAX_PAYLOAD = 256;
    constexpr size_t MAX_TICKERS = 4096;

    // Message type identifiers
    enum class MsgType : uint8_t {
        SUBSCRIBE = 1,
        UNSUBSCRIBE = 2,
        TICK = 3,
        HEARTBEAT = 4
    };

    // Tick payload
    struct TickMsg {
        uint64_t seq;
        uint32_t ticker_id;
        // Prices are in cents
        int64_t bid;
        int64_t ask;
        int64_t last;
        uint64_t timestamp_ns; // Nanoseconds since epoch
    };

    // Subscribe payload
    struct SubscribeMsg {
        uint32_t ticker_id;
    };

    // Unsubscribe payload
    struct UnsubscribeMsg {
        uint32_t ticker_id;
    };

    // Heartbeat payload, it's just for checking if the connection is alive
    struct HeartbeatMsg {
    };

    using Message = std::variant<TickMsg, SubscribeMsg, UnsubscribeMsg, HeartbeatMsg>;

    enum class DecodeStatus { Ok, NeedMoreData, Error };

    struct DecodeResult {
        DecodeStatus status;
        size_t consumed; // bytes used when status is Ok
        Message message; // valid only when status is Ok
    };

    size_t encode(const TickMsg &message, std::byte *out);

    size_t encode(const SubscribeMsg &message, std::byte *out);

    size_t encode(const UnsubscribeMsg &message, std::byte *out);

    size_t encode(const HeartbeatMsg &message, std::byte *out);

    DecodeResult decode(const std::byte *data, size_t len);
} // namespace protocol

#endif //TICKSTREAM_PROTOCOL_H
