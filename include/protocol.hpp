#ifndef TICKSTREAM_PROTOCOL_HPP
#define TICKSTREAM_PROTOCOL_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <variant>

namespace protocol {
    inline constexpr std::uint8_t VERSION = 1;
    inline constexpr std::size_t HEADER_SIZE = 4;
    inline constexpr std::size_t MAX_PAYLOAD = 44;
    inline constexpr std::size_t MAX_FRAME_SIZE = HEADER_SIZE + MAX_PAYLOAD;
    inline constexpr std::int64_t PRICE_SCALE = 10'000;
    inline constexpr std::array<std::string_view, 4> TICKERS{"AAPL", "MSFT", "GOOG", "AMZN"};

    enum class MsgType : std::uint8_t {
        Subscribe = 1, Unsubscribe = 2, Tick = 3, Ack = 4, Start = 5
    };

    // Requests future updates for one ticker.
    struct SubscribeMsg {
        std::uint32_t ticker_id;

        bool operator==(const SubscribeMsg &) const = default;
    };

    // Stops new updates from being queued for one ticker.
    struct UnsubscribeMsg {
        std::uint32_t ticker_id;

        bool operator==(const UnsubscribeMsg &) const = default;
    };

    // Carries one price update and its per-ticker sequence number.
    struct TickMsg {
        std::uint64_t seq;
        std::uint32_t ticker_id;
        std::int64_t bid;
        std::int64_t ask;
        std::int64_t last;
        std::uint64_t timestamp_ns; // Linux CLOCK_MONOTONIC, not wall-clock time.
        bool operator==(const TickMsg &) const = default;
    };

    // Confirms that a subscription request has been applied.
    struct AckMsg {
        MsgType request_type;
        std::uint32_t ticker_id;

        bool operator==(const AckMsg &) const = default;
    };

    // Triggers generation once in manual-start benchmark mode.
    struct StartMsg {
        bool operator==(const StartMsg &) const = default;
    };

    using Message = std::variant<SubscribeMsg, UnsubscribeMsg, TickMsg, AckMsg, StartMsg>;

    enum class DecodeStatus { Ok, NeedMoreData, Error };

    enum class DecodeError { None, InvalidVersion, UnknownType, InvalidLength, InvalidPayload };

    // Reports one decoded frame or why the caller cannot consume it yet.
    struct DecodeResult {
        DecodeStatus status;
        std::size_t consumed = 0;
        std::optional<Message> message;
        DecodeError error = DecodeError::None;
    };

    // Checks membership in the fixed ticker catalog.
    bool valid_ticker(std::uint32_t ticker_id);

    // Maps a message variant to its wire identifier.
    MsgType message_type(const Message &message);

    // Returns frame size, including the header, without validating payload values.
    std::size_t encoded_size(const Message &message);

    // Serializes one message into caller-owned storage.
    // Throws std::invalid_argument for invalid payloads or std::length_error for short output.
    // Validates before writing; bytes after the encoded frame are untouched.
    std::size_t encode(const Message &message, std::span<std::byte> out);

    // Decodes at most one frame. Non-Ok results consume no bytes and contain no message.
    DecodeResult decode(std::span<const std::byte> data);
} // namespace protocol
#endif
