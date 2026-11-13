#include "protocol.hpp"

#include <bit>
#include <stdexcept>
#include <type_traits>

namespace protocol {
    namespace {
        // Returns the exact payload size, or no value for an unknown wire type.
        std::optional<std::size_t> payload_size(MsgType type) {
            switch (type) {
                case MsgType::Subscribe:
                case MsgType::Unsubscribe: return 4;
                case MsgType::Tick: return 44;
                case MsgType::Ack: return 5;
                case MsgType::Start: return 0;
            }
            return std::nullopt;
        }

        // Applies payload rules shared by encoding and decoding.
        bool valid_message(const Message &message) {
            return std::visit([](const auto &value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, StartMsg>) {
                    return true;
                } else {
                    if (!valid_ticker(value.ticker_id)) return false;
                    if constexpr (std::is_same_v<T, TickMsg>) {
                        return value.seq > 0 && value.bid >= 0 && value.ask >= value.bid && value.last >= 0;
                    } else if constexpr (std::is_same_v<T, AckMsg>) {
                        return value.request_type == MsgType::Subscribe || value.request_type == MsgType::Unsubscribe;
                    }
                    return true;
                }
            }, message);
        }

        // Writes an unsigned integer in network byte order after capacity checks.
        template<typename UInt>
        void write_be(std::span<std::byte> out, std::size_t offset, UInt value) {
            static_assert(std::is_unsigned_v<UInt>);
            for (std::size_t i = 0; i < sizeof(UInt); ++i) {
                out[offset + sizeof(UInt) - 1 - i] = static_cast<std::byte>(value & 0xff);
                value >>= 8;
            }
        }

        // Reads network-order bytes after the caller has checked frame length.
        template<typename UInt>
        UInt read_be(std::span<const std::byte> in, std::size_t offset) {
            static_assert(std::is_unsigned_v<UInt>);
            UInt result = 0;
            for (std::size_t i = 0; i < sizeof(UInt); ++i) {
                result = static_cast<UInt>((result << 8) | std::to_integer<unsigned int>(in[offset + i]));
            }
            return result;
        }

        // Keeps protocol errors consistent: no message and no consumed bytes.
        DecodeResult failure(DecodeError error) {
            return {DecodeStatus::Error, 0, std::nullopt, error};
        }
    } // namespace

    bool valid_ticker(std::uint32_t ticker_id) {
        return ticker_id >= 1 && ticker_id <= TICKERS.size();
    }

    MsgType message_type(const Message &message) {
        return std::visit([](const auto &value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, SubscribeMsg>) return MsgType::Subscribe;
            else if constexpr (std::is_same_v<T, UnsubscribeMsg>) return MsgType::Unsubscribe;
            else if constexpr (std::is_same_v<T, TickMsg>) return MsgType::Tick;
            else if constexpr (std::is_same_v<T, AckMsg>) return MsgType::Ack;
            else return MsgType::Start;
        }, message);
    }

    std::size_t encoded_size(const Message &message) {
        return HEADER_SIZE + *payload_size(message_type(message));
    }

    std::size_t encode(const Message &message, std::span<std::byte> out) {
        if (!valid_message(message)) throw std::invalid_argument("Invalid protocol payload");
        const auto size = encoded_size(message);
        if (out.size() < size) throw std::length_error("Protocol output buffer is too small");
        out[0] = static_cast<std::byte>(VERSION);
        out[1] = static_cast<std::byte>(message_type(message));
        write_be<std::uint16_t>(out, 2, static_cast<std::uint16_t>(size - HEADER_SIZE));
        std::visit([&](const auto &value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TickMsg>) {
                write_be(out, 4, value.seq);
                write_be(out, 12, value.ticker_id);
                write_be(out, 16, std::bit_cast<std::uint64_t>(value.bid));
                write_be(out, 24, std::bit_cast<std::uint64_t>(value.ask));
                write_be(out, 32, std::bit_cast<std::uint64_t>(value.last));
                write_be(out, 40, value.timestamp_ns);
            } else if constexpr (std::is_same_v<T, AckMsg>) {
                out[4] = static_cast<std::byte>(value.request_type);
                write_be(out, 5, value.ticker_id);
            } else if constexpr (!std::is_same_v<T, StartMsg>) {
                write_be(out, 4, value.ticker_id);
            }
        }, message);
        return size;
    }

    DecodeResult decode(std::span<const std::byte> data) {
        if (data.size() < HEADER_SIZE) return {DecodeStatus::NeedMoreData, 0, std::nullopt};
        if (data[0] != static_cast<std::byte>(VERSION)) return failure(DecodeError::InvalidVersion);
        const auto type = static_cast<MsgType>(std::to_integer<std::uint8_t>(data[1]));
        const auto expected = payload_size(type);
        if (!expected) return failure(DecodeError::UnknownType);
        if (read_be<std::uint16_t>(data, 2) != *expected) return failure(DecodeError::InvalidLength);
        const auto size = HEADER_SIZE + *expected;
        if (data.size() < size) return {DecodeStatus::NeedMoreData, 0, std::nullopt};
        Message message = StartMsg{};
        switch (type) {
            case MsgType::Subscribe: message = SubscribeMsg{read_be<std::uint32_t>(data, 4)};
                break;
            case MsgType::Unsubscribe: message = UnsubscribeMsg{read_be<std::uint32_t>(data, 4)};
                break;
            case MsgType::Tick:
                message = TickMsg{
                    read_be<std::uint64_t>(data, 4), read_be<std::uint32_t>(data, 12),
                    std::bit_cast<std::int64_t>(read_be<std::uint64_t>(data, 16)),
                    std::bit_cast<std::int64_t>(read_be<std::uint64_t>(data, 24)),
                    std::bit_cast<std::int64_t>(read_be<std::uint64_t>(data, 32)),
                    read_be<std::uint64_t>(data, 40)
                };
                break;
            case MsgType::Ack:
                message = AckMsg{
                    static_cast<MsgType>(std::to_integer<std::uint8_t>(data[4])),
                    read_be<std::uint32_t>(data, 5)
                };
                break;
            case MsgType::Start: break;
        }
        if (!valid_message(message)) return failure(DecodeError::InvalidPayload);
        return {DecodeStatus::Ok, size, message};
    }
} // namespace protocol
