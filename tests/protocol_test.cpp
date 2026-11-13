#include "protocol.hpp"
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <limits>
#include <map>
#include <stdexcept>
#include <vector>

using namespace protocol;

namespace {
    const std::map<std::string, Message> MESSAGES{
        {"subscribe", SubscribeMsg{1}}, {"unsubscribe", UnsubscribeMsg{4}},
        {"tick", TickMsg{0x0102030405060708, 1, 100, 102, 101, 0x0a0b0c0d0e0f1011}},
        {"ack_subscribe", AckMsg{MsgType::Subscribe, 1}},
        {"ack_unsubscribe", AckMsg{MsgType::Unsubscribe, 4}}, {"start", StartMsg{}}
    };

    // Allocates an exactly sized frame for test inputs.
    std::vector<std::byte> bytes(const Message &message) {
        std::vector<std::byte> result(encoded_size(message));
        encode(message, result);
        return result;
    }

    // Verifies that malformed input returns an error without consuming bytes.
    void expect_error(const std::vector<std::byte> &data, DecodeError error) {
        const auto result = decode(data);
        EXPECT_EQ(result.status, DecodeStatus::Error);
        EXPECT_EQ(result.error, error);
        EXPECT_EQ(result.consumed, 0);
        EXPECT_FALSE(result.message);
    }
} // namespace

// Checks exact wire bytes so matching encoder/decoder mistakes cannot hide in round trips.
TEST(Protocol, SharedGoldenFramesMatchInBothDirections) {
    std::ifstream fixture(PROTOCOL_FIXTURE_PATH);
    ASSERT_TRUE(fixture.is_open());
    std::string name, hex;
    std::size_t count = 0;
    while (fixture >> name >> hex) {
        SCOPED_TRACE(name);
        ASSERT_TRUE(MESSAGES.contains(name));
        ASSERT_EQ(hex.size() % 2, 0);
        std::vector<std::byte> expected;
        for (std::size_t i = 0; i < hex.size(); i += 2) {
            expected.push_back(static_cast<std::byte>(std::stoul(hex.substr(i, 2), nullptr, 16)));
        }
        EXPECT_EQ(bytes(MESSAGES.at(name)), expected);
        const auto decoded = decode(expected);
        ASSERT_EQ(decoded.status, DecodeStatus::Ok);
        ASSERT_TRUE(decoded.message);
        EXPECT_EQ(*decoded.message, MESSAGES.at(name));
        EXPECT_EQ(decoded.consumed, expected.size());
        EXPECT_EQ(decoded.error, DecodeError::None);
        ++count;
    }
    EXPECT_EQ(count, MESSAGES.size());
}

// Simulates a read ending at every byte boundary; incomplete frames must consume nothing.
TEST(Protocol, EveryTruncatedPrefixNeedsMoreData) {
    for (const auto &[name, message]: MESSAGES) {
        SCOPED_TRACE(name);
        const auto frame = bytes(message);
        for (std::size_t length = 0; length < frame.size(); ++length) {
            const auto result = decode(std::span(frame).first(length));
            EXPECT_EQ(result.status, DecodeStatus::NeedMoreData);
            EXPECT_EQ(result.consumed, 0);
            EXPECT_FALSE(result.message);
            EXPECT_EQ(result.error, DecodeError::None);
        }
    }
}

// Checks that callers can advance one frame at a time and retain unfinished input.
TEST(Protocol, ParsesConcatenatedFramesAndRetainsPartialTail) {
    auto data = bytes(SubscribeMsg{1});
    const auto second = bytes(UnsubscribeMsg{2});
    data.insert(data.end(), second.begin(), second.end());
    data.push_back(std::byte{1});
    const auto first = decode(data);
    ASSERT_EQ(first.status, DecodeStatus::Ok);
    EXPECT_EQ(first.message, std::optional<Message>{SubscribeMsg{1}});
    const auto next = decode(std::span(data).subspan(first.consumed));
    ASSERT_EQ(next.status, DecodeStatus::Ok);
    EXPECT_EQ(next.message, std::optional<Message>{UnsubscribeMsg{2}});
    EXPECT_EQ(decode(std::span(data).subspan(first.consumed + next.consumed)).status,
              DecodeStatus::NeedMoreData);
}

// Rejects impossible headers immediately instead of waiting for an untrusted payload length.
TEST(Protocol, RejectsInvalidHeaderBeforeWaitingForPayload) {
    auto frame = bytes(SubscribeMsg{1});
    frame.resize(HEADER_SIZE);
    frame[0] = std::byte{2};
    expect_error(frame, DecodeError::InvalidVersion);
    frame[0] = std::byte{VERSION};
    frame[1] = std::byte{255};
    expect_error(frame, DecodeError::UnknownType);
    frame[1] = std::byte{1};
    for (const auto length: {0, 3, 5, 256, 65535}) {
        frame[2] = static_cast<std::byte>(length >> 8);
        frame[3] = static_cast<std::byte>(length & 255);
        expect_error(frame, DecodeError::InvalidLength);
    }
}

// Mutates otherwise valid frames to verify ticker, acknowledgement, and price validation.
TEST(Protocol, RejectsInvalidPayloadsOnDecode) {
    for (const auto id: {0u, 5u, std::numeric_limits<std::uint32_t>::max()}) {
        auto frame = bytes(SubscribeMsg{1});
        for (std::size_t i = 0; i < 4; ++i) frame[4 + i] = static_cast<std::byte>((id >> (24 - i * 8)) & 255);
        expect_error(frame, DecodeError::InvalidPayload);
    }
    auto ack = bytes(AckMsg{MsgType::Subscribe, 1});
    ack[4] = std::byte{3};
    expect_error(ack, DecodeError::InvalidPayload);
    auto tick = bytes(MESSAGES.at("tick"));
    std::fill(tick.begin() + 4, tick.begin() + 12, std::byte{0});
    expect_error(tick, DecodeError::InvalidPayload);
    tick = bytes(MESSAGES.at("tick"));
    tick[16] = std::byte{0x80}; // Negative bid.
    expect_error(tick, DecodeError::InvalidPayload);
    tick = bytes(MESSAGES.at("tick"));
    tick[31] = std::byte{99}; // Ask less than bid.
    expect_error(tick, DecodeError::InvalidPayload);
    tick = bytes(MESSAGES.at("tick"));
    tick[32] = std::byte{0x80}; // Negative last.
    expect_error(tick, DecodeError::InvalidPayload);
}

// Checks failure atomicity and ensures successful encoding leaves trailing storage untouched.
TEST(Protocol, InvalidEncodeAndShortOutputLeaveBufferUntouched) {
    std::array<std::byte, MAX_FRAME_SIZE + 1> out;
    out.fill(std::byte{0xaa});
    const auto before = out;
    EXPECT_THROW(encode(SubscribeMsg{0}, out), std::invalid_argument);
    EXPECT_THROW(encode(AckMsg{MsgType::Tick, 1}, out), std::invalid_argument);
    EXPECT_THROW(encode(TickMsg{0, 1, 100, 102, 101, 0}, out), std::invalid_argument);
    EXPECT_THROW(encode(TickMsg{1, 1, -1, 102, 101, 0}, out), std::invalid_argument);
    EXPECT_THROW(encode(TickMsg{1, 1, 103, 102, 101, 0}, out), std::invalid_argument);
    EXPECT_THROW(encode(TickMsg{1, 1, 100, 102, -1, 0}, out), std::invalid_argument);
    for (const auto &[name, message]: MESSAGES) {
        SCOPED_TRACE(name);
        for (std::size_t size = 0; size < encoded_size(message); ++size) {
            EXPECT_THROW(encode(message, std::span(out).first(size)), std::length_error);
            EXPECT_EQ(out, before);
        }
    }
    EXPECT_EQ(out, before);
    const auto size = encode(SubscribeMsg{1}, out);
    EXPECT_TRUE(std::all_of(out.begin() + size, out.end(), [](auto b) { return b == std::byte{0xaa}; }));
}

// Exercises full-width integers and valid zero values to catch narrowing or signedness mistakes.
TEST(Protocol, IntegerBoundariesRoundTrip) {
    const TickMsg tick{
        std::numeric_limits<std::uint64_t>::max(), 4, 0,
        std::numeric_limits<std::int64_t>::max(), std::numeric_limits<std::int64_t>::max(),
        std::numeric_limits<std::uint64_t>::max()
    };
    EXPECT_EQ(decode(bytes(tick)).message, std::optional<Message>{tick});
    EXPECT_EQ(decode(bytes(TickMsg{1, 1, 0, 0, 0, 0})).status, DecodeStatus::Ok);
}
