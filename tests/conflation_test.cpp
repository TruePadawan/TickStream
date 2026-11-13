#include "transport.hpp"
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <stdexcept>
#include <sys/socket.h>

using namespace tickstream;
using namespace protocol;

namespace {
    // Owns a non-blocking stream pair so tests can control writes and inspect exact bytes.
    struct SocketPair {
        UniqueFd writer;
        UniqueFd reader;

        SocketPair() {
            int sockets[2];
            if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == -1) {
                throw std::runtime_error("socketpair failed");
            }
            writer = UniqueFd(sockets[0]);
            reader = UniqueFd(sockets[1]);
        }
    };

    // Queues encoded frames with the same ticker metadata used by the real server.
    bool enqueue(PendingWrites &output, const Message &message) {
        std::array<std::byte, MAX_FRAME_SIZE> bytes{};
        const auto size = encode(message, bytes);
        const auto *tick = std::get_if<TickMsg>(&message);
        return output.enqueue(std::span(bytes).first(size), tick ? tick->ticker_id : 0);
    }

    TickMsg tick(std::uint32_t ticker, std::uint64_t seq) {
        return {seq, ticker, 100, 102, 101, seq};
    }

    // Small test payloads fit the pair's buffers; collect frames already accepted by the socket.
    std::vector<Message> read_frames(int fd) {
        std::array<std::byte, 4096> bytes{};
        std::vector<std::byte> data;
        for (;;) {
            const auto count = ::recv(fd, bytes.data(), bytes.size(), 0);
            if (count > 0) data.insert(data.end(), bytes.begin(), bytes.begin() + count);
            else if (count < 0 && errno == EINTR) continue;
            else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            else throw std::runtime_error("Unexpected read result");
        }
        std::vector<Message> messages;
        std::size_t offset = 0;
        while (offset < data.size()) {
            const auto result = decode(std::span(data).subspan(offset));
            if (result.status != DecodeStatus::Ok) throw std::runtime_error("Corrupted or incomplete frame");
            messages.push_back(*result.message);
            offset += result.consumed;
        }
        return messages;
    }
} // namespace

// Replacements retain a ticker's original position, and acknowledgements remain distinct FIFO frames.
TEST(Conflation, LatestValuesPreserveTickerFairnessAndControls) {
    PendingWrites output(1024, DeliveryMode::Conflated);
    ASSERT_TRUE(enqueue(output, tick(1, 1)));
    ASSERT_TRUE(enqueue(output, tick(2, 1)));
    ASSERT_TRUE(enqueue(output, AckMsg{MsgType::Subscribe, 3}));
    ASSERT_TRUE(enqueue(output, tick(3, 1)));
    ASSERT_TRUE(enqueue(output, AckMsg{MsgType::Unsubscribe, 4}));
    for (std::uint64_t seq = 2; seq <= 100; ++seq)
        ASSERT_TRUE(enqueue(output, tick(1, seq)));
    EXPECT_EQ(output.pending_ticks(), 3);
    EXPECT_EQ(output.size(), 3 * MAX_FRAME_SIZE + 18);
    EXPECT_EQ(output.replacements(), 99);
    SocketPair sockets;
    ASSERT_EQ(output.flush(sockets.writer.get(), 4096), FlushStatus::Drained);
    const std::vector<Message> expected{
        tick(1, 100), tick(2, 1), AckMsg{MsgType::Subscribe, 3},
        tick(3, 1), AckMsg{MsgType::Unsubscribe, 4}
    };
    EXPECT_EQ(read_frames(sockets.reader.get()), expected);
    EXPECT_EQ(output.completed_ticks(), 3);
}

// Full mode preserves every tick instead of replacing repeated symbols.
TEST(Conflation, FullModeRetainsAllUpdates) {
    PendingWrites output(3 * MAX_FRAME_SIZE, DeliveryMode::Full);
    for (std::uint64_t seq = 1; seq <= 3; ++seq)
        ASSERT_TRUE(enqueue(output, tick(1, seq)));
    EXPECT_FALSE(enqueue(output, tick(1, 4)));
    EXPECT_EQ(output.pending_ticks(), 3);
    EXPECT_EQ(output.replacements(), 0);
    SocketPair sockets;
    ASSERT_EQ(output.flush(sockets.writer.get(), 4096), FlushStatus::Drained);
    EXPECT_EQ(read_frames(sockets.reader.get()), (std::vector<Message>{tick(1, 1), tick(1, 2), tick(1, 3)}));
}

// A partially transmitted tick is immutable; newer values occupy at most four additional ticker slots.
TEST(Conflation, PartialFrameSurvivesWhilePendingUpdatesStayBounded) {
    PendingWrites output(5 * MAX_FRAME_SIZE, DeliveryMode::Conflated);
    SocketPair sockets;
    ASSERT_TRUE(enqueue(output, tick(1, 1)));
    ASSERT_EQ(output.flush(sockets.writer.get(), 7), FlushStatus::BudgetExhausted);
    for (std::uint32_t ticker = 1; ticker <= 4; ++ticker)
        ASSERT_TRUE(enqueue(output, tick(ticker, 2)));
    for (std::uint64_t seq = 3; seq <= 100; ++seq) {
        for (std::uint32_t ticker = 1; ticker <= 4; ++ticker)
            ASSERT_TRUE(enqueue(output, tick(ticker, seq)));
        ASSERT_EQ(output.pending_ticks(), 5);
        ASSERT_EQ(output.size(), 5 * MAX_FRAME_SIZE - 7);
    }
    EXPECT_EQ(output.replacements(), 98 * 4);
    ASSERT_EQ(output.flush(sockets.writer.get(), 4096), FlushStatus::Drained);
    EXPECT_EQ(read_frames(sockets.reader.get()), (std::vector<Message>{
                  tick(1, 1), tick(1, 100), tick(2, 100), tick(3, 100), tick(4, 100)}));
    EXPECT_EQ(output.completed_ticks(), 5);
}

// EAGAIN before the first byte does not freeze a tick; replacing it remains safe at capacity.
TEST(Conflation, UnstartedBlockedFrameCanBeReplacedAtCapacity) {
    SocketPair sockets;
    int capacity = 4096;
    ASSERT_EQ(::setsockopt(sockets.writer.get(), SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)), 0);
    std::array<std::byte, 65536> filler{};
    bool blocked = false;
    for (int attempts = 0; attempts < 100; ++attempts) {
        const auto sent = ::send(sockets.writer.get(), filler.data(), filler.size(), MSG_NOSIGNAL);
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            blocked = true;
            break;
        }
        ASSERT_GT(sent, 0);
    }
    ASSERT_TRUE(blocked);
    PendingWrites output(MAX_FRAME_SIZE, DeliveryMode::Conflated);
    ASSERT_TRUE(enqueue(output, tick(1, 1)));
    ASSERT_EQ(output.flush(sockets.writer.get(), 4096), FlushStatus::Blocked);
    ASSERT_TRUE(enqueue(output, tick(1, 2)));
    EXPECT_FALSE(enqueue(output, tick(2, 1)));
    EXPECT_EQ(output.pending_ticks(), 1);
    EXPECT_EQ(output.replacements(), 1);
    // Discard filler before reading the protocol frame; it was sent outside the queue.
    while (::recv(sockets.reader.get(), filler.data(), filler.size(), 0) > 0) {
    }
    ASSERT_EQ(output.flush(sockets.writer.get(), 4096), FlushStatus::Drained);
    EXPECT_EQ(read_frames(sockets.reader.get()), (std::vector<Message>{tick(1, 2)}));
}
