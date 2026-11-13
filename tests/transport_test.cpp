#include "transport.hpp"
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace tickstream;
using namespace protocol;

namespace {
    // Connects through the real loopback stack; the test drives server events explicitly.
    UniqueFd connect_client(TcpServer &server, int receive_capacity = 0) {
        UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
        if (client.get() < 0) throw std::runtime_error("client socket failed");
        if (receive_capacity > 0 && ::setsockopt(client.get(), SOL_SOCKET, SO_RCVBUF,
                                                 &receive_capacity, sizeof(receive_capacity)) == -1) {
            throw std::runtime_error("client receive buffer failed");
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(server.port());
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::connect(client.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
            throw std::runtime_error("client connect failed");
        }
        const auto flags = ::fcntl(client.get(), F_GETFL);
        if (flags == -1 || ::fcntl(client.get(), F_SETFL, flags | O_NONBLOCK) == -1) {
            throw std::runtime_error("client nonblocking failed");
        }
        server.poll_once(20);
        return client;
    }

    // Small test requests fit a local send; failure is explicit rather than a silent hang.
    void send_message(int fd, const Message &message) {
        std::array<std::byte, MAX_FRAME_SIZE> frame{};
        const auto size = encode(message, frame);
        if (::send(fd, frame.data(), size, MSG_NOSIGNAL) != static_cast<ssize_t>(size)) {
            throw std::runtime_error("test request send failed");
        }
    }

    // A test-only responder exercises output without implementing subscription state.
    void acknowledge(TcpServer &server, TcpServer::ClientId id, const Message &message) {
        if (const auto *request = std::get_if<SubscribeMsg>(&message)) {
            server.send(id, AckMsg{MsgType::Subscribe, request->ticker_id});
        } else if (const auto *request = std::get_if<UnsubscribeMsg>(&message)) {
            server.send(id, AckMsg{MsgType::Unsubscribe, request->ticker_id});
        }
    }

    // Advances the event loop while accumulating a complete frame with a strict deadline.
    std::optional<Message> receive_message(TcpServer &server, int fd) {
        std::array<std::byte, MAX_FRAME_SIZE> frame{};
        std::size_t used = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            server.poll_once(1);
            const auto received = ::recv(fd, frame.data() + used, 1, MSG_DONTWAIT);
            if (received > 0) {
                used += static_cast<std::size_t>(received);
                const auto result = decode(std::span(frame).first(used));
                if (result.status == DecodeStatus::Ok) return result.message;
                if (result.status == DecodeStatus::Error) return std::nullopt;
            } else if (received == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                return std::nullopt;
            }
        }
        return std::nullopt;
    }
} // namespace

// Closing and moving ownership must not leak descriptors or close transferred resources.
TEST(Transport, DescriptorOwnershipMovesAndCloses) {
    int original = -1;
    {
        UniqueFd first(::socket(AF_INET, SOCK_STREAM, 0));
        original = first.get();
        ASSERT_GE(original, 0);
        UniqueFd second(std::move(first));
        EXPECT_EQ(first.get(), -1);
        EXPECT_EQ(second.get(), original);
        UniqueFd third;
        third = std::move(second);
        EXPECT_EQ(second.get(), -1);
        EXPECT_NE(::fcntl(third.get(), F_GETFD), -1);
    }
    EXPECT_EQ(::fcntl(original, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

// Several clients can frame requests independently and a half-close preserves queued replies.
TEST(Transport, MultipleClientsFragmentedAndCombinedRequests) {
    TcpServer server(ServerConfig{.port = 0}, acknowledge);
    auto first = connect_client(server);
    auto second = connect_client(server);
    std::array<std::byte, MAX_FRAME_SIZE> frame{};
    const auto size = encode(SubscribeMsg{1}, frame);
    for (std::size_t i = 0; i < size; ++i) {
        ASSERT_EQ(::send(first.get(), frame.data() + i, 1, MSG_NOSIGNAL), 1);
        server.poll_once(5);
    }
    EXPECT_EQ(receive_message(server, first.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 1}}));
    std::array<std::byte, 16> combined{};
    encode(SubscribeMsg{2}, std::span(combined).first(8));
    encode(UnsubscribeMsg{2}, std::span(combined).subspan(8));
    ASSERT_EQ(::send(second.get(), combined.data(), combined.size(), MSG_NOSIGNAL), 16);
    ASSERT_EQ(::shutdown(second.get(), SHUT_WR), 0);
    EXPECT_EQ(receive_message(server, second.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 2}}));
    EXPECT_EQ(receive_message(server, second.get()), (std::optional<Message>{AckMsg{MsgType::Unsubscribe, 2}}));
    server.poll_once(5);
    EXPECT_EQ(server.client_count(), 1);
}

// Malformed, wrong-direction, and truncated frames close only the offending peer.
TEST(Transport, InvalidPeersDoNotAffectHealthyClients) {
    TcpServer server(ServerConfig{.port = 0}, acknowledge);
    auto healthy = connect_client(server);
    for (int kind = 0; kind < 3; ++kind) {
        auto bad = connect_client(server);
        if (kind == 0) {
            const std::array invalid{std::byte{1}, std::byte{1}, std::byte{255}, std::byte{255}};
            ASSERT_EQ(::send(bad.get(), invalid.data(), invalid.size(), MSG_NOSIGNAL), 4);
        } else if (kind == 1) {
            send_message(bad.get(), AckMsg{MsgType::Subscribe, 1});
        } else {
            const char prefix = 1;
            ASSERT_EQ(::send(bad.get(), &prefix, 1, MSG_NOSIGNAL), 1);
            ASSERT_EQ(::shutdown(bad.get(), SHUT_WR), 0);
        }
        for (int i = 0; i < 5; ++i) server.poll_once(5);
        EXPECT_EQ(server.client_count(), 1);
    }
    send_message(healthy.get(), SubscribeMsg{4});
    EXPECT_EQ(receive_message(server, healthy.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 4}}));
    EXPECT_EQ(server.stats().protocol_errors, 3);
}

// Overflow disconnects the owner; callbacks may erase it without invalidating the reactor.
TEST(Transport, OutputOverflowIsIsolated) {
    const auto flood = [](TcpServer &server, TcpServer::ClientId id, const Message &message) {
        if (std::get<SubscribeMsg>(message).ticker_id == 1) {
            for (int i = 0; i < 10; ++i) server.send(id, AckMsg{MsgType::Subscribe, 1});
        } else acknowledge(server, id, message);
    };
    TcpServer server(ServerConfig{.port = 0, .output_capacity = 48}, flood);
    auto slow = connect_client(server);
    auto healthy = connect_client(server);
    send_message(slow.get(), SubscribeMsg{1});
    send_message(healthy.get(), SubscribeMsg{2});
    EXPECT_EQ(receive_message(server, healthy.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 2}}));
    EXPECT_EQ(server.stats().output_overflows, 1);
    EXPECT_EQ(server.client_count(), 1);
}

// Real socket backpressure on an unread peer must not prevent a healthy peer's reply.
TEST(Transport, BlockedConsumerDoesNotBlockOtherConnections) {
    const auto flood = [](TcpServer &server, TcpServer::ClientId id, const Message &message) {
        if (std::get<SubscribeMsg>(message).ticker_id == 1) {
            for (std::uint64_t seq = 1; seq <= 8000; ++seq) {
                if (!server.send(id, TickMsg{seq, 1, 100, 102, 101, 0})) break;
            }
        } else acknowledge(server, id, message);
    };
    TcpServer server(ServerConfig{
                         .port = 0, .output_capacity = 512 * 1024, .tick_queue_capacity = 8192,
                         .send_buffer_bytes = 4096
                     }, flood);
    auto slow = connect_client(server, 4096);
    auto healthy = connect_client(server);
    send_message(slow.get(), SubscribeMsg{1});
    for (int i = 0; i < 5; ++i) server.poll_once(5);
    send_message(healthy.get(), SubscribeMsg{2});
    EXPECT_EQ(receive_message(server, healthy.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 2}}));
    EXPECT_GT(server.stats().blocked_writes, 0);
    EXPECT_EQ(server.stats().output_overflows, 0);
    EXPECT_EQ(server.client_count(), 2);
}

// Bad configuration and occupied ports fail explicitly while the existing listener stays usable.
TEST(Transport, RejectsInvalidConfigurationAndBindConflicts) {
    EXPECT_THROW((TcpServer(ServerConfig{.address = "not-an-address", .port = 0})), std::invalid_argument);
    EXPECT_THROW((TcpServer(ServerConfig{.port = 0, .max_clients = 0})), std::invalid_argument);
    EXPECT_THROW((TcpServer(ServerConfig{.port = 0, .output_capacity = 1})), std::invalid_argument);
    TcpServer server(ServerConfig{.port = 0}, acknowledge);
    EXPECT_THROW((TcpServer(ServerConfig{.port = server.port()})), std::system_error);
    auto client = connect_client(server);
    send_message(client.get(), SubscribeMsg{3});
    EXPECT_EQ(receive_message(server, client.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 3}}));
}

// Repeated accepts, closes, and rejected excess connections leave no stale connection state.
TEST(Transport, ConnectionLimitsAndReconnectCleanup) {
    TcpServer server(ServerConfig{.port = 0, .max_clients = 1});
    for (int i = 0; i < 30; ++i) {
        {
            auto client = connect_client(server);
            EXPECT_EQ(server.client_count(), 1);
            auto excess = connect_client(server);
            EXPECT_EQ(server.client_count(), 1);
        }
        for (int attempt = 0; attempt < 5 && server.client_count() != 0; ++attempt) server.poll_once(5);
        EXPECT_EQ(server.client_count(), 0);
    }
    EXPECT_EQ(server.stats().accepted, 30);
    EXPECT_EQ(server.stats().rejected, 30);
    EXPECT_EQ(server.stats().disconnected, 30);
}

// A large chunk and small socket buffer force real short writes and EAGAIN; bytes stay exact.
TEST(Transport, PartialWritesAndBackpressurePreserveBytes) {
    int pair[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair), 0);
    UniqueFd sender(pair[0]), receiver(pair[1]);
    int capacity = 4096;
    ASSERT_EQ(::setsockopt(sender.get(), SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)), 0);
    std::vector<std::byte> data(256 * 1024);
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<std::byte>(i % 251);
    PendingWrites output(data.size());
    ASSERT_TRUE(output.enqueue(data));
    EXPECT_FALSE(output.enqueue(std::span(data).first(1)));
    EXPECT_EQ(output.flush(sender.get(), data.size()), FlushStatus::Blocked);
    EXPECT_GT(output.short_writes(), 0);
    EXPECT_GT(output.blocked_writes(), 0);
    EXPECT_EQ(output.flush(sender.get(), 0), FlushStatus::BudgetExhausted);
    std::vector<std::byte> received;
    std::array<std::byte, 8192> buffer{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (received.size() < data.size() && std::chrono::steady_clock::now() < deadline) {
        const auto count = ::recv(receiver.get(), buffer.data(), buffer.size(), 0);
        if (count > 0) received.insert(received.end(), buffer.begin(), buffer.begin() + count);
        ASSERT_NE(output.flush(sender.get(), 16384), FlushStatus::Error);
    }
    EXPECT_EQ(received, data);
    EXPECT_EQ(output.size(), 0);
    EXPECT_EQ(output.flush(sender.get(), 1), FlushStatus::Drained);
}

// Broken peers return a write error rather than terminating the process with SIGPIPE.
TEST(Transport, BrokenPeerIsReportedWithoutSigpipe) {
    int pair[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair), 0);
    UniqueFd sender(pair[0]);
    ASSERT_EQ(::close(pair[1]), 0);
    PendingWrites output(1);
    const std::array<std::byte, 1> data{std::byte{42}};
    ASSERT_TRUE(output.enqueue(data));
    EXPECT_EQ(output.flush(sender.get(), 1), FlushStatus::Error);
}

// Tick completion counts advance only after the final byte reaches the socket.
TEST(Transport, TickAccountingIncludesPartiallyWrittenFrames) {
    int pair[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, pair), 0);
    UniqueFd sender(pair[0]), receiver(pair[1]);
    std::array<std::byte, MAX_FRAME_SIZE> frame{};
    const auto size = encode(TickMsg{1, 1, 100, 102, 101, 0}, frame);
    PendingWrites output(MAX_FRAME_SIZE);
    ASSERT_TRUE(output.enqueue(std::span(frame).first(size), true));
    EXPECT_EQ(output.flush(sender.get(), 7), FlushStatus::BudgetExhausted);
    EXPECT_EQ(output.size(), size - 7);
    EXPECT_EQ(output.pending_ticks(), 1);
    EXPECT_EQ(output.completed_ticks(), 0);
    EXPECT_EQ(output.flush(sender.get(), MAX_FRAME_SIZE), FlushStatus::Drained);
    EXPECT_EQ(output.pending_ticks(), 0);
    EXPECT_EQ(output.completed_ticks(), 1);
}
