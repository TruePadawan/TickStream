#include "market.hpp"
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>

using namespace tickstream;
using namespace protocol;

namespace {
    // Drives a real client without a server thread, keeping application state inspectable.
    UniqueFd connect_client(MarketServer &server) {
        UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(server.port());
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (client.get() < 0 || ::connect(client.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
            throw std::runtime_error("Test connection failed");
        }
        server.poll_once(0);
        return client;
    }

    void request(int fd, const Message &message) {
        std::array<std::byte, MAX_FRAME_SIZE> frame{};
        const auto size = encode(message, frame);
        if (::send(fd, frame.data(), size, MSG_NOSIGNAL) != static_cast<ssize_t>(size)) {
            throw std::runtime_error("Test request failed");
        }
    }

    // Reads only one frame, preserving subsequent messages in the socket for the next call.
    std::optional<Message> receive(MarketServer &server, int fd) {
        std::array<std::byte, MAX_FRAME_SIZE> frame{};
        std::size_t used = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            server.poll_once(1);
            const auto count = ::recv(fd, frame.data() + used, 1, MSG_DONTWAIT);
            if (count == 1) {
                ++used;
                const auto result = decode(std::span(frame).first(used));
                if (result.status == DecodeStatus::Ok) return result.message;
                if (result.status == DecodeStatus::Error) return std::nullopt;
            } else if (count == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                return std::nullopt;
            }
        }
        return std::nullopt;
    }
} // namespace

// Duplicate requests are harmless, and disconnect removes every subscription for its owner.
TEST(Market, SubscriptionTableIsIdempotentAndCleansUp) {
    SubscriptionTable table;
    EXPECT_TRUE(table.subscribe(10, 1));
    EXPECT_FALSE(table.subscribe(10, 1));
    EXPECT_TRUE(table.subscribe(10, 2));
    EXPECT_TRUE(table.subscribe(20, 1));
    EXPECT_EQ(table.size(), 3);
    EXPECT_TRUE(table.unsubscribe(10, 2));
    EXPECT_FALSE(table.unsubscribe(10, 2));
    table.remove(10);
    EXPECT_EQ(table.size(), 1);
    EXPECT_TRUE(table.subscribers(1).contains(20));
    EXPECT_FALSE(table.subscribers(1).contains(10));
    EXPECT_THROW(table.subscribe(10, 0), std::invalid_argument);
    EXPECT_THROW(table.subscribers(5), std::invalid_argument);
}

// Equal seeds and generation counts reproduce price paths independent of wall-clock timing.
TEST(Market, GeneratorIsReproducibleWithPerTickerSequences) {
    TickGenerator first(42), second(42), different(43);
    std::array<std::uint64_t, 4> sequences{};
    bool different_price = false;
    for (std::size_t i = 0; i < 1000; ++i) {
        const auto tick = first.next(i);
        EXPECT_EQ(tick, second.next(i));
        const auto other = different.next(i);
        different_price |= tick.last != other.last;
        EXPECT_EQ(tick.ticker_id, i % 4 + 1);
        EXPECT_EQ(tick.seq, ++sequences[tick.ticker_id - 1]);
        EXPECT_GE(tick.bid, 0);
        EXPECT_LE(tick.bid, tick.ask);
        EXPECT_EQ(tick.timestamp_ns, i);
    }
    EXPECT_TRUE(different_price);
}

// Readiness acknowledgements precede a one-shot start; routing preserves each ticker's sequence.
TEST(Market, ManualStartRoutesOnlySubscribedTickers) {
    MarketConfig config;
    config.transport.port = 0;
    config.manual_start = true;
    config.tick_rate = 100;
    MarketServer server(config);
    auto first = connect_client(server);
    auto second = connect_client(server);
    for (const auto ticker: {1u, 2u, 1u}) {
        request(first.get(), SubscribeMsg{ticker});
        EXPECT_EQ(receive(server, first.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, ticker}}));
    }
    request(second.get(), SubscribeMsg{3});
    ASSERT_EQ(receive(server, second.get()), (std::optional<Message>{AckMsg{MsgType::Subscribe, 3}}));
    EXPECT_FALSE(server.started());
    EXPECT_EQ(server.stats().generated_ticks, 0);
    EXPECT_EQ(server.subscription_count(), 3);
    request(first.get(), StartMsg{});
    std::array<std::uint64_t, 4> seen{};
    for (int i = 0; i < 6; ++i) {
        auto message = receive(server, first.get());
        ASSERT_TRUE(message && std::holds_alternative<TickMsg>(*message));
        const auto tick = std::get<TickMsg>(*message);
        ASSERT_TRUE(tick.ticker_id == 1 || tick.ticker_id == 2);
        EXPECT_EQ(tick.seq, ++seen[tick.ticker_id - 1]);
        EXPECT_GT(tick.timestamp_ns, 0);
    }
    const auto message = receive(server, second.get());
    ASSERT_TRUE(message && std::holds_alternative<TickMsg>(*message));
    EXPECT_EQ(std::get<TickMsg>(*message).ticker_id, 3);
    EXPECT_EQ(std::get<TickMsg>(*message).seq, 1);
}

// Unsubscribe changes routing state, duplicate requests still acknowledge, and close cleans up.
TEST(Market, UnsubscribeAndDisconnectRemoveRoutingState) {
    MarketConfig config;
    config.transport.port = 0;
    config.manual_start = true;
    MarketServer server(config);
    auto client = connect_client(server);
    for (const auto ticker: {1u, 2u}) {
        request(client.get(), SubscribeMsg{ticker});
        ASSERT_TRUE(receive(server, client.get()));
    }
    for (int i = 0; i < 2; ++i) {
        request(client.get(), UnsubscribeMsg{1});
        EXPECT_EQ(receive(server, client.get()), (std::optional<Message>{AckMsg{MsgType::Unsubscribe, 1}}));
        EXPECT_EQ(server.subscription_count(), 1);
    }
    client = UniqueFd{};
    for (int i = 0; i < 10 && server.subscription_count() != 0; ++i) server.poll_once(5);
    EXPECT_EQ(server.subscription_count(), 0);
}

// Invalid global start transitions close the requesting connection and remove its routing state.
TEST(Market, StartIsAllowedOnlyOnceInManualMode) {
    for (const bool manual: {false, true}) {
        MarketConfig config;
        config.transport.port = 0;
        config.manual_start = manual;
        MarketServer server(config);
        auto client = connect_client(server);
        request(client.get(), SubscribeMsg{1});
        ASSERT_TRUE(receive(server, client.get()));
        request(client.get(), StartMsg{});
        if (manual) {
            server.poll_once(1);
            EXPECT_TRUE(server.started());
            request(client.get(), StartMsg{});
        }
        for (int i = 0; i < 10 && server.transport_stats().protocol_errors == 0; ++i) server.poll_once(5);
        EXPECT_EQ(server.transport_stats().protocol_errors, 1);
        EXPECT_EQ(server.subscription_count(), 0);
    }
}

// A delayed high-rate batch is capped and skipped work is visible instead of an unlimited catch-up loop.
TEST(Market, GenerationWorkIsBoundedAndLowRatesAccumulate) {
    MarketConfig config;
    config.transport.port = 0;
    config.tick_rate = 1'000'000;
    MarketServer server(config);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    server.poll_once(0);
    EXPECT_EQ(server.stats().generated_ticks, 256);
    EXPECT_GT(server.stats().skipped_ticks, 0);
    EXPECT_EQ(server.stats().scheduled_ticks, server.stats().generated_ticks + server.stats().skipped_ticks);
    config.tick_rate = 100;
    MarketServer low_rate(config);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    while (low_rate.stats().generated_ticks == 0 && std::chrono::steady_clock::now() < deadline) low_rate.poll_once(1);
    EXPECT_GT(low_rate.stats().generated_ticks, 0);
    EXPECT_EQ(low_rate.stats().skipped_ticks, 0);
}

// Tick limits count complete pending frames and include a partially transmitted current tick.
TEST(Market, TickQueueOverflowRemovesOnlyTheOwningSubscriptions) {
    MarketConfig config;
    config.transport.port = 0;
    config.transport.tick_queue_capacity = 1;
    config.tick_rate = 1'000'000;
    config.manual_start = true;
    MarketServer server(config);
    auto subscribed = connect_client(server);
    auto healthy = connect_client(server);
    request(subscribed.get(), SubscribeMsg{1});
    ASSERT_TRUE(receive(server, subscribed.get()));
    request(subscribed.get(), StartMsg{});
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (server.transport_stats().tick_queue_overflows == 0 && std::chrono::steady_clock::now() < deadline) server.
            poll_once(1);
    EXPECT_EQ(server.transport_stats().tick_queue_overflows, 1);
    EXPECT_EQ(server.transport_stats().client_tick_high_water, 1);
    EXPECT_EQ(server.transport_stats().pending_ticks, 0);
    EXPECT_EQ(server.subscription_count(), 0);
    request(healthy.get(), UnsubscribeMsg{4});
    EXPECT_EQ(receive(server, healthy.get()), (std::optional<Message>{AckMsg{MsgType::Unsubscribe, 4}}));
}
