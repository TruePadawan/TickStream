#ifndef TICKSTREAM_MARKET_HPP
#define TICKSTREAM_MARKET_HPP

#include "transport.hpp"

#include <array>
#include <random>
#include <unordered_set>

namespace tickstream {
    // Returns nanoseconds from the same Linux clock used by Python latency measurements.
    std::uint64_t monotonic_ns();

    // Maps the fixed ticker catalog to client IDs without owning any sockets.
    class SubscriptionTable {
    public:
        bool subscribe(TcpServer::ClientId client, std::uint32_t ticker);

        bool unsubscribe(TcpServer::ClientId client, std::uint32_t ticker);

        void remove(TcpServer::ClientId client);

        const std::unordered_set<TcpServer::ClientId> &subscribers(std::uint32_t ticker) const;

        std::size_t size() const;

    private:
        std::array<std::unordered_set<TcpServer::ClientId>, protocol::TICKERS.size()> entries_;
    };

    // Seeded random walks with round-robin tickers and independent sequence numbers.
    class TickGenerator {
    public:
        explicit TickGenerator(std::uint64_t seed);

        protocol::TickMsg next(std::uint64_t timestamp_ns);

    private:
        std::mt19937_64 random_;
        std::array<std::int64_t, 4> prices_{1'500'000, 2'500'000, 3'000'000, 2'000'000};
        std::array<std::uint64_t, 4> sequences_{};
        std::size_t next_ticker_ = 0;
    };

    struct MarketConfig {
        ServerConfig transport;
        std::uint64_t tick_rate = 1000; // Aggregate rate across all four tickers.
        std::uint64_t seed = 1;
        bool manual_start = false;
    };

    // Scheduled opportunities include skipped work; generated ticks count actual output only.
    struct MarketStats {
        std::uint64_t scheduled_ticks = 0;
        std::uint64_t generated_ticks = 0;
        std::uint64_t enqueued_deliveries = 0;
        std::uint64_t skipped_ticks = 0;
        std::uint64_t delayed_batches = 0;
        std::uint64_t max_generation_lateness_ns = 0;
    };

    // Wires requests, subscriptions, generation deadlines, and bounded transport queues together.
    class MarketServer {
    public:
        explicit MarketServer(MarketConfig config = {});

        // Waits for I/O or the next 1 ms generation batch, then emits bounded work.
        void poll_once(int timeout_ms = 100);

        std::uint16_t port() const { return transport_.port(); }
        bool started() const { return started_; }
        std::size_t subscription_count() const { return subscriptions_.size(); }
        const MarketStats &stats() const { return stats_; }
        const TransportStats &transport_stats() const { return transport_.stats(); }

    private:
        void on_message(TcpServer &server, TcpServer::ClientId id, const protocol::Message &message);

        void start();

        void generate_due();

        MarketConfig config_;
        SubscriptionTable subscriptions_;
        TickGenerator generator_;
        MarketStats stats_;
        bool started_ = false;
        std::uint64_t next_batch_ns_ = 0;
        std::uint64_t fraction_ = 0;
        // Declared last so callbacks cannot outlive application state during destruction.
        TcpServer transport_;
    };
} // namespace tickstream
#endif
