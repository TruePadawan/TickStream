#include "market.hpp"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <ctime>

namespace tickstream {
    namespace {
        constexpr std::uint64_t BATCH_NS = 1'000'000;
        constexpr std::uint64_t BATCHES_PER_SECOND = 1000;
        constexpr std::uint64_t MAX_TICKS_PER_PASS = 256;

        // Validates before constructing the listener or beginning generation.
        MarketConfig validate(MarketConfig config) {
            if (config.tick_rate == 0 || config.tick_rate > 1'000'000) {
                throw std::invalid_argument("Tick rate must be between 1 and 1000000");
            }
            return config;
        }

        std::size_t ticker_index(std::uint32_t ticker) {
            if (!protocol::valid_ticker(ticker)) throw std::invalid_argument("Unknown ticker ID");
            return ticker - 1;
        }
    } // namespace

    std::uint64_t monotonic_ns() {
        timespec time{};
        if (::clock_gettime(CLOCK_MONOTONIC, &time) == -1) {
            throw std::system_error(errno, std::generic_category(), "clock_gettime");
        }
        return static_cast<std::uint64_t>(time.tv_sec) * 1'000'000'000 + static_cast<std::uint64_t>(time.tv_nsec);
    }

    bool SubscriptionTable::subscribe(TcpServer::ClientId client, std::uint32_t ticker) {
        return entries_[ticker_index(ticker)].insert(client).second;
    }

    bool SubscriptionTable::unsubscribe(TcpServer::ClientId client, std::uint32_t ticker) {
        return entries_[ticker_index(ticker)].erase(client) != 0;
    }

    void SubscriptionTable::remove(TcpServer::ClientId client) {
        for (auto &entry: entries_) entry.erase(client);
    }

    const std::unordered_set<TcpServer::ClientId> &SubscriptionTable::subscribers(std::uint32_t ticker) const {
        return entries_[ticker_index(ticker)];
    }

    std::size_t SubscriptionTable::size() const {
        std::size_t total = 0;
        for (const auto &entry: entries_) total += entry.size();
        return total;
    }

    TickGenerator::TickGenerator(std::uint64_t seed) : random_(seed) {
    }

    protocol::TickMsg TickGenerator::next(std::uint64_t timestamp_ns) {
        const auto index = next_ticker_;
        next_ticker_ = (next_ticker_ + 1) % prices_.size();
        // Modulo gives a fixed, portable mapping from mt19937_64 output to small walk steps.
        const auto step = static_cast<std::int64_t>(random_() % 21) - 10;
        prices_[index] = std::max<std::int64_t>(1, prices_[index] + step);
        if (sequences_[index] == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("Ticker sequence exhausted");
        }
        return {
            ++sequences_[index], static_cast<std::uint32_t>(index + 1),
            prices_[index] - 1, prices_[index] + 1, prices_[index], timestamp_ns
        };
    }

    MarketServer::MarketServer(MarketConfig config)
        : config_(validate(std::move(config))), generator_(config_.seed),
          transport_(config_.transport,
                     [this](TcpServer &server, auto id, const auto &message) { on_message(server, id, message); },
                     [this](auto id) { subscriptions_.remove(id); }) {
        if (!config_.manual_start) start();
    }

    void MarketServer::start() {
        started_ = true;
        generation_start_ns_ = monotonic_ns();
        next_batch_ns_ = generation_start_ns_ + BATCH_NS;
    }

    void MarketServer::on_message(TcpServer &server, TcpServer::ClientId id, const protocol::Message &message) {
        if (const auto *request = std::get_if<protocol::SubscribeMsg>(&message)) {
            subscriptions_.subscribe(id, request->ticker_id);
            server.send(id, protocol::AckMsg{protocol::MsgType::Subscribe, request->ticker_id});
        } else if (const auto *request = std::get_if<protocol::UnsubscribeMsg>(&message)) {
            subscriptions_.unsubscribe(id, request->ticker_id);
            server.send(id, protocol::AckMsg{protocol::MsgType::Unsubscribe, request->ticker_id});
        } else if (std::holds_alternative<protocol::StartMsg>(message)) {
            if (!config_.manual_start || started_) server.reject_protocol(id);
            else start();
        }
    }

    void MarketServer::generate_due() {
        const auto now = monotonic_ns();
        if (now < next_batch_ns_) return;
        const auto batches = (now - next_batch_ns_) / BATCH_NS + 1;
        stats_.max_generation_lateness_ns = std::max(stats_.max_generation_lateness_ns, now - next_batch_ns_);
        stats_.delayed_batches += batches - 1;
        next_batch_ns_ += batches * BATCH_NS;
        // Fractional credits support rates below 1000/sec without rounding them up.
        const auto credits = fraction_ + batches * config_.tick_rate;
        const auto due = credits / BATCHES_PER_SECOND;
        fraction_ = credits % BATCHES_PER_SECOND;
        const auto count = std::min(due, MAX_TICKS_PER_PASS);
        stats_.scheduled_ticks += due;
        stats_.skipped_ticks += due - count;
        for (std::uint64_t i = 0; i < count; ++i) {
            const auto tick = generator_.next(monotonic_ns());
            ++stats_.generated_ticks;
            const auto &recipients = subscriptions_.subscribers(tick.ticker_id);
            for (auto it = recipients.begin(); it != recipients.end();) {
                // Queue overflow can synchronously erase this subscriber during send().
                const auto id = *it++;
                if (transport_.send(id, tick)) ++stats_.accepted_tick_offers;
            }
        }
    }

    void MarketServer::poll_once(int timeout_ms) {
        if (started_) {
            const auto now = monotonic_ns();
            const int deadline_ms = now >= next_batch_ns_
                                        ? 0
                                        : static_cast<int>((next_batch_ns_ - now + 999'999) / 1'000'000);
            if (timeout_ms < 0 || deadline_ms < timeout_ms) timeout_ms = deadline_ms;
        }
        transport_.poll_once(timeout_ms);
        if (started_) generate_due();
    }
} // namespace tickstream
