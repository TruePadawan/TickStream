#ifndef TICKSTREAM_TRANSPORT_HPP
#define TICKSTREAM_TRANSPORT_HPP

#include "protocol.hpp"

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace tickstream {
    // Owns one Linux descriptor; moving transfers responsibility for closing it.
    class UniqueFd {
    public:
        explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {
        }

        ~UniqueFd();

        UniqueFd(const UniqueFd &) = delete;

        UniqueFd &operator=(const UniqueFd &) = delete;

        UniqueFd(UniqueFd &&other) noexcept;

        UniqueFd &operator=(UniqueFd &&other) noexcept;

        int get() const noexcept { return fd_; }

    private:
        int fd_;
    };

    enum class DeliveryMode { Full, Conflated };

    enum class FlushStatus { Drained, Blocked, BudgetExhausted, Error };

    // Bounded output with FIFO controls, optional tick replacement, and partial-write state.
    class PendingWrites {
    public:
        explicit PendingWrites(std::size_t capacity, DeliveryMode mode = DeliveryMode::Full)
            : capacity_(capacity), mode_(mode) {
        }

        // Ticker zero denotes control/raw bytes; a nonzero ID identifies a complete tick frame.
        // Conflation replaces only matching ticks whose first byte has not been sent.
        bool enqueue(std::span<const std::byte> bytes, std::uint32_t ticker_id = 0);

        FlushStatus flush(int fd, std::size_t budget);

        [[nodiscard]] std::size_t size() const { return size_; }
        [[nodiscard]] std::uint64_t replacements() const { return replacements_; }
        [[nodiscard]] std::size_t pending_ticks() const { return pending_ticks_; }
        [[nodiscard]] std::uint64_t completed_ticks() const { return completed_ticks_; }
        [[nodiscard]] std::size_t short_writes() const { return short_writes_; }
        [[nodiscard]] std::size_t blocked_writes() const { return blocked_writes_; }

    private:
        std::size_t capacity_;
        DeliveryMode mode_;

        struct Chunk {
            std::vector<std::byte> bytes;
            std::uint32_t ticker_id;
        };

        std::deque<Chunk> chunks_;
        std::size_t pending_ticks_ = 0;
        std::uint64_t completed_ticks_ = 0;
        std::uint64_t replacements_ = 0;
        std::size_t offset_ = 0;
        std::size_t size_ = 0;
        std::size_t short_writes_ = 0;
        std::size_t blocked_writes_ = 0;
    };

    // Transport limits and the per-client tick delivery policy.
    struct ServerConfig {
        std::string address = "127.0.0.1";
        std::uint16_t port = 9000; // Zero requests an ephemeral port for tests.
        std::size_t max_clients = 1024;
        std::size_t output_capacity = 64 * 1024;
        std::size_t tick_queue_capacity = 256; // Full mode only; includes a partially sent tick.
        DeliveryMode mode = DeliveryMode::Full;
        int send_buffer_bytes = 0; // Zero preserves the kernel default.
    };

    // Counters describe transport events, not application-level tick consumption.
    struct TransportStats {
        std::uint64_t accepted = 0;
        std::uint64_t rejected = 0;
        std::uint64_t disconnected = 0;
        std::uint64_t protocol_errors = 0;
        std::uint64_t output_overflows = 0;
        std::uint64_t tick_queue_overflows = 0;
        std::uint64_t tick_frames_written = 0;
        std::uint64_t conflation_replacements = 0;
        std::size_t pending_bytes = 0;
        std::size_t pending_ticks = 0;
        std::size_t client_byte_high_water = 0;
        std::size_t client_tick_high_water = 0;
        std::uint64_t short_writes = 0;
        std::uint64_t blocked_writes = 0;
    };

    // Single-threaded, level-triggered TCP reactor. Callbacks run inside poll_once().
    class TcpServer {
    public:
        using ClientId = std::uint64_t;
        using MessageHandler = std::function<void(TcpServer &, ClientId, const protocol::Message &)>;

        using DisconnectHandler = std::function<void(ClientId)>;

        explicit TcpServer(ServerConfig config = {}, MessageHandler handler = {},
                           DisconnectHandler on_disconnect = {});

        ~TcpServer();

        TcpServer(const TcpServer &) = delete;

        TcpServer &operator=(const TcpServer &) = delete;

        // Services one bounded batch of ready sockets; -1 waits indefinitely.
        void poll_once(int timeout_ms = -1);

        // Queues one frame, disconnecting the owner if its output capacity is exceeded.
        bool send(ClientId id, const protocol::Message &message);

        void disconnect(ClientId id);

        // Applies application-level protocol errors, such as an invalid Start state.
        void reject_protocol(ClientId id);

        std::uint16_t port() const { return port_; }
        std::size_t client_count() const { return clients_.size(); }
        const TransportStats &stats() const { return stats_; }

    private:
        struct Connection;

        void accept_ready();

        void read_ready(ClientId id);

        void write_ready(ClientId id);

        void update_interest(ClientId id);

        ServerConfig config_;
        MessageHandler handler_;
        DisconnectHandler on_disconnect_;
        UniqueFd listener_;
        UniqueFd epoll_;
        std::uint16_t port_ = 0;
        ClientId next_id_ = 1; // Token zero identifies the listener; IDs are never reused.
        std::unordered_map<ClientId, std::unique_ptr<Connection> > clients_;
        TransportStats stats_;
    };
} // namespace tickstream
#endif
