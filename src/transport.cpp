#include "transport.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tickstream {
    namespace {
        constexpr std::size_t READ_CAPACITY = 4096;
        constexpr std::size_t READ_BUDGET = 4096;
        constexpr std::size_t WRITE_BUDGET = 64 * 1024;
        constexpr int ACCEPT_BUDGET = 32;

        // Captures errno immediately so cleanup cannot replace the original failure.
        [[noreturn]] void system_failure(const char *operation) {
            throw std::system_error(errno, std::generic_category(), operation);
        }

        void set_option(int fd, int level, int option, int value) {
            if (::setsockopt(fd, level, option, &value, sizeof(value)) == -1) system_failure("setsockopt");
        }

        // Checks descriptor creation before a later syscall can overwrite errno.
        UniqueFd checked_fd(int fd, const char *operation) {
            if (fd < 0) system_failure(operation);
            return UniqueFd(fd);
        }
    } // namespace

    UniqueFd::~UniqueFd() {
        if (fd_ >= 0) ::close(fd_); // On Linux, do not retry close after EINTR.
    }

    UniqueFd::UniqueFd(UniqueFd &&other) noexcept : fd_(std::exchange(other.fd_, -1)) {
    }

    UniqueFd &UniqueFd::operator=(UniqueFd &&other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) ::close(fd_);
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    bool PendingWrites::enqueue(std::span<const std::byte> bytes) {
        if (bytes.size() > capacity_ - size_) return false;
        if (!bytes.empty()) {
            chunks_.emplace_back(bytes.begin(), bytes.end());
            size_ += bytes.size();
        }
        return true;
    }

    FlushStatus PendingWrites::flush(int fd, std::size_t budget) {
        // Bound syscall attempts as well as bytes so repeated EINTR cannot monopolize a pass.
        for (std::size_t attempts = 0; !chunks_.empty() && budget > 0 && attempts < 1024; ++attempts) {
            const auto &chunk = chunks_.front();
            const auto requested = std::min(chunk.size() - offset_, budget);
            const auto sent = ::send(fd, chunk.data() + offset_, requested, MSG_NOSIGNAL);
            if (sent < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    ++blocked_writes_;
                    return FlushStatus::Blocked;
                }
                return FlushStatus::Error;
            }
            if (sent == 0) return FlushStatus::Error;
            const auto count = static_cast<std::size_t>(sent);
            if (count < requested) ++short_writes_;
            offset_ += count;
            size_ -= count;
            budget -= count;
            if (offset_ == chunk.size()) {
                chunks_.pop_front();
                offset_ = 0;
            }
        }
        return chunks_.empty() ? FlushStatus::Drained : FlushStatus::BudgetExhausted;
    }

    // Connection state owns all unread bytes and unsent transport frames for one peer.
    struct TcpServer::Connection {
        UniqueFd socket;
        std::vector<std::byte> input;
        PendingWrites output;
        bool read_closed = false;

        Connection(UniqueFd fd, std::size_t capacity) : socket(std::move(fd)), output(capacity) {
            input.reserve(READ_CAPACITY);
        }
    };

    TcpServer::TcpServer(ServerConfig config, MessageHandler handler)
        : config_(std::move(config)), handler_(std::move(handler)),
          listener_(checked_fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0), "socket")),
          epoll_(checked_fd(::epoll_create1(EPOLL_CLOEXEC), "epoll_create1")) {
        if (config_.max_clients == 0 || config_.output_capacity < protocol::MAX_FRAME_SIZE || config_.send_buffer_bytes
            < 0) {
            throw std::invalid_argument("Invalid connection or buffer limit");
        }
        set_option(listener_.get(), SOL_SOCKET, SO_REUSEADDR, 1);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(config_.port);
        if (::inet_pton(AF_INET, config_.address.c_str(), &address.sin_addr) != 1) {
            throw std::invalid_argument("Expected an IPv4 address");
        }
        if (::bind(listener_.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == -1)
            system_failure("bind");
        if (::listen(listener_.get(), SOMAXCONN) == -1) system_failure("listen");
        socklen_t length = sizeof(address);
        if (::getsockname(listener_.get(), reinterpret_cast<sockaddr *>(&address), &length) == -1) system_failure(
            "getsockname");
        port_ = ntohs(address.sin_port);
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.u64 = 0;
        if (::epoll_ctl(epoll_.get(), EPOLL_CTL_ADD, listener_.get(), &event) == -1) system_failure(
            "epoll add listener");
    }

    TcpServer::~TcpServer() = default;

    void TcpServer::accept_ready() {
        for (int attempt = 0; attempt < ACCEPT_BUDGET; ++attempt) {
            UniqueFd socket(::accept4(listener_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
            if (socket.get() < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR || errno == ECONNABORTED) continue;
                system_failure("accept4");
            }
            if (clients_.size() >= config_.max_clients || next_id_ == std::numeric_limits<ClientId>::max()) {
                ++stats_.rejected;
                continue;
            }
            try {
                set_option(socket.get(), IPPROTO_TCP, TCP_NODELAY, 1);
                if (config_.send_buffer_bytes > 0) set_option(socket.get(), SOL_SOCKET, SO_SNDBUF,
                                                              config_.send_buffer_bytes);
            } catch (const std::system_error &) {
                ++stats_.rejected;
                continue; // A failed peer setup must not stop the listener.
            }
            const auto id = next_id_++;
            auto connection = std::make_unique<Connection>(std::move(socket), config_.output_capacity);
            epoll_event event{};
            event.events = EPOLLIN | EPOLLRDHUP;
            event.data.u64 = id;
            if (::epoll_ctl(epoll_.get(), EPOLL_CTL_ADD, connection->socket.get(), &event) == -1) system_failure(
                "epoll add client");
            clients_.emplace(id, std::move(connection));
            ++stats_.accepted;
        }
    }

    void TcpServer::disconnect(ClientId id) {
        const auto found = clients_.find(id);
        if (found == clients_.end()) return;
        // Closing the descriptor removes its epoll registration. Monotonic IDs reject stale events.
        clients_.erase(found);
        ++stats_.disconnected;
    }

    void TcpServer::update_interest(ClientId id) {
        const auto found = clients_.find(id);
        if (found == clients_.end()) return;
        auto &connection = *found->second;
        if (connection.read_closed && connection.output.size() == 0) {
            disconnect(id);
            return;
        }
        epoll_event event{};
        event.events = connection.read_closed ? 0u : static_cast<std::uint32_t>(EPOLLIN | EPOLLRDHUP);
        if (connection.output.size() > 0) event.events |= EPOLLOUT;
        event.data.u64 = id;
        if (::epoll_ctl(epoll_.get(), EPOLL_CTL_MOD, connection.socket.get(), &event) == -1) system_failure(
            "epoll modify client");
    }

    bool TcpServer::send(ClientId id, const protocol::Message &message) {
        const auto found = clients_.find(id);
        if (found == clients_.end()) return false;
        std::array<std::byte, protocol::MAX_FRAME_SIZE> frame{};
        const auto size = protocol::encode(message, frame);
        if (!found->second->output.enqueue(std::span(frame).first(size))) {
            ++stats_.output_overflows;
            disconnect(id);
            return false;
        }
        update_interest(id);
        return true;
    }

    void TcpServer::read_ready(ClientId id) {
        std::size_t budget = READ_BUDGET;
        for (int attempts = 0; budget > 0 && attempts < 128; ++attempts) {
            const auto found = clients_.find(id);
            if (found == clients_.end() || found->second->read_closed) return;
            auto &connection = *found->second;
            std::array<std::byte, READ_CAPACITY> buffer{};
            const auto capacity = std::min(budget, READ_CAPACITY - connection.input.size());
            const auto received = ::recv(connection.socket.get(), buffer.data(), capacity, 0);
            if (received < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                disconnect(id);
                return;
            }
            if (received == 0) {
                if (!connection.input.empty()) {
                    ++stats_.protocol_errors;
                    disconnect(id);
                } else {
                    connection.read_closed = true;
                    update_interest(id); // Preserve queued replies after a clean write-half shutdown.
                }
                return;
            }
            const auto count = static_cast<std::size_t>(received);
            budget -= count;
            connection.input.insert(connection.input.end(), buffer.begin(), buffer.begin() + count);
            while (clients_.contains(id)) {
                auto &current = *clients_.at(id);
                const auto result = protocol::decode(current.input);
                if (result.status == protocol::DecodeStatus::NeedMoreData) break;
                if (result.status == protocol::DecodeStatus::Error ||
                    std::holds_alternative<protocol::TickMsg>(*result.message) ||
                    std::holds_alternative<protocol::AckMsg>(*result.message)) {
                    ++stats_.protocol_errors;
                    disconnect(id);
                    return;
                }
                current.input.erase(current.input.begin(), current.input.begin() + result.consumed);
                // Handlers may enqueue output or disconnect this client; re-find state afterward.
                if (handler_) handler_(*this, id, *result.message);
            }
        }
    }

    void TcpServer::write_ready(ClientId id) {
        const auto found = clients_.find(id);
        if (found == clients_.end()) return;
        auto &output = found->second->output;
        const auto short_before = output.short_writes();
        const auto blocked_before = output.blocked_writes();
        const auto status = output.flush(found->second->socket.get(), WRITE_BUDGET);
        stats_.short_writes += output.short_writes() - short_before;
        stats_.blocked_writes += output.blocked_writes() - blocked_before;
        if (status == FlushStatus::Error) {
            disconnect(id);
            return;
        }
        update_interest(id);
    }

    void TcpServer::poll_once(int timeout_ms) {
        std::array<epoll_event, 64> events{};
        const int count = ::epoll_wait(epoll_.get(), events.data(), static_cast<int>(events.size()), timeout_ms);
        if (count < 0) {
            if (errno == EINTR) return;
            system_failure("epoll_wait");
        }
        for (int i = 0; i < count; ++i) {
            const auto id = events[i].data.u64;
            if (id == 0) {
                accept_ready();
                continue;
            }
            if (!clients_.contains(id)) continue;
            if (events[i].events & EPOLLERR) {
                disconnect(id);
                continue;
            }
            if (events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) read_ready(id);
            // Try replies immediately; EPOLLOUT handles any remainder on later passes.
            write_ready(id);
        }
    }
} // namespace tickstream
