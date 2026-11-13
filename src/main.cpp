#include "transport.hpp"

#include <charconv>
#include <csignal>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace {
    volatile std::sig_atomic_t stop_requested = 0;

    // Signal handlers only set a flag; resource cleanup runs in normal control flow.
    void request_stop(int) { stop_requested = 1; }

    // Rejects partial, negative, and overflowing numeric command-line values.
    std::size_t number(std::string_view text) {
        std::size_t value = 0;
        const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
        if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
            throw std::invalid_argument("Expected an unsigned integer");
        }
        return value;
    }
} // namespace

int main(int argc, char **argv) {
    try {
        tickstream::ServerConfig config;
        for (int i = 1; i < argc; ++i) {
            const std::string_view option(argv[i]);
            if (option == "--help") {
                std::cout << "Usage: tickstream_server [--address IPv4] [--port 0..65535]\n"
                        "  [--max-clients N] [--output-capacity BYTES] [--send-buffer-bytes N]\n";
                return 0;
            }

            if (i + 1 == argc) {
                throw std::invalid_argument("Missing option value");
            }

            const std::string_view value(argv[++i]);
            if (option == "--address") {
                config.address = value;
            } else if (option == "--port") {
                const auto port = number(value);
                if (port > 65535) throw std::invalid_argument("Port exceeds 65535");
                config.port = static_cast<std::uint16_t>(port);
            } else if (option == "--max-clients") config.max_clients = number(value);
            else if (option == "--output-capacity") config.output_capacity = number(value);
            else if (option == "--send-buffer-bytes") {
                const auto size = number(value);
                if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
                    throw std::invalid_argument("Socket buffer exceeds int range");
                }
                config.send_buffer_bytes = static_cast<int>(size);
            } else throw std::invalid_argument("Unknown option");
        }

        if (std::signal(SIGINT, request_stop) == SIG_ERR || std::signal(SIGTERM, request_stop) == SIG_ERR) {
            throw std::runtime_error("Cannot install shutdown handlers");
        }

        tickstream::TcpServer server(config);
        std::cout << "Listening on " << config.address << ':' << server.port() << std::endl;
        while (!stop_requested) server.poll_once(100);
        const auto &stats = server.stats();
        std::cout << "Stopped: accepted=" << stats.accepted << " rejected=" << stats.rejected
                << " protocol_errors=" << stats.protocol_errors << '\n';
    } catch (const std::exception &error) {
        std::cerr << "tickstream_server: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
