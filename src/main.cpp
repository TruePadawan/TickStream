#include "market.hpp"

#include <charconv>
#include <csignal>
#include <iostream>
#include <fstream>
#include <string>
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

    // Emits machine-readable build provenance without opening a listening socket.
    void build_info(std::ostream &out) {
        out << "{\"compiler\":\"" << TICKSTREAM_COMPILER
                << "\",\"build_type\":\"" << TICKSTREAM_BUILD_TYPE
                << "\",\"sanitizers\":" << (TICKSTREAM_SANITIZERS ? "true" : "false")
                << ",\"cxx_standard\":20}\n";
    }

    // Snapshots are cumulative; the harness derives rates using their actual timestamps.
    void write_stats(std::ostream &out, const tickstream::MarketServer &server, const char *kind) {
        const auto &market = server.stats();
        const auto &transport = server.transport_stats();
        const auto blocked = server.backpressured_clients();
        out << "{\"kind\":\"" << kind << "\",\"timestamp_ns\":" << tickstream::monotonic_ns()
                << ",\"generation_start_ns\":" << server.generation_start_ns()
                << ",\"clients\":" << server.client_count()
                << ",\"subscriptions\":" << server.subscription_count()
                << ",\"scheduled_ticks\":" << market.scheduled_ticks
                << ",\"generated_ticks\":" << market.generated_ticks
                << ",\"accepted_tick_offers\":" << market.accepted_tick_offers
                << ",\"skipped_ticks\":" << market.skipped_ticks
                << ",\"delayed_batches\":" << market.delayed_batches
                << ",\"max_generation_lateness_ns\":" << market.max_generation_lateness_ns
                << ",\"accepted\":" << transport.accepted
                << ",\"rejected\":" << transport.rejected
                << ",\"disconnected\":" << transport.disconnected
                << ",\"protocol_errors\":" << transport.protocol_errors
                << ",\"output_overflows\":" << transport.output_overflows
                << ",\"tick_queue_overflows\":" << transport.tick_queue_overflows
                << ",\"tick_frames_written\":" << transport.tick_frames_written
                << ",\"conflation_replacements\":" << transport.conflation_replacements
                << ",\"pending_bytes\":" << transport.pending_bytes
                << ",\"pending_ticks\":" << transport.pending_ticks
                << ",\"client_byte_high_water\":" << transport.client_byte_high_water
                << ",\"client_tick_high_water\":" << transport.client_tick_high_water
                << ",\"short_writes\":" << transport.short_writes
                << ",\"blocked_writes\":" << transport.blocked_writes
                << ",\"nonwritable_clients_with_output\":";
        if (blocked) out << *blocked;
        else out << "null";
        out << "}\n";
        out.flush();
    }
} // namespace

int main(int argc, char **argv) {
    try {
        // Override server defaults with validated command-line settings.
        tickstream::MarketConfig config;
        std::string stats_path;
        std::size_t stats_interval_ms = 50;
        for (int i = 1; i < argc; ++i) {
            const std::string_view option(argv[i]);
            if (option == "--build-info") {
                build_info(std::cout);
                return 0;
            }
            if (option == "--help") {
                std::cout << "Usage: tickstream_server [--address IPv4] [--port 0..65535]\n"
                        "  [--max-clients N] [--output-capacity BYTES] [--send-buffer-bytes N]\n"
                        "  [--rate TICKS_PER_SECOND] [--seed N] [--queue-capacity TICKS] [--manual-start] [--mode full|conflated]\n"
                        "  [--stats-file PATH] [--stats-interval-ms 1..10000] [--build-info]\n";
                return 0;
            }

            if (option == "--manual-start") {
                config.manual_start = true;
                continue;
            }

            if (i + 1 == argc) {
                throw std::invalid_argument("Missing option value");
            }

            const std::string_view value(argv[++i]);
            if (option == "--address") {
                config.transport.address = value;
            } else if (option == "--port") {
                const auto port = number(value);
                if (port > 65535) throw std::invalid_argument("Port exceeds 65535");
                config.transport.port = static_cast<std::uint16_t>(port);
            } else if (option == "--max-clients") config.transport.max_clients = number(value);
            else if (option == "--output-capacity") config.transport.output_capacity = number(value);
            else if (option == "--send-buffer-bytes") {
                const auto size = number(value);
                if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
                    throw std::invalid_argument("Socket buffer exceeds int range");
                }
                config.transport.send_buffer_bytes = static_cast<int>(size);
            } else if (option == "--rate") config.tick_rate = number(value);
            else if (option == "--seed") config.seed = number(value);
            else if (option == "--queue-capacity") config.transport.tick_queue_capacity = number(value);
            else if (option == "--mode") {
                if (value == "full") config.transport.mode = tickstream::DeliveryMode::Full;
                else if (value == "conflated") config.transport.mode = tickstream::DeliveryMode::Conflated;
                else throw std::invalid_argument("Mode must be full or conflated");
            } else if (option == "--stats-file") stats_path = value;
            else if (option == "--stats-interval-ms") {
                stats_interval_ms = number(value);
                if (stats_interval_ms == 0 || stats_interval_ms > 10000) {
                    throw std::invalid_argument("Stats interval must be between 1 and 10000 ms");
                }
            } else throw std::invalid_argument("Unknown option");
        }

        // Request graceful shutdown on Ctrl+C or process termination.
        if (std::signal(SIGINT, request_stop) == SIG_ERR || std::signal(SIGTERM, request_stop) == SIG_ERR) {
            throw std::runtime_error("Cannot install shutdown handlers");
        }

        // Open the listener and report the actual port, including ephemeral ports.
        tickstream::MarketServer server(config);
        // Initialize optional telemetry before advertising that startup succeeded.
        std::ofstream stats_file;
        if (!stats_path.empty()) {
            stats_file.exceptions(std::ios::badbit | std::ios::failbit);
            stats_file.open(stats_path);
            write_stats(stats_file, server, "initial");
        }
        std::cout << "Listening on " << config.transport.address << ':' << server.port() << std::endl;
        auto last_stats_ns = tickstream::monotonic_ns();
        // Service socket events, generation deadlines, and telemetry until shutdown.
        while (!stop_requested) {
            server.poll_once(stats_path.empty() ? 100 : static_cast<int>(stats_interval_ms));
            const auto now = tickstream::monotonic_ns();
            if (!stats_path.empty() && now - last_stats_ns >= stats_interval_ms * 1'000'000) {
                write_stats(stats_file, server, "periodic");
                last_stats_ns = now;
            }
        }
        if (!stats_path.empty()) write_stats(stats_file, server, "final");
        // Report generation and transport counts separately; writes do not prove client consumption.
        const auto &stats = server.transport_stats();
        const auto &market = server.stats();
        std::cout << "Stopped: accepted=" << stats.accepted << " rejected=" << stats.rejected
                << " protocol_errors=" << stats.protocol_errors
                << " generated=" << market.generated_ticks << " scheduled=" << market.scheduled_ticks
                << " skipped=" << market.skipped_ticks << " accepted_tick_offers=" << market.accepted_tick_offers
                << " delayed_batches=" << market.delayed_batches
                << " max_generation_lateness_ns=" << market.max_generation_lateness_ns
                << " tick_frames_written=" << stats.tick_frames_written
                << " replacements=" << stats.conflation_replacements
                << " queue_overflows=" << stats.tick_queue_overflows
                << " byte_overflows=" << stats.output_overflows
                << " client_tick_high_water=" << stats.client_tick_high_water
                << " client_byte_high_water=" << stats.client_byte_high_water
                << " pending_bytes=" << stats.pending_bytes << " pending_ticks=" << stats.pending_ticks
                << " blocked_writes=" << stats.blocked_writes
                << " subscriptions=" << server.subscription_count() << '\n';
    } catch (const std::exception &error) {
        // RAII releases resources while startup or event-loop errors reach this handler.
        std::cerr << "tickstream_server: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
