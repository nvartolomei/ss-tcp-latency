#include "listener.h"

#include "ss.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tcp.hh>

#include <exception>
#include <stdexcept>

ss::future<> listener::run(ss::abort_source& as) {
    _listener = ss::listen(_addr, ss::listen_options{.reuse_address = true});

    auto sub = as.subscribe([this]() noexcept { _listener.abort_accept(); });

    while (!as.abort_requested()) {
        ss::accept_result conn;
        try {
            conn = co_await _listener.accept();
        } catch (const std::runtime_error& e) {
            if (as.abort_requested()) {
                std::rethrow_exception(as.abort_requested_exception_ptr());
            } else {
                throw;
            }
        }

        std::ignore = handle_connection(as, std::move(conn.connection));
    }

    co_return;
}

ss::future<>
listener::handle_connection(ss::abort_source& as, ss::connected_socket socket) {
    auto remote_addr = socket.remote_address();
    _logger->info("Accepted connection from {}", remote_addr);

    auto gate_holder = _gate.hold();

    auto socket_istream = socket.input();
    auto socket_ostream = socket.output();

    while (!as.abort_requested()) {
        auto header_buf = co_await socket_istream.read_exactly(
          2 * sizeof(int64_t));

        if (header_buf.size() != 2 * sizeof(int64_t)) {
            if (as.abort_requested() || header_buf.empty()) {
                break;
            } else {
                throw std::runtime_error(
                  fmt::format("Partial read {}", header_buf.size()));
            }
        }

        auto payload_len = ss::read_be<int64_t>(
          header_buf.get() + sizeof(int64_t));
        auto payload_buf = co_await socket_istream.read_exactly(payload_len);

        if (_echo) {
            co_await socket_ostream.write(header_buf.share());
            co_await socket_ostream.write(std::move(payload_buf));
        } else {
            co_await socket_ostream.write(header_buf.share(0, sizeof(int64_t)));
        }
        co_await socket_ostream.flush();
    }

    _logger->info("Closed connect from {}", remote_addr);
}
