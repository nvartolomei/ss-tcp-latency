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
        auto buf = co_await socket_istream.read_exactly(sizeof(int64_t));

        if (buf.size() != sizeof(int64_t)) {
            if (as.abort_requested() || buf.empty()) {
                break;
            } else {
                throw std::runtime_error(
                  fmt::format("Partial read {}", buf.size()));
            }
        }

        auto seq_num = ss::read_be<int64_t>(buf.get());

        auto out_buf = ss::temporary_buffer<char>(sizeof(int64_t));
        ss::write_be(out_buf.get_write(), seq_num);
        co_await socket_ostream.write(out_buf.share());
        co_await socket_ostream.flush();
    }

    _logger->info("Closed connect from {}", remote_addr);
}
