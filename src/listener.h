#pragma once

#include "ss.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tcp.hh>
#include <seastar/util/log.hh>

class listener {
public:
    listener(ss::logger* logger, ss::socket_address addr, bool echo = false)
      : _logger(logger)
      , _addr(std::move(addr))
      , _echo(echo) {}

    ss::future<> run(ss::abort_source& as);
    ss::future<> close() { return _gate.close(); }

private:
    ss::future<>
    handle_connection(ss::abort_source& as, ss::connected_socket socket);

private:
    ss::logger* _logger;
    ss::socket_address _addr;
    ss::server_socket _listener;
    ss::gate _gate;
    bool _echo;
};
