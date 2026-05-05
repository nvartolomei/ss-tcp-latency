#pragma once

#include "seastar/core/timer.hh"
#include "ss.h"

#include <seastar/core/abort_source.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tcp.hh>
#include <seastar/util/log.hh>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/extended_p_square_quantile.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/stats.hpp>

#include <chrono>

namespace {
static std::array<double, 5> quantiles = {0.5, 0.75, 0.9, 0.95, 0.99};

using accumulator_type = boost::accumulators::accumulator_set<
  double,
  boost::accumulators::stats<
    boost::accumulators::tag::extended_p_square_quantile(
      boost::accumulators::quadratic),
    boost::accumulators::tag::min,
    boost::accumulators::tag::mean,
    boost::accumulators::tag::max>>;

} // namespace

class connecter {
public:
    connecter(
      ss::logger* logger,
      ss::socket_address remote_addr,
      std::chrono::microseconds send_interval,
      int64_t send_bytes,
      bool echo = false)
      : _logger(logger)
      , _remote_addr(std::move(remote_addr))
      , _send_interval(send_interval)
      , _send_bytes(send_bytes)
      , _echo(echo)
      , _latencies(
          boost::accumulators::extended_p_square_probabilities = quantiles) {}

    ss::future<> run(ss::abort_source& as);

private:
    ss::logger* _logger;
    ss::socket_address _remote_addr;
    std::chrono::microseconds _send_interval;
    int64_t _send_bytes;
    bool _echo;

    int64_t _seq_num = 0;

    accumulator_type _latencies;
    ss::steady_clock_type::time_point _last_lats_print;
};
