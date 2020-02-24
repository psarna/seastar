#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <stdint.h>
#include <utility>

class connection_manager {
    const uint16_t _port;
    const seastar::sstring _root_dir;
    seastar::gate _gate;

public:
    connection_manager(uint16_t port, seastar::sstring root_dir = ".")
        : _port(port), _root_dir(std::move(root_dir)) {}

    seastar::future<> start_server();

    seastar::future<> stop();

private:
    seastar::future<> handle_connection(seastar::connected_socket connection, seastar::socket_address remote_address);

    seastar::future<> handle_single_operation(seastar::input_stream<char>& input, seastar::output_stream<char>& output,
        seastar::file& file);

    seastar::future<> handle_open(seastar::input_stream<char>& input, seastar::output_stream<char>& output,
        seastar::file& file);
    seastar::future<> handle_close(seastar::output_stream<char>& output, seastar::file& file);
    seastar::future<> handle_pread(seastar::input_stream<char>& input, seastar::output_stream<char>& output,
        seastar::file& file);
    seastar::future<> handle_pwrite(seastar::input_stream<char>& input, seastar::output_stream<char>& output,
        seastar::file& file);
    seastar::future<> handle_readdir(seastar::input_stream<char>& input, seastar::output_stream<char>& output);
    seastar::future<> handle_getattr(seastar::input_stream<char>& input, seastar::output_stream<char>& output);
    seastar::future<> handle_mkdir(seastar::input_stream<char>& input, seastar::output_stream<char>& output);
    seastar::future<> handle_unlink(seastar::input_stream<char>& input, seastar::output_stream<char>& output);
    seastar::future<> handle_rename(seastar::input_stream<char>& input, seastar::output_stream<char>& output);
    seastar::future<> handle_truncate(seastar::input_stream<char>& input, seastar::output_stream<char>& output,
        seastar::file& file);
};
