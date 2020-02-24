#include "connection_manager.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

using namespace seastar;

int main(int argc, char** argv) {
    app_template app;
    namespace bpo = boost::program_options;
    app.add_options()
        ("root,r", bpo::value<sstring>()->default_value("."), "root directory")
        ("port,p", bpo::value<uint16_t>()->default_value(6969), "port to listen on");

    distributed<connection_manager> conn_handler;
    app.run(argc, argv, [&] {
        return async([&] {
            auto& args = app.configuration();
            auto& port = args["port"].as<uint16_t>();
            auto& root_dir = args["root"].as<sstring>();

            conn_handler.start(port, root_dir).get();
            engine().at_exit([&] {
                return conn_handler.stop();
            });

            conn_handler.invoke_on_all(&connection_manager::start_server).get();
        }).or_terminate();
    });
    return 0;
}
