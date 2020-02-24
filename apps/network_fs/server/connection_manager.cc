#include "connection_manager.hh"
#include "io.hh"

#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>

#include <filesystem>
#include <iostream>
#include <vector>

using namespace seastar;

namespace fs = std::filesystem;
using std::vector;
using std::cerr;
using std::endl;

namespace {

constexpr uint32_t alignment = 4096;

future<> general_handler(output_stream<char>& output, std::exception_ptr e) {
    int code = -1;
    try {
        std::rethrow_exception(e);
    } catch(fs::filesystem_error& fe) {
        cerr << "A filesystem exception: " << fe.code().message() << endl;
        code = -fe.code().value();
    } catch(...) {
        cerr << "An unknown exception" << endl;
    }

    return write_objects(output, std::move(code)).then([&output] {
        return output.flush();
    }).then([e] {
        return make_exception_future(e);
    });
}

}

seastar::future<> connection_manager::start_server() {
    seastar::listen_options lo;
    lo.reuse_address = true;
    return seastar::do_with(seastar::engine().listen(
            seastar::make_ipv4_address({_port}), lo), [this] (seastar::server_socket& listener) {
        return seastar::keep_doing([this, &listener] () {
            return listener.accept().then([this] (seastar::accept_result connection) {
                auto conn = seastar::with_gate(_gate, [this, connection = std::move(connection)] () mutable {
                    return handle_connection(
                        std::move(connection.connection), std::move(connection.remote_address));
                });
            });
        });
    });
}

seastar::future<> connection_manager::stop() {
    return _gate.close();
}

future<> connection_manager::handle_connection(connected_socket connection, socket_address remote_address) {
    cerr << "New connection from " << remote_address << endl;
    return do_with(connection.input(), connection.output(), file(),
            [this] (input_stream<char>& input, output_stream<char>& output, file& file) {
        return keep_doing([this, &input, &output, &file] {
            return handle_single_operation(input, output, file);
        }).finally([&output, &file] {
            return output.close().then([&file] {
                if (file) {
                    return file.close();
                }
                return make_ready_future<>();
            });
        });
    }).handle_exception([] (std::exception_ptr e) {
        cerr << "An error occurred: " << e << endl;
    }).finally([connection = std::move(connection), remote_address = std::move(remote_address)] {
        cerr << "Closing connection with " << remote_address << endl << endl;
    });
}

future<> connection_manager::handle_single_operation(
        input_stream<char>& input, output_stream<char>& output, file& file) {
    // read operation name and decide which operation handler to start
    return do_with(sstring(), [this, &input, &output, &file] (sstring& operation_name) {
        return read_objects(input, operation_name).then([this, &input, &output, &file, &operation_name] () {
            cerr << "operation: " << operation_name << endl;

            future<> operation = make_ready_future<>();
            // TODO: implement other operations
            if (operation_name == "open") {
                operation = handle_open(input, output, file);
            } else if (operation_name == "close") {
                operation = handle_close(output, file);
            } else if (operation_name == "pread") {
                operation = handle_pread(input, output, file);
            } else if (operation_name == "pwrite") {
                operation = handle_pwrite(input, output, file);
            } else if (operation_name == "readdir") {
                operation = handle_readdir(input, output);
            } else if (operation_name == "getattr") {
                operation = handle_getattr(input, output);
            } else if (operation_name == "mkdir") {
                operation = handle_mkdir(input, output);
            } else if (operation_name == "unlink" || operation_name == "rmdir") {
                operation = handle_unlink(input, output);
            } else if (operation_name == "rename") {
                operation = handle_rename(input, output);
            } else if (operation_name == "truncate") {
                operation = handle_truncate(input, output, file);
            // } else if (operation_name == "chmod") {
            //     operation = handle_chmod(input, output);
            // } else if (operation_name == "chown") {
            //     operation = handle_chown(input, output);
            } else {
                operation = make_exception_future<>(std::runtime_error("Invalid operation name"));
            }

            return operation;
        });
    });
}

// input format  - path:str flags:int
// output format - err:int
future<> connection_manager::handle_open(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([this, &input, &output, &file] {
        if (file) {
            return make_exception_future(std::runtime_error("file already initialized"));
        }
        return do_with(sstring(), 0, [this, &input, &output, &file] (sstring& path, int& flags) {
            return read_objects(input, path, flags).then([this, &file, &path, &flags] {
                return open_file_dma(_root_dir + path, static_cast<open_flags>(flags)).then([&file] (auto new_file) {
                    file = std::move(new_file);
                });
            }).then([&output] {
                return write_objects(output, 0);
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for open operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - empty
// output format - err:int
future<> connection_manager::handle_close(output_stream<char>& output, file& file) {
    return now().then([&output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return file.close().then([&output] {
            return write_objects(output, 0);
        }).then([&output] {
            return output.flush();
        }).then([&file] {
            // can't open a file after closing it, so we create a new file
            file = seastar::file();
            cerr << "Sent reply for close operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - count:size_t offset:off_t
// output format - err:int [buff:str]
future<> connection_manager::handle_pread(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return do_with((size_t)0, (off_t)0, [&input, &output, &file] (size_t& count, off_t& offset) {
            return read_objects(input, count, offset).then([&count, &offset, &file] () {
                return file.dma_read<char>(offset, count);
            }).then([&output] (temporary_buffer<char> read_buf) {
                return write_objects(output, 0, std::move(read_buf));
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for pread operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - buff:str count:size_t offset:off_t
// output format - err:int [size:size_t]
future<> connection_manager::handle_pwrite(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return do_with(temporary_buffer<char>(), (size_t)0, (off_t)0,
                [&input, &output, &file] (temporary_buffer<char>& buff, size_t& count, off_t& offset) {
            return read_objects(input, buff, count, offset).then([&buff, &count, &offset, &file] () {
                // TODO: alignment problem will be solved after migrating to seastar fs
                if (count % alignment || offset % alignment) {
                    return make_exception_future<size_t>(std::runtime_error("offset and count should be aligned"));
                } else if (count > buff.size()) {
                    return make_exception_future<size_t>(std::runtime_error("count is bigger than buffer"));
                }
                // TODO: it won't be needed to have aligned buffer after migrating to seastar fs
                auto tmp_buff = temporary_buffer<char>::aligned(alignment, count);
                std::memcpy(tmp_buff.get_write(), buff.get(), count);
                buff = std::move(tmp_buff);
                return file.dma_write<char>(offset, buff.get(), count).then([] (size_t write_size) {
                    return write_size;
                });
            }).then([&output] (size_t write_size) {
                return write_objects(output, 0, write_size);
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for pwrite operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - path:str
// output format - err:int size:int [name:str]{size}
future<> connection_manager::handle_readdir(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), [this, &input, &output] (sstring& path) {
        return read_objects(input, path).then([this, &path] () {
            return open_directory(_root_dir + path);
        }).then([] (file dir) {
            auto vec = make_lw_shared(vector<sstring>());
            auto listing_lmb = dir.list_directory([vec] (directory_entry de) {
                    vec->emplace_back(std::move(de.name));
                    return make_ready_future<>();
                });
            auto listing = make_lw_shared(std::move(listing_lmb));
            return listing->done().then([vec, listing, dir] {
                return std::move(*vec);
            });
        }).then([&output] (vector<sstring> vec) {
            return write_objects(output, 0, std::move(vec));
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for readdir operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - path:str
// output format - err:int [stat:stat]
future<> connection_manager::handle_getattr(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), [this, &input, &output] (sstring& path) {
        return read_objects(input, path).then([this, &path] () {
            return file_stat(_root_dir + path);
        }).then([&output] (struct stat_data stat_data) {
            auto time_point_to_timespec = [](const std::chrono::system_clock::time_point &tp) {
                auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
                auto ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(tp) -
                        std::chrono::time_point_cast<std::chrono::nanoseconds>(secs);
                return timespec{secs.time_since_epoch().count(), ns.count()};
            };
            struct stat stat;
            stat.st_dev = stat_data.device_id;
            stat.st_ino = stat_data.inode_number;
            stat.st_mode = stat_data.mode;
            stat.st_nlink = stat_data.number_of_links;
            stat.st_uid = stat_data.uid;
            stat.st_gid = stat_data.gid;
            stat.st_rdev = stat_data.rdev;
            stat.st_size = stat_data.size;
            stat.st_blksize = stat_data.block_size;
            stat.st_blocks = stat_data.allocated_size / 512;
            stat.st_atim = time_point_to_timespec(stat_data.time_accessed);
            stat.st_mtim = time_point_to_timespec(stat_data.time_modified);
            stat.st_ctim = time_point_to_timespec(stat_data.time_changed);
            return write_objects(output, 0, std::move(stat));
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for getattr operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - path:str mode:mode_t
// output format - err:int
future<> connection_manager::handle_mkdir(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), (mode_t)0, [this, &input, &output] (sstring& path, mode_t& mode) {
        return read_objects(input, path, mode).then([this, &path, &mode] () {
            return make_directory(_root_dir + path, static_cast<file_permissions>(mode));
        }).then([&output] () {
            return write_objects(output, 0);
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for mkdir operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - path:str
// output format - err:int
future<> connection_manager::handle_unlink(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), [this, &input, &output] (sstring& path) {
        return read_objects(input, path).then([this, &path] () {
            return remove_file(_root_dir + path);
        }).then([&output] () {
            return write_objects(output, 0);
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for unlink operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - old_name:str new_name:str
// output format - err:int
future<> connection_manager::handle_rename(input_stream<char>& input, output_stream<char>& output) {
    return do_with(sstring(), sstring(), [this, &input, &output] (sstring& old_name, sstring& new_name) {
        return read_objects(input, old_name, new_name).then([this, &old_name, &new_name] () {
            return rename_file(_root_dir + old_name, _root_dir + new_name);
        }).then([&output] () {
            return write_objects(output, 0);
        }).then([&output] {
            return output.flush();
        }).then([] {
            cerr << "Sent reply for rename operation" << endl;
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}

// input format  - len:off_t
// output format - err:int
future<> connection_manager::handle_truncate(input_stream<char>& input, output_stream<char>& output, file& file) {
    return now().then([&input, &output, &file] {
        if (!file) {
            return make_exception_future(std::runtime_error("file not initialized"));
        }
        return do_with((off_t)0, [&input, &output, &file] (off_t& length) {
            return read_objects(input, length).then([&file, &length] () {
                return file.truncate(length);
            }).then([&output] () {
                return write_objects(output, 0);
            }).then([&output] {
                return output.flush();
            }).then([] {
                cerr << "Sent reply for truncate operation" << endl;
            });
        });
    }).handle_exception([&output] (std::exception_ptr e) {
        return general_handler(output, std::move(e));
    });
}
