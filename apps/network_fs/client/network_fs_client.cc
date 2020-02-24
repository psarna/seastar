#define FUSE_USE_VERSION 31

#include "io.hh"

#include <cassert>
#include <cstring>
#include <fuse.h>
#include <iostream>
#include <netdb.h>
#include <optional>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

using namespace std::string_literals;
using std::vector;
using std::string;
using std::cerr;
using std::cout;
using std::endl;

namespace {

template<typename ...Args>
void _debug(const char* func_name, int line, Args&&... args) {
    std::stringstream s;
    s << "\e[1;31mDEBUG \e[1;37m" << func_name << "()\e[0m \e[1;32m" << line << "\e[0m ";
    (s << ... << args) << std::endl;
    std::cerr << s.str();
}

#define DEBUG(args...) _debug(__FUNCTION__, __LINE__, ##args)

// TODO: make options more c++ like (boost?)
struct options {
    const char *host;
    const char *port;
    int show_help;
} options;

#define OPTION(t, p) { t, offsetof(struct options, p), 1 }
const struct fuse_opt option_spec[] = {
    OPTION("--host=%s", host),
    OPTION("--port=%s", port),
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    FUSE_OPT_END
};

void show_help(const char* progname) {
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
           "    --host=<s>          Host address to connect\n"
           "                        (default: \"localhost\")\n"
           "    --port=<s>          Port to use while connecting\n"
           "                        (default: 6969)\n"
           "\n");
}

template<typename F>
class final_action {
private:
    F _clean;
public:
    final_action(F clean) : _clean(std::move(clean)) {}

    final_action(const final_action&) = delete;
    final_action(final_action&&) = delete;
    final_action& operator=(const final_action&) = delete;
    final_action& operator=(final_action&&) = delete;

    ~final_action() {
        try {
            _clean();
        } catch (...) {
        }
    }
};

template<typename F>
final_action<F> finally(F f) {
    return final_action<F>(std::move(f));
}

int get_connection(const string& host, const string& port) noexcept {
    addrinfo addr_hints;
    addrinfo* addr_result;

    std::memset(&addr_hints, 0, sizeof(addrinfo));
    addr_hints.ai_family = AF_INET;
    addr_hints.ai_socktype = SOCK_STREAM;
    addr_hints.ai_protocol = IPPROTO_TCP;
    if (getaddrinfo(host.c_str(), port.c_str(), &addr_hints, &addr_result) != 0) {
        return -EINVAL;
    }
    auto clean = finally([&] {
        freeaddrinfo(addr_result);
    });

    int sock;
    // TODO: set timeout on socket for connect and reads?
    if ((sock = socket(addr_result->ai_family, addr_result->ai_socktype, addr_result->ai_protocol)) < 0) {
        return -errno;
    }

    if (connect(sock, addr_result->ai_addr, addr_result->ai_addrlen) < 0) {
        close(sock);
        return -errno;
    }

    return sock;
}

int get_server_answer(int fd) noexcept {
    int err;
    if (read_objects(fd, err)) {
        return -EIO;
    } else if (err < 0) {
        return err;
    }
    return 0;
}

int network_fs_getattr(const char* path, struct stat* stbuf,
        __attribute__((unused)) struct fuse_file_info* fi) {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });


    if ((ret = write_objects(fd, fixed_buf_string("getattr"), fixed_buf_string(path))) < 0) {
        DEBUG("error while sending request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = read_objects(fd, *stbuf)) < 0) {
        DEBUG("couldn't receive an answer, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

int network_fs_readdir(const char* path, void* buf,
        fuse_fill_dir_t filler,
        __attribute__((unused)) off_t offset,
        __attribute__((unused)) struct fuse_file_info* fi,
        __attribute__((unused)) enum fuse_readdir_flags flags) {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, fixed_buf_string("readdir"), fixed_buf_string(path))) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    std::vector<string> vec;
    if ((ret = read_objects(fd, vec)) < 0) {
        DEBUG("couldn't receive an answer, error=", std::strerror(-ret));
        return ret;
    }

    filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
    for (auto&& p : vec) {
        filler(buf, p.c_str(), NULL, 0, FUSE_FILL_DIR_PLUS);
    }

    return 0;
}

int network_fs_open_helper(const char* path, int flags) {
    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        return ret;
    }

    int fd = ret;
    auto clean = [&] {
        close(fd);
    };

    if ((ret = write_objects(fd, fixed_buf_string("open"), fixed_buf_string(path), flags)) < 0) {
        clean();
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        clean();
        return ret;
    }

    return fd;
}

int network_fs_release_helper(int fd) {
    auto clean = finally([&] {
        close(fd);
    });

    int ret;
    if ((ret = write_objects(fd, fixed_buf_string("close"))) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

int network_fs_read(const char* path, char* buf, size_t size,
        off_t offset, __attribute__((unused)) struct fuse_file_info* fi) {
    DEBUG("path=", path, ", size=", size, ", offset=", offset);

    int ret;
    if ((ret = network_fs_open_helper(path, O_RDONLY)) < 0) {
        DEBUG("couldn't open the file, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        if (network_fs_release_helper(fd) < 0) {
            DEBUG("couldn't close the file");
        }
    });

    if ((ret = write_objects(fd, fixed_buf_string("pread"), size, offset)) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    fixed_buf_string buf_container(buf, 0);
    if ((ret = read_objects(fd, buf_container)) < 0) {
        DEBUG("couldn't receive an answer, error=", std::strerror(-ret));
        return ret;
    }

    return buf_container.size;
}

int network_fs_mkdir(const char *path, mode_t mode) {
    DEBUG("path=", path, ", mode=", mode);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, fixed_buf_string("mkdir"), fixed_buf_string(path), mode)) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

int network_fs_unlink(const char *path) {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, fixed_buf_string("unlink"), fixed_buf_string(path))) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

// TODO: use flags?
int network_fs_rename(const char *from, const char *to, __attribute__((unused)) unsigned int flags) {
    DEBUG("from=", from, ", to=", to);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, fixed_buf_string("rename"), fixed_buf_string(from), fixed_buf_string(to))) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

// TODO: create function like send_single_command which sends command with parameters
// and checks server's output to limit copy pasting
int network_fs_rmdir(const char *path) {
    DEBUG("path=", path);

    int ret;
    if ((ret = get_connection(options.host, options.port)) < 0) {
        DEBUG("couldn't connect, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        close(fd);
    });

    if ((ret = write_objects(fd, fixed_buf_string("rmdir"), fixed_buf_string(path))) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

// TODO: use mode?
int network_fs_create(const char *path, __attribute__((unused)) mode_t mode,
        __attribute__((unused)) struct fuse_file_info *fi) {
    DEBUG("path=", path);

    int ret;
    if ((ret = network_fs_open_helper(path, O_CREAT)) < 0) {
        DEBUG("couldn't open the file, error=", std::strerror(-ret));
        return ret;
    }
    close(ret);

    return 0;
}

int network_fs_truncate(const char *path, off_t size, __attribute__((unused)) struct fuse_file_info *fi) {
    DEBUG("path=", path, ", size=", size);

    int ret;
    if ((ret = network_fs_open_helper(path, O_RDWR)) < 0) {
        DEBUG("couldn't open the file, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        if (network_fs_release_helper(fd) < 0) {
            DEBUG("couldn't close the file");
        }
    });

    if ((ret = write_objects(fd, fixed_buf_string("truncate"), size)) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    return 0;
}

int network_fs_write(const char *path, const char *buf, size_t size, off_t offset,
        __attribute__((unused)) struct fuse_file_info *fi) {
    DEBUG("path=", path, ", size=", size, ", offset=", offset);

    int ret;
    if ((ret = network_fs_open_helper(path, O_WRONLY)) < 0) {
        DEBUG("couldn't open the file, error=", std::strerror(-ret));
        return ret;
    }

    int fd = ret;
    auto clean = finally([&] {
        if (network_fs_release_helper(fd) < 0) {
            DEBUG("couldn't close the file");
        }
    });

    if ((ret = write_objects(fd, fixed_buf_string("pwrite"), fixed_buf_string(buf, size), size, offset)) < 0) {
        DEBUG("couldn't send request, error=", std::strerror(-ret));
        return ret;
    }

    if ((ret = get_server_answer(fd)) < 0) {
        DEBUG("initial check error, error=", std::strerror(-ret));
        return ret;
    }

    int read_data;
    if ((ret = read_objects(fd, read_data)) < 0) {
        DEBUG("couldn't receive an answer, error=", std::strerror(-ret));
        return ret;
    }

    return read_data;
}

struct fuse_operations network_fs_oper = {
    .getattr = network_fs_getattr,
    .readlink = nullptr,
    .mknod = nullptr,
    .mkdir = network_fs_mkdir,
    .unlink = network_fs_unlink,
    .rmdir = network_fs_rmdir,
    .symlink = nullptr,
    .rename = network_fs_rename,
    .link = nullptr,
    .chmod = nullptr,
    .chown = nullptr,
    .truncate = network_fs_truncate,
    .open = nullptr,
    .read = network_fs_read,
    .write = network_fs_write,
    .statfs = nullptr,
    .flush = nullptr,
    .release = nullptr,
    .fsync = nullptr,
    .setxattr = nullptr,
    .getxattr = nullptr,
    .listxattr = nullptr,
    .removexattr = nullptr,
    .opendir = nullptr,
    .readdir = network_fs_readdir,
    .releasedir = nullptr,
    .fsyncdir = nullptr,
    .init = nullptr,
    .destroy = nullptr,
    .access = nullptr,
    .create = network_fs_create,
    .lock = nullptr,
    .utimens = nullptr,
    .bmap = nullptr,
    .ioctl = nullptr,
    .poll = nullptr,
    .write_buf = nullptr,
    .read_buf = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
    .copy_file_range = nullptr,
    .lseek = nullptr,
};

}

int main(int argc, char* argv[]) {
    int ret;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    options.host = strdup("localhost");
    options.port = strdup("6969");

    if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
        return 1;

    if (options.show_help) {
        show_help(argv[0]);
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    ret = fuse_main(args.argc, args.argv, &network_fs_oper, NULL);
    fuse_opt_free_args(&args);
    return ret;
}
