/* Compile with:
      gcc fuse_block_device.c -o fuse_block_device `pkg-config fuse3 --cflags --libs`
*/

#define FUSE_USE_VERSION 31

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <linux/fs.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse() would attempt to free() them when the user specifies
 * different values on the command line.
 */
static struct options {
    const char *blockdev;
    const char *filename;
    int show_help;
} options;

#define OPTION(t, p) { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
    OPTION("--blockdev=%s", blockdev),
    OPTION("--filename=%s", filename),
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    FUSE_OPT_END
};

static ssize_t get_blockdev_size(const char *path) {
    int fd, ret;
    ssize_t res = 0;

    ret = open(path, O_RDONLY);
    if (ret == -1) {
        return -errno;
    }
    fd = ret;

    ret = ioctl(fd, BLKGETSIZE64, &res);
    if (ret == -1) {
        res = -errno;
    }

    close(fd);
    return res;
}

static int block_getattr(const char *path, struct stat *stbuf,
        struct fuse_file_info *fi) {
    (void) fi;
    int res = 0;
    ssize_t blockdev_size;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    } else if (strcmp(path + 1, options.filename) == 0) {
        blockdev_size = get_blockdev_size(options.blockdev);
        if (blockdev_size >= 0) {
            stbuf->st_size = blockdev_size;
            stbuf->st_mode = S_IFREG | 0444;
            stbuf->st_nlink = 1;
        } else {
            res = blockdev_size;
        }
    } else {
        res = -ENOENT;
    }

    return res;
}

static int block_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi,
        enum fuse_readdir_flags flags) {
    (void) offset;
    (void) fi;
    (void) flags;

    if (strcmp(path, "/") != 0) {
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
    filler(buf, options.filename, NULL, 0, 0);

    return 0;
}

static int block_open(const char *path, struct fuse_file_info *fi) {
    int fd;

    if (strcmp(path + 1, options.filename) != 0) {
        return -ENOENT;
    }

    if ((fi->flags & O_ACCMODE) != O_RDONLY) {
        return -EACCES;
    }

    fd = open(options.blockdev, fi->flags);
    if (fd == -1) {
        return -errno;
    }
    fi->fh = fd;

    return 0;
}

static int block_release(const char *path, struct fuse_file_info *fi) {
    (void) path;
    close(fi->fh);
    return 0;
}

static int block_read(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi) {
    int fd, res;

    if (strcmp(path + 1, options.filename) != 0) {
        return -ENOENT;
    }

    if (fi == NULL) {
        fd = open(path, O_RDONLY);
    } else {
        fd = fi->fh;
    }

    if (fd == -1) {
        return -errno;
    }

    res = pread(fd, buf, size, offset);
    if (res == -1) {
        res = -errno;
    }

    if (fi == NULL) {
        close(fd);
    }
    return res;
}

static struct fuse_operations block_oper = {
    .getattr    = block_getattr,
    .readdir    = block_readdir,
    .open       = block_open,
    .release    = block_release,
    .read       = block_read,
};

static void show_help(const char *progname) {
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
           "    --blockdev=<s>      Path to a block device\n"
           "                        (default: \"/dev/nvme0n1\")\n"
           "    --filename=<s>      Name of the file exposed in the filesystem\n"
           "                        (default: \"blockdev\")\n"
           "\n");
}

int main(int argc, char *argv[]) {
    int ret;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    /* Set defaults -- we have to use strdup() so that
       fuse_opt_parse() can free the defaults if other
       values are specified */
    options.blockdev = strdup("/dev/nvme0n1");
    options.filename = strdup("blockdev");

    /* Parse options */
    if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1) {
        return 1;
    }

    /* When --help is specified, first print our own file-system
       specific help text, then signal fuse_main() to show
       additional help (by adding `--help` to the options again).
       Setting argv[0] to the empty string guarantees that line
       usage: /path/to/exec [options] <mountpoint>
       will be printed only in show_help() function, not in both
       show_help() and fuse_main(). */
    if (options.show_help) {
        show_help(argv[0]);
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    ret = fuse_main(args.argc, args.argv, &block_oper, NULL);
    fuse_opt_free_args(&args);
    return ret;
}
