### SeastarFS ###

SeastarFS is an R&D project aimed at providing a fully asynchronous,
log-structured, shard-friendly file system optimized for large files
and with native Seastar support.

Source files residing in this directory will be compiled only
by setting an appropriate flag.
ninja: ./configure.py --enable-experimental-fs
CMake: -DSeastar\_EXPERIMENTAL\_FS=ON
