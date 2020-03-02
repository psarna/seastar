### Parquet ###

Parquet support for Seastar is an R&D project aimed at providing
a high performance, low latency library for reading and writing
files in Parquet format.

Source files residing in this directory will be compiled only
by setting an appropriate flag.
ninja: ./configure.py --enable-experimental-parquet
CMake: -DSeastar\_EXPERIMENTAL\_PARQUET=ON
