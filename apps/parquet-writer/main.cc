/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2018 Scylladb, Ltd.
 */

#include <seastar/parquet/file_writer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <boost/algorithm/string.hpp>

#include <stdlib.h>

namespace bpo = boost::program_options;

using namespace parquet;

enum class FileType {
    numerical,
    mixed,
    nested,
    long_strings
//    all_types
};

enum class DataType {
    boolean,
    double_precision,
    floating_point,
    int8,
    int16,
    int32,
    int64
};

struct file_config {
    std::string filename;
    FileType file_type;
    format::CompressionCodec::type compression;
    int64_t rows;
    int64_t rowgroups;
};

constexpr parquet::bytes_view operator ""_bv(const char *str, size_t len) noexcept {
    return {static_cast<const uint8_t *>(static_cast<const void *>(str)), len};
}

template<typename T>
std::unique_ptr<T> box(T &&x) {
    return std::make_unique<T>(std::forward<T>(x));
}

template<typename T, typename Targ>
void vec_fill(std::vector<T> &v, Targ &&arg) {
    v.push_back(std::forward<Targ>(arg));
}

template<typename T, typename Targ, typename... Targs>
void vec_fill(std::vector<T> &v, Targ &&arg, Targs &&... args) {
    v.push_back(std::forward<Targ>(arg));
    vec_fill(v, std::forward<Targs>(args)...);
}

template<typename T, typename... Targs>
std::vector<T> vec(Targs &&... args) {
    std::vector<T> v;
    vec_fill(v, std::forward<Targs>(args)...);
    return v;
}

template<typename T>
std::vector<T> vec() {
    return std::vector<T>();
}

template<enum DataType, format::Type::type ParquetType>
void print_required(column_chunk_writer<ParquetType> &column, struct file_config config, int64_t rg);

template<>
void print_required<DataType::boolean>(column_chunk_writer<format::Type::BOOLEAN> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        bool value = (i % 2) == 0;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::double_precision>(column_chunk_writer<format::Type::DOUBLE> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        double value = i * 1.1111111;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::floating_point>(column_chunk_writer<format::Type::FLOAT> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        double value = i * 1.1111111;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::int8>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        int32_t value = (int8_t) i;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::int16>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        int32_t value = (int16_t) i;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::int32>(column_chunk_writer<format::Type::INT32> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        int32_t value = (int32_t) i;
        column_writer.put(0, 0, value);
    }
}

template<>
void print_required<DataType::int64>(column_chunk_writer<format::Type::INT64> &column_writer, struct file_config config, int64_t rg) {
    for (int64_t k = rg * config.rows; k < (rg + 1) * config.rows; k++) {
        int64_t i = k - 1;
        int64_t value = (int64_t) i;
        column_writer.put(0, 0, value);
    }
}

template<enum FileType>
void write_file(std::unique_ptr<file_writer> & fw, struct file_config config);

template<>
void write_file<FileType::numerical>(std::unique_ptr<file_writer> & fw, struct file_config config){
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        print_required<DataType::int16>(fw->column<format::Type::INT32>(0), config, rg);
        print_required<DataType::int32>(fw->column<format::Type::INT32>(1), config, rg);
        print_required<DataType::int64>(fw->column<format::Type::INT64>(2), config, rg);
        fw->flush_row_group().get0();
    }
    return;
}

template<>
void write_file<FileType::long_strings>(std::unique_ptr<file_writer> & fw, struct file_config config){
//    seastar::file_input_stream_options input_stream_options;
//    input_stream_options.read_ahead = 16;
//    seastar::file urandom = seastar::open_file_dma("/dev/urandom", seastar::open_flags::ro).get0();

    char random_bytes[100000];
    parquet::bytes_view values[100];
    for (size_t i = 0; i < 100; i++){
        values[i] = {static_cast<const uint8_t*>(static_cast<const void*>(random_bytes + 1000 * i)), 1000};
    }

    auto & column_writer = fw->column<format::Type::BYTE_ARRAY>(0);
    for (int64_t rg = 0; rg < config.rowgroups; rg++) {
        for (int64_t k = rg * config.rows / 100; k < (rg + 1) * config.rows / 100; k++) {
//            urandom.dma_read_exactly<char>(0, 100000).get();
            for (size_t i = 0; i < 100000; i++){
                random_bytes[i] = (char) rand();
            }
            for (size_t i = 0; i < 100; i++) {
                column_writer.put(0, 0, values[i]);
            }
        }
        fw->flush_row_group().get0();
    }
    return;

}

template<enum FileType>
writer_schema::schema create_schema(format::CompressionCodec::type compression);

template<>
writer_schema::schema create_schema<FileType::numerical>(format::CompressionCodec::type compression) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "int16",
                    false,
                    logical_type::INT16{},
                    {},
                    format::Encoding::PLAIN,
                    compression
            },
            primitive_node{
                    "int32",
                    false,
                    logical_type::INT32{},
                    {},
                    format::Encoding::PLAIN,
                    compression
            },
            primitive_node{
                    "int64",
                    false,
                    logical_type::INT64{},
                    {},
                    format::Encoding::PLAIN,
                    compression
            }
    )};
};

template<>
writer_schema::schema create_schema<FileType::long_strings>(format::CompressionCodec::type compression) {
    using namespace writer_schema;
    return schema{vec<node>(
            primitive_node{
                    "string",
                    false,
                    logical_type::STRING{},
                    {},
                    format::Encoding::RLE_DICTIONARY,
                    compression
            }
    )};
};

int main(int argc, char *argv[]) {
    namespace po = boost::program_options;

    seastar::app_template app;
    app.add_options()
            ("filename", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>(), "File type")
            ("rowgroups", bpo::value<int64_t>(), "Number of row groups")
            ("rows", bpo::value<int64_t>(), "Number of rows in a rowgroup")
            ("compression", bpo::value<std::string>(), "Compression of all columns");

    app.run(argc, argv, [&app] {
        auto &&config = app.configuration();
        struct file_config fc;

        if (boost::iequals(config["filetype"].as<std::string>(), "long_strings")) {
            fc.file_type = FileType::long_strings;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "mixed")) {
            fc.file_type = FileType::mixed;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "nested")) {
            fc.file_type = FileType::nested;
        } else {
            fc.file_type = FileType::numerical;
        }

        if (boost::iequals(config["compression"].as<std::string>(), "snappy")) {
            fc.compression = format::CompressionCodec::SNAPPY;
        } else if (boost::iequals(config["compression"].as<std::string>(), "gzip")) {
            fc.compression = format::CompressionCodec::GZIP;
        } else {
            fc.compression = format::CompressionCodec::UNCOMPRESSED;
        }

        fc.rowgroups = config["rowgroups"].as<int64_t>();
        fc.rows = config["rows"].as<int64_t>();
        fc.filename = config["filename"].as<std::string>();

        return seastar::async([fc] {
            writer_schema::schema writer_schema;
            switch (fc.file_type) {
                case FileType::numerical:
                    writer_schema = create_schema<FileType::numerical>(fc.compression);
                    break;
                case FileType::long_strings:
                    writer_schema = create_schema<FileType::long_strings>(fc.compression);
                    break;
                default:
                    ;
            }
            std::unique_ptr<file_writer> fw = file_writer::open(fc.filename, writer_schema).get0();
//            write_file<fc.file_type>(fw,fc);
            switch (fc.file_type) {
                case FileType::numerical:
                    write_file<FileType::numerical>(fw, fc);
                    break;
                case FileType::long_strings:
                    write_file<FileType::long_strings>(fw, fc);
                    break;
                default:
                    ;
            }
            fw->close().get0();
        });
    });
    return 0;
}