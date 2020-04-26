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

#include <seastar/parquet/file_reader.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>

#include <boost/algorithm/string.hpp>

#include <unistd.h>

namespace bpo = boost::program_options;

constexpr size_t BATCH_SIZE = 1000;

enum class FileType {
    numerical,
    mixed,
    nested,
    long_strings,
    all_types
};

template <parquet::format::Type::type T>
void readColumn(parquet::file_reader &fr, int row, int column) {
    using OutputType = typename parquet::value_decoder_traits<T>::output_type;

    auto column_reader = fr.open_column_chunk_reader<T>(row, column).get0();
    OutputType values[BATCH_SIZE];
    int16_t def_levels[BATCH_SIZE];
    int16_t rep_levels[BATCH_SIZE];
    while (int64_t rows_read = column_reader.read_batch(BATCH_SIZE, def_levels, rep_levels, (OutputType *) values).get0()) {
        ;
    }
}

template void readColumn<parquet::format::Type::BOOLEAN>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::INT32>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::INT64>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::INT96>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::FLOAT>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::DOUBLE>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::BYTE_ARRAY>(parquet::file_reader &fr, int row, int column);
template void readColumn<parquet::format::Type::FIXED_LEN_BYTE_ARRAY>(parquet::file_reader &fr, int row, int column);

int main(int argc, char *argv[]) {
    using namespace parquet;
    seastar::app_template app;
    app.add_options()
            ("file", bpo::value<std::string>(), "Parquet file path")
            ("filetype", bpo::value<std::string>(), "File type");

    app.run(argc, argv, [&app] {
        auto &&config = app.configuration();

        FileType file_type;
        if (boost::iequals(config["filetype"].as<std::string>(), "long_strings")) {
            file_type = FileType::long_strings;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "mixed")) {
            file_type = FileType::mixed;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "nested")) {
            file_type = FileType::nested;
        } else if (boost::iequals(config["filetype"].as<std::string>(), "numerical")) {
            file_type = FileType::numerical;
        } else {
            file_type = FileType::all_types;
        }

        if (sleep(10) != 0){
            throw std::runtime_error("mean signal");
        }

        std::string file = config["file"].as<std::string>();
        return seastar::async([file, file_type] {
            auto fr = file_reader::open(file).get0();
            int num_row_groups = fr.metadata().row_groups.size();
            for (int r = 0; r < num_row_groups; ++r) {
                switch (file_type) {
                    case FileType::long_strings:
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 0);
                        break;
                    case FileType::mixed:
                        readColumn<format::Type::BOOLEAN>(fr, r, 0);
                        readColumn<format::Type::INT32>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, 3);
                        break;
                    case FileType::nested:
                        readColumn<format::Type::INT64>(fr, r, 0);
                        readColumn<format::Type::INT64>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 3);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 4);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, 5);
                        break;
                    case FileType::numerical:
                        readColumn<format::Type::INT32>(fr, r, 0);
                        readColumn<format::Type::INT32>(fr, r, 1);
                        readColumn<format::Type::INT64>(fr, r, 2);
                        break;
                    case FileType::all_types:
                        int i = 0;

                        readColumn<format::Type::BOOLEAN>(fr, r, i++);
                        readColumn<format::Type::BOOLEAN>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);

                        readColumn<format::Type::INT96>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);

                        readColumn<format::Type::FLOAT>(fr, r, i++);
                        readColumn<format::Type::FLOAT>(fr, r, i++);

                        readColumn<format::Type::DOUBLE>(fr, r, i++);
                        readColumn<format::Type::DOUBLE>(fr, r, i++);

                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);

                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);
                        readColumn<format::Type::INT64>(fr, r, i++);

                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);
                        readColumn<format::Type::BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::FIXED_LEN_BYTE_ARRAY>(fr, r, i++);

                        readColumn<format::Type::INT64>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::BOOLEAN>(fr, r, i++);
                        readColumn<format::Type::BOOLEAN>(fr, r, i++);

                        readColumn<format::Type::INT32>(fr, r, i++);
                        readColumn<format::Type::INT32>(fr, r, i++);

                        readColumn<format::Type::FLOAT>(fr, r, i++);
                        readColumn<format::Type::DOUBLE>(fr, r, i++);

                        break;
                }
            }
        });
    });
    return 0;
}
