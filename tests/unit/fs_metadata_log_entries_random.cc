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
 * Copyright (C) 2020 ScyllaDB
 */

#include "fs/metadata_log/entries.hh"
#include "fs_metadata_log_entries_random.hh"
#include "fs_random.hh"
#include "seastar/core/temporary_buffer.hh"

using std::string;

namespace seastar::fs::metadata_log::entries {

template<class T>
static void randomize(T& x) {
    random_overwrite(&x, sizeof(x));
}

template<> checkpoint random_entry<checkpoint>() {
    checkpoint entry;
    randomize(entry.crc32_checksum);
    randomize(entry.checkpointed_data_length);
    return entry;
}

template<> next_metadata_cluster random_entry<next_metadata_cluster>() {
    next_metadata_cluster entry;
    randomize(entry.cluster_id);
    return entry;
}

template<> create_inode random_entry<create_inode>() {
    create_inode entry;
    randomize(entry.inode);
    randomize(entry.metadata.ftype);
    randomize(entry.metadata.perms);
    randomize(entry.metadata.uid);
    randomize(entry.metadata.gid);
    randomize(entry.metadata.btime_ns);
    randomize(entry.metadata.mtime_ns);
    randomize(entry.metadata.ctime_ns);
    return entry;
}

template<> delete_inode random_entry<delete_inode>() {
    delete_inode entry;
    randomize(entry.inode);
    return entry;
}

template<> small_write random_entry<small_write>() {
    return random_entry<small_write>(small_write::data_max_len);
}

template<> medium_write random_entry<medium_write>() {
    medium_write entry;
    randomize(entry.inode);
    randomize(entry.offset);
    randomize(entry.drange.beg);
    randomize(entry.drange.end);
    randomize(entry.time_ns);
    return entry;
}

template<> large_write_without_time random_entry<large_write_without_time>() {
    large_write_without_time entry;
    randomize(entry.inode);
    randomize(entry.offset);
    randomize(entry.data_cluster);
    return entry;
}

template<> large_write random_entry<large_write>() {
    large_write entry;
    entry.lwwt = random_entry<decltype(entry.lwwt)>();
    randomize(entry.time_ns);
    return entry;
}

template<> truncate random_entry<truncate>() {
    truncate entry;
    randomize(entry.inode);
    randomize(entry.size);
    randomize(entry.time_ns);
    return entry;
}

template<> create_dentry random_entry<create_dentry>() {
    return random_entry<create_dentry>(dentry_name_max_len);
}

template<> create_inode_as_dentry random_entry<create_inode_as_dentry>() {
    return random_entry<create_inode_as_dentry>(dentry_name_max_len);
}

template<> delete_dentry random_entry<delete_dentry>() {
    return random_entry<delete_dentry>(dentry_name_max_len);
}

template<> delete_inode_and_dentry random_entry<delete_inode_and_dentry>() {
    return random_entry<delete_inode_and_dentry>(dentry_name_max_len);
}

template<> small_write random_entry<small_write>(size_t max_data_size) {
    small_write entry;
    randomize(entry.inode);
    randomize(entry.offset);
    randomize(entry.time_ns);
    entry.data = decltype(entry.data)(random_value(0, max_data_size));
    random_overwrite(entry.data.get_write(), entry.data.size());
    return entry;
}

static string random_dentry_name(size_t max_dentry_len) {
    string res(random_value(0, max_dentry_len), '\0');
    random_overwrite(res.data(), res.size());
    return res;
}

template<> create_dentry random_entry<create_dentry>(size_t max_dentry_len) {
    create_dentry entry;
    randomize(entry.inode);
    entry.name = random_dentry_name(max_dentry_len);
    randomize(entry.dir_inode);
    return entry;
}

template<> create_inode_as_dentry random_entry<create_inode_as_dentry>(size_t max_dentry_len) {
    create_inode_as_dentry entry;
    entry.inode = random_entry<decltype(entry.inode)>();
    entry.name = random_dentry_name(max_dentry_len);
    randomize(entry.dir_inode);
    return entry;
}

template<> delete_dentry random_entry<delete_dentry>(size_t max_dentry_len) {
    delete_dentry entry;
    randomize(entry.dir_inode);
    entry.name = random_dentry_name(max_dentry_len);
    return entry;
}

template<> delete_inode_and_dentry random_entry<delete_inode_and_dentry>(size_t max_dentry_len) {
    delete_inode_and_dentry entry;
    entry.di = random_entry<decltype(entry.di)>();
    entry.dd = random_entry<decltype(entry.dd)>(max_dentry_len);
    return entry;
}

} // namespace seastar::fs::metadata_log::entries
