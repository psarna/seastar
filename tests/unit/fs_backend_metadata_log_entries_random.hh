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

#pragma once

#include "fs/backend/metadata_log/entries.hh"
#include "fs_random.hh"

namespace seastar::fs::backend::metadata_log::entries {

template<class T>
T random_entry() = delete;

template<> checkpoint random_entry<checkpoint>();
template<> next_metadata_cluster random_entry<next_metadata_cluster>();
template<> create_inode random_entry<create_inode>();
template<> delete_inode random_entry<delete_inode>();
template<> small_write random_entry<small_write>();
template<> medium_write random_entry<medium_write>();
template<> large_write_without_time random_entry<large_write_without_time>();
template<> large_write random_entry<large_write>();
template<> truncate random_entry<truncate>();
template<> create_dentry random_entry<create_dentry>();
template<> create_inode_as_dentry random_entry<create_inode_as_dentry>();
template<> delete_dentry random_entry<delete_dentry>();
template<> delete_inode_and_dentry random_entry<delete_inode_and_dentry>();

template<class T>
T random_entry(size_t) = delete;

template<> small_write random_entry<small_write>(size_t max_data_size);
template<> create_dentry random_entry<create_dentry>(size_t max_dentry_len);
template<> create_inode_as_dentry random_entry<create_inode_as_dentry>(size_t max_dentry_len);
template<> delete_dentry random_entry<delete_dentry>(size_t max_dentry_len);
template<> delete_inode_and_dentry random_entry<delete_inode_and_dentry>(size_t max_dentry_len);

} // namespace seastar::fs::backend::metadata_log::entries
