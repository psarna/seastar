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

#include <exception>

namespace seastar::fs {

struct fs_exception : public std::exception {
    const char* what() const noexcept override = 0;
};

struct cluster_size_too_small_to_perform_operation_exception : public std::exception {
    const char* what() const noexcept override { return "Cluster size is too small to perform operation"; }
};

struct invalid_inode_exception : public fs_exception {
    const char* what() const noexcept override { return "Invalid inode"; }
};

struct invalid_argument_exception : public fs_exception {
    const char* what() const noexcept override { return "Invalid argument"; }
};

struct operation_became_invalid_exception : public fs_exception {
    const char* what() const noexcept override { return "Operation became invalid"; }
};

struct no_more_space_exception : public fs_exception {
    const char* what() const noexcept override { return "No more space on device"; }
};

struct file_already_exists_exception : public fs_exception {
    const char* what() const noexcept override { return "File already exists"; }
};

struct filename_too_long_exception : public fs_exception {
    const char* what() const noexcept override { return "Filename too long"; }
};

struct is_directory_exception : public fs_exception {
    const char* what() const noexcept override { return "Is a directory"; }
};

struct directory_not_empty_exception : public fs_exception {
    const char* what() const noexcept override { return "Directory is not empty"; }
};

struct cannot_modify_root_exception : public fs_exception {
    const char* what() const noexcept override { return "Cannot modify the root"; }
};

struct file_used_on_unintended_shard_exception : public fs_exception {
    const char* what() const noexcept override { return "File used on unintended shard"; }
};

struct invalid_cluster_range_exception : public fs_exception {
    const char* what() const noexcept override { return "Invalid cluster range"; }
};

struct too_little_available_clusters_exception : public fs_exception {
    const char* what() const noexcept override { return "Too little available clusters"; }
};

struct file_has_been_closed_exception : public fs_exception {
    const char* what() const noexcept override { return "File has been closed"; }
};

struct filesystem_has_not_been_invalidated_exception : public fs_exception {
    const char* what() const noexcept override { return "File system has not been invalidated"; }

struct read_only_filesystem_exception : public fs_exception {
    const char* what() const noexcept override { return "Read only file system"; }
};

struct path_lookup_exception : public fs_exception {
    const char* what() const noexcept override = 0;
};

struct path_is_not_absolute_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "Path is not absolute"; }
};

struct invalid_path_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "Path is invalid"; }
};

struct no_such_file_or_directory_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "No such file or directory"; }
};

struct path_component_not_directory_exception : public path_lookup_exception {
    const char* what() const noexcept override { return "A component used as a directory is not a directory"; }
};

} // namespace seastar::fs
