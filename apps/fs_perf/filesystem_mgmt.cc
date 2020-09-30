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

#include "filesystem_mgmt.hh"

#include <boost/process.hpp>
#include <fmt/format.h>
#include <seastar/util/log.hh>
#include <sys/statvfs.h>
#include <unistd.h>

namespace {
seastar::logger mlogger("filesystem_mgmt");
}

namespace bp = boost::process;

void unmount(const std::string& mount_point) {
    try {
        bp::system(fmt::format("umount {}", mount_point), bp::std_out > bp::null, bp::std_err > bp::null);
    } catch (...) {
        mlogger.warn("Error while unmounting {}", mount_point);
    }
}

namespace {

void mkfs_xfs(const std::string& device_path) {
    unmount(device_path);
    if (bp::system(fmt::format("mkfs.xfs -f {}", device_path), bp::std_out > bp::null, bp::std_err > bp::null)) {
        throw std::runtime_error(fmt::format("failed to mkfs xfs on {}", device_path));
    }
}

void mkfs_ext4(const std::string& device_path) {
    unmount(device_path);
    if (bp::system(fmt::format("mkfs.ext4 -F {} -E root_owner={}:{}", device_path, getuid(), getgid()),
            bp::std_out > bp::null, bp::std_err > bp::null)) {
        throw std::runtime_error(fmt::format("failed to mkfs ext4 on {}", device_path));
    }
}

} // namespace

void mkfs(const std::string& device_path, filesystem_type fs_type) {
    switch (fs_type) {
    case filesystem_type::XFS:
        return mkfs_xfs(device_path);
    case filesystem_type::EXT4:
        return mkfs_ext4(device_path);
    }
    __builtin_unreachable();
}

void mount(const std::string& device_path, const std::string& mount_point) {
    if (bp::system(fmt::format("sudo mount {} {}", device_path, mount_point), bp::std_out > bp::null, bp::std_err > bp::null)) {
        throw std::runtime_error(fmt::format("Couldn't mount {} at {}", device_path, mount_point));
    }
}

size_t filesystem_remaining_space(const std::string& path) {
    struct statvfs stat;
    if (statvfs(path.c_str(), &stat)) {
        throw std::runtime_error(fmt::format("Couldn't get information about filesystem, path: '{}' error: '{}'",
                path, strerror(errno)));
    }
    return stat.f_bsize * stat.f_bavail;
}