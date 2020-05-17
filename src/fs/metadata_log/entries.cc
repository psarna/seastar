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
#include "fs/unix_metadata.hh"
#include "seastar/core/temporary_buffer.hh"

using std::optional;
using std::string_view;

namespace seastar::fs::metadata_log::entries {

enum ondisk_type : uint8_t {
    INVALID = 0,
    CHECKPOINT,
    NEXT_METADATA_CLUSTER,
    CREATE_INODE,
    DELETE_INODE,
    SMALL_WRITE,
    MEDIUM_WRITE,
    LARGE_WRITE_WITHOUT_MTIME,
    LARGE_WRITE,
    TRUNCATE,
    CREATE_DENTRY,
    CREATE_INODE_AS_DENTRY,
    DELETE_DENTRY,
    DELETE_INODE_AND_DENTRY,
};

template<class T, std::enable_if_t<std::is_standard_layout_v<T>, int> = 0>
static void read_val(string_view& buff, T& val) noexcept {
    assert(buff.size() >= sizeof(val));
    std::memcpy(&val, buff.data(), sizeof(val));
    buff.remove_prefix(sizeof(val));
}

template<class T, std::enable_if_t<std::is_standard_layout_v<T>, int> = 0>
static void write_val(void*& dest, const T& val) noexcept {
    std::memcpy(dest, &val, sizeof(val));
    dest = reinterpret_cast<uint8_t*>(dest) + sizeof(val);
}

template<class size_type>
static disk_offset_t ondisk_bstr_size(size_t data_len) noexcept {
    return sizeof(size_type) + data_len;
}

template<class size_type, class Dest>
[[nodiscard]] static optional<read_error> read_bstr(string_view& buff, Dest& data) noexcept {
    size_type len;
    read_val(buff, len);
    if (buff.size() < len) {
        return TOO_SMALL;
    }

    try {
        using CharT = std::remove_const_t<std::remove_reference_t<decltype(data[0])>>;
        static_assert(sizeof(CharT) == sizeof(char));
        data = Dest(reinterpret_cast<const CharT*>(buff.data()), len);
    } catch (...) {
        return NO_MEM;
    }

    buff.remove_prefix(len);
    return std::nullopt;
}

template<class size_type, class Src>
static void write_bstr(void*& dest, const Src& data) noexcept {
    assert(data.size() <= std::numeric_limits<size_type>::max());
    write_val(dest, static_cast<size_type>(data.size()));
    if constexpr (std::is_same_v<Src, std::string>) {
        std::memcpy(dest, data.data(), data.size());
    } else {
        std::memcpy(dest, data.get(), data.size());
    }
    dest = reinterpret_cast<uint8_t*>(dest) + data.size();
}

template<class EntryType, class... Args>
static optional<read_error> check_space_and_type(string_view& buff, ondisk_type type, Args&&... ondisk_size_args) noexcept {
    if (buff.size() < ondisk_size<EntryType>(std::forward<Args>(ondisk_size_args)...)) {
        return TOO_SMALL;
    }

    ondisk_type tp;
    read_val(buff, tp);
    if (tp != type) {
        return INVALID_ENTRY_TYPE;
    }

    return std::nullopt;
}

// checkpoint

template<> disk_offset_t ondisk_size<checkpoint>() noexcept {
    using T = checkpoint;
    return sizeof(ondisk_type) + sizeof(T::crc32_checksum) + sizeof(T::checkpointed_data_length);
}

template<> val_or_err<checkpoint, read_error> read<checkpoint>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    checkpoint res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, CHECKPOINT); oerr) {
        return *oerr;
    }
    read_val(buff, res.crc32_checksum);
    read_val(buff, res.checkpointed_data_length);
    orig_buff = buff;
    return res;
}

template<> void write<checkpoint>(const checkpoint& entry, void* dest) noexcept {
    write_val(dest, CHECKPOINT);
    write_val(dest, entry.crc32_checksum);
    write_val(dest, entry.checkpointed_data_length);
}

// next_metadata_cluster

template<> disk_offset_t ondisk_size<next_metadata_cluster>() noexcept {
    return sizeof(ondisk_type) + sizeof(next_metadata_cluster::cluster_id);
}

template<> val_or_err<next_metadata_cluster, read_error> read<next_metadata_cluster>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    next_metadata_cluster res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, NEXT_METADATA_CLUSTER); oerr) {
        return *oerr;
    }
    read_val(buff, res.cluster_id);
    orig_buff = buff;
    return res;
}

template<> void write<next_metadata_cluster>(const next_metadata_cluster& entry, void* dest) noexcept {
    write_val(dest, NEXT_METADATA_CLUSTER);
    write_val(dest, entry.cluster_id);
}

// unix_metadata

struct [[gnu::packed]] ondisk_unix_metadata {
    uint32_t perms;
    static_assert(sizeof(perms) >= sizeof(unix_metadata::perms));
    uint8_t ftype;
    static_assert(sizeof(ftype) >= sizeof(unix_metadata::ftype));
    uint32_t uid;
    static_assert(sizeof(uid) >= sizeof(unix_metadata::uid));
    uint32_t gid;
    static_assert(sizeof(gid) >= sizeof(unix_metadata::gid));
    uint64_t btime_ns;
    static_assert(sizeof(btime_ns) >= sizeof(unix_metadata::btime_ns));
    uint64_t mtime_ns;
    static_assert(sizeof(mtime_ns) >= sizeof(unix_metadata::mtime_ns));
    uint64_t ctime_ns;
    static_assert(sizeof(ctime_ns) >= sizeof(unix_metadata::ctime_ns));
};
static_assert(sizeof(unix_metadata) == 40, "Remember to update ondisk_unix_metadata");

template<> disk_offset_t ondisk_size<unix_metadata>() noexcept {
    return sizeof(ondisk_unix_metadata);
}

template<> void read_val(string_view& buff, unix_metadata& val) noexcept {
    ondisk_unix_metadata omd;
    read_val(buff, omd);
    val.perms = static_cast<decltype(val.perms)>(omd.perms);
    val.ftype = static_cast<decltype(val.ftype)>(omd.ftype);
    val.uid = omd.uid;
    val.gid = omd.gid;
    val.btime_ns = omd.btime_ns;
    val.mtime_ns = omd.mtime_ns;
    val.ctime_ns = omd.ctime_ns;
}

template<> void write_val(void*& dest, const unix_metadata& val) noexcept {
    ondisk_unix_metadata omd;
    omd.perms = static_cast<decltype(omd.perms)>(val.perms);
    omd.ftype = static_cast<decltype(omd.ftype)>(val.ftype);
    omd.uid = val.uid;
    omd.gid = val.gid;
    omd.btime_ns = val.btime_ns;
    omd.mtime_ns = val.mtime_ns;
    omd.ctime_ns = val.ctime_ns;
    write_val(dest, omd);
}

// create_inode

template<> disk_offset_t ondisk_size<create_inode>() noexcept {
    using T = create_inode;
    return sizeof(ondisk_type) + sizeof(T::inode) + ondisk_size<decltype(T::metadata)>();
}

void read_entry(string_view& buff, create_inode& entry) noexcept {
    read_val(buff, entry.inode);
    read_val(buff, entry.metadata);
}

void write_entry(void*& dest, const create_inode& entry) noexcept {
    write_val(dest, entry.inode);
    write_val(dest, entry.metadata);
}

template<> val_or_err<create_inode, read_error> read<create_inode>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    create_inode res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, CREATE_INODE); oerr) {
        return *oerr;
    }
    read_entry(buff, res);
    orig_buff = buff;
    return res;
}

template<> void write<create_inode>(const create_inode& entry, void* dest) noexcept {
    write_val(dest, CREATE_INODE);
    write_entry(dest, entry);
}

// delete_inode

template<> disk_offset_t ondisk_size<delete_inode>() noexcept {
    return sizeof(ondisk_type) + sizeof(delete_inode::inode);
}

void read_entry(string_view& buff, delete_inode& entry) noexcept {
    read_val(buff, entry.inode);
}

void write_entry(void*& dest, const delete_inode& entry) noexcept {
    write_val(dest, entry.inode);
}

template<> val_or_err<delete_inode, read_error> read<delete_inode>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    delete_inode res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, DELETE_INODE); oerr) {
        return *oerr;
    }
    read_entry(buff, res);
    orig_buff = buff;
    return res;
}

template<> void write<delete_inode>(const delete_inode& entry, void* dest) noexcept {
    write_val(dest, DELETE_INODE);
    write_entry(dest, entry);
}

// small_write

using small_write_data_len_t = uint16_t;
static_assert(std::numeric_limits<small_write_data_len_t>::max() == small_write::data_max_len);

template<> disk_offset_t ondisk_size<small_write>(size_t data_len) noexcept {
    using T = small_write;
    return sizeof(ondisk_type) + sizeof(T::inode) + sizeof(T::offset) + sizeof(T::time_ns) +
            ondisk_bstr_size<small_write_data_len_t>(data_len);
}

template<> val_or_err<small_write, read_error> read<small_write>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    small_write res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, SMALL_WRITE, 0); oerr) {
        return *oerr;
    }
    read_val(buff, res.inode);
    read_val(buff, res.offset);
    read_val(buff, res.time_ns);
    if (auto rd_res = read_bstr<small_write_data_len_t>(buff, res.data); rd_res) {
        return *rd_res;
    }
    orig_buff = buff;
    return res;
}

template<> void write<small_write>(const small_write& entry, void* dest) noexcept {
    write_val(dest, SMALL_WRITE);
    write_val(dest, entry.inode);
    write_val(dest, entry.offset);
    write_val(dest, entry.time_ns);
    write_bstr<small_write_data_len_t>(dest, entry.data);
}

// medium_write

template<> disk_offset_t ondisk_size<medium_write>() noexcept {
    using T = medium_write;
    return sizeof(ondisk_type) + sizeof(T::inode) + sizeof(T::offset) + sizeof(T::drange) + sizeof(T::time_ns);
}

template<> val_or_err<medium_write, read_error> read<medium_write>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    medium_write res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, MEDIUM_WRITE); oerr) {
        return *oerr;
    }
    read_val(buff, res.inode);
    read_val(buff, res.offset);
    read_val(buff, res.drange);
    read_val(buff, res.time_ns);
    orig_buff = buff;
    return res;
}

template<> void write<medium_write>(const medium_write& entry, void* dest) noexcept {
    write_val(dest, MEDIUM_WRITE);
    write_val(dest, entry.inode);
    write_val(dest, entry.offset);
    write_val(dest, entry.drange);
    write_val(dest, entry.time_ns);
}

// large_write_without_time

template<> disk_offset_t ondisk_size<large_write_without_time>() noexcept {
    using T = large_write_without_time;
    return sizeof(ondisk_type) + sizeof(T::inode) + sizeof(T::offset) + sizeof(T::data_cluster);
}

template<> val_or_err<large_write_without_time, read_error> read<large_write_without_time>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    large_write_without_time res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, LARGE_WRITE_WITHOUT_MTIME); oerr) {
        return *oerr;
    }
    read_val(buff, res.inode);
    read_val(buff, res.offset);
    read_val(buff, res.data_cluster);
    orig_buff = buff;
    return res;
}

template<> void write<large_write_without_time>(const large_write_without_time& entry, void* dest) noexcept {
    write_val(dest, LARGE_WRITE_WITHOUT_MTIME);
    write_val(dest, entry.inode);
    write_val(dest, entry.offset);
    write_val(dest, entry.data_cluster);
}

// large_write

template<> disk_offset_t ondisk_size<large_write>() noexcept {
    using T = large_write;
    return ondisk_size<decltype(large_write::lwwt)>() + sizeof(T::time_ns);
}

template<> val_or_err<large_write, read_error> read<large_write>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    large_write res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, LARGE_WRITE); oerr) {
        return *oerr;
    }
    read_val(buff, res.lwwt.inode);
    read_val(buff, res.lwwt.offset);
    read_val(buff, res.lwwt.data_cluster);
    read_val(buff, res.time_ns);
    orig_buff = buff;
    return res;
}

template<> void write<large_write>(const large_write& entry, void* dest) noexcept {
    write_val(dest, LARGE_WRITE);
    write_val(dest, entry.lwwt.inode);
    write_val(dest, entry.lwwt.offset);
    write_val(dest, entry.lwwt.data_cluster);
    write_val(dest, entry.time_ns);
}

// truncate

template<> disk_offset_t ondisk_size<truncate>() noexcept {
    using T = truncate;
    return sizeof(ondisk_type) + sizeof(T::inode) + sizeof(T::size) + sizeof(T::time_ns);
}

template<> val_or_err<truncate, read_error> read<truncate>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    truncate res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, TRUNCATE); oerr) {
        return *oerr;
    }
    read_val(buff, res.inode);
    read_val(buff, res.size);
    read_val(buff, res.time_ns);
    orig_buff = buff;
    return res;
}

template<> void write<truncate>(const truncate& entry, void* dest) noexcept {
    write_val(dest, TRUNCATE);
    write_val(dest, entry.inode);
    write_val(dest, entry.size);
    write_val(dest, entry.time_ns);
}

using dentry_name_len_t = uint16_t;
static_assert(std::numeric_limits<dentry_name_len_t>::max() == dentry_name_max_len);

// create_dentry

template<> disk_offset_t ondisk_size<create_dentry>(size_t dentry_name_len) noexcept {
    using T = create_dentry;
    return sizeof(ondisk_type) + sizeof(T::inode) + ondisk_bstr_size<dentry_name_len_t>(dentry_name_len) +
        sizeof(T::dir_inode);
}

template<> val_or_err<create_dentry, read_error> read<create_dentry>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    create_dentry res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, CREATE_DENTRY, 0); oerr) {
        return *oerr;
    }
    read_val(buff, res.inode);
    read_val(buff, res.dir_inode);
    if (auto rd_res = read_bstr<dentry_name_len_t>(buff, res.name); rd_res) {
        return *rd_res;
    }
    orig_buff = buff;
    return res;
}

template<> void write<create_dentry>(const create_dentry& entry, void* dest) noexcept {
    write_val(dest, CREATE_DENTRY);
    write_val(dest, entry.inode);
    write_val(dest, entry.dir_inode);
    write_bstr<dentry_name_len_t>(dest, entry.name);
}

// create_inode_as_dentry

template<> disk_offset_t ondisk_size<create_inode_as_dentry>(size_t dentry_name_len) noexcept {
    using T = create_inode_as_dentry;
    return ondisk_size<decltype(T::inode)>() + ondisk_bstr_size<dentry_name_len_t>(dentry_name_len) +
        sizeof(T::dir_inode);
}

template<> val_or_err<create_inode_as_dentry, read_error> read<create_inode_as_dentry>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    create_inode_as_dentry res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, CREATE_INODE_AS_DENTRY, 0); oerr) {
        return *oerr;
    }
    read_entry(buff, res.inode);
    read_val(buff, res.dir_inode);
    if (auto rd_res = read_bstr<dentry_name_len_t>(buff, res.name); rd_res) {
        return *rd_res;
    }
    orig_buff = buff;
    return res;
}

template<> void write<create_inode_as_dentry>(const create_inode_as_dentry& entry, void* dest) noexcept {
    write_val(dest, CREATE_INODE_AS_DENTRY);
    write_entry(dest, entry.inode);
    write_val(dest, entry.dir_inode);
    write_bstr<dentry_name_len_t>(dest, entry.name);
}

// delete_dentry

template<> disk_offset_t ondisk_size<delete_dentry>(size_t dentry_name_len) noexcept {
    using T = delete_dentry;
    return sizeof(ondisk_type) + sizeof(T::dir_inode) + ondisk_bstr_size<dentry_name_len_t>(dentry_name_len);
}

[[nodiscard]] static optional<read_error> read_entry(string_view& buff, delete_dentry& entry) {
    read_val(buff, entry.dir_inode);
    if (auto rd_res = read_bstr<dentry_name_len_t>(buff, entry.name); rd_res) {
        return *rd_res;
    }
    return std::nullopt;
}

template<> val_or_err<delete_dentry, read_error> read<delete_dentry>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    delete_dentry res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, DELETE_DENTRY, 0); oerr) {
        return *oerr;
    }
    if (auto rc = read_entry(buff, res); rc) {
        return *rc;
    }
    orig_buff = buff;
    return res;
}

void write_entry(void*& dest, const delete_dentry& entry) {
    write_val(dest, entry.dir_inode);
    write_bstr<dentry_name_len_t>(dest, entry.name);
}

template<> void write<delete_dentry>(const delete_dentry& entry, void* dest) noexcept {
    write_val(dest, DELETE_DENTRY);
    write_entry(dest, entry);
}

// delete_inode_and_dentry

template<> disk_offset_t ondisk_size<delete_inode_and_dentry>(size_t dentry_name_len) noexcept {
    using T = delete_inode_and_dentry;
    return ondisk_size<decltype(T::di)>() + ondisk_size<decltype(T::dd)>(dentry_name_len) - sizeof(ondisk_type);
}

template<> val_or_err<delete_inode_and_dentry, read_error> read<delete_inode_and_dentry>(string_view& orig_buff) noexcept {
    string_view buff = orig_buff;
    delete_inode_and_dentry res;
    if (auto oerr = check_space_and_type<decltype(res)>(buff, DELETE_INODE_AND_DENTRY, 0); oerr) {
        return *oerr;
    }
    read_val(buff, res.di);
    if (auto rc = read_entry(buff, res.dd); rc) {
        return *rc;
    }
    orig_buff = buff;
    return res;
}

template<> void write<delete_inode_and_dentry>(const delete_inode_and_dentry& entry, void* dest) noexcept {
    write_val(dest, DELETE_INODE_AND_DENTRY);
    write_val(dest, entry.di);
    write_entry(dest, entry.dd);
}

val_or_err<any_entry, read_error> read_any(string_view& buff) noexcept {
    ondisk_type tp;
    if (buff.size() < sizeof(tp)) {
        return TOO_SMALL;
    }
    std::memcpy(&tp, buff.data(), sizeof(tp));

    auto to_any = [&](auto&& ret) -> val_or_err<any_entry, read_error> {
        if (ret) {
            return any_entry(std::move(ret.value()));
        }
        return ret.error();
    };

    switch (tp) {
    case INVALID: break;
    case CHECKPOINT: return to_any(read<checkpoint>(buff));
    case NEXT_METADATA_CLUSTER: return to_any(read<next_metadata_cluster>(buff));
    case CREATE_INODE: return to_any(read<create_inode>(buff));
    case DELETE_INODE: return to_any(read<delete_inode>(buff));
    case SMALL_WRITE: return to_any(read<small_write>(buff));
    case MEDIUM_WRITE: return to_any(read<medium_write>(buff));
    case LARGE_WRITE: return to_any(read<large_write>(buff));
    case LARGE_WRITE_WITHOUT_MTIME: return to_any(read<large_write_without_time>(buff));
    case TRUNCATE: return to_any(read<truncate>(buff));
    case CREATE_DENTRY: return to_any(read<create_dentry>(buff));
    case CREATE_INODE_AS_DENTRY: return to_any(read<create_inode_as_dentry>(buff));
    case DELETE_DENTRY: return to_any(read<delete_dentry>(buff));
    case DELETE_INODE_AND_DENTRY: return to_any(read<delete_inode_and_dentry>(buff));
    }

    return INVALID_ENTRY_TYPE;
}

} // namespace seastar::fs::metadata_log::entries
