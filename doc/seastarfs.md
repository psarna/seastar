# SeastarFS

a fully asynchronous, sharded, user-space,
log-structured file system that is intended to become an alternative to XFS for Scylla.


## Introduction

The filesystem is optimized for:
- NVMe SSD storage
- large files
- appending files

For efficiency all metadata is stored in the memory. Metadata holds information about where each
part of the file is located and about the directory tree.

Whole filesystem is divided into filesystem shards (typically the same as number of seastar shards
for efficiency). Each shard holds its own part of the filesystem. Sharding is done by the fact that
each shard has its set of root subdirectories that it exclusively owns (one of the shards is an
owner of the root directory itself).

Each filesystem shard is divided into two parts: frontend and backend. This patch provides
an implementation of the backend shard that serves as the lower level interaction with
the filesystem. Backend shards are totally independent from each other. For interaction with
the filesystem, frontend should be used. Frontend shards are aware of each other and can allow
interacting with the filesystem as if it wasn't sharded.

Every backend shard has 3 private logs:
  - metadata log -- holds metadata and very small writes
  - medium data log -- holds medium-sized writes, which can combine data from different files (all
    writes to it are aligned for efficiency)
  - big data log -- holds data clusters, each of which belongs to a single file (this is not
    actually a log, but in the big picture it looks like it was)

Disk space is divided into clusters (typically around several MiB) that
have all equal size that is multiple of alignment (typically 4096
bytes). Each shard has its private pool of clusters (assignment is
stored in bootstrap record). Each log consumes clusters one by one -- it
writes the current one and if cluster becomes full, then log switches to
a new one that is obtained from a pool of free clusters managed by
cluster_allocator. Metadata log and medium data log write data in the
same manner: they fill up the cluster gradually from left to right. Big
data log takes a cluster and completely fills it with data at once -- it
is only used during big writes.

Backend shard is in fact a standalone file system instance that provides lower level interface
(paths and inodes) of shard's own part of the filesystem. It manages all 3 logs mentioned above and
maintains all metadata about its part of the file system that include data structures for directory
structure and file content, locking logic for safe concurrent usage, buffers for writing logs to
disk, and bootstrapping -- restoring file system structure from disk.

## Format specification

### Bootstrap record

The bootstrap record is created by SeastarFS's equivalent of mkfs and is then used to store persistent information about the file system and read upon boot.

In-memory simplified structure:
```cpp
bootstrap_record {
    static uint64_t magic_number = 0x5345415354524653; // SEASTRFS
    static uint32_t max_shards_nb = 500; // Limit of shards (a.k.a. cores) supported by the system
    static min_alignment = 4096; // Minimum on-disk alignment allowed by SeastarFS

    struct shard_info {
        cluster_id_t metadata_cluster; /// cluster id of the first metadata log cluster
        cluster_range available_clusters; /// range of clusters for data for this shard
    };

    uint64_t version; /// file system version
    disk_offset_t alignment; /// write alignment in bytes
    disk_offset_t cluster_size; /// cluster size in bytes
    inode_t root_directory; /// root dir inode number
    std::vector<shard_info> shards_info; /// basic informations about each file system shard

    static future<bootstrap_record> read_from_disk(block_device& device);
    future<> write_to_disk(block_device& device) const;
};
```

On-disk structure:
```cpp
struct bootstrap_record_disk {
    uint64_t magic;
    uint64_t version;
    disk_offset_t alignment;
    disk_offset_t cluster_size;
    inode_t root_directory;
    uint32_t shards_nb;
    bootstrap_record::shard_info shards_info[bootstrap_record::max_shards_nb];
    uint32_t crc;
};
```

### On-disk format

#### Metadata

Metadata is stored in logs. Each log is filled with entries in an "append-only" manner. When a log gets full, a next cluster is allocated, and the old log is appended with a pointer to the new log.

All types of metadata log entries are denoted here:
```cpp
enum ondisk_type : uint8_t {
    INVALID = 0,
    CHECKPOINT,
    NEXT_METADATA_CLUSTER,
    CREATE_INODE,
    DELETE_INODE,
    SMALL_WRITE,
    MEDIUM_WRITE,
    LARGE_WRITE,
    LARGE_WRITE_WITHOUT_MTIME,
    TRUNCATE,
    ADD_DIR_ENTRY,
    CREATE_INODE_AS_DIR_ENTRY,
    DELETE_DIR_ENTRY,
    DELETE_INODE_AND_DIR_ENTRY,
};
```

##### Checkpoints

Checkpoints are a mechanism of detecting bit rot and other types of data corruption for metadata entries stored on disk. The format is as follows:
```
| ondisk_checkpoint | .............................. |
                    |             data               |
                    |<-- checkpointed_data_length -->|
                                                     ^
      ______________________________________________/
     /
   there ends checkpointed data and (next checkpoint begins or metadata in the current cluster ends)

CRC is calculated from byte sequence | data | checkpointed_data_length |
E.g. if the data consist of bytes "abcd" and checkpointed_data_length of bytes "xyz" then the byte sequence
would be "abcdxyz"
```

And a code representation of the checkpoint itself is:
```cpp
struct ondisk_checkpoint {
    uint32_t crc32_code;
    unit_size_t checkpointed_data_length;
} __attribute__((packed));
```

##### Pointer to a new log

A pointer to a new log consists only of the next cluster id.

```cpp
struct ondisk_next_metadata_cluster {
    cluster_id_t cluster_id; // metadata log continues there
} __attribute__((packed));
```

##### File operations

Each operation on a file, its parameters, its path, etc. has its own entry format.

Creating a file. Format:
 - newly allocated inode number
 - flag if it's a directory
 - unix metadata (permissions, owner uid/gid, timestamps)
```cpp
struct ondisk_create_inode {
    inode_t inode;
    uint8_t is_directory;
    ondisk_unix_metadata metadata;
} __attribute__((packed));

struct ondisk_unix_metadata {
    uint32_t perms;
    uint32_t uid;
    uint32_t gid;
    uint64_t btime_ns;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
} __attribute__((packed));
```

Deleting a file. Format:
 - inode to be deleted
```cpp
struct ondisk_delete_inode {
    inode_t inode;
} __attribute__((packed));
```

Performing a small write, with data stored entirely in the metadata log. Format:
 - inode of the target file
 - logical offset of the file at which the write takes place
 - length of the write operation
 - modification time of the file (this operation's timestamp)
 - the actual data, of length `length`
```cpp
struct ondisk_small_write_header {
    inode_t inode;
    file_offset_t offset;
    uint16_t length;
    decltype(unix_metadata::mtime_ns) time_ns;
    // After header comes data
} __attribute__((packed));
```

Performing a medium write, with data stored in a data log. Format:
 - inode of the target file
 - logical offset of the file at which the write takes place
 - on-disk offset at which the data will be stored
 - length of the stored chunk of data
 - modification time of the file (this operation's timestamp)
```cpp
struct ondisk_medium_write {
    inode_t inode;
    file_offset_t offset;
    disk_offset_t disk_offset;
    uint32_t length;
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));
```

Performing a large write, with data stored in a separately allocated cluster. Format:
 - inode of the target file
 - logical offset of the file at which the write takes place
 - cluster id of the entire cluster used for this write
   * (length of the stored chunk of data is implicitly known as the current cluster size)
 - modification time of the file (this operation's timestamp)
```cpp
struct ondisk_large_write {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));
```

Same as above, but without mtime
```cpp
struct ondisk_large_write_without_mtime {
    inode_t inode;
    file_offset_t offset;
    cluster_id_t data_cluster; // length == cluster_size
} __attribute__((packed));
```

Performing a size change (a.k.a. truncate). Format:
 - inode of the target file
 - new size of the file
 - modification time of the file (this operation's timestamp)
```cpp
struct ondisk_truncate {
    inode_t inode;
    file_offset_t size;
    decltype(unix_metadata::mtime_ns) time_ns;
} __attribute__((packed));
```

Adding a directory entry. Format:
 - inode of the target parent directory
 - inode of the file for which the entry is added
 - length of the new file name
 - the actual file name, stored inline
```cpp
struct ondisk_add_dir_entry_header {
    inode_t dir_inode;
    inode_t entry_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));
```

Adding a new file straight into a directory. Format:
 - new inode data, in form of embedded "creating a file" format
 - inode of the directory in which the file is going to be linked
 - length of the new file name
 - the actual file name, stored inline
```cpp
struct ondisk_create_inode_as_dir_entry_header {
    ondisk_create_inode entry_inode;
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));
```

Unlinking the file (removing a path to it). Format:
 - inode of the target parent directory
 - length of the name of the to-be-deleted entry
 - the actual name, stored inline
```cpp
struct ondisk_delete_dir_entry_header {
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));
```

Unlinking the file along with deleting its inode. Format:
 - inode to delete
 - inode of the target parent directory
 - length of the name of the to-be-deleted entry
 - the actual name, stored inline
```cpp
struct ondisk_delete_inode_and_dir_entry_header {
    inode_t inode_to_delete;
    inode_t dir_inode;
    uint16_t entry_name_length;
    // After header comes entry name
} __attribute__((packed));
```

### In-memory format

All metadata entries are stored in memory. Files are stored in a map with inode number acting as a key. Locks for concurrent operations are also stored in maps - two supported types of locking are inode locks and directory entry locks, and both can be taken either exclusively (writer lock) or in a shared manner (reader lock). The simplified format is as follows:

```cpp
class metadata_log {
    block_device _device; // block device associated with the file system
    const unit_size_t _cluster_size; // size of a single cluster within the file system (read when booting from the bootstrap record)
    const unit_size_t _alignment; // alignment of on-disk data (read when booting from the bootstrap record)

    shared_ptr<metadata_to_disk_buffer> _curr_cluster_buff; // Handle to a current cluster, where new entries are appended
    shared_future<> _background_futures = now(); // An entry point for waiting for all kinds of background operations

    // In memory metadata
    cluster_allocator _cluster_allocator; // Used to allocate new clusters when the currently used cluster gets full
    std::map<inode_t, inode_info> _inodes; // Data about files. inode_info structure is explained below
    inode_t _root_dir; // Root directory inode of the file system. Usually it's 1, but custom configuration happens sometimes. This value is read from the bootstrap record, since it's part of the file system configuration
    shard_inode_allocator _inode_allocator; // Used to allocate new inodes - by giving them unique identifiers.
    
        // - To read or write to a file, a SL is acquired on its inode and then the operation is performed.
    class locks {
        value_shared_lock<inode_t> _inode_locks; // Locks for a single inode
        value_shared_lock<std::pair<inode_t, std::string>> _dir_entry_locks; // Locks for a single directory entry, identified by the parent directory and the entry name
    } _locks;
```

File info contains all information about a file and the format differs for regular files and for directories. The common header for both is:
```cpp
    uint32_t opened_files_count = 0; // Number of open files referencing inode
    uint32_t directories_containing_file = 0; // Number of directories pointing to a file (usually 1, more with hardlinks involved, 0 for orphaned/anonymous files)
    unix_metadata metadata; // permissions, timestamps, etc.
```

#### Regular files

Regular-file-specific fields are:
```cpp
        std::map<file_offset_t, inode_data_vec> data; // file offset => data vector that begins there (data vectors
                                                      // do not overlap)
```
... which is simply a map of extents, i.e. offsets accompanied by the location of the data residing at these offsets. Since the data may reside in different places, `inode_data_vec` is defined as follows:
```cpp
    struct in_mem_data { // Data kept in-memory, coming from small writes and stored directly in the metadata log
        temporary_buffer<uint8_t> data;
    };

    struct on_disk_data { // Data kept on-disk
        file_offset_t device_offset;
    };

    struct hole_data { }; // No materialized data - represents a hole in a file, interpreted by users as if it was filled by zeros.

    range<file_offset_t> data_range; // Range of offsets covered by this extent
    std::variant<in_mem_data, on_disk_data, hole_data> data_location; // Information about where to find data
```


#### Directories

Directory-specific fields are:
```cpp
std::map<std::string, inode_t, std::less<>> entries; // entry name => inode
```
... which is simply a map of entries, with an entry name acting as a key. Not much more to explain in this section.

### Theory of operation
TODO

