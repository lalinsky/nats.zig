// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const JetStream = @import("jetstream.zig").JetStream;
const StreamConfig = @import("jetstream.zig").StreamConfig;
const StreamInfo = @import("jetstream.zig").StreamInfo;
const ConsumerConfig = @import("jetstream.zig").ConsumerConfig;
const ConsumerInfo = @import("jetstream.zig").ConsumerInfo;
const PublishOptions = @import("jetstream.zig").PublishOptions;
const Result = @import("result.zig").Result;
const StoredMessage = @import("jetstream.zig").StoredMessage;
const Message = @import("message.zig").Message;
const timestamp = @import("timestamp.zig");
const newInbox = @import("inbox.zig").newInbox;
const nuid = @import("nuid.zig");

const log = @import("log.zig").log;

// Default chunk size (128KB)
const DEFAULT_CHUNK_SIZE: u32 = 128 * 1024;

// Object Store-specific errors
pub const ObjectStoreError = error{
    InvalidStoreName,
    InvalidObjectName,
    StoreNotFound,
    ObjectNotFound,
    ChunkMismatch,
    DigestMismatch,
    BadRequest,
};

/// Object metadata structure
pub const ObjectMeta = struct {
    /// Object name
    name: []const u8,
    /// Optional description
    description: ?[]const u8,
    /// Store/bucket name
    bucket: []const u8,
    /// Unique object identifier (NUID)
    nuid: []const u8,
    /// Total object size in bytes
    size: u64,
    /// Number of chunks
    chunks: u32,
    /// SHA-256 digest hex string
    digest: []const u8,
    /// Creation timestamp
    created: u64,
    /// Modification timestamp
    modified: u64,
    /// True if object is deleted
    deleted: bool,
};

/// Object info (lighter version of metadata)
pub const ObjectInfo = struct {
    /// Object name
    name: []const u8,
    /// Store/bucket name
    bucket: []const u8,
    /// Unique object identifier
    nuid: []const u8,
    /// Total object size
    size: u64,
    /// Number of chunks
    chunks: u32,
    /// SHA-256 digest
    digest: []const u8,
    /// Modification timestamp
    modified: u64,
    /// True if deleted
    deleted: bool,
};

/// Options for putting objects
pub const PutObjectOptions = struct {
    /// Custom chunk size (defaults to 128KB)
    chunk_size: ?u32 = null,
    /// Optional description
    description: ?[]const u8 = null,
};

/// Configuration for creating object stores
pub const ObjectStoreConfig = struct {
    /// Store name (required)
    store_name: []const u8,
    /// Description of the store
    description: ?[]const u8 = null,
    /// Maximum object size in bytes (-1 = unlimited)
    max_object_size: i64 = -1,
    /// Maximum store size in bytes (-1 = unlimited)
    max_bytes: i64 = -1,
    /// Storage type
    storage: enum { file, memory } = .file,
    /// Number of replicas
    replicas: u8 = 1,
    /// Enable compression
    compression: bool = false,
};

/// Validate store name according to object store rules
pub fn validateStoreName(name: []const u8) !void {
    if (name.len == 0) {
        return ObjectStoreError.InvalidStoreName;
    }

    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') {
            return ObjectStoreError.InvalidStoreName;
        }
    }
}

/// Validate object name according to object store rules
pub fn validateObjectName(name: []const u8) !void {
    if (name.len == 0) {
        return ObjectStoreError.InvalidObjectName;
    }

    // Check for leading or trailing slashes/dots
    if (name[0] == '/' or name[name.len - 1] == '/' or
        name[0] == '.' or name[name.len - 1] == '.')
    {
        return ObjectStoreError.InvalidObjectName;
    }

    // Validate each character
    for (name) |c| {
        const valid = std.ascii.isAlphanumeric(c) or
            c == '-' or c == '/' or c == '_' or c == '=' or c == '.';
        if (!valid) {
            return ObjectStoreError.InvalidObjectName;
        }
    }
}

/// Object Store implementation
pub const ObjectStore = struct {
    /// JetStream context
    js: *JetStream,
    /// Store name
    store_name: []const u8,
    /// Stream name (OBJ_<store_name>)
    stream_name: []const u8,
    /// Chunk subject prefix ($O.<store>.C.)
    chunk_subject_prefix: []const u8,
    /// Meta subject prefix ($O.<store>.M.)
    meta_subject_prefix: []const u8,
    /// Allocator for memory management
    allocator: std.mem.Allocator,
    /// Default chunk size
    chunk_size: u32,

    const Self = @This();

    /// Initialize ObjectStore handle
    pub fn init(allocator: std.mem.Allocator, js: *JetStream, store_name: []const u8) !ObjectStore {
        try validateStoreName(store_name);

        // Create owned copies of names
        const owned_store_name = try allocator.dupe(u8, store_name);
        errdefer allocator.free(owned_store_name);

        const stream_name = try std.fmt.allocPrint(allocator, "OBJ_{s}", .{store_name});
        errdefer allocator.free(stream_name);

        const chunk_subject_prefix = try std.fmt.allocPrint(allocator, "$O.{s}.C.", .{store_name});
        errdefer allocator.free(chunk_subject_prefix);

        const meta_subject_prefix = try std.fmt.allocPrint(allocator, "$O.{s}.M.", .{store_name});
        errdefer allocator.free(meta_subject_prefix);

        return ObjectStore{
            .js = js,
            .store_name = owned_store_name,
            .stream_name = stream_name,
            .chunk_subject_prefix = chunk_subject_prefix,
            .meta_subject_prefix = meta_subject_prefix,
            .allocator = allocator,
            .chunk_size = DEFAULT_CHUNK_SIZE,
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        self.allocator.free(self.store_name);
        self.allocator.free(self.stream_name);
        self.allocator.free(self.chunk_subject_prefix);
        self.allocator.free(self.meta_subject_prefix);
    }

    /// Calculate SHA-256 digest of data
    fn calculateDigest(_: *ObjectStore, data: []const u8) ![64]u8 {
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update(data);
        const digest_bytes = hasher.finalResult();

        var digest_hex: [64]u8 = undefined;
        _ = std.fmt.bufPrint(&digest_hex, "{s}", .{std.fmt.fmtSliceHexLower(&digest_bytes)}) catch unreachable;
        return digest_hex;
    }

    /// Get the meta subject for an object name
    fn getMetaSubject(self: *ObjectStore, object_name: []const u8) ![]u8 {
        try validateObjectName(object_name);
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.meta_subject_prefix, object_name });
    }

    /// Get the chunk subject for an object NUID
    fn getChunkSubject(self: *ObjectStore, object_nuid: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.chunk_subject_prefix, object_nuid });
    }

    /// Put an object into the store
    pub fn put(self: *ObjectStore, object_name: []const u8, data: []const u8, options: PutObjectOptions) !ObjectInfo {
        try validateObjectName(object_name);

        // Generate unique identifier for this object
        const object_nuid = try nuid.nextString(self.allocator);
        defer self.allocator.free(object_nuid);

        // Calculate digest
        const digest_array = try self.calculateDigest(data);
        const digest = digest_array[0..];

        // Determine chunk size
        const chunk_size = options.chunk_size orelse self.chunk_size;
        const num_chunks = @as(u32, @intCast((data.len + chunk_size - 1) / chunk_size)); // Round up division

        // Store chunks first
        var chunk_idx: u32 = 0;
        var offset: usize = 0;
        while (offset < data.len) : ({
            chunk_idx += 1;
            offset += chunk_size;
        }) {
            const end = @min(offset + chunk_size, data.len);
            const chunk_data = data[offset..end];

            const chunk_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}.{d}", .{ self.chunk_subject_prefix, object_nuid, chunk_idx });
            defer self.allocator.free(chunk_subject);

            const result = try self.js.publish(chunk_subject, chunk_data, .{});
            defer result.deinit();
        }

        // Create metadata
        const now = std.time.nanoTimestamp();
        const meta = ObjectMeta{
            .name = object_name,
            .description = options.description,
            .bucket = self.store_name,
            .nuid = object_nuid,
            .size = data.len,
            .chunks = num_chunks,
            .digest = digest,
            .created = @intCast(now),
            .modified = @intCast(now),
            .deleted = false,
        };

        // Serialize and store metadata
        const meta_json = try self.serializeObjectMeta(meta);
        defer self.allocator.free(meta_json);

        const meta_subject = try self.getMetaSubject(object_name);
        defer self.allocator.free(meta_subject);

        const meta_result = try self.js.publish(meta_subject, meta_json, .{});
        defer meta_result.deinit();

        return ObjectInfo{
            .name = object_name,
            .bucket = self.store_name,
            .nuid = object_nuid,
            .size = data.len,
            .chunks = num_chunks,
            .digest = digest,
            .modified = @intCast(now),
            .deleted = false,
        };
    }

    /// Get object data
    pub fn get(self: *ObjectStore, object_name: []const u8) !Result([]u8) {
        // First get metadata
        const info_result = try self.info(object_name);
        defer info_result.deinit();
        const obj_info = info_result.value;

        if (obj_info.deleted) {
            return ObjectStoreError.ObjectNotFound;
        }

        // Create arena for the result
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Allocate buffer for complete object
        const data = try arena_allocator.alloc(u8, obj_info.size);
        var offset: usize = 0;

        // Retrieve and reassemble chunks
        var chunk_idx: u32 = 1;
        while (chunk_idx <= obj_info.chunks) : (chunk_idx += 1) {
            const chunk_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}.{d}", .{ self.chunk_subject_prefix, obj_info.nuid, chunk_idx });
            defer self.allocator.free(chunk_subject);

            const chunk_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = chunk_subject, .direct = true }) catch |err| {
                return if (err == error.MessageNotFound) ObjectStoreError.ChunkMismatch else err;
            };
            defer chunk_msg.deinit();

            // Copy chunk data
            const chunk_size = chunk_msg.data.len;
            if (offset + chunk_size > obj_info.size) {
                return ObjectStoreError.ChunkMismatch;
            }

            @memcpy(data[offset .. offset + chunk_size], chunk_msg.data);
            offset += chunk_size;
        }

        if (offset != obj_info.size) {
            return ObjectStoreError.ChunkMismatch;
        }

        // Verify digest
        const calculated_digest = try self.calculateDigest(data);
        if (!std.mem.eql(u8, &calculated_digest, obj_info.digest)) {
            return ObjectStoreError.DigestMismatch;
        }

        return Result([]u8){
            .arena = arena,
            .value = data,
        };
    }

    /// Get object metadata
    pub fn info(self: *ObjectStore, object_name: []const u8) !Result(ObjectInfo) {
        const meta_subject = try self.getMetaSubject(object_name);
        defer self.allocator.free(meta_subject);

        const meta_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = meta_subject, .direct = true }) catch |err| {
            return if (err == error.MessageNotFound) ObjectStoreError.ObjectNotFound else err;
        };
        errdefer meta_msg.deinit();

        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Parse metadata JSON
        const meta = try self.parseObjectMeta(arena_allocator, meta_msg.data);

        const obj_info = ObjectInfo{
            .name = try arena_allocator.dupe(u8, meta.name),
            .bucket = try arena_allocator.dupe(u8, meta.bucket),
            .nuid = try arena_allocator.dupe(u8, meta.nuid),
            .size = meta.size,
            .chunks = meta.chunks,
            .digest = try arena_allocator.dupe(u8, meta.digest),
            .modified = meta.modified,
            .deleted = meta.deleted,
        };

        // Transfer ownership of message to arena
        _ = arena_allocator.create(Message) catch unreachable;
        meta_msg.* = undefined; // Prevent double-free

        return Result(ObjectInfo){
            .arena = arena,
            .value = obj_info,
        };
    }

    /// Delete an object (marks as deleted)
    pub fn delete(self: *ObjectStore, object_name: []const u8) !void {
        // Get current metadata
        const info_result = try self.info(object_name);
        defer info_result.deinit();
        const obj_info = info_result.value;

        if (obj_info.deleted) {
            return ObjectStoreError.ObjectNotFound;
        }

        // Create updated metadata with deleted flag
        const now = std.time.nanoTimestamp();
        const meta = ObjectMeta{
            .name = object_name,
            .description = null,
            .bucket = self.store_name,
            .nuid = obj_info.nuid,
            .size = obj_info.size,
            .chunks = obj_info.chunks,
            .digest = obj_info.digest,
            .created = 0, // Not preserving creation time for simplicity
            .modified = @intCast(now),
            .deleted = true,
        };

        // Serialize and store updated metadata
        const meta_json = try self.serializeObjectMeta(meta);
        defer self.allocator.free(meta_json);

        const meta_subject = try self.getMetaSubject(object_name);
        defer self.allocator.free(meta_subject);

        const result = try self.js.publish(meta_subject, meta_json, .{});
        defer result.deinit();
    }

    /// List all objects in the store
    pub fn list(self: *ObjectStore) !Result([]ObjectInfo) {
        // Use JetStream consumer to iterate through all metadata messages
        const meta_filter = try std.fmt.allocPrint(self.allocator, "{s}>", .{self.meta_subject_prefix});
        defer self.allocator.free(meta_filter);

        const inbox = try newInbox(self.allocator);
        defer self.allocator.free(inbox);

        const consumer_config = ConsumerConfig{
            .description = "Object store list",
            .deliver_subject = inbox,
            .deliver_policy = .last_per_subject,
            .ack_policy = .none,
            .max_ack_pending = 0,
            .filter_subjects = &.{meta_filter},
        };

        const sub = try self.js.subscribeSync(self.stream_name, consumer_config);
        defer sub.deinit();

        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();
        var objects = try std.ArrayList(ObjectInfo).initCapacity(arena_allocator, 64);

        // Collect all objects (including deleted ones, to be filtered later)
        const timeout_ms = self.js.nc.options.timeout_ms;
        while (true) {
            const js_msg = sub.nextMsg(timeout_ms) catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    break;
                }
                return err;
            };
            defer js_msg.deinit();

            // Parse metadata
            const meta = try self.parseObjectMeta(arena_allocator, js_msg.msg.data);

            // Only include non-deleted objects
            if (!meta.deleted) {
                const obj_info = ObjectInfo{
                    .name = try arena_allocator.dupe(u8, meta.name),
                    .bucket = try arena_allocator.dupe(u8, meta.bucket),
                    .nuid = try arena_allocator.dupe(u8, meta.nuid),
                    .size = meta.size,
                    .chunks = meta.chunks,
                    .digest = try arena_allocator.dupe(u8, meta.digest),
                    .modified = meta.modified,
                    .deleted = meta.deleted,
                };

                try objects.append(obj_info);
            }
        }

        return Result([]ObjectInfo){
            .arena = arena,
            .value = try objects.toOwnedSlice(),
        };
    }

    /// Serialize ObjectMeta to JSON string
    fn serializeObjectMeta(self: *ObjectStore, meta: ObjectMeta) ![]u8 {
        // Simple JSON serialization (could use a proper JSON library in production)
        const desc_str = if (meta.description) |desc|
            try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{desc})
        else
            try self.allocator.dupe(u8, "null");
        defer self.allocator.free(desc_str);

        return std.fmt.allocPrint(self.allocator,
            \\{{"name":"{s}","description":{s},"bucket":"{s}","nuid":"{s}","size":{d},"chunks":{d},"digest":"{s}","created":{d},"modified":{d},"deleted":{s}}}
        , .{
            meta.name,
            desc_str,
            meta.bucket,
            meta.nuid,
            meta.size,
            meta.chunks,
            meta.digest,
            meta.created,
            meta.modified,
            if (meta.deleted) "true" else "false",
        });
    }

    /// Parse ObjectMeta from JSON string
    fn parseObjectMeta(_: *ObjectStore, allocator: std.mem.Allocator, json_data: []const u8) !ObjectMeta {
        // Simple JSON parsing (could use a proper JSON library in production)
        // This is a basic implementation - in production, use std.json or similar

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_data, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        const name = try allocator.dupe(u8, root.get("name").?.string);
        const bucket = try allocator.dupe(u8, root.get("bucket").?.string);
        const nuid_str = try allocator.dupe(u8, root.get("nuid").?.string);
        const digest = try allocator.dupe(u8, root.get("digest").?.string);

        const description = if (root.get("description")) |desc_val|
            if (desc_val == .null) null else try allocator.dupe(u8, desc_val.string)
        else
            null;

        return ObjectMeta{
            .name = name,
            .description = description,
            .bucket = bucket,
            .nuid = nuid_str,
            .size = @intCast(root.get("size").?.integer),
            .chunks = @intCast(root.get("chunks").?.integer),
            .digest = digest,
            .created = @intCast(root.get("created").?.integer),
            .modified = @intCast(root.get("modified").?.integer),
            .deleted = root.get("deleted").?.bool,
        };
    }
};

/// Object Store Manager handles store-level operations
pub const ObjectStoreManager = struct {
    js: *JetStream,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, js: *JetStream) ObjectStoreManager {
        return ObjectStoreManager{
            .js = js,
            .allocator = allocator,
        };
    }

    /// Create a new object store
    pub fn createStore(self: *ObjectStoreManager, config: ObjectStoreConfig) !ObjectStore {
        try validateStoreName(config.store_name);

        const stream_name = try std.fmt.allocPrint(self.allocator, "OBJ_{s}", .{config.store_name});
        defer self.allocator.free(stream_name);

        const chunk_subject = try std.fmt.allocPrint(self.allocator, "$O.{s}.C.>", .{config.store_name});
        defer self.allocator.free(chunk_subject);

        const meta_subject = try std.fmt.allocPrint(self.allocator, "$O.{s}.M.>", .{config.store_name});
        defer self.allocator.free(meta_subject);

        const stream_config = StreamConfig{
            .name = stream_name,
            .description = config.description,
            .subjects = &.{ chunk_subject, meta_subject },
            .retention = .limits,
            .max_msg_size = @intCast(config.max_object_size),
            .max_bytes = config.max_bytes,
            .storage = switch (config.storage) {
                .file => .file,
                .memory => .memory,
            },
            .compression = if (config.compression) .s2 else .none,
            .num_replicas = config.replicas,
            .discard = .new,
            .allow_direct = true,
            .allow_rollup_hdrs = true,
        };

        const result = try self.js.addStream(stream_config);
        defer result.deinit();

        return try ObjectStore.init(self.allocator, self.js, config.store_name);
    }

    /// Open an existing object store
    pub fn openStore(self: *ObjectStoreManager, store_name: []const u8) !ObjectStore {
        // Verify store exists by getting stream info
        const stream_name = try std.fmt.allocPrint(self.allocator, "OBJ_{s}", .{store_name});
        defer self.allocator.free(stream_name);

        const stream_info = self.js.getStreamInfo(stream_name) catch |err| {
            return if (err == error.JetStreamError) ObjectStoreError.StoreNotFound else err;
        };
        defer stream_info.deinit();

        return try ObjectStore.init(self.allocator, self.js, store_name);
    }

    /// Delete an object store
    pub fn deleteStore(self: *ObjectStoreManager, store_name: []const u8) !void {
        try validateStoreName(store_name);

        const stream_name = try std.fmt.allocPrint(self.allocator, "OBJ_{s}", .{store_name});
        defer self.allocator.free(stream_name);

        try self.js.deleteStream(stream_name);
    }
};

test "validateStoreName" {
    try validateStoreName("valid-store_name123");
    try std.testing.expectError(ObjectStoreError.InvalidStoreName, validateStoreName(""));
    try std.testing.expectError(ObjectStoreError.InvalidStoreName, validateStoreName("foo bar"));
    try std.testing.expectError(ObjectStoreError.InvalidStoreName, validateStoreName("foo.bar"));
}

test "validateObjectName" {
    try validateObjectName("valid-object/name_123.txt");
    try std.testing.expectError(ObjectStoreError.InvalidObjectName, validateObjectName(""));
    try std.testing.expectError(ObjectStoreError.InvalidObjectName, validateObjectName("/starts-with-slash"));
    try std.testing.expectError(ObjectStoreError.InvalidObjectName, validateObjectName("ends-with-slash/"));
    try std.testing.expectError(ObjectStoreError.InvalidObjectName, validateObjectName(".starts-with-dot"));
    try std.testing.expectError(ObjectStoreError.InvalidObjectName, validateObjectName("ends-with-dot."));
}
