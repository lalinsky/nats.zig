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
const JetStreamSubscription = @import("jetstream.zig").JetStreamSubscription;
const JetStreamMessage = @import("jetstream.zig").JetStreamMessage;
const Result = @import("result.zig").Result;
const StoredMessage = @import("jetstream.zig").StoredMessage;
const Message = @import("message.zig").Message;
const timestamp = @import("timestamp.zig");
const newInbox = @import("inbox.zig").newInbox;
const nuid = @import("nuid.zig");
const validation = @import("validation.zig");

const log = @import("log.zig").log;

// Default chunk size (128KB)
const DEFAULT_CHUNK_SIZE: u32 = 128 * 1024;

// Object Store-specific errors
pub const ObjectStoreError = error{
    StoreNotFound,
    ObjectNotFound,
    ChunkMismatch,
    DigestMismatch,
    BadRequest,
};

/// Object metadata options
pub const ObjectMetaOptions = struct {
    /// Custom chunk size for this object
    chunk_size: ?u32 = null,
};

/// Object metadata structure
pub const ObjectMeta = struct {
    /// Object name
    name: []const u8,
    /// Optional description
    description: ?[]const u8 = null,
    /// Optional additional options
    opts: ?ObjectMetaOptions = null,
};

/// Object info contains metadata plus instance information (what gets stored as JSON)
pub const ObjectInfo = struct {
    /// Object name
    name: []const u8,
    /// Optional description
    description: ?[]const u8 = null,
    /// Optional additional options
    opts: ?ObjectMetaOptions = null,
    /// Store/bucket name
    bucket: []const u8,
    /// Unique object identifier (NUID)
    nuid: []const u8,
    /// Total object size in bytes
    size: u64,
    /// Number of chunks
    chunks: u32,
    /// Last modified time (from message timestamp, not stored in JSON)
    mtime: u64 = 0,
    /// SHA-256 digest hex string
    digest: []const u8,
    /// True if object is deleted
    deleted: bool = false,
};

/// ObjectInfo for JSON serialization (excludes mtime)
const ObjectInfoJson = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    opts: ?ObjectMetaOptions = null,
    bucket: []const u8,
    nuid: []const u8,
    size: u64,
    chunks: u32,
    digest: []const u8,
    deleted: bool = false,
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
    /// Chunk size
    chunk_size: u32 = DEFAULT_CHUNK_SIZE,
};

/// ObjectResult provides streaming access to object data with integrated reading capabilities
pub const ObjectResult = struct {
    info: ObjectInfo,
    arena: *std.heap.ArenaAllocator,
    // Reading state (zero-copy streaming)
    subscription: ?*JetStreamSubscription,
    current_msg: ?*JetStreamMessage,
    msg_pos: usize,
    chunk_index: u32,
    digest: std.crypto.hash.sha2.Sha256,
    eof: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, info: ObjectInfo, subscription: ?*JetStreamSubscription, arena: *std.heap.ArenaAllocator) ObjectResult {
        return ObjectResult{
            .info = info,
            .arena = arena,
            .subscription = subscription,
            .current_msg = null,
            .msg_pos = 0,
            .chunk_index = 0,
            .digest = std.crypto.hash.sha2.Sha256.init(.{}),
            .eof = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ObjectResult) void {
        if (self.current_msg) |msg| {
            msg.deinit();
        }
        if (self.subscription) |sub| {
            sub.deinit();
        }
        const child_allocator = self.arena.child_allocator;
        self.arena.deinit();
        child_allocator.destroy(self.arena);
    }

    /// Read data from the object stream
    pub fn read(self: *ObjectResult, dest: []u8) !usize {
        if (self.eof) return 0;

        // Early return if no subscription (empty objects)
        const sub = self.subscription orelse return 0;

        // If we don't have a current message, try to get one
        if (self.current_msg == null) {
            if (self.chunk_index >= self.info.chunks) {
                self.eof = true;
                return 0;
            }

            // Get next chunk from subscription
            const timeout_ms = sub.js.nc.options.timeout_ms;
            const js_msg = try sub.nextMsg(timeout_ms);

            // Store the message pointer and reset position
            self.current_msg = js_msg;
            self.msg_pos = 0;
            self.chunk_index += 1;

            // Update digest
            self.digest.update(js_msg.msg.data);

            // Validate we haven't received more chunks than expected
            if (self.chunk_index > self.info.chunks) {
                return ObjectStoreError.ChunkMismatch;
            }
        }

        // Copy from current message to destination
        const msg = self.current_msg.?;
        const available = msg.msg.data.len - self.msg_pos;
        const to_copy = @min(available, dest.len);

        @memcpy(
            dest[0..to_copy],
            msg.msg.data[self.msg_pos .. self.msg_pos + to_copy],
        );

        self.msg_pos += to_copy;

        // If we've consumed this entire message, clean it up
        if (self.msg_pos >= msg.msg.data.len) {
            msg.deinit();
            self.current_msg = null;
            self.msg_pos = 0;

            // Check if we've completed reading all expected chunks
            if (self.chunk_index >= self.info.chunks) {
                self.eof = true;
            }
        }

        return to_copy;
    }

    /// Verify the complete object integrity
    pub fn verify(self: *ObjectResult) !void {
        const calculated_digest = self.digest.finalResult();
        var digest_hex: [64]u8 = undefined;
        _ = std.fmt.bufPrint(&digest_hex, "{s}", .{std.fmt.fmtSliceHexLower(&calculated_digest)}) catch unreachable;

        if (!std.mem.eql(u8, &digest_hex, self.info.digest)) {
            return ObjectStoreError.DigestMismatch;
        }
    }
};

/// Object Store implementation
pub const ObjectStore = struct {
    /// JetStream context
    js: JetStream,
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
    pub fn init(allocator: std.mem.Allocator, js: JetStream, store_name: []const u8, chunk_size: u32) !ObjectStore {
        try validation.validateOSBucketName(store_name);

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
            .chunk_size = chunk_size,
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        self.allocator.free(self.store_name);
        self.allocator.free(self.stream_name);
        self.allocator.free(self.chunk_subject_prefix);
        self.allocator.free(self.meta_subject_prefix);
    }

    /// Get the meta subject for an object name
    fn getMetaSubject(self: *ObjectStore, object_name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.meta_subject_prefix, object_name });
    }

    /// Get the chunk subject for an object NUID
    fn getChunkSubject(self: *ObjectStore, object_nuid: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.chunk_subject_prefix, object_nuid });
    }

    /// Primary put method that accepts any reader type for efficient streaming
    pub fn put(self: *ObjectStore, meta: ObjectMeta, reader: anytype) !Result(ObjectInfo) {
        try validation.validateOSObjectName(meta.name);

        // Create arena for return value
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Generate unique identifier for this object - owned by arena
        const object_nuid = try nuid.nextString(arena_allocator);

        // Initialize digest calculation
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});

        // Determine chunk size (use meta.opts.chunk_size or store default)
        const chunk_size = if (meta.opts) |opts|
            opts.chunk_size orelse self.chunk_size
        else
            self.chunk_size;

        // Allocate chunk buffer using temporary allocator
        const chunk_buffer = try self.allocator.alloc(u8, chunk_size);
        defer self.allocator.free(chunk_buffer);

        // Stream and publish chunks
        var total_size: u64 = 0;
        var chunk_count: u32 = 0;
        const chunk_subject = try self.getChunkSubject(object_nuid);
        defer self.allocator.free(chunk_subject);

        while (true) {
            const bytes_read = reader.read(chunk_buffer) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (bytes_read == 0) break;

            // Update digest
            hasher.update(chunk_buffer[0..bytes_read]);

            // Publish chunk
            const ack = try self.js.publish(chunk_subject, chunk_buffer[0..bytes_read], .{});
            defer ack.deinit();

            total_size += bytes_read;
            chunk_count += 1;
        }

        // Create digest string - owned by arena
        const digest_bytes = hasher.finalResult();
        const digest_hex = try arena_allocator.alloc(u8, 64);
        _ = std.fmt.bufPrint(digest_hex, "{s}", .{std.fmt.fmtSliceHexLower(&digest_bytes)}) catch unreachable;

        const obj_info = ObjectInfo{
            .name = try arena_allocator.dupe(u8, meta.name),
            .description = if (meta.description) |desc| try arena_allocator.dupe(u8, desc) else null,
            .opts = meta.opts,
            .bucket = try arena_allocator.dupe(u8, self.store_name),
            .nuid = object_nuid,
            .size = total_size,
            .chunks = chunk_count,
            .digest = digest_hex,
            .deleted = false,
        };

        // Store metadata
        const info_json = try self.serializeObjectInfo(obj_info);
        defer self.allocator.free(info_json);

        const meta_subject = try self.getMetaSubject(meta.name);
        defer self.allocator.free(meta_subject);

        const meta_ack = try self.js.publish(meta_subject, info_json, .{});
        defer meta_ack.deinit();

        return Result(ObjectInfo){
            .arena = arena,
            .value = obj_info,
        };
    }

    /// Put bytes as an object into the store (convenience method)
    pub fn putBytes(self: *ObjectStore, object_name: []const u8, data: []const u8) !Result(ObjectInfo) {
        // Create a fixed buffer stream from the data
        var stream = std.io.fixedBufferStream(data);

        // Create ObjectMeta with store's default chunk size
        const meta = ObjectMeta{
            .name = object_name,
            .description = null,
            .opts = ObjectMetaOptions{
                .chunk_size = self.chunk_size,
            },
        };

        // Use the primary put() method with the stream reader
        return self.put(meta, stream.reader());
    }

    /// Primary get method that returns a streaming result
    pub fn get(self: *ObjectStore, object_name: []const u8) !ObjectResult {
        try validation.validateOSObjectName(object_name);

        // First get metadata
        const meta_subject = try self.getMetaSubject(object_name);
        defer self.allocator.free(meta_subject);

        const meta_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = meta_subject, .direct = true }) catch |err| {
            return if (err == error.MessageNotFound) ObjectStoreError.ObjectNotFound else err;
        };
        errdefer meta_msg.deinit();

        // Create arena for the result
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Parse object info JSON using arena allocator
        const parsed = try std.json.parseFromSlice(ObjectInfoJson, arena_allocator, meta_msg.data, .{});

        // Convert from JSON version to full ObjectInfo with proper string ownership
        const obj_info = ObjectInfo{
            .name = parsed.value.name,
            .description = parsed.value.description,
            .opts = parsed.value.opts,
            .bucket = parsed.value.bucket,
            .nuid = parsed.value.nuid,
            .size = parsed.value.size,
            .chunks = parsed.value.chunks,
            .mtime = meta_msg.time, // Set from message timestamp
            .digest = parsed.value.digest,
            .deleted = parsed.value.deleted,
        };

        // We can safely deinit the message now since all strings are owned by arena
        meta_msg.deinit();

        if (obj_info.deleted) {
            return ObjectStoreError.ObjectNotFound;
        }

        // Links are not supported in the current server implementation

        // For empty objects, return immediately without subscription
        if (obj_info.size == 0) {
            var result = ObjectResult.init(arena_allocator, obj_info, null, arena);
            result.eof = true; // Mark as EOF immediately since there's no data
            return result;
        }

        // Create subscription for chunks
        const chunk_subject = try self.getChunkSubject(obj_info.nuid);
        defer self.allocator.free(chunk_subject);

        const inbox = try newInbox(self.allocator);
        defer self.allocator.free(inbox);

        const consumer_config = ConsumerConfig{
            .description = "Object chunk retrieval",
            .deliver_subject = inbox,
            .deliver_policy = .all,
            .ack_policy = .none,
            .max_ack_pending = 0,
            .filter_subjects = &.{chunk_subject},
        };

        const sub = try self.js.subscribeSync(self.stream_name, consumer_config);

        // Create result with subscription
        return ObjectResult.init(arena_allocator, obj_info, sub, arena);
    }

    /// Get object data as bytes (convenience method)
    pub fn getBytes(self: *ObjectStore, object_name: []const u8) !Result([]u8) {
        // Get the streaming result
        var result = try self.get(object_name);
        defer result.deinit();

        // Create arena for the result
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        errdefer self.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Allocate buffer for complete object based on size from info
        const data = try arena_allocator.alloc(u8, result.info.size);

        // Read all data from the streaming result
        var total_read: usize = 0;
        while (total_read < result.info.size) {
            const n = try result.read(data[total_read..]);
            if (n == 0) break; // EOF
            total_read += n;
        }

        if (total_read != result.info.size) {
            return ObjectStoreError.ChunkMismatch;
        }

        // Verify the digest
        try result.verify();

        return Result([]u8){
            .arena = arena,
            .value = data,
        };
    }

    /// Get object metadata
    pub fn info(self: *ObjectStore, object_name: []const u8) !Result(ObjectInfo) {
        try validation.validateOSObjectName(object_name);

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

        // Parse object info JSON using arena allocator
        const parsed = try std.json.parseFromSlice(ObjectInfoJson, arena_allocator, meta_msg.data, .{});

        // Convert from JSON version to full ObjectInfo with proper string ownership
        const obj_info = ObjectInfo{
            .name = parsed.value.name,
            .description = parsed.value.description,
            .opts = parsed.value.opts,
            .bucket = parsed.value.bucket,
            .nuid = parsed.value.nuid,
            .size = parsed.value.size,
            .chunks = parsed.value.chunks,
            .mtime = meta_msg.time, // Set from message timestamp
            .digest = parsed.value.digest,
            .deleted = parsed.value.deleted,
        };

        // We can safely deinit the message now since all strings are owned by arena
        meta_msg.deinit();

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

        // Create updated object info with deleted flag
        const updated_info = ObjectInfo{
            .name = object_name,
            .description = obj_info.description,
            .bucket = self.store_name,
            .nuid = obj_info.nuid,
            .size = obj_info.size,
            .chunks = obj_info.chunks,
            .digest = obj_info.digest,
            .deleted = true,
        };

        // Serialize and store updated object info
        const info_json = try self.serializeObjectInfo(updated_info);
        defer self.allocator.free(info_json);

        const meta_subject = try self.getMetaSubject(object_name);
        defer self.allocator.free(meta_subject);

        const result = try self.js.publish(meta_subject, info_json, .{});
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
                if (err == error.Timeout) {
                    break;
                }
                return err;
            };
            defer js_msg.deinit();

            // Parse metadata using temporary allocator
            const parsed = try std.json.parseFromSlice(ObjectInfoJson, self.allocator, js_msg.msg.data, .{});
            defer parsed.deinit();
            const meta = parsed.value;

            // Only include non-deleted objects
            if (!meta.deleted) {
                const obj_info = ObjectInfo{
                    .name = try arena_allocator.dupe(u8, meta.name),
                    .description = if (meta.description) |desc| try arena_allocator.dupe(u8, desc) else null,
                    .opts = meta.opts,
                    .bucket = try arena_allocator.dupe(u8, meta.bucket),
                    .nuid = try arena_allocator.dupe(u8, meta.nuid),
                    .size = meta.size,
                    .chunks = meta.chunks,
                    .mtime = js_msg.msg.time,
                    .digest = try arena_allocator.dupe(u8, meta.digest),
                    .deleted = meta.deleted,
                };

                try objects.append(obj_info);
            }

            // Stop when there are no more pending messages
            if (js_msg.metadata.num_pending == 0) {
                break;
            }
        }

        return Result([]ObjectInfo){
            .arena = arena,
            .value = try objects.toOwnedSlice(),
        };
    }

    /// Serialize ObjectInfo to JSON string
    fn serializeObjectInfo(self: *ObjectStore, obj_info: ObjectInfo) ![]u8 {
        // Convert to JSON-only version (excludes mtime)
        const json_info = ObjectInfoJson{
            .name = obj_info.name,
            .description = obj_info.description,
            .opts = obj_info.opts,
            .bucket = obj_info.bucket,
            .nuid = obj_info.nuid,
            .size = obj_info.size,
            .chunks = obj_info.chunks,
            .digest = obj_info.digest,
            .deleted = obj_info.deleted,
        };
        return std.json.stringifyAlloc(self.allocator, json_info, .{});
    }
};

/// Object Store Manager handles store-level operations
pub const ObjectStoreManager = struct {
    js: JetStream,

    const Self = @This();

    pub fn init(js: JetStream) ObjectStoreManager {
        return ObjectStoreManager{
            .js = js,
        };
    }

    /// Create a new object store
    pub fn createStore(self: *ObjectStoreManager, config: ObjectStoreConfig) !ObjectStore {
        try validation.validateOSBucketName(config.store_name);

        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "OBJ_{s}", .{config.store_name});
        defer self.js.nc.allocator.free(stream_name);

        const chunk_subject = try std.fmt.allocPrint(self.js.nc.allocator, "$O.{s}.C.>", .{config.store_name});
        defer self.js.nc.allocator.free(chunk_subject);

        const meta_subject = try std.fmt.allocPrint(self.js.nc.allocator, "$O.{s}.M.>", .{config.store_name});
        defer self.js.nc.allocator.free(meta_subject);

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

        return try ObjectStore.init(self.js.nc.allocator, self.js, config.store_name, config.chunk_size);
    }

    /// Open an existing object store
    pub fn openStore(self: *ObjectStoreManager, store_name: []const u8) !ObjectStore {
        // Verify store exists by getting stream info
        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "OBJ_{s}", .{store_name});
        defer self.js.nc.allocator.free(stream_name);

        const stream_info = self.js.getStreamInfo(stream_name) catch |err| {
            return if (err == error.JetStreamError) ObjectStoreError.StoreNotFound else err;
        };
        defer stream_info.deinit();

        return try ObjectStore.init(self.js.nc.allocator, self.js, store_name, DEFAULT_CHUNK_SIZE);
    }

    /// Delete an object store
    pub fn deleteStore(self: *ObjectStoreManager, store_name: []const u8) !void {
        try validation.validateOSBucketName(store_name);

        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "OBJ_{s}", .{store_name});
        defer self.js.nc.allocator.free(stream_name);

        try self.js.deleteStream(stream_name);
    }
};

test "validation delegates to validation.zig" {
    // Test that we properly delegate to centralized validation
    try validation.validateOSBucketName("valid-store_name123");
    try std.testing.expectError(error.InvalidOSBucketName, validation.validateOSBucketName(""));
    try std.testing.expectError(error.InvalidOSBucketName, validation.validateOSBucketName("foo bar"));
    try std.testing.expectError(error.InvalidOSBucketName, validation.validateOSBucketName("foo.bar"));

    try validation.validateOSObjectName("valid-object/name_123.txt");
    try std.testing.expectError(error.InvalidOSObjectName, validation.validateOSObjectName(""));
    try std.testing.expectError(error.InvalidOSObjectName, validation.validateOSObjectName("/starts-with-slash"));
    try std.testing.expectError(error.InvalidOSObjectName, validation.validateOSObjectName("ends-with-slash/"));
    try std.testing.expectError(error.InvalidOSObjectName, validation.validateOSObjectName(".starts-with-dot"));
    try std.testing.expectError(error.InvalidOSObjectName, validation.validateOSObjectName("ends-with-dot."));
}
