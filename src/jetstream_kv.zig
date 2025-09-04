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
const Result = @import("jetstream.zig").Result;
const StoredMessage = @import("jetstream.zig").StoredMessage;
const Message = @import("message.zig").Message;

const log = @import("log.zig").log;

// KV-specific headers
const KvOperationHdr = "KV-Operation";
const NatsRollupHdr = "Nats-Rollup";
const NatsMarkerReasonHdr = "Nats-Marker-Reason";

// KV operations
pub const KVOperation = enum {
    PUT,
    DEL,
    PURGE,

    pub fn toString(self: KVOperation) []const u8 {
        return switch (self) {
            .PUT => "PUT",
            .DEL => "DEL",
            .PURGE => "PURGE",
        };
    }

    pub fn fromString(str: []const u8) ?KVOperation {
        if (std.mem.eql(u8, str, "PUT")) return .PUT;
        if (std.mem.eql(u8, str, "DEL")) return .DEL;
        if (std.mem.eql(u8, str, "PURGE")) return .PURGE;
        return null;
    }
};

// Marker reasons for TTL-based deletions
pub const MarkerReason = enum {
    MaxAge,
    Purge,
    Remove,

    pub fn toString(self: MarkerReason) []const u8 {
        return switch (self) {
            .MaxAge => "MaxAge",
            .Purge => "Purge",
            .Remove => "Remove",
        };
    }

    pub fn fromString(str: []const u8) ?MarkerReason {
        if (std.mem.eql(u8, str, "MaxAge")) return .MaxAge;
        if (std.mem.eql(u8, str, "Purge")) return .Purge;
        if (std.mem.eql(u8, str, "Remove")) return .Remove;
        return null;
    }
};

// KV-specific errors
pub const KVError = error{
    InvalidBucketName,
    InvalidKey,
    BucketNotFound,
    KeyNotFound,
    WrongLastRevision,
    KeyExists,
    BadRequest,
};

/// KV Entry represents a key-value pair with metadata
pub const KVEntry = struct {
    /// Bucket name
    bucket: []const u8,
    /// Key name
    key: []const u8,
    /// Value data
    value: []const u8,
    /// Creation timestamp as integer
    created: u64,
    /// Unique revision number
    revision: u64,
    /// Distance from latest (0=latest, 1=previous, etc.)
    delta: u64,
    /// Operation type
    operation: KVOperation,
    /// Underlying message (owns the data)
    msg: *Message,

    pub fn deinit(self: *KVEntry) void {
        self.msg.deinit();
    }
};

/// KV Status provides information about a KV bucket
pub const KVStatus = struct {
    /// Bucket name
    bucket: []const u8,
    /// Number of messages/entries in the bucket
    values: u64,
    /// History kept per key (1-64)
    history: u64,
    /// TTL for entries in nanoseconds (0 = no TTL)
    ttl: u64,
    /// TTL for limit markers in nanoseconds (0 = not supported)
    limit_marker_ttl: u64,
    /// Whether data is compressed
    is_compressed: bool,
    /// Backend type (always "JetStream" for now)
    backing_store: []const u8,
    /// Total bytes used by bucket
    bytes: u64,
};

/// Configuration for creating KV buckets
pub const KVConfig = struct {
    /// Bucket name (required)
    bucket: []const u8,
    /// Description of the bucket
    description: ?[]const u8 = null,
    /// History to keep per key (1-64, default 1)
    history: u8 = 1,
    /// TTL for entries in nanoseconds (0 = no TTL)
    ttl: u64 = 0,
    /// TTL for limit markers in nanoseconds (0 = disabled, requires >= 1s if enabled)
    limit_marker_ttl: u64 = 0,
    /// Maximum value size in bytes (-1 = unlimited)
    max_value_size: i32 = -1,
    /// Maximum bucket size in bytes (-1 = unlimited)
    max_bytes: i64 = -1,
    /// Storage type
    storage: enum { file, memory } = .file,
    /// Number of replicas
    replicas: u8 = 1,
    /// Enable compression
    compression: bool = false,
};

/// Options for Put operations
pub const PutOptions = struct {
    /// Optional TTL for this specific entry (requires limit markers enabled)
    ttl: ?u64 = null,
};

/// Validate bucket name according to KV rules: alphanumeric, underscore, hyphen only
pub fn validateBucketName(name: []const u8) !void {
    if (name.len == 0) {
        return KVError.InvalidBucketName;
    }

    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') {
            return KVError.InvalidBucketName;
        }
    }
}

/// Validate key name according to KV rules: [-/_=\.a-zA-Z0-9]+, no leading/trailing dots
pub fn validateKey(key: []const u8) !void {
    if (key.len == 0) {
        return KVError.InvalidKey;
    }

    // Check for leading or trailing dots
    if (key[0] == '.' or key[key.len - 1] == '.') {
        return KVError.InvalidKey;
    }

    // Check for reserved prefix
    if (std.mem.startsWith(u8, key, "_kv")) {
        return KVError.InvalidKey;
    }

    // Validate each character
    for (key) |c| {
        const valid = std.ascii.isAlphanumeric(c) or
            c == '-' or c == '/' or c == '_' or c == '=' or c == '.';
        if (!valid) {
            return KVError.InvalidKey;
        }
    }
}

test "validateBucketName" {
    try validateBucketName("valid-bucket_name123");
    try std.testing.expectError(KVError.InvalidBucketName, validateBucketName(""));
    try std.testing.expectError(KVError.InvalidBucketName, validateBucketName("foo bar"));
    try std.testing.expectError(KVError.InvalidBucketName, validateBucketName("foo.bar"));
    try std.testing.expectError(KVError.InvalidBucketName, validateBucketName("foo*"));
}

test "validateKey" {
    try validateKey("valid-key/name_123.foo");
    try std.testing.expectError(KVError.InvalidKey, validateKey(""));
    try std.testing.expectError(KVError.InvalidKey, validateKey(".starts-with-dot"));
    try std.testing.expectError(KVError.InvalidKey, validateKey("ends-with-dot."));
    try std.testing.expectError(KVError.InvalidKey, validateKey("_kv_reserved"));
    try std.testing.expectError(KVError.InvalidKey, validateKey("foo bar"));
    try std.testing.expectError(KVError.InvalidKey, validateKey("foo*"));
}

/// KV bucket implementation
pub const KV = struct {
    /// JetStream context
    js: *JetStream,
    /// Bucket name
    bucket_name: []const u8,
    /// Stream name (KV_<bucket_name>)
    stream_name: []const u8,
    /// Subject prefix ($KV.<bucket_name>.)
    subject_prefix: []const u8,
    /// Allocator for memory management
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize KV bucket handle
    pub fn init(allocator: std.mem.Allocator, js: *JetStream, bucket_name: []const u8) !KV {
        try validateBucketName(bucket_name);

        // Create owned copies of names
        const owned_bucket_name = try allocator.dupe(u8, bucket_name);
        errdefer allocator.free(owned_bucket_name);

        const stream_name = try std.fmt.allocPrint(allocator, "KV_{s}", .{bucket_name});
        errdefer allocator.free(stream_name);

        const subject_prefix = try std.fmt.allocPrint(allocator, "$KV.{s}.", .{bucket_name});
        errdefer allocator.free(subject_prefix);

        return KV{
            .js = js,
            .bucket_name = owned_bucket_name,
            .stream_name = stream_name,
            .subject_prefix = subject_prefix,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *KV) void {
        self.allocator.free(self.bucket_name);
        self.allocator.free(self.stream_name);
        self.allocator.free(self.subject_prefix);
    }

    /// Get the full subject for a key
    fn getKeySubject(self: *KV, key: []const u8) ![]u8 {
        try validateKey(key);
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.subject_prefix, key });
    }

    /// Parse a KV entry from a stored message
    fn parseEntry(self: *KV, stored_msg: *Message, key: []const u8, delta: u64) !KVEntry {
        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (stored_msg.headerGet(KvOperationHdr)) |op_value| {
            operation = KVOperation.fromString(op_value) orelse .PUT;
        }

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = stored_msg.data,
            .created = stored_msg.seq, // Use sequence as timestamp for now
            .revision = stored_msg.seq,
            .delta = delta,
            .operation = operation,
            .msg = stored_msg,
        };
    }

    /// Put a value into a key
    pub fn put(self: *KV, key: []const u8, value: []const u8, options: PutOptions) !u64 {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        var publish_opts = PublishOptions{};
        if (options.ttl) |ttl| {
            publish_opts.msg_ttl = ttl;
        }

        const result = try self.js.publish(subject, value, publish_opts);
        defer result.deinit();

        return result.value.seq;
    }

    /// Create a key only if it doesn't exist
    pub fn create(self: *KV, key: []const u8, value: []const u8, options: PutOptions) !u64 {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        var publish_opts = PublishOptions{
            .expected_last_subject_seq = 0, // Only succeed if first message
        };
        if (options.ttl) |ttl| {
            publish_opts.msg_ttl = ttl;
        }

        const result = self.js.publish(subject, value, publish_opts) catch |err| {
            return if (err == error.JetStreamError) KVError.KeyExists else err;
        };
        defer result.deinit();

        return result.value.seq;
    }

    /// Update a key only if it has the expected revision
    pub fn update(self: *KV, key: []const u8, value: []const u8, expected_revision: u64) !u64 {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        const publish_opts = PublishOptions{
            .expected_last_subject_seq = expected_revision,
        };

        const result = self.js.publish(subject, value, publish_opts) catch |err| {
            return if (err == error.JetStreamError) KVError.WrongLastRevision else err;
        };
        defer result.deinit();

        return result.value.seq;
    }

    /// Get the latest value for a key
    pub fn get(self: *KV, key: []const u8) !KVEntry {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        const stored_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| {
            return if (err == error.MessageNotFound) KVError.KeyNotFound else err;
        };
        errdefer stored_msg.deinit();

        return try self.parseEntry(stored_msg, key, 0);
    }

    /// Delete a key (preserves history)
    pub fn delete(self: *KV, key: []const u8) !void {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        // Create message with KV-Operation: DEL header and empty body
        const msg = try self.js.nc.newMsg();
        defer msg.deinit();

        try msg.setSubject(subject);
        try msg.setPayload("");
        try msg.headerSet(KvOperationHdr, "DEL");

        const result = try self.js.publishMsg(msg, .{});
        defer result.deinit();
    }

    /// Purge a key (removes history)
    pub fn purge(self: *KV, key: []const u8, options: PutOptions) !void {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        // Create message with KV-Operation: PURGE and Nats-Rollup: sub headers
        const msg = try self.js.nc.newMsg();
        defer msg.deinit();

        try msg.setSubject(subject);
        try msg.setPayload("");
        try msg.headerSet(KvOperationHdr, "PURGE");
        try msg.headerSet(NatsRollupHdr, "sub");

        var publish_opts = PublishOptions{};
        if (options.ttl) |ttl| {
            publish_opts.msg_ttl = ttl;
        }

        const result = try self.js.publishMsg(msg, publish_opts);
        defer result.deinit();
    }

    /// Get status information about the bucket
    pub fn status(self: *KV) !Result(KVStatus) {
        const stream_info = try self.js.getStreamInfo(self.stream_name);
        errdefer stream_info.deinit();

        const arena_allocator = stream_info.arena.allocator();

        const status_value = KVStatus{
            .bucket = try arena_allocator.dupe(u8, self.bucket_name),
            .values = stream_info.value.state.messages,
            .history = @intCast(stream_info.value.config.max_msgs_per_subject),
            .ttl = stream_info.value.config.max_age,
            .limit_marker_ttl = 0, // TODO: Extract from stream config
            .is_compressed = stream_info.value.config.compression == .s2,
            .backing_store = "JetStream",
            .bytes = stream_info.value.state.bytes,
        };

        const result: Result(KVStatus) = .{
            .arena = stream_info.arena,
            .value = status_value,
        };
        return result;
    }

    /// Destroy the entire bucket and all data
    pub fn destroy(self: *KV) !void {
        try self.js.deleteStream(self.stream_name);
    }
};

/// KV Manager handles bucket-level operations
pub const KVManager = struct {
    js: *JetStream,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, js: *JetStream) KVManager {
        return KVManager{
            .js = js,
            .allocator = allocator,
        };
    }

    /// Create a new KV bucket
    pub fn createBucket(self: *KVManager, config: KVConfig) !KV {
        try validateBucketName(config.bucket);

        if (config.history < 1 or config.history > 64) {
            return error.InvalidConfiguration;
        }

        if (config.limit_marker_ttl > 0 and config.limit_marker_ttl < std.time.ns_per_s) {
            return error.InvalidConfiguration;
        }

        const stream_name = try std.fmt.allocPrint(self.allocator, "KV_{s}", .{config.bucket});
        defer self.allocator.free(stream_name);

        const subject_pattern = try std.fmt.allocPrint(self.allocator, "$KV.{s}.>", .{config.bucket});
        defer self.allocator.free(subject_pattern);

        // Configure duplicate window based on TTL rules from ADR-8
        var duplicate_window: u64 = 0;
        if (config.ttl > 0) {
            const two_minutes_ns = 2 * 60 * std.time.ns_per_s;
            duplicate_window = if (config.ttl > two_minutes_ns) two_minutes_ns else config.ttl;
        }

        const stream_config = StreamConfig{
            .name = stream_name,
            .description = config.description,
            .subjects = &.{subject_pattern},
            .retention = .limits,
            .max_msgs_per_subject = config.history,
            .max_age = config.ttl,
            .max_msg_size = config.max_value_size,
            .max_bytes = config.max_bytes,
            .storage = @enumFromInt(@intFromEnum(config.storage)),
            .compression = if (config.compression) .s2 else .none,
            .num_replicas = config.replicas,
            .discard = .new,
            .duplicate_window = duplicate_window,
            // KV-specific stream settings (these would need to be added to StreamConfig)
            // .rollup_hdrs = true,
            // .deny_delete = true,
            // .allow_direct = true,
            // .allow_msg_ttl = config.limit_marker_ttl > 0,
            // .subject_delete_marker_ttl = config.limit_marker_ttl,
        };

        const result = try self.js.addStream(stream_config);
        defer result.deinit();

        return try KV.init(self.allocator, self.js, config.bucket);
    }

    /// Open an existing KV bucket
    pub fn openBucket(self: *KVManager, bucket_name: []const u8) !KV {
        // Verify bucket exists by getting stream info
        const stream_name = try std.fmt.allocPrint(self.allocator, "KV_{s}", .{bucket_name});
        defer self.allocator.free(stream_name);

        const stream_info = self.js.getStreamInfo(stream_name) catch |err| {
            return if (err == error.JetStreamError) KVError.BucketNotFound else err;
        };
        defer stream_info.deinit();

        return try KV.init(self.allocator, self.js, bucket_name);
    }

    /// Delete a KV bucket
    pub fn deleteBucket(self: *KVManager, bucket_name: []const u8) !void {
        try validateBucketName(bucket_name);

        const stream_name = try std.fmt.allocPrint(self.allocator, "KV_{s}", .{bucket_name});
        defer self.allocator.free(stream_name);

        try self.js.deleteStream(stream_name);
    }
};
