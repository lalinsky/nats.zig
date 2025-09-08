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
const PullSubscription = @import("jetstream.zig").PullSubscription;
const JetStreamSubscription = @import("jetstream.zig").JetStreamSubscription;
const JetStreamMessage = @import("jetstream.zig").JetStreamMessage;
const Subscription = @import("subscription.zig").Subscription;
const Message = @import("message.zig").Message;
const timestamp = @import("timestamp.zig");
const newInbox = @import("inbox.zig").newInbox;
const queue = @import("queue.zig");
const nuid = @import("nuid.zig");

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

    pub fn fromString(str: []const u8) !KVOperation {
        if (std.mem.eql(u8, str, "PUT")) return .PUT;
        if (std.mem.eql(u8, str, "DEL")) return .DEL;
        if (std.mem.eql(u8, str, "PURGE")) return .PURGE;
        return error.InvalidOperation;
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

    pub fn isDeleted(self: KVEntry) bool {
        switch (self.operation) {
            .DEL, .PURGE => return true,
            .PUT => return false,
        }
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

/// Options for Watch operations
pub const WatchOptions = struct {
    /// Include all historical values, not just latest per key
    include_history: bool = false,
    /// Ignore DELETE and PURGE operations
    ignore_deletes: bool = false,
    /// Only deliver headers, not message bodies
    meta_only: bool = false,
    /// Skip initial values, only deliver new updates
    updates_only: bool = false,
};

/// Key-Value watcher for receiving live updates
pub const KVWatcher = struct {
    kv: *KV,
    sub: *JetStreamSubscription,
    options: WatchOptions,
    init_pending: usize,
    received: usize = 0,
    init_done: bool = false,
    return_marker: bool = false,

    const Self = @This();

    pub fn init(kv: *KV, subjects: []const []const u8, options: WatchOptions) !Self {
        const allocator = kv.js.nc.allocator;

        const inbox = try newInbox(allocator);
        defer allocator.free(inbox);

        const consumer_config = ConsumerConfig{
            .description = "KV watcher",
            .deliver_subject = inbox,
            .deliver_policy = if (options.include_history) .all else .last_per_subject,
            .ack_policy = .none,
            .max_ack_pending = 0,
            .headers_only = if (options.meta_only) true else null,
            .filter_subjects = subjects,
        };

        var sub = try kv.js.subscribeSync(kv.stream_name, consumer_config);
        errdefer sub.deinit();

        // Match C logic: only use num_pending to detect empty streams
        const init_done = options.updates_only or sub.consumer_info.value.num_pending == 0;
        const return_marker = init_done and !options.updates_only;

        return .{
            .kv = kv,
            .sub = sub,
            .options = options,
            .init_pending = 0, // Always start at 0, set from first message like C code
            .init_done = init_done,
            .return_marker = return_marker,
        };
    }

    pub fn deinit(self: *KVWatcher) void {
        self.sub.deinit();
    }

    /// Get the next entry from the watcher (returns null for completion marker, error.Timeout if no entry available)
    pub fn next(self: *KVWatcher, timeout_ms: u64) !?KVEntry {
        // If we need to return the completion marker, do it now
        if (self.return_marker) {
            self.return_marker = false;
            return null;
        }

        var timer = try std.time.Timer.start();
        var remaining_ns = timeout_ms * std.time.ns_per_ms;
        while (true) {
            const remaining_ms = remaining_ns / std.time.ns_per_ms;
            log.debug("nextMsg({})", .{remaining_ms});
            var msg = try self.sub.nextMsg(remaining_ms);
            var delete_msg = true;
            defer if (delete_msg) msg.deinit();

            const delta = msg.metadata.num_pending;
            log.debug("pending: {}", .{delta});

            // Check for completion detection
            if (!self.init_done) {
                self.received += 1;
                // Set init_pending from first message if not set
                if (self.init_pending == 0) {
                    self.init_pending = delta;
                }
                // Check completion: received > initPending OR delta == 0 (match C logic)
                if (self.received > self.init_pending or delta == 0) {
                    self.init_done = true;
                    self.return_marker = true;
                }
            }

            var entry = try self.kv.parseJetStreamMessage(msg);
            if (!self.options.ignore_deletes or !entry.isDeleted()) {
                delete_msg = false;
                return entry;
            }

            const elapsed_ns = timer.lap();
            if (elapsed_ns > remaining_ns) {
                return error.Timeout;
            }
            remaining_ns -= elapsed_ns;
        }
    }
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
    js: JetStream,
    /// Bucket name
    bucket_name: []const u8,
    /// Stream name (KV_<bucket_name>)
    stream_name: []const u8,
    /// Subject prefix ($KV.<bucket_name>.)
    subject_prefix: []const u8,

    const Self = @This();

    /// Initialize KV bucket handle
    pub fn init(js: JetStream, bucket_name: []const u8) !KV {
        try validateBucketName(bucket_name);

        // Create owned copies of names
        const owned_bucket_name = try js.nc.allocator.dupe(u8, bucket_name);
        errdefer js.nc.allocator.free(owned_bucket_name);

        const stream_name = try std.fmt.allocPrint(js.nc.allocator, "KV_{s}", .{bucket_name});
        errdefer js.nc.allocator.free(stream_name);

        const subject_prefix = try std.fmt.allocPrint(js.nc.allocator, "$KV.{s}.", .{bucket_name});
        errdefer js.nc.allocator.free(subject_prefix);

        return KV{
            .js = js,
            .bucket_name = owned_bucket_name,
            .stream_name = stream_name,
            .subject_prefix = subject_prefix,
        };
    }

    pub fn deinit(self: *KV) void {
        self.js.nc.allocator.free(self.bucket_name);
        self.js.nc.allocator.free(self.stream_name);
        self.js.nc.allocator.free(self.subject_prefix);
    }

    /// Get the full subject for a key
    fn getKeySubject(self: *KV, key: []const u8) ![]u8 {
        try validateKey(key);
        return std.fmt.allocPrint(self.js.nc.allocator, "{s}{s}", .{ self.subject_prefix, key });
    }

    /// Get the raw entry (including delete/purge markers) for internal use
    fn getRawEntry(self: *KV, key: []const u8) !KVEntry {
        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

        const msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| {
            return if (err == error.MessageNotFound) KVError.KeyNotFound else err;
        };
        errdefer msg.deinit();

        return try self.parseMessage(msg);
    }

    /// Parse a KV entry from a stored message
    fn parseEntry(self: *KV, stored_msg: *Message, key: []const u8, delta: u64) !KVEntry {
        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (stored_msg.headerGet(KvOperationHdr)) |op_value| {
            operation = try KVOperation.fromString(op_value);
        }

        // Map TTL marker reasons to KV operations
        if (stored_msg.headerGet(NatsMarkerReasonHdr)) |marker_reason| {
            if (MarkerReason.fromString(marker_reason)) |reason| {
                operation = switch (reason) {
                    .MaxAge, .Purge => .PURGE,
                    .Remove => .DEL,
                };
            }
        }

        // Parse timestamp from Nats-Time-Stamp header, fallback to 0 if not present
        var created: u64 = 0;
        if (stored_msg.headerGet("Nats-Time-Stamp")) |timestamp_str| {
            created = timestamp.parseTimestamp(timestamp_str) catch 0;
        }

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = stored_msg.data,
            .created = created,
            .revision = stored_msg.seq,
            .delta = delta,
            .operation = operation,
            .msg = stored_msg,
        };
    }

    /// Put a value into a key
    pub fn put(self: *KV, key: []const u8, value: []const u8, options: PutOptions) !u64 {
        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

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
        defer self.js.nc.allocator.free(subject);

        // First try: attempt with expected_last_subject_seq = 0
        var publish_opts = PublishOptions{
            .expected_last_subject_seq = 0,
        };
        if (options.ttl) |ttl| {
            publish_opts.msg_ttl = ttl;
        }

        if (self.js.publish(subject, value, publish_opts)) |result| {
            defer result.deinit();
            return result.value.seq;
        } else |err| {
            if (err != error.StreamWrongLastSequence) return err;

            // Per ADR-8: if failed with StreamWrongLastSequence, check if latest value is delete/purge
            var entry = self.getRawEntry(key) catch |get_err| {
                // This shouldn't happen since we got StreamWrongLastSequence but couldn't get the key
                // Return the original error
                return if (get_err == KVError.KeyNotFound) err else get_err;
            };
            defer entry.deinit();
            if (entry.operation == .DEL or entry.operation == .PURGE) {
                // Latest operation is delete/purge, retry with that revision as expected
                publish_opts.expected_last_subject_seq = entry.revision;
                const retry_result = try self.js.publish(subject, value, publish_opts);
                defer retry_result.deinit();
                return retry_result.value.seq;
            } else {
                // Key exists with a PUT operation
                return KVError.KeyExists;
            }
        }
    }

    /// Update a key only if it has the expected revision
    pub fn update(self: *KV, key: []const u8, value: []const u8, expected_revision: u64) !u64 {
        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

        const publish_opts = PublishOptions{
            .expected_last_subject_seq = expected_revision,
        };

        const result = self.js.publish(subject, value, publish_opts) catch |err| {
            return if (err == error.StreamWrongLastSequence) KVError.WrongLastRevision else err;
        };
        defer result.deinit();

        return result.value.seq;
    }

    /// Get the latest value for a key
    pub fn get(self: *KV, key: []const u8) !KVEntry {
        var entry = try self.getRawEntry(key);
        errdefer entry.deinit();

        if (entry.isDeleted()) {
            return KVError.KeyNotFound;
        }
        return entry;
    }

    /// Delete a key (preserves history)
    pub fn delete(self: *KV, key: []const u8) !void {
        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

        // Create message with KV-Operation: DEL header and empty body
        const msg = try self.js.nc.newMsg();
        defer msg.deinit();

        try msg.setSubject(subject, true);
        try msg.headerSet(KvOperationHdr, "DEL");

        const result = try self.js.publishMsg(msg, .{});
        defer result.deinit();
    }

    /// Purge a key (removes history)
    pub fn purge(self: *KV, key: []const u8, options: PutOptions) !void {
        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

        // Create message with KV-Operation: PURGE and Nats-Rollup: sub headers
        const msg = try self.js.nc.newMsg();
        defer msg.deinit();

        try msg.setSubject(subject, true);
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
            .limit_marker_ttl = stream_info.value.config.subject_delete_marker_ttl orelse 0,
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

    /// Watch a key or key pattern for updates
    pub fn watch(self: *KV, key: []const u8, options: WatchOptions) !KVWatcher {
        try validateKey(key);

        const subject = try self.getKeySubject(key);
        defer self.js.nc.allocator.free(subject);

        return try KVWatcher.init(self, &.{subject}, options);
    }

    /// Watch multiple key patterns for updates
    pub fn watchMulti(self: *KV, key_patterns: []const []const u8, options: WatchOptions) !KVWatcher {
        // Validate all keys first
        for (key_patterns) |key| {
            try validateKey(key);
        }

        // Create subjects for all keys
        var subjects = try std.ArrayList([]u8).initCapacity(self.js.nc.allocator, key_patterns.len);
        defer {
            for (subjects.items) |subject| {
                self.js.nc.allocator.free(subject);
            }
            subjects.deinit();
        }

        for (key_patterns) |key| {
            const subject = try self.getKeySubject(key);
            subjects.appendAssumeCapacity(subject);
        }

        return try KVWatcher.init(self, subjects.items, options);
    }

    /// Watch all keys in the bucket
    pub fn watchAll(self: *KV, options: WatchOptions) !KVWatcher {
        const subject = try std.fmt.allocPrint(self.js.nc.allocator, "{s}>", .{self.subject_prefix});
        defer self.js.nc.allocator.free(subject);

        return try KVWatcher.init(self, &.{subject}, options);
    }

    /// Parse a JetStream message into a KVEntry
    /// Extracts Message pointer without calling js_msg.deinit() since we reference memory inside the message
    fn parseJetStreamMessage(self: *KV, js_msg: *JetStreamMessage) !KVEntry {
        const msg = js_msg.msg;

        log.debug("subject: {s}", .{msg.subject});

        // Extract key from subject
        if (msg.subject.len <= self.subject_prefix.len) {
            return error.InvalidSubject;
        }
        const key = msg.subject[self.subject_prefix.len..];

        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (msg.headerGet(KvOperationHdr)) |op_value| {
            operation = try KVOperation.fromString(op_value);
        }

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = msg.data,
            .operation = operation,
            .created = js_msg.metadata.timestamp,
            .revision = js_msg.metadata.sequence.stream,
            .delta = js_msg.metadata.num_pending,
            .msg = msg,
        };
    }

    /// Parse a JetStream message into a KVEntry
    /// Extracts Message pointer without calling js_msg.deinit() since we reference memory inside the message
    fn parseMessage(self: *KV, msg: *Message) !KVEntry {
        // Extract key from subject
        if (msg.subject.len <= self.subject_prefix.len) {
            return error.InvalidSubject;
        }
        const key = msg.subject[self.subject_prefix.len..];

        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (msg.headerGet(KvOperationHdr)) |op_value| {
            operation = try KVOperation.fromString(op_value);
        }

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = msg.data,
            .operation = operation,
            .created = msg.time,
            .revision = msg.seq,
            .delta = 0,
            .msg = msg,
        };
    }

    /// Get historical values for a key
    pub fn history(self: *KV, key: []const u8) !Result([]KVEntry) {
        var watcher = try self.watch(key, .{ .include_history = true });
        defer watcher.deinit();

        const arena = try self.js.nc.allocator.create(std.heap.ArenaAllocator);
        errdefer self.js.nc.allocator.destroy(arena);

        arena.* = std.heap.ArenaAllocator.init(self.js.nc.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        var entries = try std.ArrayListUnmanaged(KVEntry).initCapacity(arena_allocator, 64);

        errdefer {
            for (entries.items) |*entry| {
                entry.deinit();
            }
        }

        const total_timeout_ms = self.js.nc.options.timeout_ms;
        var timer = try std.time.Timer.start();

        while (true) {
            const elapsed_ms = timer.read() / std.time.ns_per_ms;
            if (elapsed_ms >= total_timeout_ms) {
                return error.Timeout;
            }

            const remaining_ms = total_timeout_ms - elapsed_ms;
            const entry = try watcher.next(remaining_ms) orelse break;

            try entries.append(arena_allocator, entry);
        }

        if (entries.items.len == 0) {
            return KVError.KeyNotFound;
        }

        return Result([]KVEntry){
            .arena = arena,
            .value = try entries.toOwnedSlice(arena_allocator),
        };
    }

    /// Get all keys in the bucket
    pub fn keys(self: *KV) !Result([][]const u8) {
        var watcher = try self.watchAll(.{ .ignore_deletes = true, .meta_only = true });
        defer watcher.deinit();

        const arena = try self.js.nc.allocator.create(std.heap.ArenaAllocator);
        errdefer self.js.nc.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.js.nc.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();
        var keys_set = std.StringHashMap(void).init(self.js.nc.allocator);
        defer keys_set.deinit();

        // Collect unique keys - use completion marker to know when done
        while (true) {
            var maybe_entry = watcher.next(5000) catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    break;
                }
                return err;
            };

            if (maybe_entry) |*entry| {
                // Copy key before entry goes out of scope
                const owned_key = try arena_allocator.dupe(u8, entry.key);
                entry.deinit();
                try keys_set.put(owned_key, {});
            } else {
                // Completion marker - all initial keys collected
                break;
            }
        }

        if (keys_set.count() == 0) {
            return KVError.KeyNotFound;
        }

        // Convert set to array
        const keys_slice = try arena_allocator.alloc([]const u8, keys_set.count());
        var i: usize = 0;
        var iterator = keys_set.iterator();
        while (iterator.next()) |entry| {
            keys_slice[i] = entry.key_ptr.*;
            i += 1;
        }

        return Result([][]const u8){
            .arena = arena,
            .value = keys_slice,
        };
    }

    /// Get all keys in the bucket matching the provided filters
    pub fn keysWithFilters(self: *KV, filters: []const []const u8) !Result([][]const u8) {
        // Create filter subjects for watching
        var filter_subjects = try std.ArrayList([]u8).initCapacity(self.js.nc.allocator, filters.len);
        defer {
            for (filter_subjects.items) |subject| {
                self.js.nc.allocator.free(subject);
            }
            filter_subjects.deinit();
        }

        // Convert filters to full subjects
        for (filters) |filter| {
            const subject = try std.fmt.allocPrint(self.js.nc.allocator, "{s}{s}", .{ self.subject_prefix, filter });
            filter_subjects.appendAssumeCapacity(subject);
        }

        // Use watchFiltered with constructed subjects
        var watcher = try KVWatcher.init(self, filter_subjects.items, .{ .ignore_deletes = true, .meta_only = true });
        defer watcher.deinit();

        const arena = try self.js.nc.allocator.create(std.heap.ArenaAllocator);
        errdefer self.js.nc.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(self.js.nc.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();
        var keys_set = std.StringHashMap(void).init(self.js.nc.allocator);
        defer keys_set.deinit();

        // Collect unique keys - use completion marker to know when done
        while (true) {
            var maybe_entry = watcher.next(5000) catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    break;
                }
                return err;
            };

            if (maybe_entry) |*entry| {
                // Copy key before entry goes out of scope
                const owned_key = try arena_allocator.dupe(u8, entry.key);
                entry.deinit();
                try keys_set.put(owned_key, {});
            } else {
                // Completion marker - all initial keys collected
                break;
            }
        }

        if (keys_set.count() == 0) {
            return KVError.KeyNotFound;
        }

        // Convert set to array
        const keys_slice = try arena_allocator.alloc([]const u8, keys_set.count());
        var i: usize = 0;
        var iterator = keys_set.iterator();
        while (iterator.next()) |entry| {
            keys_slice[i] = entry.key_ptr.*;
            i += 1;
        }

        return Result([][]const u8){
            .arena = arena,
            .value = keys_slice,
        };
    }
};

/// KV Manager handles bucket-level operations
pub const KVManager = struct {
    js: JetStream,

    const Self = @This();

    pub fn init(js: JetStream) KVManager {
        return KVManager{
            .js = js,
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

        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "KV_{s}", .{config.bucket});
        defer self.js.nc.allocator.free(stream_name);

        const subject_pattern = try std.fmt.allocPrint(self.js.nc.allocator, "$KV.{s}.>", .{config.bucket});
        defer self.js.nc.allocator.free(subject_pattern);

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
            .storage = switch (config.storage) {
                .file => .file,
                .memory => .memory,
            },
            .compression = if (config.compression) .s2 else .none,
            .num_replicas = config.replicas,
            .discard = .new,
            .duplicate_window = duplicate_window,
            .allow_direct = true,
            // KV-specific stream settings required by ADR-8
            .allow_rollup_hdrs = true,
            .deny_delete = true,
            .subject_delete_marker_ttl = if (config.limit_marker_ttl == 0) null else config.limit_marker_ttl,
        };

        const result = try self.js.addStream(stream_config);
        defer result.deinit();

        return try KV.init(self.js, config.bucket);
    }

    /// Open an existing KV bucket
    pub fn openBucket(self: *KVManager, bucket_name: []const u8) !KV {
        // Verify bucket exists by getting stream info
        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "KV_{s}", .{bucket_name});
        defer self.js.nc.allocator.free(stream_name);

        const stream_info = self.js.getStreamInfo(stream_name) catch |err| {
            return if (err == error.JetStreamError) KVError.BucketNotFound else err;
        };
        defer stream_info.deinit();

        return try KV.init(self.js, bucket_name);
    }

    /// Delete a KV bucket
    pub fn deleteBucket(self: *KVManager, bucket_name: []const u8) !void {
        try validateBucketName(bucket_name);

        const stream_name = try std.fmt.allocPrint(self.js.nc.allocator, "KV_{s}", .{bucket_name});
        defer self.js.nc.allocator.free(stream_name);

        try self.js.deleteStream(stream_name);
    }
};
