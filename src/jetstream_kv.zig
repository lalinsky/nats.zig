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
const PullSubscription = @import("jetstream.zig").PullSubscription;
const JetStreamSubscription = @import("jetstream.zig").JetStreamSubscription;
const JetStreamMessage = @import("jetstream.zig").JetStreamMessage;
const Subscription = @import("subscription.zig").Subscription;
const Message = @import("message.zig").Message;
const timestamp = @import("timestamp.zig");
const inbox = @import("inbox.zig");
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
    /// Concurrent queue for receiving KVEntry updates
    entry_queue: queue.ConcurrentQueue(KVEntry, 16),
    /// Whether initialization is complete
    init_done: std.atomic.Value(bool),
    /// JetStream subscription handle (using async push subscription for watching)
    subscription: ?*JetStreamSubscription,
    /// Deliver subject for cleanup (owned by watcher)
    deliver_subject: ?[]u8,
    /// Error that occurred during watching (if any)
    watch_error: ?anyerror,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,
    /// Whether this watcher should destroy itself in deinit
    destroy_on_deinit: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) KVWatcher {
        return KVWatcher{
            .entry_queue = queue.ConcurrentQueue(KVEntry, 16).init(allocator, .{}),
            .init_done = std.atomic.Value(bool).init(false),
            .subscription = null,
            .deliver_subject = null,
            .watch_error = null,
            .allocator = allocator,
            .destroy_on_deinit = false,
        };
    }

    pub fn deinit(self: *KVWatcher) void {
        // Clean up all entries remaining in queue
        while (true) {
            var entry = self.entry_queue.pop(0) catch break;
            entry.deinit();
        }
        self.entry_queue.deinit();

        // Clean up subscription if exists
        if (self.subscription) |sub| {
            sub.deinit();
        }

        // Note: Consumer deletion is disabled as it can cause timing issues
        // The unique consumer names should prevent conflicts
        // TODO: Consider proper consumer lifecycle management

        // Clean up deliver subject if exists
        if (self.deliver_subject) |subject| {
            self.allocator.free(subject);
        }

        // Self-destroy if heap-allocated
        if (self.destroy_on_deinit) {
            const allocator = self.allocator;
            allocator.destroy(self);
        }
    }

    /// Destroy heap-allocated watcher (convenience method)
    pub fn destroy(self: *KVWatcher, allocator: std.mem.Allocator) void {
        self.deinit();
        allocator.destroy(self);
    }

    /// Get the next entry from the watcher (returns error.Timeout if no entry available)
    pub fn next(self: *KVWatcher) !KVEntry {
        return self.entry_queue.pop(100); // 100ms timeout
    }

    /// Try to get the next entry without waiting
    pub fn tryNext(self: *KVWatcher) ?KVEntry {
        return self.entry_queue.pop(0) catch null;
    }

    /// Stop the watcher and clean up resources
    pub fn stop(self: *KVWatcher) void {
        if (self.subscription) |sub| {
            sub.deinit();
            self.subscription = null;
        }
    }

    /// Get any error that occurred during watching
    pub fn getError(self: *KVWatcher) ?anyerror {
        return self.watch_error;
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

    /// Get the raw entry (including delete/purge markers) for internal use
    fn getRawEntry(self: *KV, key: []const u8) !KVEntry {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        const stored_msg = self.js.getMsg(self.stream_name, .{ .last_by_subj = subject, .direct = true }) catch |err| {
            return if (err == error.MessageNotFound) KVError.KeyNotFound else err;
        };
        errdefer stored_msg.deinit();

        return try self.parseEntry(stored_msg, key, 0);
    }

    /// Parse a KV entry from a stored message
    fn parseEntry(self: *KV, stored_msg: *Message, key: []const u8, delta: u64) !KVEntry {
        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (stored_msg.headerGet(KvOperationHdr)) |op_value| {
            operation = KVOperation.fromString(op_value) orelse .PUT;
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
        defer self.allocator.free(subject);

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

        // Per ADR-8: deleted data should return "key not found" error in basic gets
        if (entry.operation == .DEL or entry.operation == .PURGE) {
            entry.deinit();
            return KVError.KeyNotFound;
        }

        return entry;
    }

    /// Delete a key (preserves history)
    pub fn delete(self: *KV, key: []const u8) !void {
        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        // Create message with KV-Operation: DEL header and empty body
        const msg = try self.js.nc.newMsg();
        defer msg.deinit();

        try msg.setSubject(subject, true);
        try msg.setPayload("", false);
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

        try msg.setSubject(subject, true);
        try msg.setPayload("", false);
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

    /// Destroy the entire bucket and all data
    pub fn destroy(self: *KV) !void {
        try self.js.deleteStream(self.stream_name);
    }

    /// Watch a key or key pattern for updates
    pub fn watch(self: *KV, key: []const u8, options: WatchOptions) !*KVWatcher {
        try validateKey(key);

        const subject = try self.getKeySubject(key);
        defer self.allocator.free(subject);

        return try self.watchFiltered(&.{subject}, options);
    }

    /// Watch all keys in the bucket
    pub fn watchAll(self: *KV, options: WatchOptions) !*KVWatcher {
        const all_subject = try std.fmt.allocPrint(self.allocator, "{s}>", .{self.subject_prefix});
        defer self.allocator.free(all_subject);

        return try self.watchFiltered(&.{all_subject}, options);
    }

    /// Internal method to watch filtered subjects
    fn watchFiltered(self: *KV, subjects: []const []const u8, options: WatchOptions) !*KVWatcher {
        const watcher = try self.allocator.create(KVWatcher);
        watcher.* = KVWatcher.init(self.allocator);
        watcher.destroy_on_deinit = true;
        errdefer watcher.deinit(); // This will now self-destroy

        // Generate deliver subject for push consumer and assign directly to watcher
        watcher.deliver_subject = try inbox.newInbox(self.allocator);

        // Configure consumer based on watch options (use true ephemeral consumers)
        var consumer_config = ConsumerConfig{
            .name = null, // Truly ephemeral consumer - server generates unique name
            .description = "KV watcher",
            .deliver_subject = watcher.deliver_subject.?, // Reference only, watcher owns the memory
            .deliver_policy = if (options.include_history) .all else .last_per_subject,
            .ack_policy = .none,
            .max_ack_pending = 0, // Disable ack pending to allow ack_policy = .none
            .headers_only = if (options.meta_only) true else null,
        };

        // Set up subject filter
        if (subjects.len == 1) {
            consumer_config.filter_subject = subjects[0];
        } else {
            consumer_config.filter_subjects = subjects;
        }

        // If updates_only, start from new messages
        if (options.updates_only) {
            consumer_config.deliver_policy = .new;
        }

        // Use async push subscription with handler
        const subscription = try self.js.subscribe(self.stream_name, consumer_config, kvWatchHandler, .{ self, watcher, options });

        watcher.subscription = subscription;

        return watcher;
    }

    /// Async handler for KV watch messages
    fn kvWatchHandler(js_msg: *JetStreamMessage, kv: *KV, watcher: *KVWatcher, options: WatchOptions) void {

        // Extract key from subject
        if (js_msg.msg.subject.len <= kv.subject_prefix.len) {
            // Invalid subject, skip
            return;
        }

        const key = js_msg.msg.subject[kv.subject_prefix.len..];

        // Parse the JetStream message into a KVEntry
        // Extract Message pointer without calling js_msg.deinit() - let it go out of scope
        var entry = kv.parseJetStreamEntry(js_msg, key, 0) catch {
            // Failed to parse, skip
            return;
        };

        // Apply watch filters
        if (options.ignore_deletes and (entry.operation == .DEL or entry.operation == .PURGE)) {
            entry.deinit();
            return;
        }

        // Push entry to queue (non-blocking)
        watcher.entry_queue.push(entry) catch |err| {
            // Queue error - store in watcher and cleanup entry
            watcher.watch_error = err;
            entry.deinit();
        };
    }

    /// Parse a JetStream message into a KVEntry
    /// Extracts Message pointer without calling js_msg.deinit() since we reference memory inside the message
    fn parseJetStreamEntry(self: *KV, js_msg: *JetStreamMessage, key: []const u8, delta: u64) !KVEntry {
        const msg = js_msg.msg;

        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (msg.headerGet(KvOperationHdr)) |op_value| {
            operation = KVOperation.fromString(op_value) orelse .PUT;
        }

        // Check for marker reason header
        if (msg.headerGet(NatsMarkerReasonHdr)) |marker_reason| {
            if (MarkerReason.fromString(marker_reason)) |reason| {
                // Convert marker reasons to operations per ADR-8
                operation = switch (reason) {
                    .MaxAge, .Purge => .PURGE,
                    .Remove => .DEL,
                };
            }
        }

        // Parse timestamp from Nats-Time-Stamp header, fallback to 0 if not present
        var created: u64 = 0;
        if (msg.headerGet("Nats-Time-Stamp")) |timestamp_str| {
            created = timestamp.parseTimestamp(timestamp_str) catch 0;
        }

        // Extract sequence from JetStreamMessage metadata
        const revision = js_msg.metadata.sequence.stream orelse 0;

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = msg.data,
            .created = created,
            .revision = revision,
            .delta = delta,
            .operation = operation,
            .msg = msg,
        };
    }

    fn parseJetStreamEntryAsync(self: *KV, msg: *Message, key: []const u8, delta: u64) !KVEntry {
        // Determine operation from parsed headers
        var operation = KVOperation.PUT;
        if (msg.headerGet(KvOperationHdr)) |op_value| {
            operation = KVOperation.fromString(op_value) orelse .PUT;
        }

        // Check for marker reason header
        if (msg.headerGet(NatsMarkerReasonHdr)) |marker_reason| {
            if (MarkerReason.fromString(marker_reason)) |reason| {
                // Convert marker reasons to operations per ADR-8
                operation = switch (reason) {
                    .MaxAge, .Purge => .PURGE,
                    .Remove => .DEL,
                };
            }
        }

        // Parse timestamp from Nats-Time-Stamp header, fallback to 0 if not present
        var created: u64 = 0;
        if (msg.headerGet("Nats-Time-Stamp")) |timestamp_str| {
            created = timestamp.parseTimestamp(timestamp_str) catch 0;
        }

        // For async handler, we need to extract sequence from headers since we don't have JetStreamMessage metadata
        var revision: u64 = 0;
        if (msg.headerGet("Nats-Sequence")) |seq_str| {
            revision = std.fmt.parseInt(u64, seq_str, 10) catch 0;
        }

        return KVEntry{
            .bucket = self.bucket_name,
            .key = key,
            .value = msg.data,
            .created = created,
            .revision = revision,
            .delta = delta,
            .operation = operation,
            .msg = msg,
        };
    }

    /// Get historical values for a key
    pub fn history(self: *KV, key: []const u8) ![]KVEntry {
        const watcher = try self.watch(key, .{ .include_history = true });
        defer watcher.deinit();

        var entries = std.ArrayList(KVEntry).init(self.allocator);
        errdefer {
            // Clean up entries on error
            for (entries.items) |*entry| {
                entry.deinit();
            }
            entries.deinit();
        }

        // Collect all entries - use short timeout to get initial data
        var timeout_count: u32 = 0;
        while (timeout_count < 5) { // Allow a few timeouts to collect initial data
            const entry = watcher.next() catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    timeout_count += 1;
                    continue;
                }
                entries.deinit();
                return err;
            };
            timeout_count = 0; // Reset timeout count on successful read
            try entries.append(entry);
        }

        if (entries.items.len == 0) {
            entries.deinit();
            return KVError.KeyNotFound;
        }

        return try entries.toOwnedSlice();
    }

    /// Get all keys in the bucket
    pub fn keys(self: *KV) !Result([][]const u8) {
        const watcher = try self.watchAll(.{ .ignore_deletes = true, .meta_only = true });
        defer watcher.deinit();

        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }

        const arena_allocator = arena.allocator();
        var keys_set = std.StringHashMap(void).init(arena_allocator);

        // Collect unique keys - use short timeout to get initial data
        var timeout_count: u32 = 0;
        while (timeout_count < 5) { // Allow a few timeouts to collect initial data
            var entry = watcher.next() catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    timeout_count += 1;
                    continue;
                }
                arena.deinit();
                self.allocator.destroy(arena);
                return err;
            };
            timeout_count = 0; // Reset timeout count on successful read
            defer entry.deinit();

            // Add key to set (duplicates are automatically handled)
            const owned_key = try arena_allocator.dupe(u8, entry.key);
            try keys_set.put(owned_key, {});
        }

        if (keys_set.count() == 0) {
            arena.deinit();
            self.allocator.destroy(arena);
            return KVError.KeyNotFound;
        }

        // Convert set to array
        var keys_list = try std.ArrayList([]const u8).initCapacity(arena_allocator, keys_set.count());
        var iterator = keys_set.iterator();
        while (iterator.next()) |entry| {
            keys_list.appendAssumeCapacity(entry.key_ptr.*);
        }

        const result: Result([][]const u8) = .{
            .arena = arena,
            .value = try keys_list.toOwnedSlice(),
        };
        return result;
    }

    /// Get all keys in the bucket matching the provided filters
    pub fn keysWithFilters(self: *KV, filters: []const []const u8) !Result([][]const u8) {
        // Create filter subjects for watching
        var filter_subjects = try std.ArrayList([]u8).initCapacity(self.allocator, filters.len);
        defer {
            for (filter_subjects.items) |subject| {
                self.allocator.free(subject);
            }
            filter_subjects.deinit();
        }

        // Convert filters to full subjects
        for (filters) |filter| {
            const subject = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.subject_prefix, filter });
            filter_subjects.appendAssumeCapacity(subject);
        }

        // Use watchFiltered with constructed subjects
        const watcher = try self.watchFiltered(filter_subjects.items, .{ .ignore_deletes = true, .meta_only = true });
        defer watcher.deinit();

        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }

        const arena_allocator = arena.allocator();
        var keys_set = std.StringHashMap(void).init(arena_allocator);

        // Collect unique keys - use short timeout to get initial data
        var timeout_count: u32 = 0;
        while (timeout_count < 5) { // Allow a few timeouts to collect initial data
            var entry = watcher.next() catch |err| {
                if (err == error.Timeout or err == error.QueueEmpty) {
                    timeout_count += 1;
                    continue;
                }
                arena.deinit();
                self.allocator.destroy(arena);
                return err;
            };
            timeout_count = 0; // Reset timeout count on successful read
            defer entry.deinit();

            // Add key to set (duplicates are automatically handled)
            const owned_key = try arena_allocator.dupe(u8, entry.key);
            try keys_set.put(owned_key, {});
        }

        if (keys_set.count() == 0) {
            arena.deinit();
            self.allocator.destroy(arena);
            return KVError.KeyNotFound;
        }

        // Convert set to array
        var keys_list = try std.ArrayList([]const u8).initCapacity(arena_allocator, keys_set.count());
        var iterator = keys_set.iterator();
        while (iterator.next()) |entry| {
            keys_list.appendAssumeCapacity(entry.key_ptr.*);
        }

        const result: Result([][]const u8) = .{
            .arena = arena,
            .value = try keys_list.toOwnedSlice(),
        };
        return result;
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
