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
const message_mod = @import("message.zig");
const Message = message_mod.Message;
const MessageList = message_mod.MessageList;
const STATUS_CONTROL = message_mod.STATUS_CONTROL;
const STATUS_NOT_FOUND = message_mod.STATUS_NOT_FOUND;
const STATUS_TIMEOUT = message_mod.STATUS_TIMEOUT;
const STATUS_MAX_BYTES = message_mod.STATUS_MAX_BYTES;
const Connection = @import("connection.zig").Connection;
const Subscription = @import("subscription.zig").Subscription;
const subscription_mod = @import("subscription.zig");
const jetstream_message = @import("jetstream_message.zig");
const inbox = @import("inbox.zig");
const parseTimestamp = @import("timestamp.zig").parseTimestamp;
const Result = @import("result.zig").Result;

const log = @import("log.zig").log;

// Re-export JetStream message types
pub const JetStreamMessage = jetstream_message.JetStreamMessage;
pub const MsgMetadata = jetstream_message.MsgMetadata;
pub const SequencePair = jetstream_message.SequencePair;

const default_api_prefix = "$JS.API.";
const default_request_timeout_ms = 5000;

// JetStream publish headers
const MsgIdHdr = "Nats-Msg-Id";
const ExpectedStreamHdr = "Nats-Expected-Stream";
const ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence";
const ExpectedLastSubjSeqHdr = "Nats-Expected-Last-Subject-Sequence";
const ExpectedLastSubjSeqSubjectHdr = "Nats-Expected-Last-Subject-Sequence-Subject";
const ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";
const MsgTTLHdr = "Nats-TTL";

const jetstream_errors = @import("jetstream_errors.zig");

fn isProhibitedChar(c: u8) bool {
    // Explicit prohibited characters
    if (c == '.' or c == '>' or c == '*' or c == '/' or c == '\\') {
        return true;
    }

    // Any whitespace or control character
    if (std.ascii.isWhitespace(c) or std.ascii.isControl(c)) {
        return true;
    }

    return false;
}

pub fn validateStreamName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidStreamName;
    }
    for (name) |c| {
        if (isProhibitedChar(c)) {
            return error.InvalidStreamName;
        }
    }
}

test "validateStreamName" {
    try validateStreamName("valid-stream-name");
    try std.testing.expectError(error.InvalidStreamName, validateStreamName(""));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo bar"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo.bar"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo*"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo>"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo/"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("foo\\"));
}

pub fn validateConsumerName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidConsumerName;
    }
    for (name) |c| {
        if (isProhibitedChar(c)) {
            return error.InvalidConsumerName;
        }
    }
}

test "validateConsumerName" {
    try validateConsumerName("valid-consumer-name");
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName(""));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo bar"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo.bar"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo*"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo>"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo/"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("foo\\"));
}

const ErrorResponse = struct {
    @"error": struct {
        /// HTTP like error code in the 300 to 500 range
        code: u16,
        /// A human friendly description of the error
        description: []const u8 = "",
        /// The NATS error code unique to each kind of error
        err_code: u16 = 0,
    },
};

const AccountInfoResponse = struct {
    /// Memory Storage being used for Stream Message storage
    memory: u64,
    /// File Storage being used for Stream Message storage
    storage: u64,
    // Number of active Streams
    streams: u32,
    /// Number of active Consumers
    consumers: u32,
};

pub const StreamConfig = struct {
    /// A unique name for the Stream
    name: []const u8,
    /// A short description of the purpose of this stream
    description: ?[]const u8 = null,
    /// A list of subjects to consume, supports wildcards
    subjects: []const []const u8,
    /// How messages are retained in the stream
    retention: enum { limits, interest, workqueue } = .limits,
    /// How many Consumers can be defined for a given Stream. -1 for unlimited.
    max_consumers: i64 = -1,
    /// How many messages may be in a Stream. -1 for unlimited.
    max_msgs: i64 = -1,
    /// For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit
    max_msgs_per_subject: i64 = -1,
    /// How big the Stream may be. -1 for unlimited.
    max_bytes: i64 = -1,
    /// Maximum age of any message in nanoseconds. 0 for unlimited.
    max_age: u64 = 0,
    /// The largest message that will be accepted. -1 for unlimited.
    max_msg_size: i32 = -1,
    /// The storage backend to use for the Stream
    storage: enum { file, memory } = .file,
    /// Optional compression algorithm used for the Stream
    compression: enum { none, s2 } = .none,
    /// How many replicas to keep for each message
    num_replicas: u8 = 1,
    /// Disables acknowledging messages that are received by the Stream
    no_ack: bool = false,
    /// When a Stream reaches its limits either old messages are deleted or new ones are denied
    discard: enum { old, new } = .old,
    /// The time window to track duplicate messages for, in nanoseconds. 0 for default
    duplicate_window: u64 = 0,
    /// Allow direct get on any replica instead of just the leader
    allow_direct: ?bool = null,
    /// Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message
    allow_rollup_hdrs: ?bool = null,
    /// Whether to allow delete operations on individual messages
    deny_delete: ?bool = null,
    /// TTL for delete markers when subject is deleted by TTL (nanoseconds)
    subject_delete_marker_ttl: ?u64 = null,
};

/// Response from $JS.API.STREAM.NAMES
const StreamNamesResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    streams: ?[]const []const u8,
};

/// Response from $JS.API.STREAM.LIST
const StreamListResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    streams: ?[]const StreamInfo,
};

/// Response from $JS.API.CONSUMER.NAMES
const ConsumerNamesResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    consumers: ?[]const []const u8,
};

/// Response from $JS.API.CONSUMER.LIST
const ConsumerListResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    consumers: ?[]const ConsumerInfo,
};

const StreamState = struct {
    messages: u64,
    bytes: u64,
    first_seq: u64,
    first_ts: []const u8,
    last_seq: u64,
    last_ts: []const u8,
    consumer_count: u32,
};

pub const StreamInfo = struct {
    config: StreamConfig,
    state: StreamState,
    created: []const u8,
};

pub const ConsumerConfig = struct {
    /// A unique name for a consumer
    name: ?[]const u8 = null,
    /// A unique name for a durable consumer (deprecated, use name)
    durable_name: ?[]const u8 = null,
    /// A short description of the purpose of this consumer
    description: ?[]const u8 = null,
    /// The point in the stream to receive messages from, either 'all', 'last', 'new', 'by_start_sequence', 'by_start_time', 'last_per_subject'
    deliver_policy: enum { all, last, new, by_start_sequence, by_start_time, last_per_subject } = .all,
    /// Used with deliver_policy 'by_start_sequence' to define the sequence to start at
    opt_start_seq: ?u64 = null,
    /// Used with deliver_policy 'by_start_time' to define the time to start at
    opt_start_time: ?[]const u8 = null,
    /// The subject to deliver messages to, omit for pull consumers
    deliver_subject: ?[]const u8 = null,
    /// How messages are acknowledged, either 'none', 'all', or 'explicit'
    ack_policy: enum { none, all, explicit } = .explicit,
    /// How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery
    ack_wait: u64 = 30_000_000_000, // 30 seconds
    /// The number of times a message will be redelivered to consumers if not acknowledged in time
    max_deliver: i64 = -1,
    /// Filter the stream by a single subject
    filter_subject: ?[]const u8 = null,
    /// Filter the stream by multiple subjects
    filter_subjects: ?[]const []const u8 = null,
    /// How messages are sent, either 'instant' or 'original'
    replay_policy: enum { instant, original } = .instant,
    /// The rate at which messages will be delivered to clients, expressed in bit per second
    rate_limit_bps: ?u64 = null,
    /// The maximum number of messages without acknowledgement that can be outstanding
    max_ack_pending: i64 = 1000,
    /// If the Consumer is idle for more than this many nano seconds a empty message with Status header 100 will be sent
    idle_heartbeat: ?u64 = null,
    /// For push consumers this will regularly send an empty mess with Status header 100 and a reply subject
    /// (This field was moved to the end to avoid duplication)
    /// The number of pulls that can be outstanding on a pull consumer
    max_waiting: i64 = 512,
    /// Delivers only the headers of messages in the stream and not the bodies
    headers_only: ?bool = null,
    /// The largest batch property that may be specified when doing a pull on a Pull Consumer
    max_batch: ?i64 = null,
    /// How long to allow pull requests to remain open
    max_expires: ?u64 = null,
    /// The number of replicas for this consumer's state
    num_replicas: ?u8 = null,
    /// Queue group for push consumers (load balancing)
    deliver_group: ?[]const u8 = null,
    /// Enable flow control protocol for push consumers
    flow_control: ?bool = null,
};

pub const ConsumerInfo = struct {
    /// The name of the consumer
    name: []const u8,
    /// The name of the stream this consumer belongs to
    stream_name: []const u8,
    /// The consumer configuration
    config: ConsumerConfig,
    /// The last delivered sequence for this consumer
    delivered: struct {
        consumer_seq: u64,
        stream_seq: u64,
    },
    /// The last acknowledged message
    ack_floor: struct {
        consumer_seq: u64,
        stream_seq: u64,
    },
    /// The number of pending messages for this consumer
    num_ack_pending: u64,
    /// The number of redelivered messages
    num_redelivered: u64,
    /// The number of waiting pull requests
    num_waiting: u64,
    /// The number of pending pull requests
    num_pending: u64,
    /// When this consumer was created
    created: []const u8,
};

/// Request for $JS.API.STREAM.PURGE
pub const StreamPurgeRequest = struct {
    /// Restrict purging to messages that match this subject
    filter: ?[]const u8 = null,
    /// Purge all messages up to but not including the message with this sequence
    seq: ?u64 = null,
    /// Ensures this many messages are present after the purge
    keep: ?u64 = null,
};

/// Response from $JS.API.STREAM.PURGE
const StreamPurgeResponse = struct {
    /// Indicates if this response is an error
    success: bool,
    /// The number of messages purged
    purged: u64,
};

/// Request for both $JS.API.STREAM.MSG.GET and $JS.API.DIRECT.GET
pub const GetMsgRequest = struct {
    /// Stream sequence number of the message to retrieve
    seq: ?u64 = null,
    /// Retrieves the last message for a given subject
    last_by_subj: ?[]const u8 = null,
    /// Retrieves the first message for a given subject at or after seq
    next_by_subj: ?[]const u8 = null,
    /// Number of messages to retrieve in batch mode (direct get only)
    batch: ?u32 = null,
    /// Maximum bytes to retrieve in batch mode (direct get only)
    max_bytes: ?u64 = null,
    /// Start time for time-based queries (RFC3339 format, direct get only)
    start_time: ?[]const u8 = null,
    /// Multiple subjects for multi-subject queries (direct get only)
    multi_last: ?[]const []const u8 = null,
    /// Upper bound sequence for consistent multi-subject reads (direct get only)
    up_to_seq: ?u64 = null,
    /// Upper bound time for consistent multi-subject reads (RFC3339 format, direct get only)
    up_to_time: ?[]const u8 = null,
    /// Suppress headers in response (direct get only)
    no_hdr: ?bool = null,
};

/// Options for getMsg() method
pub const GetMsgOptions = struct {
    /// Stream sequence number of the message to retrieve
    seq: ?u64 = null,
    /// Retrieves the last message for a given subject
    last_by_subj: ?[]const u8 = null,
    /// Retrieves the first message for a given subject at or after seq
    next_by_subj: ?[]const u8 = null,
    /// Use direct get API ($JS.API.DIRECT.GET) instead of legacy API
    direct: bool = false,
};

/// Request for $JS.API.STREAM.MSG.DELETE
pub const DeleteMsgRequest = struct {
    /// Stream sequence number of the message to delete
    seq: u64,
    /// Only remove the message, don't securely erase data
    no_erase: ?bool = null,
};

/// Response from $JS.API.STREAM.MSG.DELETE
const MsgDeleteResponse = struct {
    /// Indicates if the deletion was successful
    success: bool,
};

/// Response from $JS.API.STREAM.MSG.GET
const GetMsgResponse = struct {
    message: StoredMessage,
};

/// Stored message data from JetStream
pub const StoredMessage = struct {
    subject: []const u8,
    seq: u64,
    time: []const u8,
    hdrs: ?[]const u8 = null,
    data: []const u8,
};

/// Request for fetching messages from a pull consumer
pub const FetchRequest = struct {
    /// Maximum number of messages to fetch
    batch: usize = 1,
    /// Maximum bytes to fetch (optional)
    max_bytes: ?u64 = null,
    /// Request timeout in nanoseconds (default: 30 seconds)
    expires: ?u64 = null,
    /// Don't wait if no messages are available immediately
    no_wait: ?bool = null,
    /// Heartbeat interval in nanoseconds for long requests
    idle_heartbeat: ?u64 = null,
};

/// Options for getMsgs() batch method (future implementation)
pub const GetMsgsOptions = struct {
    // TODO: Define batch options based on ADR-31 when implemented
};

/// Publishing acknowledgment from JetStream
pub const PubAck = struct {
    /// Stream that received the message
    stream: []const u8,
    /// Sequence number assigned by the stream
    seq: u64,
    /// Whether this was a duplicate message
    duplicate: bool = false,
    /// JetStream domain (optional)
    domain: ?[]const u8 = null,
};

/// Options for JetStream publishing
pub const PublishOptions = struct {
    /// Message deduplication ID
    msg_id: ?[]const u8 = null,
    /// Expected target stream name
    expected_stream: ?[]const u8 = null,
    /// Expected last sequence number for optimistic concurrency control
    expected_last_seq: ?u64 = null,
    /// Expected last sequence number per subject
    expected_last_subject_seq: ?u64 = null,
    /// Override subject used for the per-subject sequence check
    expected_last_subject_seq_subject: ?[]const u8 = null,
    /// Expected last message ID
    expected_last_msg_id: ?[]const u8 = null,
    /// Message time-to-live in nanoseconds
    msg_ttl: ?u64 = null,
};

/// Batch of messages returned from fetch operation
pub const MessageBatch = struct {
    /// Allocator used for cleanup
    allocator: std.mem.Allocator,
    /// Array of JetStream messages
    messages: []*JetStreamMessage,
    /// Any error that occurred during fetch
    err: ?anyerror = null,

    pub fn deinit(self: *MessageBatch) void {
        // Clean up each message individually
        for (self.messages) |msg| {
            msg.deinit();
        }
        // Free the messages array
        self.allocator.free(self.messages);
    }
};

/// JetStream pull subscription
pub const PullSubscription = struct {
    /// JetStream context
    js: *JetStream,
    /// Stream name
    stream_name: []const u8,
    /// Consumer name
    consumer_name: []const u8,
    /// Consumer information
    consumer_info: Result(ConsumerInfo),
    /// Persistent wildcard inbox subscription
    inbox_subscription: *Subscription,
    /// Inbox prefix for reply subjects (e.g., "_INBOX.abc123.")
    inbox_prefix: []u8,
    /// Fetch ID counter for unique reply subjects
    fetch_id_counter: u64 = 0,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex = .{},

    pub fn deinit(self: *PullSubscription) void {
        self.consumer_info.deinit();
        self.js.nc.unsubscribe(self.inbox_subscription);
        self.js.allocator.free(self.inbox_prefix);
        self.js.allocator.destroy(self);
    }

    /// Fetch a batch of messages from the pull consumer
    pub fn fetch(self: *PullSubscription, batch: usize, timeout_ms: u64) !MessageBatch {
        if (batch == 0) return error.InvalidBatchSize;

        self.mutex.lock();
        defer self.mutex.unlock();

        // Generate unique fetch ID and reply subject
        self.fetch_id_counter += 1;
        const fetch_id = self.fetch_id_counter;

        const reply_subject = try std.fmt.allocPrint(self.js.allocator, "{s}{d}", .{ self.inbox_prefix, fetch_id });
        defer self.js.allocator.free(reply_subject);

        const request = FetchRequest{
            .batch = batch,
            .expires = timeout_ms * std.time.ns_per_ms,
        };

        // Serialize the fetch request to JSON
        const request_json = try std.json.stringifyAlloc(self.js.allocator, request, .{
            .emit_null_optional_fields = false,
        });
        defer self.js.allocator.free(request_json);

        // Build the full API subject
        const api_subject = try std.fmt.allocPrint(self.js.allocator, "{s}CONSUMER.MSG.NEXT.{s}.{s}", .{ default_api_prefix, self.stream_name, self.consumer_name });
        defer self.js.allocator.free(api_subject);

        // Send the pull request with reply subject
        try self.js.nc.publishRequest(api_subject, reply_subject, request_json);

        // Collect messages
        var messages = std.ArrayList(*JetStreamMessage).init(self.js.allocator);
        defer messages.deinit();

        var batch_complete = false;
        var fetch_error: ?anyerror = null;

        // Collect messages until batch is complete or timeout
        while (!batch_complete and messages.items.len < request.batch) {
            if (self.inbox_subscription.nextMsg(timeout_ms * 2)) |raw_msg| {
                log.debug("Message: subject={s}, reply={s}, data='{s}'", .{ raw_msg.subject, raw_msg.reply orelse "none", raw_msg.data });
                // JetStream messages arrive with original subjects and ACK reply subjects
                // The timestamp in the ACK subject ensures messages belong to this fetch request
                // (timestamps are monotonically increasing and unique per message delivery)

                if (raw_msg.status_code == STATUS_NOT_FOUND) {
                    // No messages available
                    raw_msg.deinit();
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_TIMEOUT) {
                    // Request timeout
                    raw_msg.deinit();
                    if (messages.items.len == 0) {
                        fetch_error = error.Timeout;
                    }
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_MAX_BYTES) {
                    // Consumer sequence mismatch
                    raw_msg.deinit();
                    fetch_error = error.ConsumerSequenceMismatch;
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_CONTROL) {
                    // Heartbeat - continue waiting
                    raw_msg.deinit();
                    continue;
                } else if (raw_msg.status_code > 0) {
                    // Unknown status code - clean up and continue
                    raw_msg.deinit();
                } else {
                    // This is a regular message - convert to JetStream message
                    const js_msg_ptr = try jetstream_message.createJetStreamMessage(self.js.nc, raw_msg);
                    errdefer js_msg_ptr.deinit();

                    try messages.append(js_msg_ptr);
                }
            } else |err| switch (err) {
                error.Timeout => {
                    // Timeout occurred
                    batch_complete = true;
                },
            }
        }

        // Convert ArrayList to owned slice
        const messages_slice = try messages.toOwnedSlice();

        return MessageBatch{
            .messages = messages_slice,
            .err = fetch_error,
            .allocator = self.js.allocator,
        };
    }
};

/// JetStream push subscription
pub const JetStreamSubscription = struct {
    /// Underlying NATS subscription
    subscription: *Subscription,
    /// JetStream context
    js: *JetStream,
    /// Consumer information (Result wrapper)
    consumer_info: Result(ConsumerInfo),

    pub fn deinit(self: *JetStreamSubscription) void {
        self.consumer_info.deinit();
        self.subscription.deinit();
        self.js.allocator.destroy(self);
    }

    /// Get the next JetStream message synchronously (for sync subscriptions)
    pub fn nextMsg(self: *JetStreamSubscription, timeout_ms: u64) !*JetStreamMessage {
        // Get the next message from the underlying subscription
        const msg = try self.subscription.nextMsg(timeout_ms);
        errdefer msg.deinit();

        // Convert to JetStream message
        return try jetstream_message.createJetStreamMessage(self.js.nc, msg);
    }
};

pub const JetStreamOptions = struct {
    request_timeout_ms: u64 = default_request_timeout_ms,
    // Add options here
};

pub const JetStream = struct {
    allocator: std.mem.Allocator,
    nc: *Connection,
    opts: JetStreamOptions,

    pub fn init(allocator: std.mem.Allocator, nc: *Connection, options: JetStreamOptions) JetStream {
        return .{
            .allocator = allocator,
            .nc = nc,
            .opts = options,
        };
    }

    pub fn deinit(self: *JetStream) void {
        _ = self;
    }

    fn sendRequest(self: *JetStream, subject: []const u8, payload: []const u8) !*Message {
        const full_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ default_api_prefix, subject });
        defer self.allocator.free(full_subject);

        return try self.nc.request(full_subject, payload, self.opts.request_timeout_ms);
    }

    /// Parse an error response from the server, if present.
    fn maybeParseErrorResponse(_: *JetStream, msg: *Message) !void {
        var buf: [1024]u8 = undefined;
        var allocator = std.heap.FixedBufferAllocator.init(&buf);

        const response = std.json.parseFromSliceLeaky(ErrorResponse, allocator.allocator(), msg.data, .{
            .allocate = .alloc_if_needed,
            .ignore_unknown_fields = true,
        }) catch return;

        const info = response.@"error";
        log.err("JetStream error: code={d} err_code={d} description={s}", .{ info.code, info.err_code, info.description });
        log.debug("Full response: {s}", .{msg.data});

        // Map specific JetStream error codes to proper error types
        return jetstream_errors.mapErrorCode(info.err_code);
    }

    /// Parse a response from the server, handling errors if present.
    fn parseResponse(self: *JetStream, comptime T: type, msg: *Message) !Result(T) {
        try self.maybeParseErrorResponse(msg);

        const parsed = std.json.parseFromSlice(T, self.allocator, msg.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| {
            log.err("Failed to parse response: {}", .{err});
            log.debug("Full response: {s}", .{msg.data});
            return error.JetStreamParseError;
        };

        // Reuse the arena from std.json.Parsed in our Result
        return Result(T){
            .arena = parsed.arena,
            .value = parsed.value,
        };
    }

    // Retrieves stats and limits for the connected user's account.
    pub fn getAccountInfo(self: *JetStream) !Result(AccountInfoResponse) {
        const msg = try self.sendRequest("INFO", "");
        defer msg.deinit();

        return try self.parseResponse(AccountInfoResponse, msg);
    }

    /// Retrieves a list of stream names.
    pub fn listStreamNames(self: *JetStream) !Result([]const []const u8) {
        const msg = try self.sendRequest("STREAM.NAMES", "");
        defer msg.deinit();

        const page_result = try self.parseResponse(StreamNamesResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const streams = page_result.value.streams orelse &[_][]const u8{};
        std.debug.assert(page_result.value.total == streams.len);

        const result: Result([]const []const u8) = .{
            .arena = page_result.arena,
            .value = streams,
        };
        return result;
    }

    /// Retrieves a list of streams with full information.
    pub fn listStreams(self: *JetStream) !Result([]const StreamInfo) {
        const msg = try self.sendRequest("STREAM.LIST", "");
        defer msg.deinit();

        const page_result = try self.parseResponse(StreamListResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const streams = page_result.value.streams orelse &[_]StreamInfo{};
        std.debug.assert(page_result.value.total == streams.len);

        const result: Result([]const StreamInfo) = .{
            .arena = page_result.arena,
            .value = streams,
        };
        return result;
    }

    /// Creates a new stream with the provided configuration.
    pub fn addStream(self: *JetStream, config: StreamConfig) !Result(StreamInfo) {
        try validateStreamName(config.name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.CREATE.{s}", .{config.name});
        defer self.allocator.free(subject);

        // Serialize the config to JSON
        const config_json = try std.json.stringifyAlloc(self.allocator, config, .{});
        defer self.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Updates a stream with the provided configuration.
    pub fn updateStream(self: *JetStream, config: StreamConfig) !Result(StreamInfo) {
        try validateStreamName(config.name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.UPDATE.{s}", .{config.name});
        defer self.allocator.free(subject);

        // Serialize the config to JSON
        const config_json = try std.json.stringifyAlloc(self.allocator, config, .{});
        defer self.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Deletes a stream.
    pub fn deleteStream(self: *JetStream, stream_name: []const u8) !void {
        try validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.DELETE.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        // Just check for errors, don't need to parse the response
        try self.maybeParseErrorResponse(msg);
    }

    /// Gets information about a specific stream.
    pub fn getStreamInfo(self: *JetStream, stream_name: []const u8) !Result(StreamInfo) {
        try validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.INFO.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Retrieves a list of consumer names for a stream.
    pub fn listConsumerNames(self: *JetStream, stream_name: []const u8) !Result([]const []const u8) {
        try validateStreamName(stream_name);

        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.NAMES.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        const page_result = try self.parseResponse(ConsumerNamesResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const consumers = page_result.value.consumers orelse &[_][]const u8{};
        std.debug.assert(page_result.value.total == consumers.len);

        const result: Result([]const []const u8) = .{
            .arena = page_result.arena,
            .value = consumers,
        };
        return result;
    }

    /// Retrieves a list of consumers with full information for a stream.
    pub fn listConsumers(self: *JetStream, stream_name: []const u8) !Result([]const ConsumerInfo) {
        try validateStreamName(stream_name);

        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.LIST.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        const page_result = try self.parseResponse(ConsumerListResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const consumers = page_result.value.consumers orelse &[_]ConsumerInfo{};
        std.debug.assert(page_result.value.total == consumers.len);

        const result: Result([]const ConsumerInfo) = .{
            .arena = page_result.arena,
            .value = consumers,
        };
        return result;
    }

    /// Creates a new consumer with the provided configuration.
    /// Uses DURABLE endpoint only if durable_name is provided, otherwise creates ephemeral consumer.
    pub fn addConsumer(self: *JetStream, stream_name: []const u8, config: ConsumerConfig) !Result(ConsumerInfo) {
        try validateStreamName(stream_name);
        if (config.name) |n| try validateConsumerName(n);
        if (config.durable_name) |n| try validateConsumerName(n);

        log.info("adding consumer", .{});
        const subject = if (config.durable_name) |durable_name|
            try std.fmt.allocPrint(self.allocator, "CONSUMER.DURABLE.CREATE.{s}.{s}", .{ stream_name, durable_name })
        else
            try std.fmt.allocPrint(self.allocator, "CONSUMER.CREATE.{s}", .{stream_name});
        defer self.allocator.free(subject);

        // Create request payload
        const request_payload = struct {
            stream_name: []const u8,
            config: ConsumerConfig,
        }{ .stream_name = stream_name, .config = config };

        const config_json = try std.json.stringifyAlloc(self.allocator, request_payload, .{});
        defer self.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(ConsumerInfo, msg);
    }

    /// Gets information about a specific consumer.
    pub fn getConsumerInfo(self: *JetStream, stream_name: []const u8, consumer_name: []const u8) !Result(ConsumerInfo) {
        try validateStreamName(stream_name);
        try validateConsumerName(consumer_name);

        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.INFO.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(ConsumerInfo, msg);
    }

    /// Deletes a consumer.
    pub fn deleteConsumer(self: *JetStream, stream_name: []const u8, consumer_name: []const u8) !void {
        try validateStreamName(stream_name);
        try validateConsumerName(consumer_name);

        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.DELETE.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        // Just check for errors, don't need to parse the response
        try self.maybeParseErrorResponse(msg);
    }

    /// Purges messages from a stream.
    pub fn purgeStream(self: *JetStream, stream_name: []const u8, request: StreamPurgeRequest) !Result(StreamPurgeResponse) {
        try validateStreamName(stream_name);

        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.PURGE.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const request_json = try std.json.stringifyAlloc(self.allocator, request, .{});
        defer self.allocator.free(request_json);

        const msg = try self.sendRequest(subject, request_json);
        defer msg.deinit();

        return try self.parseResponse(StreamPurgeResponse, msg);
    }

    /// Internal function for getting messages from the stream using legacy API
    fn getMsgLegacy(self: *JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        try validateStreamName(stream_name);

        // Validation already done in getMsg(), no need to repeat

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.MSG.GET.{s}", .{stream_name});
        defer self.allocator.free(subject);

        // Create GetMsgRequest from options
        const request = GetMsgRequest{
            .seq = options.seq,
            .last_by_subj = options.last_by_subj,
            .next_by_subj = options.next_by_subj,
        };

        // Serialize the request to JSON, omitting null fields
        const request_json = try std.json.stringifyAlloc(self.allocator, request, .{
            .emit_null_optional_fields = false,
        });
        defer self.allocator.free(request_json);

        const resp = try self.sendRequest(subject, request_json);
        defer resp.deinit();

        // Parse the response to extract the message
        const parsed_resp = try self.parseResponse(GetMsgResponse, resp);
        defer parsed_resp.deinit();

        const stored_msg = parsed_resp.value.message;

        // Create a new Message using the proper API
        const msg = try self.nc.newMsg();
        errdefer msg.deinit();

        // Set the subject
        try msg.setSubject(stored_msg.subject, true);

        // Set the sequence number
        msg.seq = stored_msg.seq;
        msg.time = try parseTimestamp(stored_msg.time);

        // Decode base64 data
        if (stored_msg.data.len > 0) {
            const decoder = std.base64.standard.Decoder;
            const data_len = try decoder.calcSizeForSlice(stored_msg.data);
            const decoded_data = try msg.arena.allocator().alloc(u8, data_len);
            try decoder.decode(decoded_data, stored_msg.data);
            try msg.setPayload(decoded_data, false);
        } else {
            try msg.setPayload("", false);
        }

        // Decode base64 headers if present
        if (stored_msg.hdrs) |hdrs_b64| {
            const decoder = std.base64.standard.Decoder;
            const hdrs_len = try decoder.calcSizeForSlice(hdrs_b64);
            const decoded_headers = try msg.arena.allocator().alloc(u8, hdrs_len);
            try decoder.decode(decoded_headers, hdrs_b64);
            try msg.setRawHeaders(decoded_headers, false);
        }

        return msg;
    }

    /// Gets a message from the stream using the specified options
    pub fn getMsg(self: *JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        // Validate options combinations:
        // 1. seq only - get message by sequence
        // 2. last_by_subj only - get last message for subject
        // 3. seq + next_by_subj - get next message for subject at or after sequence
        const has_seq = options.seq != null;
        const has_last_by_subj = options.last_by_subj != null;
        const has_next_by_subj = options.next_by_subj != null;

        if (has_last_by_subj and (has_seq or has_next_by_subj)) {
            // last_by_subj cannot be combined with seq or next_by_subj
            return error.InvalidGetMessageOptions;
        } else if (has_next_by_subj and !has_seq) {
            // next_by_subj requires seq to be set
            return error.InvalidGetMessageOptions;
        } else if (!has_seq and !has_last_by_subj and !has_next_by_subj) {
            // At least one option must be set
            return error.InvalidGetMessageOptions;
        }

        if (options.direct) {
            return self.getMsgDirect(stream_name, options);
        } else {
            return self.getMsgLegacy(stream_name, options);
        }
    }

    /// Gets multiple messages from the stream (batch operation) - NOT IMPLEMENTED
    pub fn getMsgs(self: *JetStream, stream_name: []const u8, options: GetMsgsOptions) !MessageList {
        _ = self;
        _ = stream_name;
        _ = options;
        return error.NotImplemented;
    }

    /// Internal function for direct get messages from any stream replica
    fn getMsgDirect(self: *JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        log.debug("getMsgDirect: Starting with stream_name={s}", .{stream_name});
        try validateStreamName(stream_name);
        log.debug("getMsgDirect: Stream name validation passed", .{});

        // Build the subject for the direct get API call
        const subject = try std.fmt.allocPrint(self.allocator, "DIRECT.GET.{s}", .{stream_name});
        defer self.allocator.free(subject);

        // Convert GetMsgOptions to GetMsgRequest
        const request = GetMsgRequest{
            .seq = options.seq,
            .last_by_subj = options.last_by_subj,
            .next_by_subj = options.next_by_subj,
        };

        // Serialize the request to JSON, omitting null fields
        const request_json = try std.json.stringifyAlloc(self.allocator, request, .{
            .emit_null_optional_fields = false,
        });
        defer self.allocator.free(request_json);

        const resp = try self.sendRequest(subject, request_json);
        errdefer resp.deinit();

        // Check for error status codes
        if (resp.status_code == STATUS_NOT_FOUND) {
            return error.MessageNotFound;
        } else if (resp.status_code == STATUS_TIMEOUT) {
            return error.BadRequest;
        } else if (resp.status_code == 413) {
            return error.TooManySubjects;
        }

        // For direct get, extract metadata from JetStream headers
        if (resp.headerGet("Nats-Subject")) |nats_subject| {
            try resp.setSubject(nats_subject, false);
        }

        if (resp.headerGet("Nats-Sequence")) |nats_seq_str| {
            resp.seq = std.fmt.parseInt(u64, nats_seq_str, 10) catch 0;
        }

        return resp;
    }

    /// Internal function for deleting messages from the stream
    fn deleteMsgInternal(self: *JetStream, stream_name: []const u8, request: DeleteMsgRequest) !bool {
        try validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.MSG.DELETE.{s}", .{stream_name});
        defer self.allocator.free(subject);

        // Serialize the request to JSON
        const request_json = try std.json.stringifyAlloc(self.allocator, request, .{});
        defer self.allocator.free(request_json);

        const msg = try self.sendRequest(subject, request_json);
        defer msg.deinit();

        const response = try self.parseResponse(MsgDeleteResponse, msg);
        defer response.deinit();

        return response.value.success;
    }

    /// Deletes a message from the stream (marks as deleted, doesn't erase from storage)
    pub fn deleteMsg(self: *JetStream, stream_name: []const u8, seq: u64) !bool {
        return self.deleteMsgInternal(stream_name, DeleteMsgRequest{ .seq = seq, .no_erase = true });
    }

    /// Erases a message from the stream (securely removes from storage)
    pub fn eraseMsg(self: *JetStream, stream_name: []const u8, seq: u64) !bool {
        return self.deleteMsgInternal(stream_name, DeleteMsgRequest{ .seq = seq, .no_erase = null });
    }

    /// Subscribe to a JetStream push consumer with callback handler
    /// Handle JetStream status messages (heartbeats and flow control)
    fn handleStatusMessage(msg: *Message, js: *JetStream) !void {
        // Debug: Print all headers to understand the actual format
        log.debug("Status message headers:", .{});
        var header_iter = msg.headers.iterator();
        while (header_iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const values = entry.value_ptr.*;
            for (values.items) |value| {
                log.debug("  {s}: {s}", .{ key, value });
            }
        }

        // Get the description header to distinguish between heartbeats and flow control
        if (msg.headers.get("Description")) |desc_values| {
            if (desc_values.items.len > 0) {
                const description = desc_values.items[0];

                if (std.mem.eql(u8, description, "Idle Heartbeat")) {
                    // This is an idle heartbeat - just log it (optional)
                    log.debug("Received idle heartbeat from JetStream", .{});
                    return;
                } else if (std.mem.eql(u8, description, "FlowControl Request")) {
                    // This is a flow control request - we need to respond
                    log.debug("Received flow control request from JetStream", .{});

                    if (msg.reply) |reply_subject| {
                        // Respond with empty message to acknowledge flow control
                        try js.nc.publish(reply_subject, "");
                        log.debug("Sent flow control response to: {s}", .{reply_subject});
                    } else {
                        log.warn("Flow control request missing reply subject", .{});
                    }
                    return;
                }

                // Unknown status message description
                log.warn("Unknown status message description: {s}", .{description});
            }
        } else {
            // Status message without description - treat as heartbeat
            log.debug("Received status message without description (likely heartbeat)", .{});
        }
    }

    pub fn subscribe(self: *JetStream, stream_name: []const u8, consumer_config: ConsumerConfig, comptime handlerFn: anytype, args: anytype) !*JetStreamSubscription {
        // Validate that this is a push consumer configuration
        const deliver_subject = consumer_config.deliver_subject orelse return error.MissingDeliverSubject;

        // Create push consumer config by removing pull-only fields
        var push_config = consumer_config;
        push_config.max_waiting = 0; // Push consumers don't support max_waiting
        push_config.max_batch = null; // Push consumers don't support max_batch
        push_config.max_expires = null; // Push consumers don't support max_expires

        // Create the push consumer first
        var consumer_info = try self.addConsumer(stream_name, push_config);
        errdefer consumer_info.deinit();

        // Define the handler inline to avoid the two-level context issue
        const JSHandler = struct {
            fn wrappedHandler(msg: *Message, js: *JetStream, user_args: @TypeOf(args)) anyerror!void {
                // Check for status messages (heartbeats and flow control)
                if (msg.status_code == STATUS_CONTROL) {
                    // Handle status message internally, don't pass to user callback
                    handleStatusMessage(msg, js) catch |err| {
                        log.err("Failed to handle status message: {}", .{err});
                    };
                    msg.deinit(); // Clean up status message
                    return;
                }

                // Create JetStream message wrapper for regular messages
                const js_msg = jetstream_message.createJetStreamMessage(js.nc, msg) catch {
                    msg.deinit(); // Clean up on error
                    return;
                };

                // Call user handler with JetStream message - handler owns cleanup responsibility
                // Support both void and fallible handlers
                const ReturnType = @typeInfo(@TypeOf(handlerFn)).@"fn".return_type.?;
                if (ReturnType == void) {
                    @call(.auto, handlerFn, .{js_msg} ++ user_args);
                } else {
                    try @call(.auto, handlerFn, .{js_msg} ++ user_args);
                }
            }
        };

        // Subscribe to the delivery subject with simple arguments
        const subscription = try self.nc.subscribe(deliver_subject, JSHandler.wrappedHandler, .{ self, args });

        // Create JetStream subscription wrapper
        const js_sub = try self.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = subscription,
            .js = self,
            .consumer_info = consumer_info,
        };

        return js_sub;
    }

    /// Create a synchronous push subscription for manual message consumption
    pub fn subscribeSync(self: *JetStream, stream_name: []const u8, consumer_config: ConsumerConfig) !*JetStreamSubscription {
        // Validate that this is a push consumer configuration with deliver_subject
        const deliver_subject = consumer_config.deliver_subject orelse return error.MissingDeliverSubject;

        // Create push consumer config
        var push_config = consumer_config;
        push_config.max_waiting = 0; // Push consumers don't support max_waiting
        push_config.max_batch = null; // Push consumers don't support max_batch
        push_config.max_expires = null; // Push consumers don't support max_expires

        // Create the push consumer
        var consumer_info = try self.addConsumer(stream_name, push_config);
        errdefer consumer_info.deinit();

        // Create synchronous subscription (no callback handler)
        const subscription = try self.nc.subscribeSync(deliver_subject);
        errdefer self.nc.unsubscribe(subscription);

        // Create JetStream subscription wrapper
        const js_sub = try self.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = subscription,
            .js = self,
            .consumer_info = consumer_info,
        };
        return js_sub;
    }

    /// Create a pull subscription for the specified stream
    pub fn pullSubscribe(self: *JetStream, stream_name: []const u8, consumer_config: ConsumerConfig) !*PullSubscription {
        // Create pull consumer config with appropriate defaults
        var pull_config = consumer_config;
        pull_config.deliver_subject = null; // Force null for pull consumers
        if (pull_config.max_waiting == 0) pull_config.max_waiting = 512; // Default max waiting pulls

        // Create the consumer
        var consumer_info = try self.addConsumer(stream_name, pull_config);
        errdefer consumer_info.deinit();

        // Get the consumer name (use name first, then durable_name)
        const consumer_name = consumer_info.value.config.name orelse
            consumer_info.value.config.durable_name orelse
            return error.MissingConsumerName;

        // Generate unique inbox prefix for this pull subscription
        const inbox_base = try inbox.newInbox(self.allocator);
        defer self.allocator.free(inbox_base);

        const inbox_prefix = try std.fmt.allocPrint(self.allocator, "{s}.", .{inbox_base});
        errdefer self.allocator.free(inbox_prefix);

        // Create wildcard subscription subject
        const wildcard_subject = try std.fmt.allocPrint(self.allocator, "{s}*", .{inbox_prefix});
        defer self.allocator.free(wildcard_subject);

        // Create the persistent wildcard inbox subscription
        const inbox_subscription = try self.nc.subscribeSync(wildcard_subject);
        errdefer self.nc.unsubscribe(inbox_subscription);

        // Allocate PullSubscription
        const pull_subscription = try self.allocator.create(PullSubscription);
        pull_subscription.* = PullSubscription{
            .js = self,
            .stream_name = stream_name,
            .consumer_name = consumer_name,
            .consumer_info = consumer_info,
            .inbox_subscription = inbox_subscription,
            .inbox_prefix = inbox_prefix,
        };

        return pull_subscription;
    }

    /// Publish a message to JetStream
    pub fn publish(self: *JetStream, subject: []const u8, data: []const u8, options: PublishOptions) !Result(PubAck) {
        // Create a temporary message
        const msg = try self.nc.newMsg();
        defer msg.deinit();

        // Set message fields
        try msg.setSubject(subject, false);
        try msg.setPayload(data, false);

        // Use publishMsg to handle the actual publishing
        return self.publishMsgInternal(msg, options);
    }

    /// Publish a pre-constructed message to JetStream
    pub fn publishMsg(self: *JetStream, msg: *Message, options: PublishOptions) !Result(PubAck) {
        return self.publishMsgInternal(msg, options);
    }

    /// Internal function to publish a message with header processing
    fn publishMsgInternal(self: *JetStream, msg: *Message, options: PublishOptions) !Result(PubAck) {
        // Set JetStream-specific headers based on options
        if (options.msg_id) |id| {
            try msg.headerSet(MsgIdHdr, id);
        }
        if (options.expected_stream) |stream| {
            try msg.headerSet(ExpectedStreamHdr, stream);
        }
        if (options.expected_last_seq) |seq| {
            var buf: [256]u8 = undefined;
            const seq_str = try std.fmt.bufPrint(&buf, "{d}", .{seq});
            try msg.headerSet(ExpectedLastSeqHdr, seq_str);
        }
        if (options.expected_last_subject_seq) |seq| {
            var buf: [256]u8 = undefined;
            const seq_str = try std.fmt.bufPrint(&buf, "{d}", .{seq});
            try msg.headerSet(ExpectedLastSubjSeqHdr, seq_str);
        }
        if (options.expected_last_subject_seq_subject) |subject| {
            try msg.headerSet(ExpectedLastSubjSeqSubjectHdr, subject);
        }
        if (options.expected_last_msg_id) |id| {
            try msg.headerSet(ExpectedLastMsgIdHdr, id);
        }
        if (options.msg_ttl) |ttl| {
            var buf: [256]u8 = undefined;
            const ttl_str = try std.fmt.bufPrint(&buf, "{d}ns", .{ttl});
            try msg.headerSet(MsgTTLHdr, ttl_str);
        }

        // Send request without retry logic
        const resp = self.nc.requestMsg(msg, self.opts.request_timeout_ms) catch |request_err| {
            return if (request_err == error.NoResponders) error.NoStreamResponse else request_err;
        };

        defer resp.deinit();

        // Parse the publish acknowledgment using parseResponse for consistency
        const parsed_resp = self.parseResponse(PubAck, resp) catch |err| {
            if (err == error.JetStreamParseError) {
                return error.InvalidJSAck;
            }
            return err;
        };

        return parsed_resp;
    }

    /// Create a KV manager for bucket operations
    pub fn kvManager(self: *JetStream) @import("jetstream_kv.zig").KVManager {
        const jetstream_kv = @import("jetstream_kv.zig");
        return jetstream_kv.KVManager.init(self.allocator, self);
    }

    /// Open an existing KV bucket
    pub fn kvBucket(self: *JetStream, bucket_name: []const u8) !*@import("jetstream_kv.zig").KV {
        var manager = self.kvManager();
        return try manager.openBucket(bucket_name);
    }
};
