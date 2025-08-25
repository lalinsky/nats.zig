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
const Message = @import("message.zig").Message;
const Connection = @import("connection.zig").Connection;
const Subscription = @import("subscription.zig").Subscription;
const subscription_mod = @import("subscription.zig");

const log = std.log.scoped(.jetstream);

const default_api_prefix = "$JS.API.";
const default_request_timeout_ms = 5000;

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

/// JetStream message acknowledgment types
pub const AckType = enum {
    ack,     // +ACK - Acknowledge message 
    nak,     // -NAK - Negative acknowledge (retry)
    term,    // +TERM - Terminate delivery (don't retry)
    progress, // +WPI - Work in progress (extend ack wait)
};

/// Enhanced message wrapper for JetStream push subscriptions
pub const JetStreamMessage = struct {
    /// Underlying NATS message
    msg: *Message,
    /// JetStream context for acknowledgments
    js: *JetStream,
    
    /// JetStream metadata (parsed from headers)
    stream: ?[]const u8 = null,
    sequence: ?u64 = null,
    subject: ?[]const u8 = null,
    timestamp: ?[]const u8 = null,
    reply: ?[]const u8 = null,
    
    pub fn deinit(self: *JetStreamMessage) void {
        self.msg.deinit();
    }
    
    /// Acknowledge successful processing
    pub fn ack(self: *JetStreamMessage) !void {
        try self.sendAck(.ack);
    }
    
    /// Negative acknowledge - request redelivery
    pub fn nak(self: *JetStreamMessage) !void {
        try self.sendAck(.nak);
    }
    
    /// Terminate delivery - don't redeliver this message
    pub fn term(self: *JetStreamMessage) !void {
        try self.sendAck(.term);
    }
    
    /// Indicate work in progress - extend ack wait timer
    pub fn inProgress(self: *JetStreamMessage) !void {
        try self.sendAck(.progress);
    }
    
    /// Send acknowledgment to JetStream
    fn sendAck(self: *JetStreamMessage, ack_type: AckType) !void {
        if (self.reply) |reply_subject| {
            const ack_payload = switch (ack_type) {
                .ack => "+ACK",
                .nak => "-NAK", 
                .term => "+TERM",
                .progress => "+WPI",
            };
            try self.js.nc.publish(reply_subject, ack_payload);
        }
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
    /// JS context for cleanup (stored as anyopaque to avoid type issues)
    js_context: *anyopaque,
    
    pub fn deinit(self: *JetStreamSubscription) void {
        self.consumer_info.deinit();
        // Note: js_context cleanup is handled by the subscription's handler cleanup
        self.js.allocator.destroy(self);
    }
    
    /// Unsubscribe from the delivery subject
    pub fn unsubscribe(self: *JetStreamSubscription) !void {
        try self.js.nc.unsubscribe(self.subscription);
    }
};

pub const JetStreamOptions = struct {
    request_timeout_ms: u64 = default_request_timeout_ms,
    // Add options here
};

pub const Result = std.json.Parsed;

/// Parse JetStream headers from a message and create JetStreamMessage wrapper
fn createJetStreamMessage(js: *JetStream, msg: *Message) !*JetStreamMessage {
    const js_msg = try js.allocator.create(JetStreamMessage);
    js_msg.* = JetStreamMessage{
        .msg = msg,
        .js = js,
        .reply = msg.reply,
    };
    
    // Parse JetStream headers if present
    if (msg.headers.get("Nats-Stream")) |stream_values| {
        if (stream_values.items.len > 0) js_msg.stream = stream_values.items[0];
    }
    if (msg.headers.get("Nats-Subject")) |subject_values| {
        if (subject_values.items.len > 0) js_msg.subject = subject_values.items[0];
    }
    if (msg.headers.get("Nats-Time-Stamp")) |timestamp_values| {
        if (timestamp_values.items.len > 0) js_msg.timestamp = timestamp_values.items[0];
    }
    
    // Parse sequence number
    if (msg.headers.get("Nats-Sequence")) |seq_values| {
        if (seq_values.items.len > 0) {
            js_msg.sequence = std.fmt.parseInt(u64, seq_values.items[0], 10) catch null;
        }
    }
    
    return js_msg;
}

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

        // TODO: Handle specific error cases
        std.debug.print("JetStream error: code={d} err_code={d} description={s}\n", .{ info.code, info.err_code, info.description });
        return error.JetStreamError;
    }

    /// Parse a response from the server, handling errors if present.
    fn parseResponse(self: *JetStream, comptime T: type, msg: *Message) !Result(T) {
        try self.maybeParseErrorResponse(msg);

        return std.json.parseFromSlice(T, self.allocator, msg.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| {
            log.err("Failed to parse response: {}", .{err});
            log.debug("Full response: {s}", .{msg.data});
            return error.JetStreamParseError;
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
        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.INFO.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Retrieves a list of consumer names for a stream.
    pub fn listConsumerNames(self: *JetStream, stream_name: []const u8) !Result([]const []const u8) {
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
        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.INFO.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(ConsumerInfo, msg);
    }

    /// Deletes a consumer.
    pub fn deleteConsumer(self: *JetStream, stream_name: []const u8, consumer_name: []const u8) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "CONSUMER.DELETE.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        // Just check for errors, don't need to parse the response
        try self.maybeParseErrorResponse(msg);
    }

    /// Purges messages from a stream.
    pub fn purgeStream(self: *JetStream, stream_name: []const u8, request: StreamPurgeRequest) !Result(StreamPurgeResponse) {
        const subject = try std.fmt.allocPrint(self.allocator, "STREAM.PURGE.{s}", .{stream_name});
        defer self.allocator.free(subject);

        const request_json = try std.json.stringifyAlloc(self.allocator, request, .{});
        defer self.allocator.free(request_json);

        const msg = try self.sendRequest(subject, request_json);
        defer msg.deinit();

        return try self.parseResponse(StreamPurgeResponse, msg);
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

    pub fn subscribe(
        self: *JetStream,
        stream_name: []const u8,
        consumer_config: ConsumerConfig,
        comptime handlerFn: anytype,
        args: anytype
    ) !*JetStreamSubscription {
        // Validate that this is a push consumer configuration
        if (consumer_config.deliver_subject == null) {
            return error.MissingDeliverSubject;
        }

        // Create push consumer config by removing pull-only fields
        var push_config = consumer_config;
        push_config.max_waiting = 0;  // Push consumers don't support max_waiting
        push_config.max_batch = null; // Push consumers don't support max_batch
        push_config.max_expires = null; // Push consumers don't support max_expires

        // Create the push consumer first
        var consumer_info = try self.addConsumer(stream_name, push_config);
        errdefer consumer_info.deinit();

        const deliver_subject = consumer_config.deliver_subject.?;

        // Define context struct
        const JSContext = struct {
            js: *JetStream,
            user_args: @TypeOf(args),
        };

        // Create a handler that wraps NATS messages as JetStream messages
        const JSWrapper = struct {
            fn wrappedHandler(msg: *Message, ctx_ptr: *anyopaque) void {
                const ctx: *JSContext = @ptrCast(@alignCast(ctx_ptr));
                
                // Check for status messages (heartbeats and flow control) 
                if (msg.headers.get("Status")) |status_values| {
                    if (status_values.items.len > 0) {
                        const status = status_values.items[0];
                        if (std.mem.eql(u8, status, "100")) {
                            // Handle status message internally, don't pass to user callback
                            handleStatusMessage(msg, ctx.js) catch |err| {
                                log.err("Failed to handle status message: {}", .{err});
                            };
                            msg.deinit(); // Clean up status message
                            return;
                        }
                    }
                }
                
                // Create JetStream message wrapper for regular messages
                const js_msg = createJetStreamMessage(ctx.js, msg) catch {
                    msg.deinit(); // Clean up on error
                    return;
                };
                defer ctx.js.allocator.destroy(js_msg);

                // Call user handler with JetStream message
                @call(.auto, handlerFn, .{js_msg} ++ ctx.user_args);
            }
        };

        const js_context = try self.allocator.create(JSContext);
        js_context.* = JSContext{
            .js = self,
            .user_args = args,
        };

        // Subscribe to the delivery subject using connection.subscribe
        const subscription = try self.nc.subscribe(deliver_subject, JSWrapper.wrappedHandler, .{js_context});

        // Create JetStream subscription wrapper
        const js_sub = try self.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = subscription,
            .js = self,
            .consumer_info = consumer_info,
            .js_context = js_context,
        };

        return js_sub;
    }
};
