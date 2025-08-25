const std = @import("std");
const Message = @import("message.zig").Message;
const Connection = @import("connection.zig").Connection;

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
    flow_control: ?bool = null,
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

pub const JetStreamOptions = struct {
    request_timeout_ms: u64 = default_request_timeout_ms,
    // Add options here
};

pub const Result = std.json.Parsed;

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
};
