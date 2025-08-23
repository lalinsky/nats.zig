const std = @import("std");
const Message = @import("message.zig").Message;
const Connection = @import("connection.zig").Connection;

const log = std.log.scoped(.jetstream);

const default_api_prefix = "$JS.API.";
const default_request_timeout_ms = 5000;

const ErrorResponse = struct {
    @"error": ?struct {
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

        return try self.nc.request(full_subject, payload, self.opts.request_timeout_ms) orelse {
            return error.NoResponse;
        };
    }

    /// Parse an error response from the server, if present.
    fn maybeParseErrorResponse(self: *JetStream, msg: *Message) !void {
        if (std.mem.indexOfPos(u8, msg.data, 0, "error") != null) {
            // this should not allocate any memory, so we don't need to clean up
            const response = std.json.parseFromSliceLeaky(ErrorResponse, self.allocator, msg.data, .{
                .allocate = .alloc_if_needed,
                .ignore_unknown_fields = true,
            }) catch return;
            log.err("JetStream error: {any}", .{response.@"error"});
            // TODO: Handle specific error cases
            return error.JetStreamError;
        }
    }

    /// Parse a response from the server, handling errors if present.
    fn parseResponse(self: *JetStream, comptime T: type, msg: *Message) !Result(T) {
        try self.maybeParseErrorResponse(msg);

        return try std.json.parseFromSlice(T, self.allocator, msg.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
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
};
