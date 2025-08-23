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

/// Response from $JS.API.STREAM.NAMES
const StreamNamesResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    streams: ?[]const []const u8,
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

    fn sendRequest(self: *JetStream, comptime method: []const u8, payload: []const u8) !*Message {
        return try self.nc.request(default_api_prefix ++ method, payload, self.opts.request_timeout_ms) orelse {
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
};
