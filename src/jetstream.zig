const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const connection_mod = @import("connection.zig");
const Connection = connection_mod.Connection;
const Message = @import("message.zig").Message;
const inbox = @import("inbox.zig");

const log = std.log.scoped(.jetstream);

// JetStream specific errors
pub const JetStreamError = error{
    // Stream errors
    StreamNotFound,
    StreamNameRequired,
    StreamInvalidConfig,
    StreamWrongLastSequence,
    StreamAlreadyExists,
    
    // Consumer errors  
    ConsumerNotFound,
    ConsumerNameInUse,
    ConsumerInvalidConfig,
    ConsumerMaxDeliveryExceeded,
    
    // Message errors
    MessageNotFound,
    MessageSizeExceedsMaximum,
    MessageSequenceNotFound,
    NoMessageAvailable,
    
    // API errors
    AccountResourcesExceeded,
    InsufficientResources,
    ApiError,
    ApiTimeout,
    InvalidResponse,
} || Allocator.Error || connection_mod.ConnectionError;

// Stream configuration following JetStream stream config
pub const StreamConfig = struct {
    name: []const u8,
    subjects: []const []const u8,
    
    // Retention policy
    retention: enum { limits, interest, workqueue } = .limits,
    
    // Storage limits  
    max_consumers: i32 = -1,
    max_msgs: i64 = -1,
    max_bytes: i64 = -1,
    max_age: i64 = 0, // nanoseconds
    max_msg_size: i32 = -1,
    
    // Storage and replication
    storage: enum { file, memory } = .file,
    num_replicas: u32 = 1,
    
    // Advanced features
    duplicate_window: i64 = 0, // nanoseconds
    compression: enum { none, s2 } = .none,
    
    // Flags
    sealed: bool = false,
    deny_delete: bool = false,
    deny_purge: bool = false,
    allow_rollup_hdrs: bool = false,
    
    // Subject transformation
    subject_transform: ?SubjectTransformConfig = null,
    
    // Mirroring and sourcing
    mirror: ?StreamSource = null,
    sources: ?[]StreamSource = null,
    
    // Placement
    placement: ?Placement = null,
};

pub const SubjectTransformConfig = struct {
    src: []const u8,
    dest: []const u8,
};

pub const StreamSource = struct {
    name: []const u8,
    opt_start_seq: u64 = 0,
    opt_start_time: ?i64 = null,
    filter_subject: ?[]const u8 = null,
    external: ?ExternalStream = null,
};

pub const ExternalStream = struct {
    api: []const u8,
    deliver: ?[]const u8 = null,
};

pub const Placement = struct {
    cluster: ?[]const u8 = null,
    tags: ?[][]const u8 = null,
};

// Stream state information
pub const StreamState = struct {
    messages: u64,
    bytes: u64,
    first_seq: u64,
    first_ts: i64, // RFC3339 timestamp in nanoseconds
    last_seq: u64,
    last_ts: i64, // RFC3339 timestamp in nanoseconds
    consumer_count: u32,
    deleted: ?[]u64 = null,
};

// Stream information
pub const StreamInfo = struct {
    config: StreamConfig,
    state: StreamState,
    created: i64, // RFC3339 timestamp in nanoseconds
    ts: i64, // RFC3339 timestamp in nanoseconds
    
    // Memory management using arena
    arena: std.heap.ArenaAllocator,
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }
};

// Account information
pub const AccountInfo = struct {
    memory: u64,
    storage: u64,
    streams: u32,
    consumers: u32,
    limits: AccountLimits,
    api: ApiStats,
    
    // Memory management using arena
    arena: std.heap.ArenaAllocator,
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }
};

pub const AccountLimits = struct {
    max_memory: i64 = -1,
    max_storage: i64 = -1,
    max_streams: i32 = -1,
    max_consumers: i32 = -1,
    max_ack_pending: i32 = -1,
    memory_max_stream_bytes: i64 = -1,
    storage_max_stream_bytes: i64 = -1,
    max_bytes_required: bool = false,
};

pub const ApiStats = struct {
    total: u64,
    errors: u64,
};

// Publish acknowledgment
pub const PubAck = struct {
    stream: []const u8,
    seq: u64,
    duplicate: bool,
    domain: ?[]const u8,
    
    // Memory management using arena
    arena: std.heap.ArenaAllocator,
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }
};

// Purge request and response
pub const PurgeRequest = struct {
    filter: ?[]const u8 = null,
    seq: u64 = 0,
    keep: u64 = 0,
};

pub const PurgeResponse = struct {
    success: bool,
    purged: u64,
};

// JetStream context options
pub const JetStreamOptions = struct {
    prefix: []const u8 = "$JS.API",
    domain: ?[]const u8 = null,
    timeout_ns: u64 = 5_000_000_000, // 5 seconds
};

// API response wrapper
pub const ApiResponse = struct {
    type: []const u8,
    @"error": ?ApiError = null,
    
    const Self = @This();
    
    pub fn isError(self: *const Self) bool {
        return self.@"error" != null;
    }
};

pub const ApiError = struct {
    code: u32,
    err_code: ?u32 = null,
    description: []const u8,
};

// Core JetStream context - top-level management interface
pub const JetStream = struct {
    connection: *Connection,
    allocator: Allocator,
    options: JetStreamOptions,
    
    const Self = @This();
    
    pub fn init(conn: *Connection, allocator: Allocator, options: JetStreamOptions) Self {
        return Self{
            .connection = conn,
            .allocator = allocator,
            .options = options,
        };
    }
    
    pub fn deinit(self: *Self) void {
        // JetStream context doesn't own the connection, just cleans up itself
        _ = self;
    }
    
    // Account information
    pub fn accountInfo(self: *Self) !*AccountInfo {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.INFO", .{self.options.prefix});
        defer self.allocator.free(subject);
        
        const response_msg = try self.apiRequest(subject, null);
        defer response_msg.deinit();
        
        return self.parseAccountInfo(response_msg);
    }
    
    // Stream management
    pub fn createStream(self: *Self, config: StreamConfig) !*Stream {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.CREATE.{s}", .{ self.options.prefix, config.name });
        defer self.allocator.free(subject);
        
        const request_json = try self.encodeStreamConfig(config);
        defer self.allocator.free(request_json);
        
        const response_msg = try self.apiRequest(subject, request_json);
        defer response_msg.deinit();
        
        const stream_info = try self.parseStreamInfo(response_msg);
        
        return Stream.init(self, stream_info);
    }
    
    pub fn updateStream(self: *Self, name: []const u8, config: StreamConfig) !*Stream {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.UPDATE.{s}", .{ self.options.prefix, name });
        defer self.allocator.free(subject);
        
        const request_json = try self.encodeStreamConfig(config);
        defer self.allocator.free(request_json);
        
        const response_msg = try self.apiRequest(subject, request_json);
        defer response_msg.deinit();
        
        const stream_info = try self.parseStreamInfo(response_msg);
        
        return Stream.init(self, stream_info);
    }
    
    pub fn deleteStream(self: *Self, name: []const u8) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.DELETE.{s}", .{ self.options.prefix, name });
        defer self.allocator.free(subject);
        
        const response_msg = try self.apiRequest(subject, null);
        defer response_msg.deinit();
        
        // Check for API error
        try self.checkApiError(response_msg);
    }
    
    pub fn getStream(self: *Self, name: []const u8) !*Stream {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.INFO.{s}", .{ self.options.prefix, name });
        defer self.allocator.free(subject);
        
        const response_msg = try self.apiRequest(subject, null);
        defer response_msg.deinit();
        
        const stream_info = try self.parseStreamInfo(response_msg);
        
        return Stream.init(self, stream_info);
    }
    
    pub fn listStreams(self: *Self, allocator: Allocator) ![]StreamInfo {
        const subject = try std.fmt.allocPrint(self.allocator, "{s}.STREAM.LIST", .{self.options.prefix});
        defer self.allocator.free(subject);
        
        const response_msg = try self.apiRequest(subject, null);
        defer response_msg.deinit();
        
        return self.parseStreamList(response_msg, allocator);
    }
    
    // Publishing
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !*PubAck {
        try self.connection.publish(subject, data);
        return self.waitForAck(subject);
    }
    
    pub fn publishMsg(self: *Self, msg: *Message) !*PubAck {
        try self.connection.publishMsg(msg);
        return self.waitForAck(msg.subject);
    }
    
    // Internal API request handling
    fn apiRequest(self: *Self, subject: []const u8, data: ?[]const u8) !*Message {
        // Wait for response with timeout
        const response = try self.connection.request(subject, data orelse "", self.options.timeout_ns / 1_000_000); // Convert to ms
        
        return response orelse JetStreamError.ApiTimeout;
    }
    
    fn checkApiError(self: *Self, response: *Message) !void {
        // Parse JSON response and check for errors
        const parsed = std.json.parseFromSlice(ApiResponse, self.allocator, response.data, .{}) catch |err| {
            log.warn("Failed to parse API response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        defer parsed.deinit();
        
        if (parsed.value.isError()) {
            const api_error = parsed.value.@"error".?;
            log.warn("JetStream API error {d}: {s}", .{ api_error.code, api_error.description });
            return JetStreamError.ApiError;
        }
    }
    
    fn encodeStreamConfig(self: *Self, config: StreamConfig) ![]u8 {
        var string = ArrayList(u8).init(self.allocator);
        defer string.deinit();
        
        try std.json.stringify(config, .{}, string.writer());
        return try string.toOwnedSlice();
    }
    
    fn parseStreamInfo(self: *Self, response: *Message) !*StreamInfo {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        
        // Parse the JSON response
        _ = std.json.parseFromSlice(std.json.Value, arena.allocator(), response.data, .{}) catch |err| {
            log.warn("Failed to parse stream info response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        
        const stream_info = try arena.allocator().create(StreamInfo);
        stream_info.arena = arena;
        
        // Extract stream configuration and state from parsed JSON
        // This is a simplified version - in reality you'd need proper JSON parsing
        stream_info.config = StreamConfig{
            .name = try arena.allocator().dupe(u8, "test"), // TODO: Parse from JSON
            .subjects = &[_][]const u8{}, // TODO: Parse from JSON
        };
        
        stream_info.state = StreamState{
            .messages = 0, // TODO: Parse from JSON
            .bytes = 0,
            .first_seq = 1,
            .first_ts = 0,
            .last_seq = 0,
            .last_ts = 0,
            .consumer_count = 0,
        };
        
        stream_info.created = 0; // TODO: Parse from JSON
        stream_info.ts = @intCast(std.time.nanoTimestamp());
        
        return stream_info;
    }
    
    fn parseAccountInfo(self: *Self, response: *Message) !*AccountInfo {
        _ = response;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        
        const account_info = try arena.allocator().create(AccountInfo);
        account_info.arena = arena;
        
        // TODO: Parse JSON response properly
        account_info.memory = 0;
        account_info.storage = 0;
        account_info.streams = 0;
        account_info.consumers = 0;
        account_info.limits = AccountLimits{};
        account_info.api = ApiStats{ .total = 0, .errors = 0 };
        
        return account_info;
    }
    
    fn parseStreamList(self: *Self, response: *Message, allocator: Allocator) ![]StreamInfo {
        // TODO: Parse JSON response properly and return list of streams
        _ = self;
        _ = response;
        return try allocator.alloc(StreamInfo, 0);
    }
    
    fn waitForAck(self: *Self, subject: []const u8) !*PubAck {
        // TODO: Implement proper acknowledgment waiting
        // For now, return a dummy ack
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        
        const pub_ack = try arena.allocator().create(PubAck);
        pub_ack.arena = arena;
        pub_ack.stream = try arena.allocator().dupe(u8, "UNKNOWN");
        pub_ack.seq = 1;
        pub_ack.duplicate = false;
        pub_ack.domain = null;
        
        _ = subject;
        
        return pub_ack;
    }
};

// Stream interface - manages consumers and stream-specific operations  
pub const Stream = struct {
    js: *JetStream,
    info: *StreamInfo,
    
    const Self = @This();
    
    pub fn init(js: *JetStream, info: *StreamInfo) !*Self {
        const stream = try js.allocator.create(Self);
        stream.* = .{
            .js = js,
            .info = info,
        };
        return stream;
    }
    
    pub fn deinit(self: *Self) void {
        self.info.deinit();
        self.js.allocator.destroy(self);
    }
    
    // Stream operations
    pub fn getInfo(self: *Self) !*StreamInfo {
        // Refresh stream info
        const updated_info = try self.js.getStream(self.info.config.name);
        defer updated_info.deinit();
        
        // Return a copy of the info
        return try self.copyStreamInfo(updated_info.info);
    }
    
    pub fn purge(self: *Self, filter: ?PurgeRequest) !PurgeResponse {
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.PURGE.{s}", .{ self.js.options.prefix, self.info.config.name });
        defer self.js.allocator.free(subject);
        
        var request_data: ?[]u8 = null;
        defer if (request_data) |data| self.js.allocator.free(data);
        
        if (filter) |f| {
            var string = ArrayList(u8).init(self.js.allocator);
            defer string.deinit();
            try std.json.stringify(f, .{}, string.writer());
            request_data = try string.toOwnedSlice();
        }
        
        const response_msg = try self.js.apiRequest(subject, request_data);
        defer response_msg.deinit();
        
        // TODO: Parse purge response properly
        return PurgeResponse{
            .success = true,
            .purged = 0,
        };
    }
    
    pub fn getMessage(self: *Self, seq: u64) !*Message {
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.MSG.GET.{s}", .{ self.js.options.prefix, self.info.config.name });
        defer self.js.allocator.free(subject);
        
        const request = try std.fmt.allocPrint(self.js.allocator, "{{\"seq\":{d}}}", .{seq});
        defer self.js.allocator.free(request);
        
        const response_msg = try self.js.apiRequest(subject, request);
        // Don't defer - we return this message
        
        return response_msg;
    }
    
    pub fn deleteMessage(self: *Self, seq: u64) !bool {
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.MSG.DELETE.{s}", .{ self.js.options.prefix, self.info.config.name });
        defer self.js.allocator.free(subject);
        
        const request = try std.fmt.allocPrint(self.js.allocator, "{{\"seq\":{d}}}", .{seq});
        defer self.js.allocator.free(request);
        
        const response_msg = try self.js.apiRequest(subject, request);
        defer response_msg.deinit();
        
        try self.js.checkApiError(response_msg);
        return true;
    }
    
    // Convenience methods
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !*PubAck {
        return self.js.publish(subject, data);
    }
    
    fn copyStreamInfo(self: *Self, info: *StreamInfo) !*StreamInfo {
        // TODO: Implement proper deep copy of StreamInfo
        _ = self;
        _ = info;
        return error.NotImplemented;
    }
};