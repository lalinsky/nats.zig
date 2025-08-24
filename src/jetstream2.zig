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

// JSON response structure to match NATS JetStream API
const JsonStreamConfig = struct {
    name: []const u8,
    subjects: ?[][]const u8 = null,
    retention: ?[]const u8 = null,
    max_consumers: ?i32 = null,
    max_msgs: ?i64 = null,
    max_bytes: ?i64 = null,
    max_age: ?i64 = null,
    max_msg_size: ?i32 = null,
    storage: ?[]const u8 = null,
    num_replicas: ?u32 = null,
    duplicate_window: ?i64 = null,
    sealed: ?bool = null,
    deny_delete: ?bool = null,
    deny_purge: ?bool = null,
    allow_rollup_hdrs: ?bool = null,
};

const JsonStreamState = struct {
    messages: ?u64 = null,
    bytes: ?u64 = null,
    first_seq: ?u64 = null,
    first_ts: ?[]const u8 = null,
    last_seq: ?u64 = null,
    last_ts: ?[]const u8 = null,
    consumer_count: ?u32 = null,
};

const JsonStreamInfoResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiError = null,
    config: ?JsonStreamConfig = null,
    state: ?JsonStreamState = null,
    created: ?[]const u8 = null,
    ts: ?[]const u8 = null,
};

// Stream information - holds parsed JSON directly
pub const StreamInfo = struct {
    parsed: std.json.Parsed(JsonStreamInfoResponse),
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.parsed.deinit();
    }
    
    pub fn config(self: *const Self) StreamConfig {
        const json_config = self.parsed.value.config.?;
        
        // Parse retention policy enum
        const RetentionType = enum { limits, interest, workqueue };
        const retention = if (json_config.retention) |ret_str| blk: {
            if (std.mem.eql(u8, ret_str, "limits")) break :blk RetentionType.limits;
            if (std.mem.eql(u8, ret_str, "interest")) break :blk RetentionType.interest;
            if (std.mem.eql(u8, ret_str, "workqueue")) break :blk RetentionType.workqueue;
            break :blk RetentionType.limits;
        } else RetentionType.limits;
        
        // Parse storage type enum
        const StorageType = enum { file, memory };
        const storage = if (json_config.storage) |storage_str| blk: {
            if (std.mem.eql(u8, storage_str, "memory")) break :blk StorageType.memory;
            break :blk StorageType.file;
        } else StorageType.file;
        
        return StreamConfig{
            .name = json_config.name,
            .subjects = json_config.subjects orelse &[_][]const u8{},
            .retention = switch (retention) {
                RetentionType.limits => .limits,
                RetentionType.interest => .interest,
                RetentionType.workqueue => .workqueue,
            },
            .max_consumers = json_config.max_consumers orelse -1,
            .max_msgs = json_config.max_msgs orelse -1,
            .max_bytes = json_config.max_bytes orelse -1,
            .max_age = json_config.max_age orelse 0,
            .max_msg_size = json_config.max_msg_size orelse -1,
            .storage = switch (storage) {
                StorageType.file => .file,
                StorageType.memory => .memory,
            },
            .num_replicas = json_config.num_replicas orelse 1,
            .duplicate_window = json_config.duplicate_window orelse 0,
            .sealed = json_config.sealed orelse false,
            .deny_delete = json_config.deny_delete orelse false,
            .deny_purge = json_config.deny_purge orelse false,
            .allow_rollup_hdrs = json_config.allow_rollup_hdrs orelse false,
        };
    }
    
    pub fn state(self: *const Self) StreamState {
        const json_state = self.parsed.value.state.?;
        
        // Parse timestamps (RFC3339 format to nanoseconds since epoch)
        const parseTimestamp = struct {
            fn parse(timestamp_str: ?[]const u8) i64 {
                _ = timestamp_str;
                // For now return current time - proper RFC3339 parsing would require additional logic
                return @intCast(std.time.nanoTimestamp());
            }
        }.parse;
        
        return StreamState{
            .messages = json_state.messages orelse 0,
            .bytes = json_state.bytes orelse 0,
            .first_seq = json_state.first_seq orelse 0,
            .first_ts = parseTimestamp(json_state.first_ts),
            .last_seq = json_state.last_seq orelse 0,
            .last_ts = parseTimestamp(json_state.last_ts),
            .consumer_count = json_state.consumer_count orelse 0,
        };
    }
    
    pub fn created(self: *const Self) i64 {
        const parseTimestamp = struct {
            fn parse(timestamp_str: ?[]const u8) i64 {
                _ = timestamp_str;
                return @intCast(std.time.nanoTimestamp());
            }
        }.parse;
        return parseTimestamp(self.parsed.value.created);
    }
    
    pub fn ts(self: *const Self) i64 {
        const parseTimestamp = struct {
            fn parse(timestamp_str: ?[]const u8) i64 {
                _ = timestamp_str;
                return @intCast(std.time.nanoTimestamp());
            }
        }.parse;
        return parseTimestamp(self.parsed.value.ts);
    }
};

// JSON structures for account info
const JsonAccountLimits = struct {
    max_memory: ?i64 = null,
    max_storage: ?i64 = null,
    max_streams: ?i32 = null,
    max_consumers: ?i32 = null,
    max_ack_pending: ?i32 = null,
    memory_max_stream_bytes: ?i64 = null,
    storage_max_stream_bytes: ?i64 = null,
    max_bytes_required: ?bool = null,
};

const JsonApiStats = struct {
    total: ?u64 = null,
    errors: ?u64 = null,
};

const JsonAccountInfoResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiError = null,
    memory: ?u64 = null,
    storage: ?u64 = null,
    streams: ?u32 = null,
    consumers: ?u32 = null,
    limits: ?JsonAccountLimits = null,
    api: ?JsonApiStats = null,
};

// Account information - holds parsed JSON directly
pub const AccountInfo = struct {
    parsed: std.json.Parsed(JsonAccountInfoResponse),
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.parsed.deinit();
    }
    
    pub fn memory(self: *const Self) u64 {
        return self.parsed.value.memory orelse 0;
    }
    
    pub fn storage(self: *const Self) u64 {
        return self.parsed.value.storage orelse 0;
    }
    
    pub fn streams(self: *const Self) u32 {
        return self.parsed.value.streams orelse 0;
    }
    
    pub fn consumers(self: *const Self) u32 {
        return self.parsed.value.consumers orelse 0;
    }
    
    pub fn limits(self: *const Self) AccountLimits {
        if (self.parsed.value.limits) |json_limits| {
            return AccountLimits{
                .max_memory = json_limits.max_memory orelse -1,
                .max_storage = json_limits.max_storage orelse -1,
                .max_streams = json_limits.max_streams orelse -1,
                .max_consumers = json_limits.max_consumers orelse -1,
                .max_ack_pending = json_limits.max_ack_pending orelse -1,
                .memory_max_stream_bytes = json_limits.memory_max_stream_bytes orelse -1,
                .storage_max_stream_bytes = json_limits.storage_max_stream_bytes orelse -1,
                .max_bytes_required = json_limits.max_bytes_required orelse false,
            };
        } else {
            return AccountLimits{};
        }
    }
    
    pub fn api(self: *const Self) ApiStats {
        if (self.parsed.value.api) |json_api| {
            return ApiStats{
                .total = json_api.total orelse 0,
                .errors = json_api.errors orelse 0,
            };
        } else {
            return ApiStats{ .total = 0, .errors = 0 };
        }
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

// JSON structure for publish acknowledgment
const JsonPubAckResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiError = null,
    stream: ?[]const u8 = null,
    seq: ?u64 = null,
    duplicate: ?bool = null,
    domain: ?[]const u8 = null,
};

// Publish acknowledgment - holds parsed JSON directly
pub const PubAck = struct {
    parsed: std.json.Parsed(JsonPubAckResponse),
    
    const Self = @This();
    
    pub fn deinit(self: *Self) void {
        self.parsed.deinit();
    }
    
    pub fn stream(self: *const Self) []const u8 {
        return self.parsed.value.stream orelse "UNKNOWN";
    }
    
    pub fn seq(self: *const Self) u64 {
        return self.parsed.value.seq orelse 0;
    }
    
    pub fn duplicate(self: *const Self) bool {
        return self.parsed.value.duplicate orelse false;
    }
    
    pub fn domain(self: *const Self) ?[]const u8 {
        return self.parsed.value.domain;
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
    
    pub fn listStreams(self: *Self, allocator: Allocator) ![]*StreamInfo {
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
        // Parse JSON response directly - no deep copying
        const parsed = std.json.parseFromSliceLeaky(
            JsonStreamInfoResponse,
            self.allocator,
            response.data,
            .{ .ignore_unknown_fields = true }
        ) catch |err| {
            log.warn("Failed to parse stream info response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        
        // Check for API errors
        if (parsed.@"error") |api_error| {
            log.warn("JetStream API error {d}: {s}", .{ api_error.code, api_error.description });
            return JetStreamError.ApiError;
        }
        
        // Validate required fields
        if (parsed.config == null or parsed.state == null) {
            return JetStreamError.InvalidResponse;
        }
        
        const stream_info = try self.allocator.create(StreamInfo);
        stream_info.parsed = .{ .arena = undefined, .value = parsed };
        
        return stream_info;
    }
    
    fn parseAccountInfo(self: *Self, response: *Message) !*AccountInfo {
        // Parse JSON response directly - no deep copying
        const parsed = std.json.parseFromSliceLeaky(
            JsonAccountInfoResponse,
            self.allocator,
            response.data,
            .{ .ignore_unknown_fields = true }
        ) catch |err| {
            log.warn("Failed to parse account info response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        
        // Check for API errors
        if (parsed.@"error") |api_error| {
            log.warn("JetStream API error {d}: {s}", .{ api_error.code, api_error.description });
            return JetStreamError.ApiError;
        }
        
        const account_info = try self.allocator.create(AccountInfo);
        account_info.parsed = .{ .arena = undefined, .value = parsed };
        
        return account_info;
    }
    
    fn parseStreamList(self: *Self, response: *Message, allocator: Allocator) ![]*StreamInfo {
        _ = self;
        // Define JSON response structure for stream list
        const JsonStreamListResponse = struct {
            type: ?[]const u8 = null,
            @"error": ?ApiError = null,
            total: ?u32 = null,
            offset: ?u32 = null,
            limit: ?u32 = null,
            streams: ?[]JsonStreamInfoResponse = null,
        };
        
        // Parse the JSON response directly
        const parsed = std.json.parseFromSliceLeaky(
            JsonStreamListResponse,
            allocator,
            response.data,
            .{ .ignore_unknown_fields = true }
        ) catch |err| {
            log.warn("Failed to parse stream list response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        
        // Check for API errors
        if (parsed.@"error") |api_error| {
            log.warn("JetStream API error {d}: {s}", .{ api_error.code, api_error.description });
            return JetStreamError.ApiError;
        }
        
        const json_streams = parsed.streams orelse return try allocator.alloc(*StreamInfo, 0);
        
        // Allocate array for StreamInfo pointers
        var stream_list = try allocator.alloc(*StreamInfo, json_streams.len);
        errdefer {
            for (stream_list) |stream_ptr| {
                stream_ptr.deinit();
                allocator.destroy(stream_ptr);
            }
            allocator.free(stream_list);
        }
        
        // Create StreamInfo objects from parsed data
        for (json_streams, 0..) |json_stream, i| {
            // Validate required fields
            if (json_stream.config == null or json_stream.state == null) {
                return JetStreamError.InvalidResponse;
            }
            
            const stream_info = try allocator.create(StreamInfo);
            stream_info.parsed = .{ .arena = undefined, .value = json_stream };
            stream_list[i] = stream_info;
        }
        
        return stream_list;
    }
    
    fn waitForAck(self: *Self, subject: []const u8) !*PubAck {
        // Create a temporary reply inbox for acknowledgment
        const reply_inbox = try inbox.newInbox(self.allocator);
        defer self.allocator.free(reply_inbox);
        
        // Subscribe to the reply inbox for acknowledgment
        const subscription = try self.connection.subscribeSync(reply_inbox);
        defer subscription.deinit();
        
        // Wait for acknowledgment message with timeout
        const ack_msg = subscription.nextMsg(self.options.timeout_ns / 1_000_000) orelse {
            return JetStreamError.ApiTimeout;
        };
        defer ack_msg.deinit();
        
        // Parse JSON response directly - no deep copying
        const parsed = std.json.parseFromSliceLeaky(
            JsonPubAckResponse,
            self.allocator,
            ack_msg.data,
            .{ .ignore_unknown_fields = true }
        ) catch |err| {
            log.warn("Failed to parse publish ack response: {}", .{err});
            return JetStreamError.InvalidResponse;
        };
        
        // Check for API errors
        if (parsed.@"error") |api_error| {
            log.warn("JetStream publish error {d}: {s}", .{ api_error.code, api_error.description });
            
            // Map specific error codes to appropriate JetStream errors
            return switch (api_error.err_code orelse api_error.code) {
                10014 => JetStreamError.MessageNotFound,
                10058 => JetStreamError.StreamNotFound,
                10059 => JetStreamError.StreamAlreadyExists,
                10060 => JetStreamError.ConsumerNotFound,
                10013 => JetStreamError.InsufficientResources,
                else => JetStreamError.ApiError,
            };
        }
        
        const pub_ack = try self.allocator.create(PubAck);
        pub_ack.parsed = .{ .arena = undefined, .value = parsed };
        
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
        const updated_info = try self.js.getStream(self.info.config().name);
        defer updated_info.deinit();
        
        // Return a copy of the info
        return try self.copyStreamInfo(updated_info.info);
    }
    
    pub fn purge(self: *Self, filter: ?PurgeRequest) !PurgeResponse {
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.PURGE.{s}", .{ self.js.options.prefix, self.info.config().name });
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
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.MSG.GET.{s}", .{ self.js.options.prefix, self.info.config().name });
        defer self.js.allocator.free(subject);
        
        const request = try std.fmt.allocPrint(self.js.allocator, "{{\"seq\":{d}}}", .{seq});
        defer self.js.allocator.free(request);
        
        const response_msg = try self.js.apiRequest(subject, request);
        // Don't defer - we return this message
        
        return response_msg;
    }
    
    pub fn deleteMessage(self: *Self, seq: u64) !bool {
        const subject = try std.fmt.allocPrint(self.js.allocator, "{s}.STREAM.MSG.DELETE.{s}", .{ self.js.options.prefix, self.info.config().name });
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
        // Create a new StreamInfo by duplicating the JSON data
        const new_stream_info = try self.js.allocator.create(StreamInfo);
        
        // Create a copy of the parsed value
        new_stream_info.parsed = .{
            .arena = undefined,
            .value = info.parsed.value, // JSON data is immutable, safe to share
        };
        
        return new_stream_info;
    }
};