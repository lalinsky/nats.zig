const std = @import("std");
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const Subscription = @import("subscription.zig").Subscription;
const MsgHandler = @import("subscription.zig").MsgHandler;
const inbox = @import("inbox.zig");
const nuid = @import("nuid.zig");
const Connection = @import("connection.zig").Connection;
const ConnectionError = @import("connection.zig").ConnectionError;

const log = std.log.scoped(.response_manager);

pub const ResponseInfo = struct {
    mutex: std.Thread.Mutex = .{},
    condition: std.Thread.Condition = .{},
    result: ?anyerror!*Message = null,

    pub fn timedWait(self: *ResponseInfo, timeout_ns: u64) !*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        var timer = std.time.Timer.start() catch unreachable;
        while (self.result == null) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }
            const remaining_ns = timeout_ns - elapsed_ns;
            try self.condition.timedWait(&self.mutex, remaining_ns);
        }

        // Return the stored result (error or message)
        return self.result.?;
    }

    pub fn complete(self: *ResponseInfo, msg: *Message) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.result == null) {
            self.result = msg;
            self.condition.signal();
        }
    }

    pub fn completeWithError(self: *ResponseInfo, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.result == null) {
            self.result = err;
            self.condition.signal();
        }
    }
};

const INBOX_BASE_PREFIX = "_INBOX.";
const INBOX_BASE_PREFIX_LEN = INBOX_BASE_PREFIX.len;

const INBOX_PREFIX_LEN = INBOX_BASE_PREFIX_LEN + nuid.NUID_TOTAL_LEN + 1; // "_INBOX.{22-char-nuid}." = 7 + 22 + 1 = 30

pub const ResponseManager = struct {
    allocator: Allocator,

    resp_sub_prefix_buf: [INBOX_PREFIX_LEN]u8 = undefined,
    resp_sub_prefix: []u8 = &.{},

    resp_mux: ?*Subscription = null,
    resp_map: std.StringHashMapUnmanaged(*ResponseInfo) = .{},
    resp_mutex: std.Thread.Mutex = .{},

    // Token generation state (simple counter)
    token_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(allocator: Allocator) ResponseManager {
        return ResponseManager{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ResponseManager) void {
        // No need to free inbox_prefix - it's now a static buffer

        // Subscription teardown is handled by Connection; just forget our pointer.
        self.resp_mux = null;

        // Clean up any remaining tokens (keys only, ResponseInfo is managed elsewhere)
        if (self.resp_map.count() > 0) {
            log.warn("Cleaning up {} remaining token keys during shutdown", .{self.resp_map.count()});
            var iterator = self.resp_map.iterator();
            while (iterator.next()) |entry| {
                const key = entry.key_ptr.*;
                self.allocator.free(key);
            }
        }
        self.resp_map.deinit(self.allocator);
    }

    pub fn ensureInitialized(self: *ResponseManager, connection: *Connection) !void {
        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();

        // Already initialized by another thread
        if (self.resp_mux != null) return;

        // Create an unique identifier for the response manager
        var nuid_buf: [nuid.NUID_TOTAL_LEN]u8 = undefined;
        nuid.next(&nuid_buf);
        self.resp_sub_prefix = std.fmt.bufPrint(&self.resp_sub_prefix_buf, "_INBOX.{s}.", .{&nuid_buf}) catch unreachable;

        // Create wildcard subscription using a temporary buffer
        var subject_buf: [INBOX_PREFIX_LEN + 1]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}*", .{self.resp_sub_prefix}) catch unreachable;

        // Subscribe to the wildcard subject
        self.resp_mux = try connection.subscribe(subject, responseHandlerWrapper, .{self});

        log.debug("Initialized response manager with prefix: {s}, wildcard: {s}", .{ self.resp_sub_prefix, subject });
    }

    pub fn createRequest(self: *ResponseManager, subject: []const u8, _: []const u8) !struct {
        response_info: *ResponseInfo,
        reply_subject: []u8,
        token: []u8
    } {
        const token = try self.generateToken();
        errdefer self.allocator.free(token);

        const reply_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.resp_sub_prefix, token });
        errdefer self.allocator.free(reply_subject);

        const response_info = try self.allocator.create(ResponseInfo);
        errdefer self.allocator.destroy(response_info);

        response_info.* = .{};

        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();
        try self.resp_map.put(self.allocator, token, response_info);

        log.debug("Created request for {s}, reply: {s}, token: {s}", .{ subject, reply_subject, token });
        return .{
            .response_info = response_info,
            .reply_subject = reply_subject,
            .token = token
        };
    }

    pub fn cleanupRequest(self: *ResponseManager, token: []const u8) void {
        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();
        if (self.resp_map.fetchRemove(token)) |entry| {
            log.debug("Cleaned up request map entry with token: {s}", .{entry.key});
            self.allocator.free(entry.key);
            // ResponseInfo is destroyed by the caller (request()), after wait() returns.
        }
    }

    fn generateToken(self: *ResponseManager) ![]u8 {
        const value = self.token_counter.fetchAdd(1, .monotonic);
        return try std.fmt.allocPrint(self.allocator, "{d}", .{value});
    }

    fn responseHandlerWrapper(msg: *Message, manager: *ResponseManager) void {
        // Regular subscribe handler wrapper
        manager.responseHandler(msg);
    }

    fn responseHandler(self: *ResponseManager, msg: *Message) void {
        var msg_used = false;
        defer if (!msg_used) msg.deinit();

        const token = self.extractToken(msg.subject) orelse {
            log.warn("Received response with invalid token: {s}", .{msg.subject});
            return;
        };

        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();

        const resp = self.resp_map.get(token) orelse {
            log.warn("Received response for unknown token: {s}", .{msg.subject});
            return;
        };

        if (msg.isNoResponders()) {
            resp.completeWithError(error.NoResponders);
        } else {
            resp.complete(msg);
            msg_used = true; // Message ownership transferred to response
        }
    }

    fn extractToken(self: *ResponseManager, subject: []const u8) ?[]const u8 {
        if (std.mem.startsWith(u8, subject, self.resp_sub_prefix)) {
            return subject[self.resp_sub_prefix.len..];
        }
        return null;
    }

    /// Get the wildcard subscription for registering with connection
    pub fn getSubscription(self: *ResponseManager) ?*Subscription {
        return self.resp_mux;
    }
};

test "response manager basic functionality" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var manager = ResponseManager.init(allocator);
    defer manager.deinit();

    // Test that we can generate tokens
    const token1 = try manager.generateToken();
    defer allocator.free(token1);
    const token2 = try manager.generateToken();
    defer allocator.free(token2);

    try testing.expect(!std.mem.eql(u8, token1, token2));
    try testing.expectEqualStrings("0", token1);
    try testing.expectEqualStrings("1", token2);
}

test "token generation sequence" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var manager = ResponseManager.init(allocator);
    defer manager.deinit();

    // Test simple numeric sequence
    const token1 = try manager.generateToken();
    defer allocator.free(token1);
    try testing.expectEqualStrings("0", token1);

    const token2 = try manager.generateToken();
    defer allocator.free(token2);
    try testing.expectEqualStrings("1", token2);

    const token3 = try manager.generateToken();
    defer allocator.free(token3);
    try testing.expectEqualStrings("2", token3);
}

test "response info future functionality" {
    const testing = std.testing;

    var response_info = ResponseInfo{};

    // Test timeout behavior
    const start = std.time.nanoTimestamp();
    const result = response_info.timedWait(1_000_000); // 1ms timeout (in nanoseconds)
    const duration = std.time.nanoTimestamp() - start;

    try testing.expectError(error.Timeout, result);
    try testing.expect(duration >= 1_000_000); // At least 1ms passed
}
