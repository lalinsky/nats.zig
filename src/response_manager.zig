const std = @import("std");
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const Subscription = @import("subscription.zig").Subscription;
const MsgHandler = @import("subscription.zig").MsgHandler;
const inbox = @import("inbox.zig");
const ConnectionError = @import("connection.zig").ConnectionError;

const log = std.log.scoped(.response_manager);

pub const ResponseInfo = struct {
    mutex: std.Thread.Mutex = .{},
    condition: std.Thread.Condition = .{},
    result: ?anyerror!*Message = null,
    
    pub fn wait(self: *ResponseInfo, timeout_ms: u64) !?*Message {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        const deadline = std.time.nanoTimestamp();
        const timeout_ns = timeout_ms * std.time.ns_per_ms;
        const end_time = deadline + @as(i64, @intCast(timeout_ns));
        
        while (self.result == null) {
            const now = std.time.nanoTimestamp();
            if (now >= end_time) {
                return null; // timeout
            }
            
            const remaining_ns = @as(u64, @intCast(end_time - now));
            self.condition.timedWait(&self.mutex, remaining_ns) catch {
                return null; // timeout
            };
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

const INBOX_PREFIX_LEN = 30; // "_INBOX.{22-char-nuid}." = 7 + 22 + 1 = 30

pub const ResponseManager = struct {
    allocator: Allocator,
    
    // Response handling state (static buffer for inbox prefix)
    resp_sub_prefix: [INBOX_PREFIX_LEN]u8 = undefined,
    resp_sub_prefix_len: u8 = 0,
    resp_mux: ?*Subscription = null,
    resp_map: std.StringHashMapUnmanaged(*ResponseInfo),
    resp_mutex: std.Thread.Mutex = .{},
    
    // Token generation state (simple counter)
    token_counter: u64 = 0,
    
    pub fn init(allocator: Allocator) ResponseManager {
        return ResponseManager{
            .allocator = allocator,
            .resp_map = .{},
        };
    }
    
    pub fn deinit(self: *ResponseManager) void {
        // No need to free resp_sub_prefix - it's now a static buffer
        
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
    
    pub fn ensureInitialized(self: *ResponseManager, connection: anytype) !void {
        // Use a simple check first (without lock for performance)
        if (self.resp_mux != null) return; // Already initialized
        
        // Double-check with lock to prevent race conditions
        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();
        if (self.resp_mux != null) return; // Already initialized by another thread
        
        // Generate unique inbox prefix using static buffer
        const nuid_bytes = @import("nuid.zig").next();
        const prefix_slice = std.fmt.bufPrint(&self.resp_sub_prefix, "_INBOX.{s}.", .{nuid_bytes}) catch unreachable;
        self.resp_sub_prefix_len = @intCast(prefix_slice.len);
        
        // Create wildcard subscription using a temporary buffer
        var wildcard_buffer: [INBOX_PREFIX_LEN + 1]u8 = undefined;
        const wildcard_slice = std.fmt.bufPrint(&wildcard_buffer, "{s}*", .{self.getPrefix()}) catch unreachable;
        const wildcard_subject = wildcard_slice;
        
        // Temporarily release mutex for subscription creation
        self.resp_mutex.unlock();
        const subscription = connection.subscribe(wildcard_subject, responseHandlerWrapper, .{self}) catch |err| {
            self.resp_mutex.lock();
            return err;
        };
        self.resp_mutex.lock();
        
        // Final check to ensure we didn't race with another thread
        if (self.resp_mux != null) {
            // Another thread beat us to it, clean up our subscription
            // (The connection will handle cleanup when it's torn down)
            return;
        }
        
        self.resp_mux = subscription;
        log.debug("Initialized response manager with prefix: {s}, wildcard: {s}", .{ self.getPrefix(), wildcard_subject });
    }
    
    fn getPrefix(self: *const ResponseManager) []const u8 {
        return self.resp_sub_prefix[0..self.resp_sub_prefix_len];
    }
    
    pub fn createRequest(self: *ResponseManager, subject: []const u8, _: []const u8) !struct { 
        response_info: *ResponseInfo, 
        reply_subject: []u8, 
        token: []u8 
    } {
        const token = try self.generateToken();
        errdefer self.allocator.free(token);
        
        const response_info = try self.allocator.create(ResponseInfo);
        response_info.* = ResponseInfo{};
        errdefer self.allocator.destroy(response_info);
        
        const token_copy = try self.allocator.dupe(u8, token);
        errdefer self.allocator.free(token_copy);
        
        self.resp_mutex.lock();
        defer self.resp_mutex.unlock();
        try self.resp_map.put(self.allocator, token_copy, response_info);
        
        const reply_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.getPrefix(), token });
        
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
            self.allocator.free(entry.key);
            // ResponseInfo is destroyed by the caller (request()), after wait() returns.
            log.debug("Cleaned up request map entry with token: {s}", .{token});
        }
    }
    
    fn generateToken(self: *ResponseManager) ![]u8 {
        defer self.token_counter += 1;
        return try std.fmt.allocPrint(self.allocator, "{d}", .{self.token_counter});
    }
    
    fn responseHandlerWrapper(msg: *Message, manager: *ResponseManager) void {
        // Regular subscribe handler wrapper
        manager.responseHandler(msg);
    }
    
    fn responseHandler(self: *ResponseManager, msg: *Message) void {        
        log.debug("Response handler received message on: {s} (prefix: {s})", .{ msg.subject, self.getPrefix() });
        
        if (extractResponseToken(msg.subject, self.getPrefix())) |token| {
            log.debug("Extracted token: '{s}'", .{token});
            self.resp_mutex.lock();
            const response_info = self.resp_map.get(token);
            self.resp_mutex.unlock();
            
            if (response_info) |ri| {
                log.debug("Found response info for token: {s}, completing request", .{token});
                
                // Check if this is a "No Responders" error message
                if (msg.isNoResponders()) {
                    log.debug("Received No Responders error, completing with error", .{});
                    msg.deinit(); // Clean up message since we're not passing it along
                    ri.completeWithError(ConnectionError.NoResponders);
                } else {
                    ri.complete(msg);
                }
                return;
            } else {
                log.warn("No response info found for token: '{s}', map size: {}", .{ token, self.resp_map.count() });
                // Debug: print all keys in map
                self.resp_mutex.lock();
                var iterator = self.resp_map.iterator();
                while (iterator.next()) |entry| {
                    log.debug("Map contains key: '{s}'", .{entry.key_ptr.*});
                }
                self.resp_mutex.unlock();
            }
        } else {
            log.warn("Could not extract token from subject: {s}, expected prefix: {s}", .{ msg.subject, self.getPrefix() });
        }
        
        // No one waiting, clean up message
        msg.deinit();
    }
    
    
    fn extractResponseToken(subject: []const u8, prefix: []const u8) ?[]const u8 {
        if (std.mem.startsWith(u8, subject, prefix)) {
            return subject[prefix.len..];
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
    
    // Test immediate timeout
    const start = std.time.nanoTimestamp();
    const result = response_info.wait(1); // 1ms timeout
    const duration = std.time.nanoTimestamp() - start;
    
    try testing.expectEqual(@as(?*Message, null), result);
    try testing.expect(duration >= 1_000_000); // At least 1ms passed
}