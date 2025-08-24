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
    message: ?*Message = null,
    completed: bool = false,
    error_status: ?anyerror = null,
    timeout_expired: bool = false,
    
    pub fn wait(self: *ResponseInfo, timeout_ms: u64) !?*Message {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        const deadline = std.time.nanoTimestamp() + (timeout_ms * 1_000_000);
        
        while (!self.completed and !self.timeout_expired and self.error_status == null) {
            const now = std.time.nanoTimestamp();
            if (now >= deadline) {
                self.timeout_expired = true;
                break;
            }
            
            const remaining_ns = @as(u64, @intCast(deadline - now));
            self.condition.timedWait(&self.mutex, remaining_ns) catch {
                self.timeout_expired = true;
                break;
            };
        }
        
        // Check if an error occurred
        if (self.error_status) |err| {
            return err;
        }
        
        return self.message;
    }
    
    pub fn complete(self: *ResponseInfo, msg: *Message) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        if (!self.completed) {
            self.message = msg;
            self.completed = true;
            self.condition.signal();
        }
    }
    
    pub fn completeWithError(self: *ResponseInfo, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        if (!self.completed) {
            self.error_status = err;
            self.completed = true;
            self.condition.signal();
        }
    }
};

pub const ResponseManager = struct {
    allocator: Allocator,
    
    // Response handling state
    resp_sub_prefix: ?[]u8 = null,
    resp_sub_len: usize = 0,
    resp_mux: ?*Subscription = null,
    resp_map: std.HashMap([]const u8, *ResponseInfo, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    
    // Token generation state (simple counter)
    token_counter: u64 = 0,
    
    pub fn init(allocator: Allocator) ResponseManager {
        return ResponseManager{
            .allocator = allocator,
            .resp_map = std.HashMap([]const u8, *ResponseInfo, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
        };
    }
    
    pub fn deinit(self: *ResponseManager) void {
        if (self.resp_sub_prefix) |prefix| {
            self.allocator.free(prefix);
        }
        
        if (self.resp_mux) |sub| {
            sub.deinit();
        }
        
        // Clean up any remaining response infos
        var iterator = self.resp_map.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.resp_map.deinit();
    }
    
    pub fn ensureInitialized(self: *ResponseManager, connection: anytype) !void {
        if (self.resp_mux != null) return; // Already initialized
        
        // Generate unique inbox prefix
        const base_inbox = try inbox.newInbox(self.allocator);
        defer self.allocator.free(base_inbox);
        
        self.resp_sub_prefix = try std.fmt.allocPrint(self.allocator, "{s}.", .{base_inbox});
        self.resp_sub_len = self.resp_sub_prefix.?.len - 1;
        
        // Create wildcard subscription
        const wildcard_subject = try std.fmt.allocPrint(self.allocator, "{s}*", .{self.resp_sub_prefix.?});
        defer self.allocator.free(wildcard_subject);
        
        // Create single shared subscription with response handler
        const handler = MsgHandler{
            .ptr = self,
            .callFn = responseHandler,
            .cleanupFn = responseCleanup,
        };
        
        self.resp_mux = try connection.subscribeInternal(wildcard_subject, handler);
        
        log.debug("Initialized response manager with prefix: {s}, wildcard: {s}", .{ self.resp_sub_prefix.?, wildcard_subject });
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
        
        try self.resp_map.put(token_copy, response_info);
        
        const reply_subject = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.resp_sub_prefix.?, token });
        
        log.debug("Created request for {s}, reply: {s}, token: {s}", .{ subject, reply_subject, token });
        
        return .{ 
            .response_info = response_info, 
            .reply_subject = reply_subject, 
            .token = token 
        };
    }
    
    pub fn cleanupRequest(self: *ResponseManager, token: []const u8) void {
        if (self.resp_map.fetchRemove(token)) |entry| {
            self.allocator.free(entry.key);
            self.allocator.destroy(entry.value);
            log.debug("Cleaned up request with token: {s}", .{token});
        }
    }
    
    fn generateToken(self: *ResponseManager) ![]u8 {
        defer self.token_counter += 1;
        return try std.fmt.allocPrint(self.allocator, "{d}", .{self.token_counter});
    }
    
    fn responseHandler(ptr: *anyopaque, msg: *Message) void {
        const self: *ResponseManager = @ptrCast(@alignCast(ptr));
        
        log.debug("Response handler received message on: {s} (prefix: {s})", .{ msg.subject, self.resp_sub_prefix.? });
        
        if (extractResponseToken(msg.subject, self.resp_sub_prefix.?)) |token| {
            log.debug("Extracted token: '{s}'", .{token});
            if (self.resp_map.get(token)) |response_info| {
                log.debug("Found response info for token: {s}, completing request", .{token});
                
                // Check if this is a "No Responders" error message
                if (msg.isNoResponders()) {
                    log.debug("Received No Responders error, completing with error", .{});
                    msg.deinit(); // Clean up message since we're not passing it along
                    response_info.completeWithError(ConnectionError.NoResponders);
                } else {
                    response_info.complete(msg);
                }
                return;
            } else {
                log.warn("No response info found for token: '{s}', map size: {}", .{ token, self.resp_map.count() });
                // Debug: print all keys in map
                var iterator = self.resp_map.iterator();
                while (iterator.next()) |entry| {
                    log.debug("Map contains key: '{s}'", .{entry.key_ptr.*});
                }
            }
        } else {
            log.warn("Could not extract token from subject: {s}, expected prefix: {s}", .{ msg.subject, self.resp_sub_prefix.? });
        }
        
        // No one waiting, clean up message
        msg.deinit();
    }
    
    fn responseCleanup(ptr: *anyopaque, allocator: Allocator) void {
        _ = ptr;
        _ = allocator;
        // Nothing to cleanup for the handler context
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
    try testing.expect(response_info.timeout_expired);
}