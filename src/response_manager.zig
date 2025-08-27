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
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const Subscription = @import("subscription.zig").Subscription;
const MsgHandler = @import("subscription.zig").MsgHandler;
const inbox = @import("inbox.zig");
const nuid = @import("nuid.zig");
const Connection = @import("connection.zig").Connection;
const ConnectionError = @import("connection.zig").ConnectionError;

const log = std.log.scoped(.response_manager);

// Define explicit type for response results
const ResponseResult = anyerror!*Message;

pub const RequestHandle = struct {
    rid: u64,
};

const INBOX_BASE_PREFIX = "_INBOX.";
const INBOX_BASE_PREFIX_LEN = INBOX_BASE_PREFIX.len;

const INBOX_PREFIX_LEN = INBOX_BASE_PREFIX_LEN + nuid.NUID_TOTAL_LEN + 1; // "_INBOX.{22-char-nuid}." = 7 + 22 + 1 = 30

pub const ResponseManager = struct {
    allocator: Allocator,

    resp_sub_prefix_buf: [INBOX_PREFIX_LEN]u8 = undefined,
    resp_sub_prefix: []u8 = &.{},

    resp_mux: ?*Subscription = null,
    
    // Single shared sync primitives instead of per-request
    // NOTE: If the shared condition variable becomes a performance bottleneck due to
    // "thundering herd" effects with many concurrent requests, we can optimize by
    // storing per-request condition variables in the pending_responses map and
    // signaling only the specific waiting thread. This trades memory for CPU efficiency.
    pending_mutex: std.Thread.Mutex = .{},
    pending_condition: std.Thread.Condition = .{},
    is_closed: bool = false,
    
    // Map of rid -> result (null means still pending)
    pending_responses: std.AutoHashMapUnmanaged(u64, ?ResponseResult) = .{},

    // Request ID generation state (simple counter)
    rid_counter: u64 = 0,

    pub fn init(allocator: Allocator) ResponseManager {
        return ResponseManager{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ResponseManager) void {
        // Signal shutdown and wake up all waiters
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        
        // Explicitly release the resp_mux subscription to reduce callback races
        if (self.resp_mux) |subscription| {
            subscription.deinit();
        }
        self.resp_mux = null;
        
        self.is_closed = true;
        self.pending_condition.broadcast(); // Wake up all waiters
        
        // Clean up any remaining pending responses
        if (self.pending_responses.count() > 0) {
            log.warn("Cleaning up {} remaining pending responses during shutdown", .{self.pending_responses.count()});
            var iterator = self.pending_responses.iterator();
            while (iterator.next()) |entry| {
                // If there's a pending message, clean it up
                if (entry.value_ptr.*) |result| {
                    if (result) |msg| {
                        msg.deinit();
                    } else |_| {
                        // Error result, nothing to clean up
                    }
                }
            }
        }
        
        self.pending_responses.deinit(self.allocator);
    }

    pub fn ensureInitialized(self: *ResponseManager, connection: *Connection) !void {
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();

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

    pub fn createRequest(self: *ResponseManager) !RequestHandle {
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        
        if (self.is_closed) return error.ConnectionClosed;
        
        const rid = self.rid_counter;
        self.rid_counter += 1;
        
        try self.pending_responses.put(self.allocator, rid, null);
        
        return RequestHandle{ .rid = rid };
    }

    pub fn getReplySubject(self: *ResponseManager, allocator: std.mem.Allocator, handle: RequestHandle) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{s}{d}", .{ self.resp_sub_prefix, handle.rid });
    }

    pub fn cleanupRequest(self: *ResponseManager, handle: RequestHandle) void {
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        
        if (self.pending_responses.fetchRemove(handle.rid)) |entry| {
            log.debug("Cleaned up request map entry with rid: {d}", .{handle.rid});
            
            // If there's a pending message, clean it up
            if (entry.value) |result| {
                if (result) |msg| {
                    msg.deinit();
                } else |_| {
                    // Error result, nothing to clean up
                }
            }
            
            // Wake any threads waiting for this response
            self.pending_condition.broadcast();
        }
    }

    pub fn waitForResponse(self: *ResponseManager, handle: RequestHandle, timeout_ns: u64) !*Message {
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        
        if (self.is_closed) return error.ConnectionClosed;
        
        var timer = std.time.Timer.start() catch unreachable;
        while (true) {
            // Look up entry fresh each iteration - previous pointers may be invalid after timedWait
            const entry = self.pending_responses.getEntry(handle.rid) orelse {
                return error.UnknownRequest;
            };
            
            if (entry.value_ptr.*) |result| {
                // Got response - remove entry and transfer ownership to caller
                self.pending_responses.removeByPtr(entry.key_ptr);
                return result;
            }
            
            if (self.is_closed) return error.ConnectionClosed;
            
            const elapsed = timer.read();
            if (elapsed >= timeout_ns) return error.Timeout;
            
            // After this call, any entry pointers become invalid due to potential HashMap modifications
            try self.pending_condition.timedWait(&self.pending_mutex, timeout_ns - elapsed);
        }
    }

    fn responseHandlerWrapper(msg: *Message, manager: *ResponseManager) anyerror!void {
        // Regular subscribe handler wrapper
        try manager.responseHandler(msg);
    }

    fn responseHandler(self: *ResponseManager, msg: *Message) anyerror!void {
        var own_msg = true;
        defer if (own_msg) msg.deinit();

        const rid = self.extractRid(msg.subject) orelse {
            log.warn("Received response with invalid rid: {s}", .{msg.subject});
            return;
        };

        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        
        // Don't process responses after shutdown
        if (self.is_closed) return;

        const entry = self.pending_responses.getEntry(rid) orelse {
            log.warn("Received response for unknown rid: {d}", .{rid});
            return;
        };

        if (msg.isNoResponders()) {
            entry.value_ptr.* = error.NoResponders;
            // Keep own_msg = true, so message gets cleaned up
        } else {
            entry.value_ptr.* = msg;
            own_msg = false; // Message ownership transferred to response
        }
        
        self.pending_condition.broadcast(); // Wake up waiting threads
    }

    fn extractRid(self: *ResponseManager, subject: []const u8) ?u64 {
        if (std.mem.startsWith(u8, subject, self.resp_sub_prefix)) {
            const rid_str = subject[self.resp_sub_prefix.len..];
            return std.fmt.parseInt(u64, rid_str, 10) catch null;
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

    // Test that we can create request handles with different rids
    const handle1 = try manager.createRequest();
    defer manager.cleanupRequest(handle1);
    
    const handle2 = try manager.createRequest();
    defer manager.cleanupRequest(handle2);

    try testing.expect(handle1.rid != handle2.rid);
    try testing.expectEqual(@as(u64, 0), handle1.rid);
    try testing.expectEqual(@as(u64, 1), handle2.rid);
}

test "request handle timeout functionality" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var manager = ResponseManager.init(allocator);
    defer manager.deinit();

    const handle = try manager.createRequest();
    defer manager.cleanupRequest(handle);

    // Test timeout behavior
    const start = std.time.nanoTimestamp();
    const result = manager.waitForResponse(handle, 1_000_000); // 1ms timeout
    const duration = std.time.nanoTimestamp() - start;

    try testing.expectError(error.Timeout, result);
    try testing.expect(duration >= 1_000_000); // At least 1ms passed
}
