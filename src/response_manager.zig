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
const xsync = @import("xsync");
const Io = std.Io;
const io_util = @import("io_util.zig");
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const MessageList = @import("message.zig").MessageList;
const Subscription = @import("subscription.zig").Subscription;
const MsgHandler = @import("subscription.zig").MsgHandler;
const inbox = @import("inbox.zig");
const nuid = @import("nuid.zig");
const Connection = @import("connection.zig").Connection;
const ConnectionError = @import("connection.zig").ConnectionError;

const log = @import("log.zig").log;

const SingleResponseResult = struct {
    msg: ?*Message = null,
};

const MultiResponseResult = struct {
    msgs: MessageList = .{},
};

const ResponseResult = union(enum) {
    single: SingleResponseResult,
    multi: MultiResponseResult,
};

pub const RequestHandle = struct {
    rid: u64,
};

const INBOX_BASE_PREFIX = "_INBOX.";
const INBOX_BASE_PREFIX_LEN = INBOX_BASE_PREFIX.len;

const INBOX_PREFIX_LEN = INBOX_BASE_PREFIX_LEN + nuid.NUID_TOTAL_LEN + 1; // "_INBOX.{22-char-nuid}." = 7 + 22 + 1 = 30

pub const ResponseManager = struct {
    allocator: Allocator,
    io: Io,

    resp_sub_prefix_buf: [INBOX_PREFIX_LEN]u8 = undefined,
    resp_sub_prefix: []u8 = &.{},

    resp_mux: ?*Subscription = null,

    // Single shared sync primitives instead of per-request
    // NOTE: If the shared condition variable becomes a performance bottleneck due to
    // "thundering herd" effects with many concurrent requests, we can optimize by
    // storing per-request condition variables in the pending_responses map and
    // signaling only the specific waiting fiber. This trades memory for CPU efficiency.
    pending_mutex: xsync.Mutex = .init,
    pending_condition: xsync.Condition = .init,
    is_closed: bool = false,

    // Map of rid -> result
    pending_responses: std.AutoHashMapUnmanaged(u64, ResponseResult) = .{},

    // Request ID generation state (simple counter)
    rid_counter: u64 = 0,

    pub fn init(allocator: Allocator, io: Io) ResponseManager {
        return ResponseManager{
            .allocator = allocator,
            .io = io,
        };
    }

    pub fn deinit(self: *ResponseManager) void {
        // Signal shutdown and wake up all waiters
        self.pending_mutex.lockUncancelable(self.io);
        defer self.pending_mutex.unlock(self.io);

        // Clean up the response subscription if it exists
        if (self.resp_mux) |sub| {
            sub.deinit(); // This will unsubscribe and release the user reference
            self.resp_mux = null;
        }

        self.is_closed = true;
        self.pending_condition.broadcast(self.io); // Wake up all waiters

        // Clean up any remaining pending responses
        if (self.pending_responses.count() > 0) {
            log.warn("Cleaning up {} remaining pending responses during shutdown", .{self.pending_responses.count()});
            var iterator = self.pending_responses.iterator();
            while (iterator.next()) |entry| {
                switch (entry.value_ptr.*) {
                    .single => |*result| {
                        if (result.msg) |msg| {
                            msg.deinit();
                        }
                    },
                    .multi => |*result| {
                        while (result.msgs.pop()) |msg| {
                            msg.deinit();
                        }
                    },
                }
            }
        }

        self.pending_responses.deinit(self.allocator);
    }

    pub fn ensureInitialized(self: *ResponseManager, connection: *Connection) !void {
        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

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
        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

        if (self.is_closed) return error.ConnectionClosed;

        const rid = self.rid_counter;
        self.rid_counter += 1;

        try self.pending_responses.put(self.allocator, rid, .{ .single = .{} });

        return RequestHandle{ .rid = rid };
    }

    pub fn createMultiRequest(self: *ResponseManager) !RequestHandle {
        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

        if (self.is_closed) return error.ConnectionClosed;

        const rid = self.rid_counter;
        self.rid_counter += 1;

        try self.pending_responses.put(self.allocator, rid, .{ .multi = .{} });

        return RequestHandle{ .rid = rid };
    }

    pub fn getReplySubject(self: *ResponseManager, allocator: std.mem.Allocator, handle: RequestHandle) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{s}{d}", .{ self.resp_sub_prefix, handle.rid });
    }

    pub fn cleanupRequestInternal(self: *ResponseManager, entry: std.AutoHashMapUnmanaged(u64, ResponseResult).Entry) void {
        // If there's a pending message, clean it up
        switch (entry.value_ptr.*) {
            .single => |*result| {
                if (result.msg) |msg| {
                    msg.deinit();
                }
            },
            .multi => |*result| {
                while (result.msgs.pop()) |msg| {
                    msg.deinit();
                }
            },
        }

        self.pending_responses.removeByPtr(entry.key_ptr);
        self.pending_condition.broadcast(self.io);
    }

    pub fn cleanupRequest(self: *ResponseManager, handle: RequestHandle) void {
        self.pending_mutex.lockUncancelable(self.io);
        defer self.pending_mutex.unlock(self.io);

        const entry = self.pending_responses.getEntry(handle.rid) orelse return;
        self.cleanupRequestInternal(entry);

        log.debug("Cleaned up request map entry with rid: {d}", .{handle.rid});
    }

    pub fn waitForResponse(self: *ResponseManager, handle: RequestHandle, timeout: Io.Duration) !*Message {
        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

        if (self.is_closed) return error.ConnectionClosed;

        const deadline = io_util.deadline(self.io, timeout);
        while (true) {
            // Look up entry fresh each iteration - previous pointers may be invalid after timedWait
            const entry = self.pending_responses.getEntry(handle.rid) orelse {
                return error.UnknownRequest;
            };

            var cleanup = false;
            defer if (cleanup) self.cleanupRequestInternal(entry);

            std.debug.assert(entry.value_ptr.* == .single);
            const result = &entry.value_ptr.single;

            if (result.msg) |msg| {
                cleanup = true;
                if (msg.isNoResponders()) {
                    return error.NoResponders;
                }
                result.msg = null; // Transfer ownership to caller
                return msg;
            }

            if (self.is_closed) {
                cleanup = true;
                return error.ConnectionClosed;
            }

            if (io_util.expired(self.io, deadline)) {
                cleanup = true;
                return error.Timeout;
            }

            // After this call, any entry pointers become invalid due to potential HashMap modifications
            cleanup = false;
            self.pending_condition.waitTimeout(self.io, &self.pending_mutex, deadline) catch |err| switch (err) {
                error.Canceled => |e| return e,
                error.Timeout => {}, // Continue loop to check conditions
            };
        }
    }

    /// Options for controlling requestMany behavior (ADR-47 compliant)
    pub const WaitForMultiResponseOptions = struct {
        /// Maximum number of messages to collect before stopping
        max_messages: ?usize = null,

        /// Sentinel function: return true to continue processing, false to stop (ADR-47)
        /// This follows ADR-47 specification (differs from Go implementation)
        sentinelFn: ?*const fn (*Message) bool = null,

        /// Stall timeout: max time to wait between subsequent messages.
        /// After the first message, if no new messages arrive within this
        /// time, collection stops
        stall: ?Io.Duration = null,
    };

    pub fn waitForMultiResponse(self: *ResponseManager, handle: RequestHandle, timeout: Io.Duration, options: WaitForMultiResponseOptions) !MessageList {
        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

        if (self.is_closed) return error.ConnectionClosed;

        var msgs: MessageList = .{};
        var done = false;

        errdefer {
            while (msgs.pop()) |msg| {
                msg.deinit();
            }
        }

        const deadline = io_util.deadline(self.io, timeout);
        while (true) {
            // Look up entry fresh each iteration - previous pointers may be invalid after timedWait
            const entry = self.pending_responses.getEntry(handle.rid) orelse {
                return error.UnknownRequest;
            };

            var cleanup = false;
            defer if (cleanup) self.cleanupRequestInternal(entry);

            std.debug.assert(entry.value_ptr.* == .multi);
            const result = &entry.value_ptr.multi;

            while (result.msgs.pop()) |msg| {
                if (msg.isNoResponders()) {
                    msg.deinit();
                    done = true;
                    break;
                }

                msgs.push(msg);

                if (options.max_messages) |max_messages| {
                    if (msgs.len >= max_messages) {
                        done = true;
                        break;
                    }
                }

                // ADR-47: Sentinel function returns true to continue, false to stop
                if (options.sentinelFn) |sentinelFn| {
                    if (msgs.tail) |last_msg| {
                        if (!sentinelFn(last_msg)) {
                            done = true;
                            break;
                        }
                    }
                }
            }

            if (done) {
                cleanup = true;
                if (msgs.len > 0) {
                    return msgs;
                }
                return error.NoResponders;
            }

            if (self.is_closed) {
                cleanup = true;
                return error.ConnectionClosed;
            }

            if (io_util.expired(self.io, deadline)) {
                cleanup = true;
                if (msgs.len > 0) {
                    return msgs;
                }
                return error.Timeout;
            }

            // Calculate the wait deadline: use the stall timeout after the
            // first message, if configured
            var wait_deadline = deadline;
            if (msgs.len > 0) {
                if (options.stall) |stall| {
                    wait_deadline = io_util.earlierDeadline(deadline, io_util.deadline(self.io, stall));
                }
            }

            // After this call, any entry pointers become invalid due to potential HashMap modifications
            self.pending_condition.waitTimeout(self.io, &self.pending_mutex, wait_deadline) catch |err| switch (err) {
                error.Canceled => |e| return e,
                error.Timeout => {
                    // Timeout or stall - return what we have collected so
                    // far. The map entry is left for the caller's
                    // cleanupRequest; `entry` may be stale at this point and
                    // must not be touched.
                    if (msgs.len > 0) {
                        return msgs;
                    }
                    return error.Timeout;
                },
            };
        }
    }

    fn responseHandlerWrapper(msg: *Message, manager: *ResponseManager) !void {
        // Regular subscribe handler wrapper
        try manager.responseHandler(msg);
    }

    fn responseHandler(self: *ResponseManager, msg: *Message) !void {
        var own_msg = true;
        defer if (own_msg) msg.deinit();

        const rid = self.extractRid(msg.subject) orelse {
            log.warn("Received response with invalid rid: {s}", .{msg.subject});
            return;
        };

        try self.pending_mutex.lock(self.io);
        defer self.pending_mutex.unlock(self.io);

        // Don't process responses after shutdown
        if (self.is_closed) return;

        const entry = self.pending_responses.getEntry(rid) orelse {
            log.warn("Received response for unknown rid: {d}", .{rid});
            return;
        };

        switch (entry.value_ptr.*) {
            .single => |*result| {
                if (result.msg != null) {
                    log.warn("Received multiple responses for rid: {d}", .{rid});
                    return;
                }
                result.msg = msg;
                own_msg = false; // Message ownership transferred to response
            },
            .multi => |*result| {
                result.msgs.push(msg);
                own_msg = false; // Message ownership transferred to response
            },
        }

        self.pending_condition.broadcast(self.io); // Wake up waiting fibers
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

    var manager = ResponseManager.init(allocator, std.testing.io);
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

    var manager = ResponseManager.init(allocator, std.testing.io);
    defer manager.deinit();

    const handle = try manager.createRequest();
    defer manager.cleanupRequest(handle);

    // Test timeout behavior
    const start = io_util.now(std.testing.io);
    const result = manager.waitForResponse(handle, .fromMilliseconds(1)); // 1ms timeout
    const duration = start.durationTo(io_util.now(std.testing.io)).nanoseconds;

    try testing.expectError(error.Timeout, result);
    try testing.expect(duration >= 1_000_000); // At least 1ms passed
}

test "multi-response request creation and timeout" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var manager = ResponseManager.init(allocator, std.testing.io);
    defer manager.deinit();

    // Test that we can create multi-response request handles
    const handle = try manager.createMultiRequest();
    defer manager.cleanupRequest(handle);

    // Test timeout behavior for multi-response
    const result = manager.waitForMultiResponse(handle, .fromMilliseconds(1), .{}); // 1ms timeout
    try testing.expectError(error.Timeout, result);
}
