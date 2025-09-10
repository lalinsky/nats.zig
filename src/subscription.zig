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
const RefCounter = @import("ref_counter.zig").RefCounter;
const ConcurrentQueue = @import("queue.zig").ConcurrentQueue;
const Dispatcher = @import("dispatcher.zig").Dispatcher;
const Connection = @import("connection.zig").Connection;

const log = @import("log.zig").log;

// Message handler storage for type-erased callback
// Error set for message handlers

pub const MsgHandler = struct {
    ptr: *anyopaque,
    callFn: *const fn (ptr: *anyopaque, msg: *Message) anyerror!void,
    cleanupFn: *const fn (ptr: *anyopaque, allocator: Allocator) void,

    pub fn call(self: *const MsgHandler, msg: *Message) anyerror!void {
        return self.callFn(self.ptr, msg);
    }

    pub fn cleanup(self: *const MsgHandler, allocator: Allocator) void {
        self.cleanupFn(self.ptr, allocator);
    }
};

pub const Subscription = struct {
    nc: *Connection,
    sid: u64,
    subject: []const u8,
    queue: ?[]const u8 = null,
    messages: MessageQueue,

    // Reference counting for safe cleanup
    ref_counter: RefCounter(u32) = RefCounter(u32).init(),

    // Callback support
    handler: ?MsgHandler = null,

    // Assigned dispatcher (for async subscriptions only)
    dispatcher: ?*Dispatcher = null,

    // Track pending messages and bytes for both sync and async subscriptions
    pending_msgs: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    pending_bytes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    // Autounsubscribe state
    max_msgs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0), // 0 means no limit
    delivered_msgs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    // Drain state
    draining: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    drain_complete: std.Thread.ResetEvent = .{},

    pub const MessageQueue = ConcurrentQueue(*Message, 1024); // 1K chunk size

    pub fn create(nc: *Connection, sid: u64, subject: []const u8, queue_group: ?[]const u8, handler: ?MsgHandler) !*Subscription {
        const sub = try nc.allocator.create(Subscription);
        errdefer nc.allocator.destroy(sub);

        const subject_copy = try nc.allocator.dupe(u8, subject);
        errdefer nc.allocator.free(subject_copy);

        const queue_group_copy = if (queue_group) |group| try nc.allocator.dupe(u8, group) else null;
        errdefer if (queue_group_copy) |group| nc.allocator.free(group);

        sub.* = Subscription{
            .nc = nc,
            .sid = sid,
            .subject = subject_copy,
            .queue = queue_group_copy,
            .messages = MessageQueue.init(nc.allocator, .{}),
            .handler = handler,
        };

        // Subscription starts with 1 reference (from RefCounter.init())
        // Add an additional reference for the user - total will be 2 refs:
        // 1. Connection reference (for hashmap storage)
        // 2. User reference (for returned pointer)
        sub.retain();

        return sub;
    }

    /// Unsubscribe from the server and release the user reference.
    /// After calling this, the subscription should not be used.
    pub fn deinit(self: *Subscription) void {
        self.nc.unsubscribe(self);
        self.release(); // Release user reference
    }

    fn destroy(self: *Subscription) void {
        self.nc.allocator.free(self.subject);

        if (self.queue) |queue_group| {
            self.nc.allocator.free(queue_group);
        }

        // Clean up handler context if present
        if (self.handler) |handler| {
            handler.cleanup(self.nc.allocator);
        }

        // Close the queue to prevent new messages and clean up pending messages
        self.messages.close();
        while (self.messages.tryPop()) |msg| {
            msg.deinit();
        }
        self.messages.deinit();

        self.nc.allocator.destroy(self);
    }

    pub fn retain(self: *Subscription) void {
        self.ref_counter.incr();
    }

    pub fn release(self: *Subscription) void {
        if (self.ref_counter.decr()) {
            // Last reference - actually free the subscription
            self.destroy();
        }
    }

    pub fn drain(self: *Subscription) void {
        // Temporarily increment pending, to avoid race conditions
        incrementPending(self, 0);
        defer decrementPending(self, 0);

        // Mark as draining
        const prev_state = self.draining.cmpxchgStrong(false, true, .acq_rel, .acquire);
        if (prev_state != null) return; // Already draining

        // Send UNSUB to server
        self.nc.unsubscribeInternal(self.sid);
    }

    pub fn isDraining(self: *Subscription) bool {
        return self.draining.load(.acquire);
    }

    pub fn isDrainComplete(self: *Subscription) bool {
        return self.draining.load(.acquire) and self.drain_complete.isSet();
    }

    pub fn waitForDrainCompletion(self: *Subscription, timeout_ms: u64) !void {
        if (!self.draining.load(.acquire)) {
            return error.NotDraining;
        }

        if (timeout_ms == 0) {
            // No timeout - wait indefinitely
            self.drain_complete.wait();
        } else {
            // Convert timeout to nanoseconds
            const timeout_ns = timeout_ms * std.time.ns_per_ms;
            self.drain_complete.timedWait(timeout_ns) catch {
                return error.Timeout;
            };
        }
    }

    pub const AutoUnsubscribeError = error{
        MaxAlreadyReached,
        InvalidMax,
        SubscriptionClosed,
        SendFailed,
    };

    pub fn autoUnsubscribe(self: *Subscription, max: u64) AutoUnsubscribeError!void {
        if (max == 0) return AutoUnsubscribeError.InvalidMax;

        const current_delivered = self.delivered_msgs.load(.acquire);
        if (current_delivered >= max) {
            return AutoUnsubscribeError.MaxAlreadyReached;
        }

        // Send protocol message to server first
        self.nc.unsubscribeInternal(self.sid, max) catch {
            return AutoUnsubscribeError.SendFailed;
        };

        // Only set the limit after successfully sending UNSUB
        self.max_msgs.store(max, .release);
    }

    pub fn nextMsg(self: *Subscription, timeout_ms: u64) error{Timeout}!*Message {
        // Check if subscription has reached autounsubscribe limit
        const max = self.max_msgs.load(.acquire);
        if (max > 0 and self.delivered_msgs.load(.acquire) >= max) {
            return error.Timeout; // Consistent with "no more messages" semantics
        }

        const msg = self.messages.pop(timeout_ms) catch |err| switch (err) {
            error.BufferFrozen => return error.Timeout,
            error.QueueEmpty => return error.Timeout,
            error.QueueClosed => return error.Timeout, // TODO: this should be mapped to ConnectionClosed
        };

        // Increment delivered counter with proper memory ordering
        const delivered = self.delivered_msgs.fetchAdd(1, .acq_rel) + 1;

        // Check if we've reached the autounsubscribe limit
        const max_limit = self.max_msgs.load(.acquire);
        if (max_limit > 0 and delivered >= max_limit) {
            // Remove subscription from connection
            self.nc.removeSubscriptionInternal(self.sid);
        }

        // Decrement pending counters when message is consumed
        decrementPending(self, msg.data.len);

        return msg;
    }
};

// Helper function to create type-erased handlers
pub fn createMsgHandler(allocator: Allocator, comptime handlerFn: anytype, args: anytype) !MsgHandler {
    // Create a context struct to hold the function and arguments
    const Context = struct {
        args: @TypeOf(args),

        pub fn call(ctx: *anyopaque, msg: *Message) anyerror!void {
            const self_ctx: *@This() = @ptrCast(@alignCast(ctx));

            // Handle both fallible and non-fallible user handler functions
            const ReturnType = @typeInfo(@TypeOf(handlerFn)).@"fn".return_type.?;
            if (ReturnType == void) {
                // Non-fallible handler - just call it
                @call(.auto, handlerFn, .{msg} ++ self_ctx.args);
            } else {
                // Fallible handler - propagate error directly
                try @call(.auto, handlerFn, .{msg} ++ self_ctx.args);
            }
        }

        pub fn cleanup(ctx: *anyopaque, alloc: Allocator) void {
            const self_ctx: *@This() = @ptrCast(@alignCast(ctx));
            alloc.destroy(self_ctx);
        }
    };

    // Allocate context on heap
    const context = try allocator.create(Context);
    context.* = .{ .args = args };

    return MsgHandler{
        .ptr = context,
        .callFn = Context.call,
        .cleanupFn = Context.cleanup,
    };
}

// Internal functions for pending counter management (not part of public API)
pub fn incrementPending(sub: *Subscription, msg_size: usize) void {
    _ = sub.pending_msgs.fetchAdd(1, .acq_rel);
    _ = sub.pending_bytes.fetchAdd(msg_size, .acq_rel);
}

pub fn decrementPending(sub: *Subscription, msg_size: usize) void {
    const remaining_msgs = sub.pending_msgs.fetchSub(1, .acq_rel);
    _ = sub.pending_bytes.fetchSub(msg_size, .acq_rel);

    // Check if drain is complete (we just decremented from 1 to 0)
    if (sub.draining.load(.acquire) and remaining_msgs == 1) {
        log.debug("Subscription {d} drain completed", .{sub.sid});
        sub.drain_complete.set();
        sub.nc.notifySubscriptionDrainComplete();
    }
}
