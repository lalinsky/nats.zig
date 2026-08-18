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
const RefCounter = @import("ref_counter.zig").RefCounter;
const ConcurrentQueue = @import("queue.zig").ConcurrentQueue;
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

    // Handler task group (for async subscriptions only)
    handler_group: Io.Group = .init,

    // Track pending messages and bytes for both sync and async subscriptions
    pending_msgs: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    pending_bytes: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    // Slow-consumer protection: when either pending limit is exceeded,
    // incoming messages for this subscription are dropped and counted
    // (0 = unlimited). Defaults match nats.go.
    pending_msgs_limit: std.atomic.Value(u32) = std.atomic.Value(u32).init(default_pending_msgs_limit),
    pending_bytes_limit: std.atomic.Value(u64) = std.atomic.Value(u64).init(default_pending_bytes_limit),
    dropped_msgs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    slow_consumer: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Armed when a synchronous subscription enters a slow-consumer episode;
    // consumed by the next synchronous receive. Multiple episodes before a
    // receive coalesce into one report; dropped_msgs provides the exact count.
    sc_error_pending: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    // Autounsubscribe state
    max_msgs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0), // 0 means no limit
    delivered_msgs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    // Drain state
    draining: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    drain_complete: xsync.Event = .init,
    drain_succeeded: std.atomic.Value(bool) = .init(false),
    connection_closed: std.atomic.Value(bool) = .init(false),

    pub const MessageQueue = ConcurrentQueue(*Message, 1024); // 1K chunk size

    // Default pending limits, matching nats.go.
    pub const default_pending_msgs_limit: u32 = 500_000;
    pub const default_pending_bytes_limit: u64 = 64 * 1024 * 1024;

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
            .messages = MessageQueue.init(nc.allocator, nc.io, .{}),
            .handler = handler,
        };

        // Subscription starts with 1 reference (from RefCounter.init())
        // Add an additional reference for the user - total will be 2 refs:
        // 1. Connection reference (for hashmap storage)
        // 2. User reference (for returned pointer)
        sub.retain();

        return sub;
    }

    /// Start the handler fiber for async subscriptions.
    /// This should be called after the subscription is fully registered.
    pub fn startHandler(self: *Subscription) !void {
        if (self.handler == null) return; // Sync subscription, no handler fiber needed

        try self.handler_group.concurrent(self.nc.io, handlerLoop, .{self});
    }

    /// Handler fiber loop - waits for messages and calls the handler
    fn handlerLoop(self: *Subscription) void {
        log.debug("Handler fiber started for subscription {}", .{self.sid});

        while (true) {
            // Wait for a message with timeout (allows periodic checking)
            const msg = self.messages.pop(.{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }) catch |err| {
                if (err == error.Closed or err == error.Canceled) {
                    log.debug("Subscription {} queue closed, stopping handler", .{self.sid});
                    break;
                }
                // Timeout - continue loop
                continue;
            };

            // Check autounsubscribe limit
            const max = self.max_msgs.load(.acquire);
            const delivered = self.delivered_msgs.fetchAdd(1, .acq_rel) + 1;

            // Save message data length before handler is called
            const message_data_len = msg.data.len;

            // Call the handler
            var canceled = false;
            if (self.handler) |handler| {
                handler.call(msg) catch |err| {
                    if (err == error.Canceled) {
                        canceled = true;
                    } else {
                        log.err("Message handler failed for subscription {}: {}", .{ self.sid, err });
                    }
                };
            } else {
                // No handler - shouldn't happen for async subscriptions
                log.warn("Received message for subscription {} without handler", .{self.sid});
                msg.deinit();
            }

            // Decrement pending counters after handler completes
            decrementPending(self, message_data_len);

            if (canceled) {
                log.debug("Handler fiber for subscription {} canceled, stopping", .{self.sid});
                break;
            }

            // Check if we've reached autounsubscribe limit
            if (max > 0 and delivered >= max) {
                log.debug("Subscription {} reached autounsubscribe limit ({}), removing", .{ self.sid, max });
                self.nc.removeSubscriptionInternal(self.sid);
                break;
            }
        }

        log.debug("Handler fiber stopped for subscription {}", .{self.sid});
    }

    /// Unsubscribe from the server and release the user reference.
    /// After calling this, the subscription should not be used.
    pub fn deinit(self: *Subscription) void {
        self.nc.unsubscribe(self);
        self.release(); // Release user reference
    }

    fn destroy(self: *Subscription) void {
        // Close the queue before canceling the handler task group: cancelation
        // is delivered only once, and a user callback may swallow it (e.g. an
        // `io.sleep(...) catch {}`), in which case the handler loop keeps
        // polling the queue and only a `Closed` result makes it exit.
        self.messages.close();

        // Cancel handler task group and wait for completion
        self.handler_group.cancel(self.nc.io);

        self.nc.allocator.free(self.subject);

        if (self.queue) |queue_group| {
            self.nc.allocator.free(queue_group);
        }

        // Clean up handler context if present
        if (self.handler) |handler| {
            handler.cleanup(self.nc.allocator);
        }

        // Clean up messages still pending in the closed queue
        while (self.messages.tryPop()) |msg| {
            msg.deinit();
        }
        self.messages.deinit();

        self.nc.allocator.destroy(self);
    }

    /// Wake subscription receivers and drain waiters when their owning
    /// connection becomes terminal. The subscription object remains valid
    /// until its normal reference-counted destruction.
    pub fn closeFromConnection(self: *Subscription) void {
        self.connection_closed.store(true, .release);
        self.messages.close();
        self.drain_complete.set(self.nc.io);
    }

    /// Set the pending limits for this subscription (0 = unlimited).
    /// Messages arriving while a limit is exceeded are dropped and counted
    /// in `dropped()`, and the connection's `slow_consumer_cb` is notified
    /// once per slow-consumer episode.
    pub fn setPendingLimits(self: *Subscription, msgs_limit: u32, bytes_limit: u64) void {
        self.pending_msgs_limit.store(msgs_limit, .release);
        self.pending_bytes_limit.store(bytes_limit, .release);
    }

    /// Number of messages dropped for this subscription because a pending
    /// limit was exceeded.
    pub fn dropped(self: *const Subscription) u64 {
        return self.dropped_msgs.load(.acquire);
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
        self.nc.unsubscribeInternal(self.sid, null) catch |err| {
            if (err == error.Canceled) {
                // This is a void-returning path; re-arm the cancelation so
                // the caller's next cancelation point still observes it.
                self.nc.io.recancel();
            } else {
                // Even with this failing, once we set draining to true,
                // messages will be dropped, so it's OK to continue
                log.err("Failed to send UNSUB for sid {d}: {}", .{ self.sid, err });
            }
        };
    }

    pub fn isDraining(self: *Subscription) bool {
        return self.draining.load(.acquire);
    }

    pub fn isDrainComplete(self: *Subscription) bool {
        return self.draining.load(.acquire) and self.drain_succeeded.load(.acquire);
    }

    /// Wait for the subscription drain to finish.
    pub fn waitForDrainCompletion(self: *Subscription, timeout: Io.Timeout) !void {
        if (self.drain_succeeded.load(.acquire)) return;
        if (self.connection_closed.load(.acquire)) return error.ConnectionClosed;
        if (!self.draining.load(.acquire)) {
            return error.NotDraining;
        }

        if (timeout == .none) {
            try self.drain_complete.wait(self.nc.io);
        } else {
            try self.drain_complete.waitTimeout(self.nc.io, timeout);
        }

        if (!self.drain_succeeded.load(.acquire)) {
            return error.ConnectionClosed;
        }
    }

    pub const AutoUnsubscribeError = error{
        MaxAlreadyReached,
        InvalidMax,
        SubscriptionClosed,
        SendFailed,
    };

    /// Issues an automatic unsubscribe that is processed by the server when 'max' messages have been received.
    /// This can be useful when sending a request to an unknown number of subscribers.
    pub fn autoUnsubscribe(self: *Subscription, max: u64) (Io.Cancelable || AutoUnsubscribeError)!void {
        if (max == 0) return AutoUnsubscribeError.InvalidMax;

        const current_delivered = self.delivered_msgs.load(.acquire);
        if (current_delivered >= max) {
            return AutoUnsubscribeError.MaxAlreadyReached;
        }

        // Send protocol message to server first
        self.nc.unsubscribeInternal(self.sid, max) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return AutoUnsubscribeError.SendFailed;
        };

        // Only set the limit after successfully sending UNSUB
        self.max_msgs.store(max, .release);
    }

    pub const ReceiveError = Io.Cancelable || error{
        ConnectionClosed,
        /// One or more messages were dropped for this subscription because a
        /// pending limit was exceeded. Reports the start of a slow-consumer
        /// episode; multiple episodes may coalesce if the error is not
        /// consumed between them. See `dropped()` for exact loss accounting.
        SlowConsumer,
    };

    pub const ReceiveTimeoutError = ReceiveError || Io.Timeout.Error;

    /// Wait indefinitely for the next message.
    pub fn nextMsg(self: *Subscription) ReceiveError!*Message {
        return self.nextMsgInternal(.none) catch |err| switch (err) {
            error.Timeout => unreachable,
            else => |other| return other,
        };
    }

    /// Wait up to `timeout` for the next message.
    /// Returns `error.ConnectionClosed` once the queue is closed and drained.
    pub fn nextMsgTimeout(self: *Subscription, timeout: Io.Timeout) ReceiveTimeoutError!*Message {
        return self.nextMsgInternal(timeout);
    }

    fn nextMsgInternal(self: *Subscription, timeout: Io.Timeout) ReceiveTimeoutError!*Message {
        // Report dropped messages in-band so synchronous consumers learn
        // about the loss even without a slow_consumer_cb.
        if (self.sc_error_pending.swap(false, .acq_rel)) {
            return error.SlowConsumer;
        }

        // Check if subscription has reached autounsubscribe limit
        const max = self.max_msgs.load(.acquire);
        if (max > 0 and self.delivered_msgs.load(.acquire) >= max) {
            return switch (timeout) {
                .none => error.ConnectionClosed,
                .duration, .deadline => error.Timeout,
            };
        }

        const msg = self.messages.pop(timeout) catch |err| switch (err) {
            error.Timeout => return error.Timeout,
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.Canceled,
        };

        self.messageConsumed(msg);
        return msg;
    }

    /// Return the next immediately available message, or null.
    /// This operation does not block and is not a cancelation point.
    pub fn tryNextMsg(self: *Subscription) ?*Message {
        if (self.reachedAutoUnsubscribeLimit()) return null;

        const msg = self.messages.tryPop() orelse return null;
        self.messageConsumed(msg);
        return msg;
    }

    /// Wait indefinitely for at least one message, then drain up to
    /// `output.len` currently available messages into `output`.
    pub fn nextMsgBatch(self: *Subscription, output: []*Message) ReceiveError!usize {
        if (output.len == 0) return 0;

        output[0] = try self.nextMsg();
        return self.drainBatch(output, 1);
    }

    /// Wait up to `timeout` for at least one message, then drain up to
    /// `output.len` currently available messages into `output`.
    pub fn nextMsgBatchTimeout(self: *Subscription, output: []*Message, timeout: Io.Timeout) ReceiveTimeoutError!usize {
        if (output.len == 0) return 0;

        output[0] = try self.nextMsgTimeout(timeout);
        return self.drainBatch(output, 1);
    }

    fn drainBatch(self: *Subscription, output: []*Message, start: usize) usize {
        var count = start;
        while (count < output.len) : (count += 1) {
            output[count] = self.tryNextMsg() orelse break;
        }

        return count;
    }

    /// Drain up to `output.len` immediately available messages into `output`.
    /// Returns zero when no messages are available.
    pub fn tryNextMsgBatch(self: *Subscription, output: []*Message) usize {
        var count: usize = 0;
        while (count < output.len) : (count += 1) {
            output[count] = self.tryNextMsg() orelse break;
        }
        return count;
    }

    fn reachedAutoUnsubscribeLimit(self: *Subscription) bool {
        const max = self.max_msgs.load(.acquire);
        return max > 0 and self.delivered_msgs.load(.acquire) >= max;
    }

    fn messageConsumed(self: *Subscription, msg: *Message) void {
        // Increment delivered counter with proper memory ordering
        const delivered = self.delivered_msgs.fetchAdd(1, .acq_rel) + 1;

        // Decrement pending counters when message is consumed
        decrementPending(self, msg.data.len);

        // Check if we've reached the autounsubscribe limit
        const max_limit = self.max_msgs.load(.acquire);
        if (max_limit > 0 and delivered >= max_limit) {
            // Remove subscription from connection
            self.nc.removeSubscriptionInternal(self.sid);
        }
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
        if (sub.connection_closed.load(.acquire)) return;
        log.debug("Subscription {d} drain completed", .{sub.sid});
        sub.drain_succeeded.store(true, .release);
        sub.drain_complete.set(sub.nc.io);
        sub.nc.notifySubscriptionDrainComplete();
    }
}
