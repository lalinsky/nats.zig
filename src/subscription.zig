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

    // Drain state
    draining: bool = false,
    drain_timeout_ms: ?u64 = null,

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
        return sub;
    }

    pub fn deinit(self: *Subscription) void {
        self.nc.unsubscribe(self);
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

    pub fn nextMsg(self: *Subscription, timeout_ms: u64) error{Timeout}!*Message {
        return self.messages.pop(timeout_ms) catch |err| switch (err) {
            error.BufferFrozen => error.Timeout,
            error.QueueEmpty => error.Timeout,
            error.QueueClosed => error.Timeout, // TODO: this should be mapped to ConnectionClosed
        };
    }

    pub fn drain(self: *Subscription, timeout_ms: ?u64) void {
        // Set draining state to prevent new messages from being queued
        self.draining = true;
        self.drain_timeout_ms = timeout_ms;

        // Send UNSUB command to server and flush
        self.nc.unsubscribeInternal(self.sid);

        // For async subscriptions, the dispatcher will continue processing
        // messages until the queue is empty or timeout is reached.
        // For sync subscriptions, messages remain available via nextMsg().

        log.debug("Started draining subscription {d} on subject '{s}'", .{ self.sid, self.subject });
    }

    pub fn isDraining(self: *Subscription) bool {
        return self.draining;
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
