const std = @import("std");
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const RefCounter = @import("ref_counter.zig").RefCounter;
const ConcurrentQueue = @import("queue.zig").ConcurrentQueue;

const log = std.log.scoped(.subscription);

// Message handler storage for type-erased callback
pub const MsgHandler = struct {
    ptr: *anyopaque,
    callFn: *const fn (ptr: *anyopaque, msg: *Message) void,
    cleanupFn: *const fn (ptr: *anyopaque, allocator: Allocator) void,

    pub fn call(self: *const MsgHandler, msg: *Message) void {
        self.callFn(self.ptr, msg);
    }

    pub fn cleanup(self: *const MsgHandler, allocator: Allocator) void {
        self.cleanupFn(self.ptr, allocator);
    }
};

pub const Subscription = struct {
    sid: u64,
    subject: []const u8,
    messages: MessageQueue,
    allocator: Allocator,

    // Reference counting for safe cleanup
    ref_counter: RefCounter(u32) = RefCounter(u32).init(),

    // Callback support
    handler: ?MsgHandler = null,

    pub const MessageQueue = ConcurrentQueue(*Message, 1024); // 1K chunk size

    pub fn init(allocator: Allocator, sid: u64, subject: []const u8, handler: ?MsgHandler) !*Subscription {
        const sub = try allocator.create(Subscription);
        const subject_copy = try allocator.dupe(u8, subject);
        errdefer allocator.free(subject_copy);

        sub.* = Subscription{
            .sid = sid,
            .subject = subject_copy,
            .messages = MessageQueue.init(allocator, .{}),
            .allocator = allocator,
            .handler = handler,
        };
        return sub;
    }

    pub fn retain(self: *Subscription) void {
        self.ref_counter.incr();
    }

    pub fn release(self: *Subscription) void {
        if (self.ref_counter.decr()) {
            // Last reference - actually free the subscription
            self.deinitInternal();
        }
    }

    fn deinitInternal(self: *Subscription) void {
        self.allocator.free(self.subject);

        // Clean up handler context if present
        if (self.handler) |handler| {
            handler.cleanup(self.allocator);
        }

        // Close the queue to prevent new messages and clean up pending messages
        self.messages.close();
        while (self.messages.tryPop()) |msg| {
            msg.deinit();
        }
        self.messages.deinit();
        self.allocator.destroy(self);
    }

    /// Public API method - users call this to clean up subscriptions
    pub fn deinit(self: *Subscription) void {
        self.release();
    }

    pub fn nextMsg(self: *Subscription, timeout_ms: u64) ?*Message {
        return self.messages.pop(timeout_ms) catch null;
    }
};

// Helper function to create type-erased handlers
pub fn createMsgHandler(allocator: Allocator, comptime handlerFn: anytype, args: anytype) !MsgHandler {
    // Create a context struct to hold the function and arguments
    const Context = struct {
        args: @TypeOf(args),

        pub fn call(ctx: *anyopaque, msg: *Message) void {
            const self_ctx: *@This() = @ptrCast(@alignCast(ctx));
            @call(.auto, handlerFn, .{msg} ++ self_ctx.args);
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
