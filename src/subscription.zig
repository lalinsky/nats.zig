const std = @import("std");
const Allocator = std.mem.Allocator;
const Message = @import("message.zig").Message;
const RefCounter = @import("ref_counter.zig").RefCounter;

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
    messages: std.fifo.LinearFifo(*Message, .Dynamic),
    mutex: std.Thread.Mutex = .{},
    allocator: Allocator,

    // Reference counting for safe cleanup
    ref_counter: RefCounter(u32) = RefCounter(u32).init(),

    // Callback support
    handler: ?MsgHandler = null,

    pub fn initSync(allocator: Allocator, sid: u64, subject: []const u8) !*Subscription {
        const sub = try allocator.create(Subscription);
        sub.* = Subscription{
            .sid = sid,
            .subject = try allocator.dupe(u8, subject),
            .messages = std.fifo.LinearFifo(*Message, .Dynamic).init(allocator),
            .allocator = allocator,
            .handler = null,
        };
        return sub;
    }

    pub fn initAsync(allocator: Allocator, sid: u64, subject: []const u8, handler: MsgHandler) !*Subscription {
        const sub = try allocator.create(Subscription);
        sub.* = Subscription{
            .sid = sid,
            .subject = try allocator.dupe(u8, subject),
            .messages = std.fifo.LinearFifo(*Message, .Dynamic).init(allocator),
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

        // Clean up pending messages
        while (self.messages.readItem()) |msg| {
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
        if (timeout_ms == 0) {
            // Non-blocking mode
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.messages.readItem();
        }

        // Blocking mode with timeout
        const timeout_ns = timeout_ms * std.time.ns_per_ms;
        const deadline = std.time.nanoTimestamp() + @as(i128, timeout_ns);

        while (std.time.nanoTimestamp() < deadline) {
            self.mutex.lock();
            const msg = self.messages.readItem();
            self.mutex.unlock();

            if (msg) |m| {
                return m;
            }
            // Sleep for 1ms before retrying
            std.time.sleep(1_000_000);
        }
        return null;
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
