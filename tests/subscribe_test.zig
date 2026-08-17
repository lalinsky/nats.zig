const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");
const Message = nats.Message;

const log = std.log.default;

test "subscribeSync smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt.io());
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try sub.nextMsg(.fromSeconds(1));
    defer msg.deinit();

    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "queueSubscribeSync smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt.io());
    defer utils.closeConnection(conn);

    const sub = try conn.queueSubscribeSync("test", "workers");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try sub.nextMsg(.fromSeconds(1));
    defer msg.deinit();

    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

const MessageCollector = struct {
    result: ?*Message = null,
    mutex: zio.Mutex = .{},
    cond: zio.Condition = .{},

    pub fn deinit(self: *@This()) void {
        if (self.result) |msg| msg.deinit();
    }

    pub fn processMsg(msg: *Message, self: *@This()) !void {
        try self.mutex.lock();
        defer self.mutex.unlock();

        self.result = msg;
        self.cond.broadcast();
    }

    pub fn timedWait(self: *@This(), io: std.Io, timeout: std.Io.Duration) !*Message {
        try self.mutex.lock();
        defer self.mutex.unlock();

        const start = std.Io.Timestamp.now(io, .awake);
        while (self.result == null) {
            const elapsed = start.untilNow(io, .awake);
            if (elapsed.nanoseconds >= timeout.nanoseconds) {
                return error.Timeout;
            }
            try self.cond.timedWait(&self.mutex, .fromNanoseconds(@intCast(timeout.nanoseconds - elapsed.nanoseconds)));
        }
        return self.result.?;
    }
};

test "subscribe smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt.io());
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
    defer collector.deinit();

    const sub = try conn.subscribe("test", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(rt.io(), .fromSeconds(1));
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "queueSubscribe smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt.io());
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
    defer collector.deinit();

    const sub = try conn.queueSubscribe("test", "workers", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(rt.io(), .fromSeconds(1));
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}
