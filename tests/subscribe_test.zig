const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");
const Message = nats.Message;

const log = std.log.default;

test "subscribeSync smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try sub.nextMsg(1000);
    defer msg.deinit();

    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "queueSubscribeSync smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    const sub = try conn.queueSubscribeSync("test", "workers");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try sub.nextMsg(1000);
    defer msg.deinit();

    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

const MessageCollector = struct {
    result: ?*Message = null,
    mutex: zio.Mutex = .{},
    cond: zio.Condition = .{},
    rt: *zio.Runtime,

    pub fn deinit(self: *@This()) void {
        if (self.result) |msg| msg.deinit();
    }

    pub fn processMsg(msg: *Message, self: *@This()) !void {
        try self.mutex.lock(self.rt);
        defer self.mutex.unlock(self.rt);

        self.result = msg;
        self.cond.broadcast(self.rt);
    }

    pub fn timedWait(self: *@This(), timeout_ms: u64) !*Message {
        try self.mutex.lock(self.rt);
        defer self.mutex.unlock(self.rt);

        const timeout_ns = timeout_ms * std.time.ns_per_ms;
        var timer = std.time.Timer.start() catch unreachable;
        while (self.result == null) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }
            try self.cond.timedWait(self.rt, &self.mutex, .fromNanoseconds(timeout_ns - elapsed_ns));
        }
        return self.result.?;
    }
};

test "subscribe smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{ .rt = rt };
    defer collector.deinit();

    const sub = try conn.subscribe("test", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(1000);
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "queueSubscribe smoke test" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{ .rt = rt };
    defer collector.deinit();

    const sub = try conn.queueSubscribe("test", "workers", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(1000);
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}
