const std = @import("std");
const nats = @import("nats");
const xsync = @import("xsync");
const utils = @import("utils.zig");
const Message = nats.Message;

const log = std.log.default;

test "subscribeSync smoke test" {
    var conn = try utils.createDefaultConnection(std.testing.io);
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
    var conn = try utils.createDefaultConnection(std.testing.io);
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
    mutex: xsync.Mutex = .init,
    cond: xsync.Condition = .init,

    pub fn deinit(self: *@This()) void {
        if (self.result) |msg| msg.deinit();
    }

    pub fn processMsg(msg: *Message, self: *@This()) !void {
        const io = std.testing.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        self.result = msg;
        self.cond.broadcast(io);
    }

    pub fn timedWait(self: *@This(), io: std.Io, timeout: std.Io.Duration) !*Message {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const deadline = (std.Io.Timeout{ .duration = .{ .raw = timeout, .clock = .awake } }).toDeadline(io);
        while (self.result == null) {
            self.cond.waitTimeout(io, &self.mutex, deadline) catch |err| switch (err) {
                error.Timeout => return error.Timeout,
                error.Canceled => return error.Canceled,
            };
        }
        return self.result.?;
    }
};

test "subscribe smoke test" {
    var conn = try utils.createDefaultConnection(std.testing.io);
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
    defer collector.deinit();

    const sub = try conn.subscribe("test", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(std.testing.io, .fromSeconds(1));
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "queueSubscribe smoke test" {
    var conn = try utils.createDefaultConnection(std.testing.io);
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
    defer collector.deinit();

    const sub = try conn.queueSubscribe("test", "workers", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(std.testing.io, .fromSeconds(1));
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}
