const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");
const Message = nats.Message;

const log = std.log.default;

test "subscribeSync smoke test" {
    var conn = try utils.createDefaultConnection();
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
    var conn = try utils.createDefaultConnection();
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
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},

    pub fn deinit(self: *@This()) void {
        if (self.result) |msg| msg.deinit();
    }

    pub fn processMsg(msg: *Message, self: *@This()) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.result = msg;
        self.cond.broadcast();
    }

    pub fn timedWait(self: *@This(), timeout_ms: u64) !*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        const timeout_ns = timeout_ms * std.time.ns_per_ms;
        var timer = std.time.Timer.start() catch unreachable;
        while (self.result == null) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }
            try self.cond.timedWait(&self.mutex, timeout_ns - elapsed_ns);
        }
        return self.result.?;
    }
};

test "subscribe smoke test" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
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
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var collector: MessageCollector = .{};
    defer collector.deinit();

    const sub = try conn.queueSubscribe("test", "workers", MessageCollector.processMsg, .{&collector});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const msg = try collector.timedWait(1000);
    try std.testing.expectEqualStrings("test", msg.subject);
    try std.testing.expectEqualStrings("Hello world!", msg.data);
}

test "subscription drain functionality" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test.drain");
    defer sub.deinit();

    // Send a message before draining
    try conn.publish("test.drain", "message 1");
    try conn.flush();

    // Receive the first message to ensure subscription is working
    const msg1 = try sub.nextMsg(1000);
    defer msg1.deinit();
    try std.testing.expectEqualStrings("message 1", msg1.data);

    // Start draining the subscription
    try std.testing.expect(!sub.isDraining());
    sub.drain(5000);
    try std.testing.expect(sub.isDraining());

    // Send messages after drain - these should be dropped
    try conn.publish("test.drain", "message 2");
    try conn.publish("test.drain", "message 3");
    try conn.flush();

    // Wait a bit to ensure messages would have arrived if not draining
    std.time.sleep(100 * std.time.ns_per_ms);

    // Try to receive - should timeout since new messages are dropped during drain
    const result = sub.nextMsg(500);
    try std.testing.expectError(error.Timeout, result);
}

const DrainMessageCounter = struct {
    count: u32 = 0,
    mutex: std.Thread.Mutex = .{},

    pub fn processMsg(msg: *Message, self: *@This()) !void {
        defer msg.deinit();

        self.mutex.lock();
        defer self.mutex.unlock();

        self.count += 1;
        log.debug("DrainMessageCounter received message #{d}: {s}", .{ self.count, msg.data });
    }

    pub fn getCount(self: *@This()) u32 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.count;
    }
};

test "async subscription drain functionality" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var counter: DrainMessageCounter = .{};

    const sub = try conn.subscribe("test.async.drain", DrainMessageCounter.processMsg, .{&counter});
    defer sub.deinit();

    // Send a message before draining
    try conn.publish("test.async.drain", "async message 1");
    try conn.flush();

    // Wait for the first message to be processed
    var attempts: u32 = 0;
    while (counter.getCount() == 0 and attempts < 100) {
        std.time.sleep(10 * std.time.ns_per_ms);
        attempts += 1;
    }
    try std.testing.expect(counter.getCount() == 1);

    // Start draining
    try std.testing.expect(!sub.isDraining());
    sub.drain(5000);
    try std.testing.expect(sub.isDraining());

    // Send messages after drain - these should be dropped
    try conn.publish("test.async.drain", "async message 2");
    try conn.publish("test.async.drain", "async message 3");
    try conn.flush();

    // Wait to ensure messages would have arrived if not draining
    std.time.sleep(200 * std.time.ns_per_ms);

    // Count should still be 1 since new messages are dropped during drain
    try std.testing.expect(counter.getCount() == 1);
}
