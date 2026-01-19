const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;
const testing = std.testing;

var tracker: CallbackTracker = .{};

const CallbackTracker = struct {
    disconnected_called: u32 = 0,
    reconnected_called: u32 = 0,
    closed_called: u32 = 0,
    error_called: u32 = 0,
    mutex: zio.Mutex = .{},
    cond: zio.Condition = .{},

    fn reset(self: *@This()) void {
        self.disconnected_called = 0;
        self.reconnected_called = 0;
        self.closed_called = 0;
        self.error_called = 0;
    }

    fn disconnectedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(conn.rt);
        defer self.mutex.unlock(conn.rt);
        self.disconnected_called += 1;
        self.cond.signal(conn.rt);
    }

    fn reconnectedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(conn.rt);
        defer self.mutex.unlock(conn.rt);
        self.reconnected_called += 1;
        self.cond.signal(conn.rt);
    }

    fn closedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(conn.rt);
        defer self.mutex.unlock(conn.rt);
        self.closed_called += 1;
        self.cond.signal(conn.rt);
    }

    fn errorCallback(conn: *nats.Connection, msg: []const u8) void {
        var self = &tracker;
        self.mutex.lockUncancelable(conn.rt);
        defer self.mutex.unlock(conn.rt);
        self.error_called += 1;
        self.cond.signal(conn.rt);
        _ = msg;
    }

    fn timedWait(self: *@This(), rt: *zio.Runtime, timeout_ms: u32) !void {
        try self.mutex.lock(rt);
        defer self.mutex.unlock(rt);
        return self.cond.timedWait(rt, &self.mutex, .fromMilliseconds(timeout_ms));
    }
};

test "basic reconnection when server stops" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    tracker.reset();

    const nc = try utils.createConnection(rt, .node1, .{
        .trace = true,
        .reconnect = .{
            .allow_reconnect = true,
        },
        .callbacks = .{
            .disconnected_cb = CallbackTracker.disconnectedCallback,
            .reconnected_cb = CallbackTracker.reconnectedCallback,
            .closed_cb = CallbackTracker.closedCallback,
            .error_cb = CallbackTracker.errorCallback,
        },
    });
    defer utils.closeConnection(nc);

    // Publish a test message to ensure connection works
    log.debug("Publishing test message before", .{});
    try nc.publish("test.before", "hello before");

    log.debug("Restarting nats-1", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Wait for reconnection before publishing
    var timer = try std.time.Timer.start();
    while (!nc.isConnected()) {
        const elapsed = timer.read();
        if (elapsed >= 10000 * std.time.ns_per_ms) {
            return error.StillNotConnected;
        }

        try rt.sleep(.fromMilliseconds(10));
    }

    // Verify connection works after reconnection
    log.debug("Publishing after reconnection", .{});
    try nc.publish("test.after", "hello after reconnection");

    // Verify both disconnected and reconnected callbacks were called
    tracker.mutex.lockUncancelable(rt);
    defer tracker.mutex.unlock(rt);
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
}

test "manual reconnection with nc.reconnect()" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    tracker.reset();

    const nc = try utils.createConnection(rt, .node1, .{
        .trace = true,
        .reconnect = .{
            .allow_reconnect = true,
            .reconnect_wait_ms = 100,
        },
        .callbacks = .{
            .disconnected_cb = CallbackTracker.disconnectedCallback,
            .reconnected_cb = CallbackTracker.reconnectedCallback,
            .closed_cb = CallbackTracker.closedCallback,
            .error_cb = CallbackTracker.errorCallback,
        },
    });
    defer utils.closeConnection(nc);

    // Create a subscription to verify it survives reconnection
    const sub = try nc.subscribeSync("test.manual");
    defer sub.deinit();

    // Ensure initial connection is working
    log.debug("Publishing test message before manual reconnection", .{});
    try nc.publish("test.manual", "before reconnect");
    try nc.flush();

    // Verify message was received
    {
        const msg = try sub.nextMsg(1000);
        defer msg.deinit();
        try testing.expectEqualStrings("before reconnect", msg.data);
    }

    // Trigger manual reconnection
    log.debug("Triggering manual reconnection", .{});
    try nc.reconnect();

    // Wait for reconnection to complete
    var timer = try std.time.Timer.start();
    while (tracker.reconnected_called == 0) {
        if (timer.read() >= 5000 * std.time.ns_per_ms) {
            return error.ReconnectionTimeout;
        }
        try tracker.timedWait(rt, 100);
    }

    log.debug("Manual reconnection completed", .{});

    // Verify callbacks were called
    tracker.mutex.lockUncancelable(rt);
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
    tracker.mutex.unlock(rt);

    // Verify connection is working after reconnection
    log.debug("Publishing test message after manual reconnection", .{});
    try nc.publish("test.manual", "after reconnect");
    try nc.flush();

    // Verify subscription survived reconnection
    {
        const msg = try sub.nextMsg(1000);
        defer msg.deinit();
        try testing.expectEqualStrings("after reconnect", msg.data);
    }
}

test "reconnect() errors when disabled" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const nc = try utils.createConnection(rt, .node1, .{
        .reconnect = .{
            .allow_reconnect = false,
        },
    });
    defer utils.closeConnection(nc);

    // Should return error when reconnection is disabled
    try testing.expectError(error.ReconnectDisabled, nc.reconnect());
}

test "reconnect() errors when connection closed" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const nc = try utils.createConnection(rt, .node1, .{});
    defer utils.closeConnection(nc);

    nc.close();

    try testing.expectError(error.ConnectionClosed, nc.reconnect());
}
