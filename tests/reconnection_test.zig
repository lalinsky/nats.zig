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

    fn disconnectedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.disconnected_called += 1;
        self.cond.signal();
    }

    fn reconnectedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.reconnected_called += 1;
        self.cond.signal();
    }

    fn closedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.closed_called += 1;
        self.cond.signal();
    }

    fn errorCallback(_: *nats.Connection, msg: []const u8) void {
        var self = &tracker;
        self.mutex.lockUncancelable();
        defer self.mutex.unlock();
        self.error_called += 1;
        self.cond.signal();
        _ = msg;
    }

    fn waitForDisconnected(self: *@This(), io: std.Io, timeout: std.Io.Duration) !void {
        try self.mutex.lock();
        defer self.mutex.unlock();

        const start = std.Io.Timestamp.now(io, .awake);
        while (self.disconnected_called == 0) {
            if (start.untilNow(io, .awake).nanoseconds >= timeout.nanoseconds) {
                return error.DisconnectTimeout;
            }
            self.cond.timedWait(&self.mutex, .fromMilliseconds(100)) catch {};
        }
    }

    fn waitForReconnected(self: *@This(), io: std.Io, timeout: std.Io.Duration) !void {
        try self.mutex.lock();
        defer self.mutex.unlock();

        const start = std.Io.Timestamp.now(io, .awake);
        while (self.reconnected_called == 0) {
            if (start.untilNow(io, .awake).nanoseconds >= timeout.nanoseconds) {
                return error.ReconnectionTimeout;
            }
            self.cond.timedWait(&self.mutex, .fromMilliseconds(100)) catch {};
        }
    }
};

test "basic reconnection when server stops" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    tracker.reset();

    const nc = try utils.createConnection(rt.io(), .node1, .{
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

    // Wait for disconnect and reconnection callbacks
    try tracker.waitForDisconnected(rt.io(), .fromSeconds(10));
    try tracker.waitForReconnected(rt.io(), .fromSeconds(10));

    // Verify connection works after reconnection
    log.debug("Publishing after reconnection", .{});
    try nc.publish("test.after", "hello after reconnection");

    // Verify both disconnected and reconnected callbacks were called
    tracker.mutex.lockUncancelable();
    defer tracker.mutex.unlock();
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
}

test "manual reconnection with nc.reconnect()" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    tracker.reset();

    const nc = try utils.createConnection(rt.io(), .node1, .{
        .trace = true,
        .reconnect = .{
            .allow_reconnect = true,
            .reconnect_wait = .fromMilliseconds(100),
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
        const msg = try sub.nextMsg(.fromSeconds(1));
        defer msg.deinit();
        try testing.expectEqualStrings("before reconnect", msg.data);
    }

    // Trigger manual reconnection
    log.debug("Triggering manual reconnection", .{});
    try nc.reconnect();

    // Wait for reconnection to complete
    try tracker.waitForReconnected(rt.io(), .fromSeconds(5));
    log.debug("Manual reconnection completed", .{});

    // Verify callbacks were called
    tracker.mutex.lockUncancelable();
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
    tracker.mutex.unlock();

    // Verify connection is working after reconnection
    log.debug("Publishing test message after manual reconnection", .{});
    try nc.publish("test.manual", "after reconnect");
    try nc.flush();

    // Verify subscription survived reconnection
    {
        const msg = try sub.nextMsg(.fromSeconds(1));
        defer msg.deinit();
        try testing.expectEqualStrings("after reconnect", msg.data);
    }
}

test "reconnect() errors when disabled" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const nc = try utils.createConnection(rt.io(), .node1, .{
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

    const nc = try utils.createConnection(rt.io(), .node1, .{});
    defer utils.closeConnection(nc);

    nc.close();

    try testing.expectError(error.ConnectionClosed, nc.reconnect());
}
