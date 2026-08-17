const std = @import("std");
const nats = @import("nats");
const xsync = @import("xsync");
const utils = @import("utils.zig");

const log = std.log.default;
const testing = std.testing;

var tracker: CallbackTracker = .{};

const CallbackTracker = struct {
    disconnected_called: u32 = 0,
    reconnected_called: u32 = 0,
    closed_called: u32 = 0,
    error_called: u32 = 0,
    mutex: xsync.Mutex = .init,
    cond: xsync.Condition = .init,

    fn reset(self: *@This()) void {
        self.disconnected_called = 0;
        self.reconnected_called = 0;
        self.closed_called = 0;
        self.error_called = 0;
    }

    fn disconnectedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(std.testing.io);
        defer self.mutex.unlock(std.testing.io);
        self.disconnected_called += 1;
        self.cond.signal(std.testing.io);
    }

    fn reconnectedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(std.testing.io);
        defer self.mutex.unlock(std.testing.io);
        self.reconnected_called += 1;
        self.cond.signal(std.testing.io);
    }

    fn closedCallback(_: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lockUncancelable(std.testing.io);
        defer self.mutex.unlock(std.testing.io);
        self.closed_called += 1;
        self.cond.signal(std.testing.io);
    }

    fn errorCallback(_: *nats.Connection, msg: []const u8) void {
        var self = &tracker;
        self.mutex.lockUncancelable(std.testing.io);
        defer self.mutex.unlock(std.testing.io);
        self.error_called += 1;
        self.cond.signal(std.testing.io);
        _ = msg;
    }

    fn waitForDisconnected(self: *@This(), io: std.Io, timeout: std.Io.Duration) !void {
        try self.mutex.lock(std.testing.io);
        defer self.mutex.unlock(std.testing.io);

        const start = std.Io.Timestamp.now(io, .awake);
        while (self.disconnected_called == 0) {
            if (start.untilNow(io, .awake).nanoseconds >= timeout.nanoseconds) {
                return error.DisconnectTimeout;
            }
            self.cond.waitTimeout(std.testing.io, &self.mutex, utils.ioTimeout(.fromMilliseconds(100))) catch {};
        }
    }

    fn waitForReconnected(self: *@This(), io: std.Io, timeout: std.Io.Duration) !void {
        try self.mutex.lock(std.testing.io);
        defer self.mutex.unlock(std.testing.io);

        const start = std.Io.Timestamp.now(io, .awake);
        while (self.reconnected_called == 0) {
            if (start.untilNow(io, .awake).nanoseconds >= timeout.nanoseconds) {
                return error.ReconnectionTimeout;
            }
            self.cond.waitTimeout(std.testing.io, &self.mutex, utils.ioTimeout(.fromMilliseconds(100))) catch {};
        }
    }
};

test "basic reconnection when server stops" {
    const io = std.testing.io;

    tracker.reset();

    const nc = try utils.createConnection(io, .node1, .{
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
    try tracker.waitForDisconnected(io, .fromSeconds(10));
    try tracker.waitForReconnected(io, .fromSeconds(10));

    // Verify connection works after reconnection
    log.debug("Publishing after reconnection", .{});
    try nc.publish("test.after", "hello after reconnection");

    // Verify both disconnected and reconnected callbacks were called
    tracker.mutex.lockUncancelable(std.testing.io);
    defer tracker.mutex.unlock(std.testing.io);
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
}

test "manual reconnection with nc.reconnect()" {
    const io = std.testing.io;

    tracker.reset();

    const nc = try utils.createConnection(io, .node1, .{
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
        const msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        defer msg.deinit();
        try testing.expectEqualStrings("before reconnect", msg.data);
    }

    // Trigger manual reconnection
    log.debug("Triggering manual reconnection", .{});
    try nc.reconnect();

    // Wait for reconnection to complete
    try tracker.waitForReconnected(io, .fromSeconds(5));
    log.debug("Manual reconnection completed", .{});

    // Verify callbacks were called
    tracker.mutex.lockUncancelable(std.testing.io);
    try testing.expectEqual(@as(u32, 1), tracker.disconnected_called);
    try testing.expectEqual(@as(u32, 1), tracker.reconnected_called);
    tracker.mutex.unlock(std.testing.io);

    // Verify connection is working after reconnection
    log.debug("Publishing test message after manual reconnection", .{});
    try nc.publish("test.manual", "after reconnect");
    try nc.flush();

    // Verify subscription survived reconnection
    {
        const msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        defer msg.deinit();
        try testing.expectEqualStrings("after reconnect", msg.data);
    }
}

const PendingFlushTracker = struct {
    var disconnected: std.atomic.Value(bool) = .init(false);
    var reconnected: std.atomic.Value(bool) = .init(false);

    fn reset() void {
        disconnected.store(false, .release);
        reconnected.store(false, .release);
    }

    fn disconnectedCallback(_: *nats.Connection) void {
        disconnected.store(true, .release);
    }

    fn reconnectedCallback(nc: *nats.Connection) void {
        // The callback must observe a fully restored connection: this
        // publish has to arrive after the restored subscription and after
        // the publishes buffered while the server was down.
        nc.publish("test.pending.flush", "from-callback") catch |err| {
            log.err("Publish from reconnected callback failed: {}", .{err});
        };
        reconnected.store(true, .release);
    }

    fn waitFor(io: std.Io, flag: *std.atomic.Value(bool), timeout: std.Io.Duration) !void {
        const start = std.Io.Timestamp.now(io, .awake);
        while (!flag.load(.acquire)) {
            if (start.untilNow(io, .awake).nanoseconds >= timeout.nanoseconds) {
                return error.CallbackTimeout;
            }
            try io.sleep(.fromMilliseconds(50), .awake);
        }
    }
};

test "publishes buffered during reconnect are flushed after restoration" {
    const io = std.testing.io;

    PendingFlushTracker.reset();

    // The standalone token server has no cluster peers, so the client can
    // only ever reconnect to this one server; that makes the buffered
    // publish path deterministic.
    const nc = try utils.createConnection(io, .token_auth, .{
        .token = "test_token_123",
        .reconnect = .{
            .allow_reconnect = true,
            .reconnect_wait = .fromMilliseconds(100),
            .max_reconnect = 300,
        },
        .callbacks = .{
            .disconnected_cb = PendingFlushTracker.disconnectedCallback,
            .reconnected_cb = PendingFlushTracker.reconnectedCallback,
        },
    });
    defer utils.closeConnection(nc);

    const sub = try nc.subscribeSync("test.pending.flush");
    defer sub.deinit();

    log.debug("Stopping nats-token-auth", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "stop", "nats-token-auth" });
    try PendingFlushTracker.waitFor(io, &PendingFlushTracker.disconnected, .fromSeconds(10));

    // These are buffered in pending_buffer while the server is down.
    try nc.publish("test.pending.flush", "buffered 1");
    try nc.publish("test.pending.flush", "buffered 2");

    log.debug("Starting nats-token-auth", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "start", "nats-token-auth" });
    try PendingFlushTracker.waitFor(io, &PendingFlushTracker.reconnected, .fromSeconds(30));

    // All three messages must arrive, in order: the buffered publishes
    // first (flushed after the subscription was restored), then the
    // publish made inside the reconnected callback.
    const expected = [_][]const u8{ "buffered 1", "buffered 2", "from-callback" };
    for (expected) |want| {
        const msg = try sub.nextMsgTimeout(utils.ioTimeout(.fromSeconds(5)));
        defer msg.deinit();
        try testing.expectEqualStrings(want, msg.data);
    }
}

test "reconnect() errors when disabled" {
    const io = std.testing.io;

    const nc = try utils.createConnection(io, .node1, .{
        .reconnect = .{
            .allow_reconnect = false,
        },
    });
    defer utils.closeConnection(nc);

    // Should return error when reconnection is disabled
    try testing.expectError(error.ReconnectDisabled, nc.reconnect());
}

test "reconnect() errors when connection closed" {
    const io = std.testing.io;

    const nc = try utils.createConnection(io, .node1, .{});
    defer utils.closeConnection(nc);

    nc.close();

    try testing.expectError(error.ConnectionClosed, nc.reconnect());
}
