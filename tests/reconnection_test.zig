const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;
const testing = std.testing;

var tracker: CallbackTracker = .{};

const CallbackTracker = struct {
    disconnected_called: u32 = 0,
    reconnected_called: u32 = 0,
    closed_called: u32 = 0,
    error_called: u32 = 0,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},

    fn reset(self: *@This()) void {
        self.disconnected_called = 0;
        self.reconnected_called = 0;
        self.closed_called = 0;
        self.error_called = 0;
    }

    fn disconnectedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lock();
        defer self.mutex.unlock();
        self.disconnected_called += 1;
        self.cond.signal();
        _ = conn;
    }

    fn reconnectedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lock();
        defer self.mutex.unlock();
        self.reconnected_called += 1;
        self.cond.signal();
        _ = conn;
    }

    fn closedCallback(conn: *nats.Connection) void {
        var self = &tracker;
        self.mutex.lock();
        defer self.mutex.unlock();
        self.closed_called += 1;
        self.cond.signal();
        _ = conn;
    }

    fn errorCallback(conn: *nats.Connection, msg: []const u8) void {
        var self = &tracker;
        self.mutex.lock();
        defer self.mutex.unlock();
        self.error_called += 1;
        self.cond.signal();
        _ = conn;
        _ = msg;
    }

    fn timedWait(self: *@This(), timeout_ms: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.cond.timedWait(self.mutex, timeout_ms * std.time.ns_per_ms);
    }
};

test "basic reconnection when server stops" {
    tracker.reset();

    const nc = try utils.createConnection(.node1, .{
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

        std.time.sleep(10 * std.time.ns_per_ms);
    }

    // Verify connection works after reconnection
    log.debug("Publishing after reconnection", .{});
    try nc.publish("test.after", "hello after reconnection");

    try testing.expectEqual(1, tracker.reconnected_called);
}
