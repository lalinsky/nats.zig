const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;
const testing = std.testing;

var tracker: CallbackTracker = .{};

const CallbackTracker = struct {
    disconnected_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reconnected_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closed_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    error_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn reset(self: *@This()) void {
        self.disconnected_called.store(false, .release);
        self.reconnected_called.store(false, .release);
        self.closed_called.store(false, .release);
        self.error_called.store(false, .release);
    }

    fn disconnectedCallback(conn: *nats.Connection) void {
        _ = conn;
        tracker.disconnected_called.store(true, .release);
    }

    fn reconnectedCallback(conn: *nats.Connection) void {
        _ = conn;
        tracker.reconnected_called.store(true, .release);
    }

    fn closedCallback(conn: *nats.Connection) void {
        _ = conn;
        tracker.closed_called.store(true, .release);
    }

    fn errorCallback(conn: *nats.Connection, msg: []const u8) void {
        _ = conn;
        _ = msg;
        tracker.error_called.store(true, .release);
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
    try nc.flush();

    log.debug("Restarting nats-1", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Verify connection works after reconnection
    log.debug("Trying to publish after reconnection", .{});
    try nc.publish("test.after", "hello after reconnection");
    try nc.flush();

    try testing.expectEqual(true, tracker.reconnected_called.load(.acquire));
}
