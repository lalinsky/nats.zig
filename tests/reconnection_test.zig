const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

var global_tracker: CallbackTracker = .{};

const CallbackTracker = struct {
    disconnected_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reconnected_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closed_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    error_called: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    error_message: ?[]const u8 = null,
    mutex: std.Thread.Mutex = .{},

    fn reset(self: *@This()) void {
        self.disconnected_called.store(false, .release);
        self.reconnected_called.store(false, .release);
        self.closed_called.store(false, .release);
        self.error_called.store(false, .release);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.error_message = null;
    }

    fn disconnectedCallback(conn: *nats.Connection) void {
        _ = conn;
        global_tracker.disconnected_called.store(true, .release);
    }

    fn reconnectedCallback(conn: *nats.Connection) void {
        _ = conn;
        global_tracker.reconnected_called.store(true, .release);
    }

    fn closedCallback(conn: *nats.Connection) void {
        _ = conn;
        global_tracker.closed_called.store(true, .release);
    }

    fn errorCallback(conn: *nats.Connection, msg: []const u8) void {
        _ = conn;
        global_tracker.mutex.lock();
        defer global_tracker.mutex.unlock();
        global_tracker.error_called.store(true, .release);
        global_tracker.error_message = msg;
    }

    fn waitForCallback(self: *@This(), callback_field: anytype, timeout_ms: u64) bool {
        _ = self;
        const start_time = std.time.milliTimestamp();
        while (!callback_field.load(.acquire)) {
            if (std.time.milliTimestamp() - start_time > timeout_ms) {
                return false;
            }
            std.time.sleep(10 * std.time.ns_per_ms);
        }
        return true;
    }
};

test "basic reconnection when server stops" {
    const nc = try utils.createConnection(.node1);
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
}

test "disconnected callback on connection loss" {
    global_tracker.reset();

    var conn = try std.testing.allocator.create(nats.Connection);
    defer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{
        .trace = true,
        .reconnect = .{
            .allow_reconnect = false, // Disable reconnect to only test disconnection
        },
        .callbacks = .{
            .disconnected_cb = CallbackTracker.disconnectedCallback,
        },
    });
    defer conn.deinit();

    try conn.connect("nats://127.0.0.1:14222");

    // Force disconnection by restarting server
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Wait for disconnected callback with timeout
    const callback_called = global_tracker.waitForCallback(&global_tracker.disconnected_called, 10000);
    try std.testing.expect(callback_called);
}

test "reconnected callback on successful reconnection" {
    global_tracker.reset();

    var conn = try std.testing.allocator.create(nats.Connection);
    defer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{
        .trace = true,
        .callbacks = .{
            .disconnected_cb = CallbackTracker.disconnectedCallback,
            .reconnected_cb = CallbackTracker.reconnectedCallback,
        },
    });
    defer conn.deinit();

    try conn.connect("nats://127.0.0.1:14222");

    // Add other servers for reconnection
    _ = try conn.addServer("nats://127.0.0.1:14223");
    _ = try conn.addServer("nats://127.0.0.1:14224");

    // Force disconnection and reconnection
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Wait for both callbacks
    const disconnected_called = global_tracker.waitForCallback(&global_tracker.disconnected_called, 10000);
    try std.testing.expect(disconnected_called);

    const reconnected_called = global_tracker.waitForCallback(&global_tracker.reconnected_called, 20000);
    try std.testing.expect(reconnected_called);

    // Verify connection works after callbacks
    try conn.publish("test.reconnected", "hello after reconnection");
    try conn.flush();
}

test "closed callback on explicit close" {
    global_tracker.reset();

    var conn = try std.testing.allocator.create(nats.Connection);
    defer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{
        .trace = true,
        .callbacks = .{
            .closed_cb = CallbackTracker.closedCallback,
        },
    });
    defer conn.deinit();

    try conn.connect("nats://127.0.0.1:14222");

    // Explicitly close the connection
    conn.close();

    // Wait for closed callback
    const callback_called = global_tracker.waitForCallback(&global_tracker.closed_called, 1000);
    try std.testing.expect(callback_called);
}

test "error callback on server error" {
    global_tracker.reset();

    var conn = try std.testing.allocator.create(nats.Connection);
    defer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{
        .trace = true,
        .callbacks = .{
            .error_cb = CallbackTracker.errorCallback,
        },
    });
    defer conn.deinit();

    try conn.connect("nats://127.0.0.1:14222");

    // We can't easily trigger a real server error in this test environment,
    // but we can verify that the error_cb would be called by directly testing
    // the processErr function which is how server errors are handled
    try conn.processErr("'Invalid Subject'");

    // Wait for error callback
    const callback_called = global_tracker.waitForCallback(&global_tracker.error_called, 1000);
    try std.testing.expect(callback_called);

    // Check that error message was captured
    global_tracker.mutex.lock();
    defer global_tracker.mutex.unlock();
    try std.testing.expect(global_tracker.error_message != null);
    try std.testing.expectEqualStrings("'Invalid Subject'", global_tracker.error_message.?);
}

test "all callbacks during reconnection lifecycle" {
    global_tracker.reset();

    var conn = try std.testing.allocator.create(nats.Connection);
    defer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{
        .trace = true,
        .callbacks = .{
            .disconnected_cb = CallbackTracker.disconnectedCallback,
            .reconnected_cb = CallbackTracker.reconnectedCallback,
            .closed_cb = CallbackTracker.closedCallback,
            .error_cb = CallbackTracker.errorCallback,
        },
    });
    defer conn.deinit();

    try conn.connect("nats://127.0.0.1:14222");

    // Add other servers for reconnection
    _ = try conn.addServer("nats://127.0.0.1:14223");
    _ = try conn.addServer("nats://127.0.0.1:14224");

    // Test initial connection works
    try conn.publish("test.initial", "initial message");
    try conn.flush();

    // Trigger disconnection and reconnection
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Wait for disconnected callback
    const disconnected_called = global_tracker.waitForCallback(&global_tracker.disconnected_called, 10000);
    try std.testing.expect(disconnected_called);

    // Wait for reconnected callback
    const reconnected_called = global_tracker.waitForCallback(&global_tracker.reconnected_called, 20000);
    try std.testing.expect(reconnected_called);

    // Test connection works after reconnection
    try conn.publish("test.after", "after reconnection");
    try conn.flush();

    // Explicitly close and verify closed callback
    conn.close();
    const closed_called = global_tracker.waitForCallback(&global_tracker.closed_called, 1000);
    try std.testing.expect(closed_called);
}
