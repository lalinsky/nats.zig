const std = @import("std");
const nats = @import("nats");

const log = std.log.scoped(.testing);

fn createConnection() !*nats.Connection {
    const default_url = "nats://localhost:4222";
    const url = std.process.getEnvVarOwned(std.testing.allocator, "TEST_NATS_URL") catch default_url;
    defer if (url.ptr != default_url.ptr) std.testing.allocator.free(url);

    var conn = try std.testing.allocator.create(nats.Connection);
    conn.* = nats.Connection.init(std.testing.allocator, .{});
    try conn.connect(url);
    return conn;
}

fn closeConnection(conn: *nats.Connection) void {
    conn.deinit();
    std.testing.allocator.destroy(conn);
}

test "connect" {
    const conn = try createConnection();
    defer closeConnection(conn);
}

test "basic publish and subscribe" {
    var conn = try createConnection();
    defer closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.minimal");
    defer sub.deinit();

    // Publish a message
    try conn.publish("test.minimal", "Hello from minimal test!");
    try conn.flush();

    // Try to receive the message
    if (sub.nextMsg(100)) |msg| {
        defer msg.deinit();

        try std.testing.expectEqualStrings("test.minimal", msg.subject);
        try std.testing.expectEqualStrings("Hello from minimal test!", msg.data);
    } else {
        return error.NoMessageReceived;
    }
}

test "async subscribe" {
    var conn = try createConnection();
    defer closeConnection(conn);

    // Message handler function
    const Handler = struct {
        count: u32 = 0,
        data: []const u8 = "",
        subject: []const u8 = "",
        called: std.Thread.ResetEvent = .{},

        fn handleMsg(msg: *nats.Message, self: *@This()) void {
            defer self.called.set();
            defer msg.deinit();
            self.count += 1;
            self.data = std.testing.allocator.dupe(u8, msg.data) catch unreachable;
            self.subject = std.testing.allocator.dupe(u8, msg.subject) catch unreachable;
        }

        pub fn deinit(self: @This()) void {
            std.testing.allocator.free(self.data);
            std.testing.allocator.free(self.subject);
        }
    };

    var handler: Handler = .{};
    defer handler.deinit();

    // Create async subscription
    const sub = try conn.subscribe("test.async", Handler.handleMsg, .{&handler});
    defer sub.deinit();

    // Publish a message
    try conn.publish("test.async", "Hello from async test!");
    try conn.flush();

    // Wait a bit for async processing
    try handler.called.timedWait(100 * std.time.ns_per_ms);

    // Check if message was received by handler
    try std.testing.expect(handler.count == 1);
    try std.testing.expectEqualStrings("test.async", handler.subject);
    try std.testing.expectEqualStrings("Hello from async test!", handler.data);
}
