const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "connect" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
}

test "connect wrong port" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = utils.createConnectionWrongPort() catch return;
    defer utils.closeConnection(conn);

    try std.testing.expect(false);
}

test "basic publish and subscribe" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.minimal");
    defer sub.deinit();

    // Publish a message
    try conn.publish("test.minimal", "Hello from minimal test!");
    try conn.flush();

    // Try to receive the message
    var msg = sub.nextMsg(100) catch return error.NoMessageReceived;
    defer msg.deinit();

    try std.testing.expectEqualStrings("test.minimal", msg.subject);
    try std.testing.expectEqualStrings("Hello from minimal test!", msg.data);
}

test "async subscribe" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Message handler function
    const Handler = struct {
        count: u32 = 0,
        data: []const u8 = "",
        subject: []const u8 = "",
        called: zio.ResetEvent = .init,

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
    try handler.called.timedWait(.fromMilliseconds(100));

    // Check if message was received by handler
    try std.testing.expect(handler.count == 1);
    try std.testing.expectEqualStrings("test.async", handler.subject);
    try std.testing.expectEqualStrings("Hello from async test!", handler.data);
}
