// Asynchronous subscriber example - uses callback for message handling
const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");

fn messageHandler(msg: *nats.Message, counter: *u32, prefix: []const u8) void {
    defer msg.deinit();

    counter.* += 1;
    std.log.info("{s} #{d}: {s} - {s}", .{ prefix, counter.*, msg.subject, msg.data });
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Starting async subscriber for subject 'foo'", .{});

    // Initialize zio runtime
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Connect to NATS server
    var conn = nats.Connection.init(allocator, rt.io(), .{});
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Counter and prefix for the callback
    var counter: u32 = 0;
    const prefix = "Callback";

    // Subscribe with callback
    const sub = try conn.subscribe("foo", messageHandler, .{ &counter, prefix });
    defer sub.deinit();

    std.log.info("Subscribed with callback handler. Waiting for messages (10 seconds)...", .{});

    // Keep the program running to receive messages
    zio.sleep(.fromSeconds(10)) catch {};

    std.log.info("Shutting down after receiving {} messages", .{counter});
}
