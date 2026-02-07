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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Starting async subscriber for subject 'foo'", .{});

    // Initialize zio runtime
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Connect to NATS server
    var conn = nats.Connection.init(allocator, .{});
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
    std.Thread.sleep(10 * std.time.ns_per_s);

    std.log.info("Shutting down after receiving {} messages", .{counter});
}
