// Simple subscriber example - listens for a single message
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Listening for message on subject 'foo'", .{});

    // Connect to NATS server
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Subscribe to subject (sync)
    const sub = try conn.subscribeSync("foo");

    std.log.info("Waiting for message (5 second timeout)...", .{});

    // Wait for message (poll every 100ms for 5 seconds)
    var attempts: u32 = 0;
    const max_attempts = 50; // 50 * 100ms = 5 seconds

    while (attempts < max_attempts) {
        if (sub.nextMsg(100) catch null) |msg| {
            defer msg.deinit();

            std.log.info("Received message: {s} - {s}", .{ msg.subject, msg.data });
            return;
        }

        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;
    }

    std.log.info("No message received within timeout", .{});
}
