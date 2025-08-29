const std = @import("std");
const nats = @import("nats");

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS echo-server benchmark\n", .{});

    // Creates a connection to the default NATS URL
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        std.process.exit(2);
    };

    // Creates a synchronous subscription on subject "echo",
    // waiting for requests. When a message arrives, echo it back.
    const sub = conn.subscribeSync("echo") catch |err| {
        std.debug.print("Failed to subscribe: {}\n", .{err});
        std.process.exit(2);
    };
    defer {
        conn.unsubscribe(sub) catch {};
        sub.deinit(allocator);
    }

    std.debug.print("Echo server listening on subject 'echo'...\n", .{});

    var msg_count: u64 = 0;
    const start_time = std.time.nanoTimestamp();

    // Wait for messages in a loop
    while (true) {
        // Wait for the next message (with timeout)
        if (sub.nextMsg(1000)) |msg| {
            defer msg.deinit();

            msg_count += 1;

            // Send echo reply if there's a reply subject
            if (msg.reply) |reply_subject| {
                conn.publish(reply_subject, msg.data) catch |err| {
                    std.debug.print("Failed to send echo reply: {}\n", .{err});
                };
            }

            // Print stats every 10000 messages
            if (msg_count % 10000 == 0) {
                const current_time = std.time.nanoTimestamp();
                const elapsed_ns = current_time - start_time;
                const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
                const msg_per_s = @as(f64, @floatFromInt(msg_count)) / elapsed_s;
                std.debug.print("Processed {} messages, {d:.2} msg/s\n", .{ msg_count, msg_per_s });
            }
        } else {
            // Sleep a bit if no message
            std.time.sleep(10 * std.time.ns_per_ms);
        }
    }
}
