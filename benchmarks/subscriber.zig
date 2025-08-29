const std = @import("std");
const nats = @import("nats");

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS subscriber benchmark\n", .{});

    // Creates a connection to the default NATS URL
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        return err;
    };

    const subject = "benchmark.data";

    // Creates a synchronous subscription on subject
    const sub = conn.subscribeSync(subject) catch |err| {
        std.debug.print("Failed to subscribe: {}\n", .{err});
        return err;
    };
    defer {
        conn.unsubscribe(sub) catch {};
        sub.deinit(allocator);
    }

    std.debug.print("Subscriber listening on subject '{s}'...\n", .{subject});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    var msg_count: u64 = 0;
    var last_msg_count: u64 = 0;
    const start_time = std.time.nanoTimestamp();
    var last_report_time = start_time;

    // Wait for messages in a loop
    while (true) {
        // Wait for the next message (with timeout)
        if (sub.nextMsg(1000)) |msg| {
            defer msg.deinit();

            msg_count += 1;

            // Print stats every 10000 messages
            if (msg_count % 10000 == 0) {
                const current_time = std.time.nanoTimestamp();
                const interval_ns = current_time - last_report_time;
                const interval_msgs = msg_count - last_msg_count;
                const interval_s = @as(f64, @floatFromInt(interval_ns)) / std.time.ns_per_s;
                const msg_per_s = @as(f64, @floatFromInt(interval_msgs)) / interval_s;
                std.debug.print("Received {} messages, {d:.2} msg/s\n", .{ msg_count, msg_per_s });

                last_msg_count = msg_count;
                last_report_time = current_time;
            }
        } else {
            // Sleep a bit if no message (timeout occurred)
            std.time.sleep(10 * std.time.ns_per_ms);
        }
    }
}
