const std = @import("std");
const nats = @import("nats");

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS publisher benchmark\n", .{});

    // Creates a connection to the default NATS URL
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        return err;
    };

    const subject = "benchmark.data";
    const message_data = "Hello, NATS Subscribers! This is benchmark data.";

    var msg_count: u64 = 0;
    var error_count: u64 = 0;
    var last_msg_count: u64 = 0;
    const start_time = std.time.nanoTimestamp();
    var last_report_time = start_time;

    std.debug.print("Publishing messages to subject '{s}'...\n", .{subject});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    // Continuous loop publishing messages as fast as possible
    while (true) {
        // Publish message
        conn.publish(subject, message_data) catch |err| {
            error_count += 1;
            if (error_count % 1000 == 0) {
                std.debug.print("Error #{}: {}\n", .{ error_count, err });
            }
            continue;
        };

        msg_count += 1;

        // Print stats every 10000 messages
        if (msg_count % 10000 == 0) {
            const current_time = std.time.nanoTimestamp();
            const interval_ns = current_time - last_report_time;
            const interval_msgs = msg_count - last_msg_count;
            const interval_s = @as(f64, @floatFromInt(interval_ns)) / std.time.ns_per_s;
            const msg_per_s = @as(f64, @floatFromInt(interval_msgs)) / interval_s;
            const error_rate = @as(f64, @floatFromInt(error_count)) / @as(f64, @floatFromInt(msg_count)) * 100.0;
            std.debug.print("Published {} messages, {d:.2} msg/s, {d:.2}% errors\n", .{ msg_count, msg_per_s, error_rate });

            last_msg_count = msg_count;
            last_report_time = current_time;
        }
    }
}
