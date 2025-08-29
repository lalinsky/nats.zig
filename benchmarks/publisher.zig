const std = @import("std");
const nats = @import("nats");
const bench_util = @import("bench_util.zig");

const REPORT_INTERVAL = 10000;

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS publisher benchmark\n", .{});

    // Initialize statistics
    var stats = bench_util.BenchStats.init();

    // Creates a connection to the default NATS URL
    var conn = try bench_util.connect(allocator, null);
    defer bench_util.cleanup(conn);

    const subject = "benchmark.data";
    const message_data = "Hello, NATS Subscribers! This is benchmark data.";

    std.debug.print("Publishing messages to subject '{s}'...\n", .{subject});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    // Continuous loop publishing messages as fast as possible
    while (bench_util.keep_running) {
        // Publish message
        conn.publish(subject, message_data) catch |err| {
            stats.printError(err);
            continue;
        };

        stats.msg_count += 1;

        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            stats.printThroughput("Published");
        }
    }

    // Print final statistics
    stats.printSummary("messages published");
}
