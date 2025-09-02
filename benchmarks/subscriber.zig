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

    std.debug.print("Starting NATS subscriber benchmark\n", .{});

    try bench_util.setupSignals();

    // Initialize statistics
    var stats = bench_util.BenchStats.init();

    // Creates a connection to the default NATS URL
    var conn = try bench_util.connect(allocator, null);
    defer bench_util.cleanup(conn);

    const subject = "benchmark.data";

    // Creates a synchronous subscription on subject
    const sub = conn.subscribeSync(subject) catch |err| {
        std.debug.print("Failed to subscribe: {}\n", .{err});
        return err;
    };
    defer {
        conn.unsubscribe(sub) catch {};
        sub.deinit();
    }

    std.debug.print("Subscriber listening on subject '{s}'...\n", .{subject});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    // Wait for messages in a loop
    while (bench_util.keep_running) {
        // Wait for the next message (with timeout)
        var msg = sub.nextMsg(1000) catch break;
        defer msg.deinit();

        stats.msg_count += 1;

        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            stats.printThroughput("Received");
        }
    }

    // Print final statistics
    stats.printSummary("messages received");
}
