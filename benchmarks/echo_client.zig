const std = @import("std");
const nats = @import("nats");
const bench_util = @import("bench_util.zig");

const REPORT_INTERVAL = 1000;
const REQUEST_TIMEOUT_MS = std.time.ms_per_s * 5; // 5 second timeout

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS echo-client benchmark\n", .{});

    try bench_util.setupSignals();

    // Initialize statistics
    var stats = bench_util.BenchStats.init();

    // Creates a connection to the default NATS URL
    var conn = bench_util.connect(allocator, null) catch |err| {
        return err;
    };
    defer bench_util.cleanup(conn);

    const message_data = "Hello, NATS Echo Server!";

    std.debug.print("Sending echo requests to subject 'echo'...\n", .{});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    // Continuous loop sending requests as fast as possible
    while (bench_util.keep_running) {
        stats.msg_count += 1;

        // Send request and wait for echo reply
        const reply = conn.request("echo", message_data, REQUEST_TIMEOUT_MS) catch |err| {
            stats.printError(err);
            continue;
        };
        defer reply.deinit();

        stats.success_count += 1;

        // Verify echo (optional - could be removed for pure performance test)
        if (!std.mem.eql(u8, reply.data, message_data)) {
            std.debug.print("Warning: Echo mismatch! Expected: '{s}', Got: '{s}'\n", .{ message_data, reply.data });
        }

        // Print stats every REPORT_INTERVAL successful messages
        if (stats.success_count % REPORT_INTERVAL == 0) {
            const interval_s = stats.elapsedS(stats.last_report_time);
            const interval_requests = stats.msg_count - stats.last_msg_count;
            const interval_success = stats.success_count - stats.last_success_count;
            const interval_errors = stats.error_count - stats.last_error_count;
            const req_per_s = @as(f64, @floatFromInt(interval_success)) / interval_s;
            const interval_error_rate = (@as(f64, @floatFromInt(interval_errors)) / @as(f64, @floatFromInt(interval_requests))) * 100.0;
            std.debug.print("Sent {} requests, {} successful in {d:.1}s, {d:.2} req/s, {d:.2}% errors\n", .{ interval_requests, interval_success, interval_s, req_per_s, interval_error_rate });

            stats.last_msg_count = stats.msg_count;
            stats.last_success_count = stats.success_count;
            stats.last_error_count = stats.error_count;
            stats.last_report_time = std.time.nanoTimestamp();
        }
    }

    // Print final statistics
    std.debug.print("\nShutting down...\n", .{});
    std.debug.print("Total requests sent: {}\n", .{stats.msg_count});
    std.debug.print("Successful requests: {}\n", .{stats.success_count});
    std.debug.print("Failed requests: {}\n", .{stats.error_count});

    const total_elapsed = stats.elapsedS(stats.start_time);
    const avg_rate = @as(f64, @floatFromInt(stats.success_count)) / total_elapsed;
    std.debug.print("Average rate: {d:.2} req/s\n", .{avg_rate});
}
