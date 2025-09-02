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

    std.debug.print("Starting NATS echo-server benchmark\n", .{});

    try bench_util.setupSignals();

    // Initialize statistics
    var stats = bench_util.BenchStats.init();

    // Creates a connection to the default NATS URL
    var conn = try bench_util.connect(allocator, null);
    defer bench_util.cleanup(conn);

    // Creates a synchronous subscription on subject "echo",
    // waiting for requests. When a message arrives, echo it back.
    const sub = conn.subscribeSync("echo") catch |err| {
        std.debug.print("Failed to subscribe: {}\n", .{err});
        return err;
    };
    defer {
        conn.unsubscribe(sub) catch {};
        sub.deinit();
    }

    std.debug.print("Echo server listening on subject 'echo'...\n", .{});
    std.debug.print("Press Ctrl+C to stop\n", .{});

    // Wait for messages in a loop
    while (bench_util.keep_running) {
        // Wait for the next message (with timeout)
        var msg = sub.nextMsg(1000) catch break;
        defer msg.deinit();

        stats.msg_count += 1;

        // Send echo reply if there's a reply subject
        if (msg.reply) |reply_subject| {
            const result = conn.publish(reply_subject, msg.data);
            if (result) {
                stats.success_count += 1;
            } else |err| {
                std.debug.print("Failed to send echo reply: {}\n", .{err});
                stats.error_count += 1;
            }
        }

        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            stats.printThroughput("Processed");
        }
    }

    // Print final statistics
    stats.printSummary("messages processed");
}
