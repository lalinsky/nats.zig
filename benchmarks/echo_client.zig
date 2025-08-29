const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting NATS echo-client benchmark\n", .{});

    // Creates a connection to the default NATS URL
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        std.process.exit(2);
    };

    const message_data = "Hello, NATS Echo Server!";
    const timeout_ns = std.time.ns_per_s * 5; // 5 second timeout

    var msg_count: u64 = 0;
    var success_count: u64 = 0;
    var error_count: u64 = 0;
    const start_time = std.time.nanoTimestamp();

    std.debug.print("Sending echo requests to subject 'echo'...\n", .{});

    // Continuous loop sending requests as fast as possible
    while (true) {
        msg_count += 1;

        // Send request and wait for echo reply
        const reply = conn.request("echo", message_data, timeout_ns) catch |err| {
            error_count += 1;
            if (error_count % 1000 == 0) {
                std.debug.print("Error #{}: {}\n", .{ error_count, err });
            }
            continue;
        };
        defer reply.deinit();

        success_count += 1;

        // Verify echo (optional - could be removed for pure performance test)
        if (!std.mem.eql(u8, reply.data, message_data)) {
            std.debug.print("Warning: Echo mismatch! Expected: '{s}', Got: '{s}'\n", .{ message_data, reply.data });
        }

        // Print stats every 1000 successful messages
        if (success_count % 1000 == 0) {
            const current_time = std.time.nanoTimestamp();
            const elapsed_ns = current_time - start_time;
            const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
            const msg_per_s = @as(f64, @floatFromInt(success_count)) / elapsed_s;
            const error_rate = @as(f64, @floatFromInt(error_count)) / @as(f64, @floatFromInt(msg_count)) * 100.0;
            std.debug.print("Sent {} requests, {} successful, {d:.2} req/s, {d:.2}% errors\n", .{ msg_count, success_count, msg_per_s, error_rate });
        }
    }
}
