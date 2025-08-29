const std = @import("std");
const nats = @import("nats");

// Global flag for signal handling
pub var keep_running: bool = true;

// Statistics structure
pub const BenchStats = struct {
    msg_count: u64 = 0,
    success_count: u64 = 0,
    error_count: u64 = 0,
    last_msg_count: u64 = 0,
    last_success_count: u64 = 0,
    last_error_count: u64 = 0,
    start_time: i128 = 0,
    last_report_time: i128 = 0,

    pub fn init() BenchStats {
        const now = std.time.nanoTimestamp();
        return BenchStats{
            .start_time = now,
            .last_report_time = now,
        };
    }

    pub fn elapsedS(self: *const BenchStats, start_time: i128) f64 {
        _ = self;
        const now = std.time.nanoTimestamp();
        const elapsed_ns = now - start_time;
        return @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
    }

    pub fn printThroughput(self: *BenchStats, action: []const u8) void {
        const interval_s = self.elapsedS(self.last_report_time);
        const interval_msgs = self.msg_count - self.last_msg_count;
        const interval_errors = self.error_count - self.last_error_count;
        const msg_per_s = @as(f64, @floatFromInt(interval_msgs)) / interval_s;

        if (interval_errors > 0) {
            const interval_error_rate = (@as(f64, @floatFromInt(interval_errors)) / @as(f64, @floatFromInt(interval_msgs + interval_errors))) * 100.0;
            std.debug.print("{s} {} messages in {d:.1}s, {d:.2} msg/s, {d:.2}% errors\n", .{ action, interval_msgs, interval_s, msg_per_s, interval_error_rate });
        } else {
            std.debug.print("{s} {} messages in {d:.1}s, {d:.2} msg/s\n", .{ action, interval_msgs, interval_s, msg_per_s });
        }

        self.last_msg_count = self.msg_count;
        self.last_success_count = self.success_count;
        self.last_error_count = self.error_count;
        self.last_report_time = std.time.nanoTimestamp();
    }

    pub fn printError(self: *BenchStats, err: anyerror) void {
        self.error_count += 1;
        if (self.error_count % 1000 == 0) {
            std.debug.print("Error #{}: {}\n", .{ self.error_count, err });
        }
    }

    pub fn printSummary(self: *const BenchStats, metric_name: []const u8) void {
        std.debug.print("\nShutting down...\n", .{});
        std.debug.print("Total {s}: {}\n", .{ metric_name, self.msg_count });
        if (self.success_count > 0) {
            std.debug.print("Successful: {}\n", .{self.success_count});
        }
        if (self.error_count > 0) {
            std.debug.print("Failed: {}\n", .{self.error_count});
        }

        const total_elapsed = self.elapsedS(self.start_time);
        const avg_rate = @as(f64, @floatFromInt(self.msg_count)) / total_elapsed;
        std.debug.print("Average rate: {d:.2} msg/s\n", .{avg_rate});
    }
};

fn benchSignalHandler() void {
    keep_running = false;
}

// Signal handling setup
pub fn setupSignals() !void {
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = benchSignalHandler },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
}

// Connect to NATS server
pub fn connect(allocator: std.mem.Allocator, url: ?[]const u8) !*nats.Connection {
    var conn = try allocator.create(nats.Connection);
    errdefer allocator.destroy(conn);

    conn.* = nats.Connection.init(allocator, .{});

    const connect_url = url orelse "nats://localhost:4222";
    conn.connect(connect_url) catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at {s}\n", .{connect_url});
        return err;
    };

    return conn;
}

// Common cleanup
pub fn cleanup(conn: *nats.Connection) void {
    conn.deinit();
}
