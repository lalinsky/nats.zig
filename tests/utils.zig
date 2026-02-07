const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");

const log = std.log.default;

pub const Node = enum(u16) {
    node1 = 14222,
    node2 = 14223,
    node3 = 14224,
    token_auth = 14225,
    unknown = 14226,
};

pub fn createConnection(node: Node, opts: nats.ConnectionOptions) !*nats.Connection {
    const port = @intFromEnum(node);
    const url = try std.fmt.allocPrint(std.testing.allocator, "nats://127.0.0.1:{d}", .{port});
    defer std.testing.allocator.free(url);

    var conn = try std.testing.allocator.create(nats.Connection);
    errdefer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, opts);
    errdefer conn.deinit();

    try conn.connect(url);

    return conn;
}

pub fn createDefaultConnection() !*nats.Connection {
    return createConnection(.node1, .{});
}

pub fn createConnectionWrongPort() !*nats.Connection {
    return createConnection(.unknown, .{});
}

pub fn closeConnection(conn: *nats.Connection) void {
    conn.deinit();
    std.testing.allocator.destroy(conn);
}

pub fn runDockerComposeCapture(allocator: std.mem.Allocator, compose_args: []const []const u8) !std.process.Child.RunResult {
    var args: std.ArrayListUnmanaged([]const u8) = .{};
    defer args.deinit(allocator);

    try args.appendSlice(allocator, &.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "nats-zig-test" });
    try args.appendSlice(allocator, compose_args);

    return try std.process.Child.run(.{
        .allocator = allocator,
        .argv = args.items,
    });
}

pub fn runDockerCompose(allocator: std.mem.Allocator, compose_args: []const []const u8) !void {
    const result = try runDockerComposeCapture(allocator, compose_args);
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);
}

pub fn waitForHealthyServices(allocator: std.mem.Allocator, timeout_ms: i64) !void {
    const deadline = std.time.milliTimestamp() + timeout_ms;
    while (true) {
        if (std.time.milliTimestamp() > deadline) {
            return error.ServicesNotHealthy;
        }

        // Check service health status using docker compose ps --format "table {{.Health}}"
        const result = try runDockerComposeCapture(allocator, &.{ "ps", "-a", "--format", "table {{ .Health }}" });
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);

        // Count "healthy" occurrences in the output
        var healthy_count: u32 = 0;
        var lines = std.mem.splitScalar(u8, result.stdout, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r\n");
            if (std.mem.eql(u8, trimmed, "healthy")) {
                healthy_count += 1;
            }
        }

        if (healthy_count >= 3) {
            return;
        }

        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}

var global_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

pub fn generateUniqueName(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    const timestamp = std.time.microTimestamp();
    const counter = global_counter.fetchAdd(1, .monotonic);

    return std.fmt.allocPrint(allocator, "{s}_{d}_{d}", .{ prefix, timestamp, counter });
}

pub fn generateUniqueStreamName(allocator: std.mem.Allocator) ![]u8 {
    return generateUniqueName(allocator, "TEST_STREAM");
}

pub fn generateUniqueConsumerName(allocator: std.mem.Allocator) ![]u8 {
    return generateUniqueName(allocator, "TEST_CONSUMER");
}

pub fn generateSubjectFromStreamName(allocator: std.mem.Allocator, stream_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}.*", .{stream_name});
}
