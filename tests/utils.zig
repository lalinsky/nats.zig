const std = @import("std");
const nats = @import("nats");

const log = std.log.default;

/// Duration-form `std.Io.Timeout` on the awake clock, for xsync wait calls.
pub fn ioTimeout(duration: std.Io.Duration) std.Io.Timeout {
    return .{ .duration = .{ .raw = duration, .clock = .awake } };
}

// Blocking `std.Io` instance for test utilities that must outlive any
// per-test io instance (docker compose invocations, sleeps). Lazily
// initialized, never deinitialized; lives for the whole test process.
var blocking_io_instance: ?std.Io.Threaded = null;

fn blockingIo() std.Io {
    if (blocking_io_instance == null) {
        blocking_io_instance = std.Io.Threaded.init(std.heap.page_allocator, .{});
    }
    return blocking_io_instance.?.io();
}

pub const Node = enum(u16) {
    node1 = 14222,
    node2 = 14223,
    node3 = 14224,
    token_auth = 14225,
    unknown = 14226,
    user_pass = 14227,
};

pub fn createConnection(io: std.Io, node: Node, opts: nats.ConnectionOptions) !*nats.Connection {
    const port = @intFromEnum(node);
    const url = try std.fmt.allocPrint(std.testing.allocator, "nats://127.0.0.1:{d}", .{port});
    defer std.testing.allocator.free(url);

    return createConnectionWithUrl(io, url, opts);
}

pub fn createConnectionWithUrl(io: std.Io, url: []const u8, opts: nats.ConnectionOptions) !*nats.Connection {
    var conn = try std.testing.allocator.create(nats.Connection);
    errdefer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, io, opts);
    errdefer conn.deinit();

    try conn.connect(url);

    return conn;
}

pub fn createDefaultConnection(io: std.Io) !*nats.Connection {
    return createConnection(io, .node1, .{});
}

pub fn createConnectionWrongPort(io: std.Io) !*nats.Connection {
    return createConnection(io, .unknown, .{});
}

pub fn closeConnection(conn: *nats.Connection) void {
    conn.deinit();
    std.testing.allocator.destroy(conn);
}

pub fn runDockerComposeCapture(allocator: std.mem.Allocator, compose_args: []const []const u8) !std.process.RunResult {
    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    defer args.deinit(allocator);

    try args.appendSlice(allocator, &.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "nats-zig-test" });
    try args.appendSlice(allocator, compose_args);

    return try std.process.run(allocator, blockingIo(), .{
        .argv = args.items,
    });
}

pub fn runDockerCompose(allocator: std.mem.Allocator, compose_args: []const []const u8) !void {
    const result = try runDockerComposeCapture(allocator, compose_args);
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);
}

pub fn waitForHealthyServices(allocator: std.mem.Allocator, timeout: std.Io.Duration) !void {
    const io = blockingIo();
    const start = std.Io.Timestamp.now(io, .awake);
    while (true) {
        if (start.untilNow(io, .awake).nanoseconds > timeout.nanoseconds) {
            return error.ServicesNotHealthy;
        }

        // Check service health status; one line per service, no header.
        const result = try runDockerComposeCapture(allocator, &.{ "ps", "-a", "--format", "{{ .Health }}" });
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);

        // Count service health states in the output; require every service
        // to be healthy, so that a server restarted by a previous test
        // cannot slip through while it is still coming up.
        var healthy_count: u32 = 0;
        var total_count: u32 = 0;
        var lines = std.mem.splitScalar(u8, result.stdout, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r\n");
            if (trimmed.len == 0) continue;
            total_count += 1;
            if (std.mem.eql(u8, trimmed, "healthy")) {
                healthy_count += 1;
            }
        }

        if (total_count > 0 and healthy_count == total_count and allServerPortsOpen(io)) {
            return;
        }

        blockingIo().sleep(.fromMilliseconds(100), .awake) catch {};
    }
}

/// Docker reporting a container healthy is not the same as the NATS client
/// port accepting connections; probe the real thing so a test never starts
/// against a server that is still coming up.
fn allServerPortsOpen(io: std.Io) bool {
    for ([_]Node{ .node1, .node2, .node3, .token_auth, .user_pass }) |node| {
        const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(@intFromEnum(node)) };
        const stream = address.connect(io, .{ .mode = .stream, .protocol = .tcp }) catch return false;
        stream.close(io);
    }
    return true;
}

/// Publish into a JetStream stream and wait for the PubAck, so the message
/// is durably stored before the test goes on to read stream state. A core
/// publish + flush only confirms the connected server processed the message;
/// in a cluster the stream may not have stored it yet.
pub fn jsPublish(js: *nats.JetStream, subject: []const u8, data: []const u8) !void {
    var result = try js.publish(subject, data, .{});
    result.deinit();
}

var global_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

pub fn generateUniqueName(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    const timestamp = @divTrunc(std.Io.Timestamp.now(blockingIo(), .real).nanoseconds, std.time.ns_per_us);
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
