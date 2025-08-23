const std = @import("std");
const net = std.net;
const time = std.time;

// Import all test modules
pub const minimal_tests = @import("minimal_test.zig");
pub const headers_tests = @import("headers_test.zig");

fn runDockerCompose(compose_args: []const []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = std.ArrayList([]const u8).init(allocator);
    defer args.deinit();

    try args.appendSlice(&.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "nats-zig-test" });
    try args.appendSlice(compose_args);

    var child = std.process.Child.init(args.items, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();
    const result = try child.wait();

    switch (result) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("Docker compose command failed with exit code: {}\n", .{code});
                return error.DockerComposeFailed;
            }
        },
        else => return error.DockerComposeFailed,
    }
}

fn waitForNatsServer(port: u16, max_attempts: u32) !void {
    var attempts: u32 = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        time.sleep(1 * time.ns_per_ms);
        const address = try net.Address.parseIp("127.0.0.1", port);
        const stream = net.tcpConnectToAddress(address) catch |err| switch (err) {
            error.ConnectionRefused => {
                std.debug.print("Attempt {}/{}: NATS server on port {} not ready, waiting...\n", .{ attempts + 1, max_attempts, port });
                time.sleep(100 * time.ns_per_s);
                continue;
            },
            else => return err,
        };
        stream.close();
        std.debug.print("NATS server on port {} is ready!\n", .{port});
        return;
    }
    std.debug.print("Failed to connect to NATS server on port {} after {} attempts\n", .{ port, max_attempts });
    return error.ServerNotReady;
}

test "tests:beforeAll" {
    std.debug.print("Starting NATS cluster...\n", .{});
    try runDockerCompose(&.{ "up", "-d" });
    std.debug.print("Waiting for NATS servers to be ready...\n", .{});

    // Wait for all three NATS servers to be ready
    try waitForNatsServer(14222, 30); // nats-1
    try waitForNatsServer(14223, 30); // nats-2
    try waitForNatsServer(14224, 30); // nats-3

    std.debug.print("All NATS servers are ready!\n", .{});
}

test "tests:afterAll" {
    std.debug.print("Shutting down NATS cluster...\n", .{});
    try runDockerCompose(&.{"down"});
    std.debug.print("NATS cluster shutdown complete\n", .{});
}

// Re-export test declarations so they run
test {
    std.testing.refAllDecls(@This());

    // Print test header
    std.debug.print("\n=== NATS.zig Integration Tests ===\n", .{});
    std.debug.print("Make sure Docker is running and ports 14222-14224 are available\n", .{});
    std.debug.print("Run 'docker compose -f docker-compose.test.yml up -d' to start test servers\n\n", .{});
}
