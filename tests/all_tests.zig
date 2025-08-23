const std = @import("std");
const net = std.net;
const time = std.time;
const nats = @import("nats");

// Import all test modules
pub const minimal_tests = @import("minimal_test.zig");
pub const headers_tests = @import("headers_test.zig");
// pub const reconnection_tests = @import("reconnection_test.zig");
pub const jetstream_tests = @import("jetstream_test.zig");

const utils = @import("utils.zig");

test "tests:beforeAll" {
    // Clean up any existing containers before starting
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{"down"});
}

test "tests:beforeEach" {
    // Start fresh NATS cluster before each test instead of just cleaning streams
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Stop any running containers first
    _ = utils.runDockerCompose(allocator, &.{"down"}) catch {};
    
    // Start fresh cluster
    try utils.runDockerCompose(allocator, &.{ "up", "-d" });
    try utils.waitForHealthyServices(allocator, 10_000);
}

test "tests:afterAll" {
    // Clean up containers
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{"down"});
}

// Re-export test declarations so they run
test {
    std.testing.refAllDecls(@This());
}
