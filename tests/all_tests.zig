const std = @import("std");
const net = std.net;
const time = std.time;

// Import all test modules
pub const minimal_tests = @import("minimal_test.zig");
pub const headers_tests = @import("headers_test.zig");
pub const reconnection_tests = @import("reconnection_test.zig");
pub const jetstream_tests = @import("jetstream_test.zig");

const utils = @import("utils.zig");

test "tests:beforeAll" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{ "up", "-d" });
    try utils.waitForHealthyServices(allocator, 10_000);
}

test "tests:afterAll" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{"down"});
}

// Re-export test declarations so they run
test {
    std.testing.refAllDecls(@This());
}
