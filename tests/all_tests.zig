const std = @import("std");
const net = std.net;
const time = std.time;
const nats = @import("nats");

// Import all test modules
pub const minimal_tests = @import("minimal_test.zig");
pub const headers_tests = @import("headers_test.zig");
pub const core_request_reply_tests = @import("core_request_reply_test.zig");
// pub const reconnection_tests = @import("reconnection_test.zig");
pub const jetstream_tests = @import("jetstream_test.zig");
pub const jetstream_stream_tests = @import("jetstream_stream_test.zig");
pub const jetstream_stream_purge_tests = @import("jetstream_stream_purge_test.zig");

const utils = @import("utils.zig");

test "tests:beforeAll" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{ "up", "-d" });
    try utils.waitForHealthyServices(allocator, 10_000);
}

test "tests:beforeEach" {
    // Clean up all streams and consumers before each test - fail if cleanup fails
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Get all stream names and delete them (consumers are deleted automatically when stream is deleted)
    var stream_names = try js.listStreamNames();
    defer stream_names.deinit();

    for (stream_names.value) |stream_name| {
        try js.deleteStream(stream_name);
    }
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
