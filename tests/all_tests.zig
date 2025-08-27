const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

test {
    _ = @import("minimal_test.zig");
    _ = @import("headers_test.zig");
    _ = @import("core_request_reply_test.zig");
    _ = @import("jetstream_test.zig");
    _ = @import("jetstream_stream_test.zig");
    _ = @import("jetstream_push_test.zig");
    _ = @import("jetstream_nak_test.zig");
    _ = @import("jetstream_sync_test.zig");
    _ = @import("jetstream_stream_purge_test.zig");
    _ = @import("jetstream_pull_test.zig");
    _ = @import("jetstream_msg_test.zig");
    _ = @import("jetstream_duplicate_ack_test.zig");
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
