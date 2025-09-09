const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

test {
    _ = @import("socket_test.zig");
    _ = @import("minimal_test.zig");
    _ = @import("headers_test.zig");
    _ = @import("subscribe_test.zig");
    _ = @import("pending_msgs_test.zig");
    _ = @import("core_request_reply_test.zig");
    _ = @import("reconnection_test.zig");
    _ = @import("jetstream_test.zig");
    _ = @import("jetstream_stream_test.zig");
    _ = @import("jetstream_push_test.zig");
    _ = @import("jetstream_nak_test.zig");
    _ = @import("jetstream_sync_test.zig");
    _ = @import("jetstream_stream_purge_test.zig");
    _ = @import("jetstream_pull_test.zig");
    _ = @import("jetstream_get_msg_test.zig");
    _ = @import("jetstream_get_msg_direct_test.zig");
    _ = @import("jetstream_delete_msg_test.zig");
    _ = @import("jetstream_duplicate_ack_test.zig");
    _ = @import("jetstream_large_list_test.zig");
    _ = @import("jetstream_kv_simple_test.zig");
    _ = @import("jetstream_kv_test.zig");
    _ = @import("jetstream_kv_history_test.zig");
}

test "tests:beforeEach" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.waitForHealthyServices(allocator, 10_000);
}

test "tests:beforeAll" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{ "up", "-d" });
}

test "tests:afterAll" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try utils.runDockerCompose(allocator, &.{"down"});
}
