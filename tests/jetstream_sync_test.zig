const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "JetStream synchronous subscription basic functionality" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_STREAM",
        .subjects = &.{"test.sync.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "sync_test_consumer",
        .deliver_subject = "push.sync.test",
        .ack_policy = .explicit,
    };

    var sync_sub = try js.subscribeSync("TEST_SYNC_STREAM", consumer_config);
    defer sync_sub.deinit();

    // Publish a test message
    const test_message = "Sync test message";
    try conn.publish("test.sync.message", test_message);

    // Wait for message using nextMsg
    const js_msg = try sync_sub.nextMsg(5000);
    defer js_msg.deinit(); // This cleans up everything via arena.deinit()

    // Verify message content
    try testing.expectEqualStrings(test_message, js_msg.msg.data);

    // Verify we can acknowledge the message
    try js_msg.ack();

    log.info("Synchronous subscription test completed successfully", .{});
}

test "JetStream synchronous subscription timeout" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_TIMEOUT_STREAM",
        .subjects = &.{"test.sync.timeout.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "sync_timeout_consumer",
        .deliver_subject = "push.sync.timeout",
        .ack_policy = .explicit,
    };

    var sync_sub = try js.subscribeSync("TEST_SYNC_TIMEOUT_STREAM", consumer_config);
    defer sync_sub.deinit();

    // Test timeout (should return error.Timeout after timeout)
    const start = std.time.milliTimestamp();
    const result = sync_sub.nextMsg(100); // 100ms timeout
    const duration = std.time.milliTimestamp() - start;

    try testing.expectError(error.Timeout, result);
    try testing.expect(duration >= 100); // Should have waited at least 100ms

    log.info("Synchronous subscription timeout test completed successfully", .{});
}

test "JetStream synchronous subscription multiple messages" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_MULTI_STREAM",
        .subjects = &.{"test.sync.multi.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "sync_multi_consumer",
        .deliver_subject = "push.sync.multi",
        .ack_policy = .explicit,
    };

    var sync_sub = try js.subscribeSync("TEST_SYNC_MULTI_STREAM", consumer_config);
    defer sync_sub.deinit();

    // Publish multiple test messages
    const messages = [_][]const u8{ "Message 1", "Message 2", "Message 3" };
    for (messages) |msg| {
        try conn.publish("test.sync.multi.msg", msg);
    }

    // Receive and verify all messages
    for (messages, 0..) |expected, i| {
        const js_msg = try sync_sub.nextMsg(5000);
        defer js_msg.deinit(); // This cleans up everything via arena.deinit()

        try testing.expectEqualStrings(expected, js_msg.msg.data);
        try js_msg.ack();

        log.info("Received and acknowledged message {}: {s}", .{ i, js_msg.msg.data });
    }

    log.info("Multiple message synchronous subscription test completed successfully", .{});
}
