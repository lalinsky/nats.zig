const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "JetStream synchronous subscription basic functionality" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_STREAM",
        .subjects = &.{"test.sync.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    var sync_sub = try js.subscribeSync("test.sync.*", .{
        .stream = "TEST_SYNC_STREAM",
        .durable = "sync_test_consumer",
    });
    defer sync_sub.deinit();

    // Publish a test message
    const test_message = "Sync test message";
    try conn.publish("test.sync.message", test_message);

    // Wait for message using nextMsg
    const js_msg = try sync_sub.nextMsg();
    defer js_msg.deinit(); // This cleans up everything via arena.deinit()

    // Verify message content
    try testing.expectEqualStrings(test_message, js_msg.msg.data);

    // Verify we can acknowledge the message
    try js_msg.ack();

    log.info("Synchronous subscription test completed successfully", .{});
}

test "JetStream synchronous subscription timeout" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_TIMEOUT_STREAM",
        .subjects = &.{"test.sync.timeout.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    var sync_sub = try js.subscribeSync("test.sync.timeout.*", .{
        .stream = "TEST_SYNC_TIMEOUT_STREAM",
        .durable = "sync_timeout_consumer",
    });
    defer sync_sub.deinit();

    // Test timeout (should return error.Timeout after timeout)
    const start = std.Io.Timestamp.now(io, .awake);
    const result = sync_sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }); // 100ms timeout
    const duration = start.untilNow(io, .awake).toMilliseconds();

    try testing.expectError(error.Timeout, result);
    try testing.expect(duration >= 100); // Should have waited at least 100ms

    log.info("Synchronous subscription timeout test completed successfully", .{});
}

test "JetStream synchronous subscription multiple messages" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_SYNC_MULTI_STREAM",
        .subjects = &.{"test.sync.multi.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous subscription
    var sync_sub = try js.subscribeSync("test.sync.multi.*", .{
        .stream = "TEST_SYNC_MULTI_STREAM",
        .durable = "sync_multi_consumer",
    });
    defer sync_sub.deinit();

    // Publish multiple test messages
    const messages = [_][]const u8{ "Message 1", "Message 2", "Message 3" };
    for (messages) |msg| {
        try conn.publish("test.sync.multi.msg", msg);
    }

    // Receive and verify all messages
    for (messages, 0..) |expected, i| {
        const js_msg = try sync_sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        defer js_msg.deinit(); // This cleans up everything via arena.deinit()

        try testing.expectEqualStrings(expected, js_msg.msg.data);
        try js_msg.ack();

        log.info("Received and acknowledged message {}: {s}", .{ i, js_msg.msg.data });
    }

    log.info("Multiple message synchronous subscription test completed successfully", .{});
}

test "JetStream synchronous queue subscription basic functionality" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_QUEUE_SYNC_STREAM",
        .subjects = &.{"test.queue.sync.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create synchronous queue subscription
    var queue_sub = try js.queueSubscribeSync("test.queue.sync.*", "test_queue", .{
        .stream = "TEST_QUEUE_SYNC_STREAM",
        .durable = "sync_queue_consumer",
    });
    defer queue_sub.deinit();

    // Publish a test message
    const test_message = "Queue sync test message";
    try conn.publish("test.queue.sync.message", test_message);

    // Wait for message using nextMsg
    const js_msg = try queue_sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer js_msg.deinit();

    // Verify message content
    try testing.expectEqualStrings(test_message, js_msg.msg.data);
}

test "JetStream pending limits remain bounded and track large ack windows" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    const stream_name = try utils.generateUniqueStreamName(std.testing.allocator);
    defer std.testing.allocator.free(stream_name);
    const subject_filter = try utils.generateSubjectFromStreamName(std.testing.allocator, stream_name);
    defer std.testing.allocator.free(subject_filter);

    var stream_info = try js.addStream(.{
        .name = stream_name,
        .subjects = &.{subject_filter},
    });
    defer stream_info.deinit();

    // AckNone has no redelivery safety, but it must remain bounded: otherwise
    // an unread subscription recreates the unbounded-memory problem.
    var no_ack_sub = try js.subscribeSync(subject_filter, .{
        .stream = stream_name,
        .config = .{ .ack_policy = .none, .max_ack_pending = 0 },
    });
    defer no_ack_sub.deinit();

    try std.testing.expectEqual(nats.Subscription.default_pending_msgs_limit, no_ack_sub.subscription.pending_msgs_limit.load(.acquire));
    try std.testing.expectEqual(nats.Subscription.default_pending_bytes_limit, no_ack_sub.subscription.pending_bytes_limit.load(.acquire));

    // Match nats.go by raising the client message limit when the server's
    // max-ack-pending window exceeds the default. This reduces avoidable
    // client-side drops without claiming that MaxAckPending guarantees none.
    var big_window_sub = try js.subscribeSync(subject_filter, .{
        .stream = stream_name,
        .durable = "big_window",
        .config = .{ .ack_policy = .explicit, .max_ack_pending = 600_000 },
    });
    defer big_window_sub.deinit();

    try std.testing.expectEqual(@as(u32, 600_000), big_window_sub.subscription.pending_msgs_limit.load(.acquire));
    try std.testing.expect(big_window_sub.subscription.pending_bytes_limit.load(.acquire) >= 600_000 * 1024 * 1024);

    // A default-sized ack window keeps the default limits.
    var default_sub = try js.subscribeSync(subject_filter, .{
        .stream = stream_name,
        .durable = "default_window",
        .config = .{ .ack_policy = .explicit },
    });
    defer default_sub.deinit();

    try std.testing.expectEqual(nats.Subscription.default_pending_msgs_limit, default_sub.subscription.pending_msgs_limit.load(.acquire));
}

test "JetStream subscribe attaches to an existing consumer named in the config" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    var stream_info = try js.addStream(.{
        .name = "TEST_ATTACH_STREAM",
        .subjects = &.{"test.attach.*"},
        .max_msgs = 100,
    });
    defer stream_info.deinit();

    // A consumer already delivering to a subject of its own choosing.
    var existing = try js.addConsumer("TEST_ATTACH_STREAM", .{
        .name = "attach_consumer",
        .durable_name = "attach_consumer",
        .deliver_subject = "test.attach.deliver.existing",
        .filter_subject = "test.attach.*",
        .ack_policy = .explicit,
    });
    defer existing.deinit();

    // Subscribing by that name must attach to it and receive on its deliver
    // subject, not create a second consumer or wait on the requested one.
    var sub = try js.subscribeSync(null, .{
        .stream = "TEST_ATTACH_STREAM",
        .config = .{
            .name = "attach_consumer",
            .deliver_subject = "test.attach.deliver.requested",
        },
    });
    defer sub.deinit();

    try testing.expectEqualStrings("attach_consumer", sub.consumer_info.value.name);
    try testing.expectEqualStrings("test.attach.deliver.existing", sub.consumer_info.value.config.deliver_subject.?);

    try conn.publish("test.attach.message", "attached");

    const js_msg = try sub.nextMsg();
    defer js_msg.deinit();

    try testing.expectEqualStrings("attached", js_msg.msg.data);
    try js_msg.ack();
}
