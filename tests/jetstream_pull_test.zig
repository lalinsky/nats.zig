const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");
const Connection = nats.Connection;
const JetStream = nats.JetStream;
const StreamConfig = nats.StreamConfig;
const ConsumerConfig = nats.ConsumerConfig;
const FetchRequest = nats.FetchRequest;
const MessageBatch = nats.MessageBatch;
const PullSubscription = nats.PullSubscription;

test "JetStream pull consumer basic fetch" {
    // Use existing utility functions
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    const stream_name = "TEST_PULL_STREAM";
    const consumer_name = "test_pull_consumer";

    // Clean up any existing stream
    js.deleteStream(stream_name) catch {};

    // Create a test stream
    const stream_config = StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.pull.*"},
        .storage = .memory,
        .max_msgs = 10,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a pull consumer
    const consumer_config = ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .filter_subject = "test.pull.>",
    };

    var pull_subscription = try js.pullSubscribe(stream_name, consumer_config);
    defer pull_subscription.deinit();

    // Publish some test messages
    try conn.publish("test.pull.msg1", "Hello Pull 1");
    try conn.publish("test.pull.msg2", "Hello Pull 2");
    try conn.publish("test.pull.msg3", "Hello Pull 3");

    // Wait a moment for messages to be stored
    std.time.sleep(100 * std.time.ns_per_ms);

    // Fetch messages
    const fetch_request = FetchRequest{
        .batch = 3,
        .expires = 5_000_000_000, // 5 seconds
        .no_wait = false,
    };

    var batch = try pull_subscription.fetch(fetch_request);
    defer batch.deinit();

    // Verify we got the expected messages
    try testing.expect(!batch.hasError());
    try testing.expect(batch.messages.len == 3);

    // Check message contents
    try testing.expectEqualStrings("Hello Pull 1", batch.messages[0].msg.data);
    try testing.expectEqualStrings("Hello Pull 2", batch.messages[1].msg.data);
    try testing.expectEqualStrings("Hello Pull 3", batch.messages[2].msg.data);

    // Acknowledge messages
    try batch.messages[0].ack();
    try batch.messages[1].ack();
    try batch.messages[2].ack();

    // Clean up
    try js.deleteStream(stream_name);
}

test "JetStream pull consumer fetchNoWait" {
    // Use existing utility functions
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    const stream_name = "TEST_PULL_NOWAIT_STREAM";
    const consumer_name = "test_pull_nowait_consumer";

    // Clean up any existing stream
    js.deleteStream(stream_name) catch {};

    // Create a test stream
    const stream_config = StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.nowait.*"},
        .storage = .memory,
        .max_msgs = 5,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a pull consumer
    const consumer_config = ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .filter_subject = "test.nowait.>",
    };

    var pull_subscription = try js.pullSubscribe(stream_name, consumer_config);
    defer pull_subscription.deinit();

    // Test fetchNoWait with no messages - should return immediately
    var empty_batch = try pull_subscription.fetchNoWait(5);
    defer empty_batch.deinit();

    try testing.expect(empty_batch.messages.len == 0);

    // Now publish some messages
    try conn.publish("test.nowait.msg1", "Hello NoWait 1");
    try conn.publish("test.nowait.msg2", "Hello NoWait 2");

    // Wait for messages to be stored
    std.time.sleep(100 * std.time.ns_per_ms);

    // Fetch with no wait - should get available messages immediately
    var batch = try pull_subscription.fetchNoWait(5);
    defer batch.deinit();

    try testing.expect(!batch.hasError());
    try testing.expect(batch.messages.len == 2);

    // Check message contents
    try testing.expectEqualStrings("Hello NoWait 1", batch.messages[0].msg.data);
    try testing.expectEqualStrings("Hello NoWait 2", batch.messages[1].msg.data);

    // Acknowledge messages
    try batch.messages[0].ack();
    try batch.messages[1].ack();

    // Clean up
    try js.deleteStream(stream_name);
}

test "JetStream pull consumer next() method" {
    // Use existing utility functions
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    const stream_name = "TEST_PULL_NEXT_STREAM";
    const consumer_name = "test_pull_next_consumer";

    // Clean up any existing stream
    js.deleteStream(stream_name) catch {};

    // Create a test stream
    const stream_config = StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.next.*"},
        .storage = .memory,
        .max_msgs = 5,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a pull consumer
    const consumer_config = ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .filter_subject = "test.next.>",
    };

    var pull_subscription = try js.pullSubscribe(stream_name, consumer_config);
    defer pull_subscription.deinit();

    // Publish a test message
    try conn.publish("test.next.single", "Hello Next Message");

    // Wait for message to be stored
    std.time.sleep(100 * std.time.ns_per_ms);

    // Fetch single message using next()
    const timeout_ns = 5_000_000_000; // 5 seconds
    if (try pull_subscription.next(timeout_ns)) |msg| {
        defer msg.deinit();
        
        try testing.expectEqualStrings("Hello Next Message", msg.msg.data);
        try msg.ack();
    } else {
        try testing.expect(false); // Should have received a message
    }

    // Try to get another message (should be none available)
    const no_msg = try pull_subscription.next(1_000_000_000); // 1 second timeout
    try testing.expect(no_msg == null);

    // Clean up
    try js.deleteStream(stream_name);
}