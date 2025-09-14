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
    const nc = try utils.createDefaultConnection();
    defer utils.closeConnection(nc);

    var js = nc.jetstream(.{});

    const stream_name = "TEST_PULL_STREAM";
    const consumer_name = "test_pull_consumer";

    // Create a test stream
    const stream_config = StreamConfig{
        .name = stream_name,
        .subjects = &[_][]const u8{"test.pull.*"},
        .storage = .memory,
        .max_msgs = 10,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a pull consumer (ack_policy defaults to .explicit)
    var subscription = try js.pullSubscribe("test.pull.*", consumer_name, .{
        .stream = stream_name,
    });
    defer subscription.deinit();

    // Publish some test messages
    try nc.publish("test.pull.msg1", "Hello Pull 1");
    try nc.publish("test.pull.msg2", "Hello Pull 2");
    try nc.publish("test.pull.msg3", "Hello Pull 3");
    try nc.flush();

    // Fetch the messages
    var batch = try subscription.fetch(2, 1000);
    defer batch.deinit();

    // Verify we got the expected messages
    try testing.expectEqual(null, batch.err);
    try testing.expectEqual(2, batch.messages.len);

    // Check message contents
    try testing.expectEqualStrings("Hello Pull 1", batch.messages[0].msg.data);
    try testing.expectEqualStrings("Hello Pull 2", batch.messages[1].msg.data);

    // Acknowledge messages
    try batch.messages[0].ack();
    try batch.messages[1].ack();

    // Fetch the messages
    var batch2 = try subscription.fetch(2, 1000);
    defer batch2.deinit();

    // Verify we got the expected messages
    try testing.expectEqual(null, batch2.err);
    std.log.debug("Received messages: {}", .{batch2.messages.len});
    for (batch2.messages) |msg| {
        std.log.debug("Message: {s}", .{msg.msg.data});
    }
    try testing.expectEqual(1, batch2.messages.len);

    // Check message contents
    try testing.expectEqualStrings("Hello Pull 3", batch2.messages[0].msg.data);

    // Acknowledge messages
    try batch2.messages[0].ack();
}
