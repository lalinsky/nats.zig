const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "purge stream" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_STREAM",
        .subjects = &.{"test.purge.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish some messages to the stream
    try conn.publish("test.purge.msg1", "First message");
    try conn.publish("test.purge.msg2", "Second message");
    try conn.publish("test.purge.msg3", "Third message");

    // Flush to ensure messages are sent to the server
    try conn.flush();

    // Test basic purge (purge all messages)
    const purge_request = nats.StreamPurgeRequest{};
    var purge_result = try js.purgeStream("TEST_PURGE_STREAM", purge_request);
    defer purge_result.deinit();

    // Verify purge was successful and messages were removed
    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged > 0);
}

test "purge stream with filter" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_FILTER_STREAM",
        .subjects = &.{"test.filter.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages with different subjects
    try conn.publish("test.filter.keep", "Keep this message");
    try conn.publish("test.filter.purge", "Purge this message");
    try conn.publish("test.filter.purge", "Purge this message too");

    // Flush to ensure messages are sent to the server
    try conn.flush();

    // Test purge with filter (only purge messages with "purge" subject)
    const purge_request = nats.StreamPurgeRequest{
        .filter = "test.filter.purge",
    };
    var purge_result = try js.purgeStream("TEST_PURGE_FILTER_STREAM", purge_request);
    defer purge_result.deinit();

    // Should have purged 2 messages with the matching filter
    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 2);
}

test "purge stream with sequence limit" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_SEQ_STREAM",
        .subjects = &.{"test.seq.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages
    try conn.publish("test.seq.msg", "Message 1");
    try conn.publish("test.seq.msg", "Message 2");
    try conn.publish("test.seq.msg", "Message 3");
    try conn.publish("test.seq.msg", "Message 4");

    // Flush to ensure messages are sent to the server
    try conn.flush();

    // Test purge up to sequence 3 (should purge messages 1 and 2)
    const purge_request = nats.StreamPurgeRequest{
        .seq = 3,
    };
    var purge_result = try js.purgeStream("TEST_PURGE_SEQ_STREAM", purge_request);
    defer purge_result.deinit();

    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 2);
}

test "purge stream with keep parameter" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_KEEP_STREAM",
        .subjects = &.{"test.keep.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages
    try conn.publish("test.keep.msg", "Message 1");
    try conn.publish("test.keep.msg", "Message 2");
    try conn.publish("test.keep.msg", "Message 3");
    try conn.publish("test.keep.msg", "Message 4");
    try conn.publish("test.keep.msg", "Message 5");

    // Flush to ensure messages are sent to the server
    try conn.flush();

    // Test purge with keep=2 (should keep the 2 most recent messages)
    const purge_request = nats.StreamPurgeRequest{
        .keep = 2,
    };
    var purge_result = try js.purgeStream("TEST_PURGE_KEEP_STREAM", purge_request);
    defer purge_result.deinit();

    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 3);
}