const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "get message by sequence" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_GET",
        .subjects = &.{"test.msg.get"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish("test.msg.get", "First message");
    try conn.publish("test.msg.get", "Second message");
    try conn.publish("test.msg.get", "Third message");
    try conn.flush();

    // Get message by sequence
    const msg = try js.getMsg("TEST_MSG_GET", 1);
    defer msg.deinit();

    try testing.expectEqualStrings("test.msg.get", msg.value.subject);
    try testing.expectEqualStrings("First message", msg.value.data);
    try testing.expectEqual(@as(u64, 1), msg.value.seq);

    // Get second message
    const msg2 = try js.getMsg("TEST_MSG_GET", 2);
    defer msg2.deinit();

    try testing.expectEqualStrings("Second message", msg2.value.data);
    try testing.expectEqual(@as(u64, 2), msg2.value.seq);
}

test "get last message by subject" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_LAST",
        .subjects = &.{"test.last.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages to different subjects
    try conn.publish("test.last.foo", "First foo");
    try conn.publish("test.last.bar", "First bar");
    try conn.publish("test.last.foo", "Second foo");
    try conn.publish("test.last.bar", "Second bar");
    try conn.publish("test.last.foo", "Third foo"); // This should be the last for foo
    try conn.flush();

    // Get last message by subject
    const last_foo = try js.getLastMsg("TEST_MSG_LAST", "test.last.foo");
    defer last_foo.deinit();

    try testing.expectEqualStrings("test.last.foo", last_foo.value.subject);
    try testing.expectEqualStrings("Third foo", last_foo.value.data);
    try testing.expectEqual(@as(u64, 5), last_foo.value.seq);

    const last_bar = try js.getLastMsg("TEST_MSG_LAST", "test.last.bar");
    defer last_bar.deinit();

    try testing.expectEqualStrings("test.last.bar", last_bar.value.subject);
    try testing.expectEqualStrings("Second bar", last_bar.value.data);
    try testing.expectEqual(@as(u64, 4), last_bar.value.seq);
}

test "delete message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_DELETE",
        .subjects = &.{"test.msg.delete"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish("test.msg.delete", "Message 1");
    try conn.publish("test.msg.delete", "Message 2");
    try conn.publish("test.msg.delete", "Message 3");
    try conn.flush();

    // Verify message exists before deletion
    const msg2_before = try js.getMsg("TEST_MSG_DELETE", 2);
    defer msg2_before.deinit();
    try testing.expectEqualStrings("Message 2", msg2_before.value.data);

    // Delete message 2
    const deleted = try js.deleteMsg("TEST_MSG_DELETE", 2);
    try testing.expect(deleted);

    // Verify other messages still exist
    const msg1_after = try js.getMsg("TEST_MSG_DELETE", 1);
    defer msg1_after.deinit();
    try testing.expectEqualStrings("Message 1", msg1_after.value.data);

    const msg3_after = try js.getMsg("TEST_MSG_DELETE", 3);
    defer msg3_after.deinit();
    try testing.expectEqualStrings("Message 3", msg3_after.value.data);

    // Attempting to get deleted message should fail
    const get_deleted_result = js.getMsg("TEST_MSG_DELETE", 2);
    try testing.expectError(error.JetStreamError, get_deleted_result);
}

test "erase message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_ERASE",
        .subjects = &.{"test.msg.erase"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish("test.msg.erase", "Message 1");
    try conn.publish("test.msg.erase", "Message 2");
    try conn.publish("test.msg.erase", "Message 3");
    try conn.flush();

    // Verify message exists before erasure
    const msg2_before = try js.getMsg("TEST_MSG_ERASE", 2);
    defer msg2_before.deinit();
    try testing.expectEqualStrings("Message 2", msg2_before.value.data);

    // Erase message 2 (securely remove from storage)
    const erased = try js.eraseMsg("TEST_MSG_ERASE", 2);
    try testing.expect(erased);

    // Verify other messages still exist
    const msg1_after = try js.getMsg("TEST_MSG_ERASE", 1);
    defer msg1_after.deinit();
    try testing.expectEqualStrings("Message 1", msg1_after.value.data);

    const msg3_after = try js.getMsg("TEST_MSG_ERASE", 3);
    defer msg3_after.deinit();
    try testing.expectEqualStrings("Message 3", msg3_after.value.data);

    // Attempting to get erased message should fail
    const get_erased_result = js.getMsg("TEST_MSG_ERASE", 2);
    try testing.expectError(error.JetStreamError, get_erased_result);
}

test "get message with headers" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_HEADERS",
        .subjects = &.{"test.msg.headers"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create message with headers
    var msg = try js.nc.newMsg();
    defer msg.deinit();

    try msg.setSubject("test.msg.headers");
    try msg.setPayload("Message with headers");

    try msg.headerSet("X-Test-Header", "test-value");
    try msg.headerSet("X-Another-Header", "another-value");

    // Publish message with headers
    try conn.publishMsg(msg);
    try conn.flush();

    // Get the message back
    const retrieved = try js.getMsg("TEST_MSG_HEADERS", 1);
    defer retrieved.deinit();

    // Verify message properties
    try testing.expectEqualStrings("test.msg.headers", retrieved.value.subject);
    try testing.expectEqualStrings("Message with headers", retrieved.value.data);
    try testing.expectEqual(@as(u64, 1), retrieved.value.seq);

    // Verify headers are preserved (raw header data available in retrieved.value.hdrs)
    try testing.expect(retrieved.value.hdrs != null);
    const headers = retrieved.value.hdrs.?;
    try testing.expect(std.mem.indexOf(u8, headers, "X-Test-Header: test-value") != null);
    try testing.expect(std.mem.indexOf(u8, headers, "X-Another-Header: another-value") != null);
}

test "message operations error cases" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Test with non-existent stream
    const get_nonexistent_stream = js.getMsg("NON_EXISTENT_STREAM", 1);
    try testing.expectError(error.JetStreamError, get_nonexistent_stream);

    // Create stream for other error tests
    const stream_config = nats.StreamConfig{
        .name = "TEST_MSG_ERRORS",
        .subjects = &.{"test.msg.errors"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test getting non-existent message by sequence
    const get_nonexistent_msg = js.getMsg("TEST_MSG_ERRORS", 999);
    try testing.expectError(error.JetStreamError, get_nonexistent_msg);

    // Test getting non-existent message by subject
    const get_nonexistent_subject = js.getLastMsg("TEST_MSG_ERRORS", "non.existent.subject");
    try testing.expectError(error.JetStreamError, get_nonexistent_subject);

    // Test deleting non-existent message
    const delete_result = js.deleteMsg("TEST_MSG_ERRORS", 999);
    try testing.expectError(error.JetStreamError, delete_result);
}
