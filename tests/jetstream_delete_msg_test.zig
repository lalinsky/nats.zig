const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "delete message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish(subject, "Message 1");
    try conn.publish(subject, "Message 2");
    try conn.publish(subject, "Message 3");
    try conn.flush();

    // Verify message exists before deletion
    const msg2_before = try js.getMsg(stream_name, .{ .seq = 2 });
    defer msg2_before.deinit();
    try testing.expectEqualStrings("Message 2", msg2_before.data);

    // Delete message 2
    const deleted = try js.deleteMsg(stream_name, 2);
    try testing.expect(deleted);

    // Verify other messages still exist
    const msg1_after = try js.getMsg(stream_name, .{ .seq = 1 });
    defer msg1_after.deinit();
    try testing.expectEqualStrings("Message 1", msg1_after.data);

    const msg3_after = try js.getMsg(stream_name, .{ .seq = 3 });
    defer msg3_after.deinit();
    try testing.expectEqualStrings("Message 3", msg3_after.data);

    // Attempting to get deleted message should fail
    const get_deleted_result = js.getMsg(stream_name, .{ .seq = 2 });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_deleted_result);
}

test "erase message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish(subject, "Message 1");
    try conn.publish(subject, "Message 2");
    try conn.publish(subject, "Message 3");
    try conn.flush();

    // Verify message exists before erasure
    const msg2_before = try js.getMsg(stream_name, .{ .seq = 2 });
    defer msg2_before.deinit();
    try testing.expectEqualStrings("Message 2", msg2_before.data);

    // Erase message 2 (securely remove from storage)
    const erased = try js.eraseMsg(stream_name, 2);
    try testing.expect(erased);

    // Verify other messages still exist
    const msg1_after = try js.getMsg(stream_name, .{ .seq = 1 });
    defer msg1_after.deinit();
    try testing.expectEqualStrings("Message 1", msg1_after.data);

    const msg3_after = try js.getMsg(stream_name, .{ .seq = 3 });
    defer msg3_after.deinit();
    try testing.expectEqualStrings("Message 3", msg3_after.data);

    // Attempting to get erased message should fail
    const get_erased_result = js.getMsg(stream_name, .{ .seq = 2 });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_erased_result);
}

test "delete vs erase message behavior" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish(subject, "Message 1");
    try conn.publish(subject, "Message 2");
    try conn.publish(subject, "Message 3");
    try conn.publish(subject, "Message 4");
    try conn.flush();

    // Test deleteMsg (marks as deleted, doesn't erase from storage)
    const deleted = try js.deleteMsg(stream_name, 2);
    try testing.expect(deleted);

    // Test eraseMsg (securely removes from storage)
    const erased = try js.eraseMsg(stream_name, 3);
    try testing.expect(erased);

    // Verify other messages still exist
    const msg1_after = try js.getMsg(stream_name, .{ .seq = 1 });
    defer msg1_after.deinit();
    try testing.expectEqualStrings("Message 1", msg1_after.data);

    const msg4_after = try js.getMsg(stream_name, .{ .seq = 4 });
    defer msg4_after.deinit();
    try testing.expectEqualStrings("Message 4", msg4_after.data);

    // Both deleted and erased messages should be inaccessible
    const get_deleted_result = js.getMsg(stream_name, .{ .seq = 2 });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_deleted_result);

    const get_erased_result = js.getMsg(stream_name, .{ .seq = 3 });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_erased_result);
}

test "delete message error cases" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Test with non-existent stream
    const delete_nonexistent_stream = js.deleteMsg("NON_EXISTENT_STREAM", 1);
    try testing.expectError(nats.JetStreamError.StreamNotFound, delete_nonexistent_stream);

    // Create stream for further tests
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish one message
    try conn.publish(subject, "Test message");
    try conn.flush();

    // Test deleting non-existent message
    const delete_nonexistent_msg = js.deleteMsg(stream_name, 999);
    try testing.expectError(nats.JetStreamError.StreamMsgDeleteFailed, delete_nonexistent_msg);

    // Test erasing non-existent message
    const erase_nonexistent_msg = js.eraseMsg(stream_name, 999);
    try testing.expectError(nats.JetStreamError.StreamMsgDeleteFailed, erase_nonexistent_msg);
}

test "erase message error cases" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);

    // Test with non-existent stream
    const erase_nonexistent_stream = js.eraseMsg("NON_EXISTENT_STREAM", 1);
    try testing.expectError(nats.JetStreamError.StreamNotFound, erase_nonexistent_stream);
}
