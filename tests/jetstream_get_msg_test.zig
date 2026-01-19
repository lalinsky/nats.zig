const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

test "get message by sequence" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
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
    try conn.publish(subject, "First message");
    try conn.publish(subject, "Second message");
    try conn.publish(subject, "Third message");
    try conn.flush();

    // Get message by sequence
    const msg = try js.getMsg(stream_name, .{ .seq = 1 });
    defer msg.deinit();

    try testing.expectEqualStrings(subject, msg.subject);
    try testing.expectEqualStrings("First message", msg.data);
    try testing.expectEqual(@as(u64, 1), msg.seq);

    // Get second message
    const msg2 = try js.getMsg(stream_name, .{ .seq = 2 });
    defer msg2.deinit();

    try testing.expectEqualStrings("Second message", msg2.data);
    try testing.expectEqual(@as(u64, 2), msg2.seq);
}

test "get last message by subject" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subjects
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);

    const base_subject = try std.fmt.allocPrint(testing.allocator, "{s}.test", .{stream_name});
    defer testing.allocator.free(base_subject);

    const foo_subject = try std.fmt.allocPrint(testing.allocator, "{s}.foo", .{base_subject});
    defer testing.allocator.free(foo_subject);

    const bar_subject = try std.fmt.allocPrint(testing.allocator, "{s}.bar", .{base_subject});
    defer testing.allocator.free(bar_subject);

    const wildcard_subject = try std.fmt.allocPrint(testing.allocator, "{s}.*", .{base_subject});
    defer testing.allocator.free(wildcard_subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{wildcard_subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages to different subjects
    try conn.publish(foo_subject, "First foo");
    try conn.publish(bar_subject, "First bar");
    try conn.publish(foo_subject, "Second foo");
    try conn.publish(bar_subject, "Second bar");
    try conn.publish(foo_subject, "Third foo - last");
    try conn.flush();

    // Get last message for foo subject
    const last_foo = try js.getMsg(stream_name, .{ .last_by_subj = foo_subject });
    defer last_foo.deinit();

    try testing.expectEqualStrings(foo_subject, last_foo.subject);
    try testing.expectEqualStrings("Third foo - last", last_foo.data);
    try testing.expectEqual(@as(u64, 5), last_foo.seq);

    // Get last message for bar subject
    const last_bar = try js.getMsg(stream_name, .{ .last_by_subj = bar_subject });
    defer last_bar.deinit();

    try testing.expectEqualStrings(bar_subject, last_bar.subject);
    try testing.expectEqualStrings("Second bar", last_bar.data);
    try testing.expectEqual(@as(u64, 4), last_bar.seq);
}

test "get next message by subject" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subjects
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);

    const base_subject = try std.fmt.allocPrint(testing.allocator, "{s}.next", .{stream_name});
    defer testing.allocator.free(base_subject);

    const target_subject = try std.fmt.allocPrint(testing.allocator, "{s}.target", .{base_subject});
    defer testing.allocator.free(target_subject);

    const other_subject = try std.fmt.allocPrint(testing.allocator, "{s}.other", .{base_subject});
    defer testing.allocator.free(other_subject);

    const wildcard_subject = try std.fmt.allocPrint(testing.allocator, "{s}.*", .{base_subject});
    defer testing.allocator.free(wildcard_subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{wildcard_subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish interleaved messages
    try conn.publish(other_subject, "other 1"); // seq 1
    try conn.publish(target_subject, "target 1"); // seq 2
    try conn.publish(other_subject, "other 2"); // seq 3
    try conn.publish(target_subject, "target 2"); // seq 4
    try conn.publish(other_subject, "other 3"); // seq 5
    try conn.publish(target_subject, "target 3"); // seq 6
    try conn.flush();

    // Get first target message at or after sequence 1
    const next_target_1 = try js.getMsg(stream_name, .{ .seq = 1, .next_by_subj = target_subject });
    defer next_target_1.deinit();

    try testing.expectEqualStrings(target_subject, next_target_1.subject);
    try testing.expectEqualStrings("target 1", next_target_1.data);
    try testing.expectEqual(@as(u64, 2), next_target_1.seq);

    // Get first target message at or after sequence 3
    const next_target_3 = try js.getMsg(stream_name, .{ .seq = 3, .next_by_subj = target_subject });
    defer next_target_3.deinit();

    try testing.expectEqualStrings(target_subject, next_target_3.subject);
    try testing.expectEqualStrings("target 2", next_target_3.data);
    try testing.expectEqual(@as(u64, 4), next_target_3.seq);

    // Get first target message at or after sequence 5
    const next_target_5 = try js.getMsg(stream_name, .{ .seq = 5, .next_by_subj = target_subject });
    defer next_target_5.deinit();

    try testing.expectEqualStrings(target_subject, next_target_5.subject);
    try testing.expectEqualStrings("target 3", next_target_5.data);
    try testing.expectEqual(@as(u64, 6), next_target_5.seq);
}

test "get message with headers" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
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

    // Create message with headers
    var msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject(subject, true);
    try msg.setPayload("Message with headers", true);
    try msg.headerSet("X-Custom-Header", "custom-value");
    try msg.headerSet("X-Test-ID", "12345");

    // Publish the message
    const pub_ack = try js.publishMsg(msg, .{});
    defer pub_ack.deinit();

    // Get the message back
    const retrieved = try js.getMsg(stream_name, .{ .seq = 1 });
    defer retrieved.deinit();

    try testing.expectEqualStrings(subject, retrieved.subject);
    try testing.expectEqualStrings("Message with headers", retrieved.data);
    try testing.expectEqual(@as(u64, 1), retrieved.seq);

    // Check headers
    const custom_header = retrieved.headerGet("X-Custom-Header");
    try testing.expect(custom_header != null);
    try testing.expectEqualStrings("custom-value", custom_header.?);

    const test_id = retrieved.headerGet("X-Test-ID");
    try testing.expect(test_id != null);
    try testing.expectEqualStrings("12345", test_id.?);
}

test "message operations error cases" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Test with non-existent stream
    const get_nonexistent_stream = js.getMsg("NON_EXISTENT_STREAM", .{ .seq = 1 });
    try testing.expectError(nats.JetStreamError.StreamNotFound, get_nonexistent_stream);

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

    // Test getting non-existent message by sequence
    const get_nonexistent_msg = js.getMsg(stream_name, .{ .seq = 999 });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_nonexistent_msg);

    // Test getting non-existent message by subject
    const get_nonexistent_subject = js.getMsg(stream_name, .{ .last_by_subj = "non.existent.subject" });
    try testing.expectError(nats.JetStreamError.NoMessageFound, get_nonexistent_subject);
}

test "invalid option combinations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);

    // Test: seq + last_by_subj combination should fail
    const result1 = js.getMsg(stream_name, .{ .seq = 1, .last_by_subj = "test.subject" });
    try testing.expectError(error.InvalidGetMessageOptions, result1);

    // Test: next_by_subj without seq should fail
    const result2 = js.getMsg(stream_name, .{ .next_by_subj = "test.subject" });
    try testing.expectError(error.InvalidGetMessageOptions, result2);

    // Test: last_by_subj + next_by_subj should fail
    const result3 = js.getMsg(stream_name, .{ .last_by_subj = "test.subject", .next_by_subj = "test.subject" });
    try testing.expectError(error.InvalidGetMessageOptions, result3);

    // Test: no options should fail
    const result4 = js.getMsg(stream_name, .{});
    try testing.expectError(error.InvalidGetMessageOptions, result4);

    // Test: all three options should fail
    const result5 = js.getMsg(stream_name, .{ .seq = 1, .last_by_subj = "test.subject", .next_by_subj = "test.subject" });
    try testing.expectError(error.InvalidGetMessageOptions, result5);
}

test "getMsgs stub returns not implemented" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique stream name
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);

    // getMsgs should return NotImplemented error
    const result = js.getMsgs(stream_name, .{});
    try testing.expectError(error.NotImplemented, result);
}
