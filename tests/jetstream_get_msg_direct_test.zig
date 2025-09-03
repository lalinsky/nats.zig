const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "get message by sequence with direct API" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream with allow_direct enabled
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .allow_direct = true,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test messages
    try conn.publish(subject, "Direct message 1");
    try conn.publish(subject, "Direct message 2");
    try conn.flush();

    // Get message by sequence (direct API)
    const msg1 = try js.getMsg(stream_name, .{ .seq = 1, .direct = true });
    defer msg1.deinit();

    try testing.expectEqualStrings(subject, msg1.subject);
    try testing.expectEqualStrings("Direct message 1", msg1.data);

    // Get second message (direct API)
    const msg2 = try js.getMsg(stream_name, .{ .seq = 2, .direct = true });
    defer msg2.deinit();

    try testing.expectEqualStrings("Direct message 2", msg2.data);
}

test "get last message by subject with direct API" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subjects
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    
    const base_subject = try std.fmt.allocPrint(testing.allocator, "{s}.direct", .{stream_name});
    defer testing.allocator.free(base_subject);
    
    const foo_subject = try std.fmt.allocPrint(testing.allocator, "{s}.foo", .{base_subject});
    defer testing.allocator.free(foo_subject);
    
    const bar_subject = try std.fmt.allocPrint(testing.allocator, "{s}.bar", .{base_subject});
    defer testing.allocator.free(bar_subject);

    const wildcard_subject = try std.fmt.allocPrint(testing.allocator, "{s}.*", .{base_subject});
    defer testing.allocator.free(wildcard_subject);

    // Create stream with allow_direct enabled
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{wildcard_subject},
        .allow_direct = true,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages to different subjects
    try conn.publish(foo_subject, "Direct foo 1");
    try conn.publish(bar_subject, "Direct bar 1");
    try conn.publish(foo_subject, "Direct foo 2");
    try conn.publish(bar_subject, "Direct bar 2");
    try conn.publish(foo_subject, "Direct foo 3 - last");
    try conn.flush();

    // Get last message for foo subject (direct API)
    const last_foo = try js.getMsg(stream_name, .{ .last_by_subj = foo_subject, .direct = true });
    defer last_foo.deinit();

    try testing.expectEqualStrings(foo_subject, last_foo.subject);
    try testing.expectEqualStrings("Direct foo 3 - last", last_foo.data);

    // Get last message for bar subject (direct API)
    const last_bar = try js.getMsg(stream_name, .{ .last_by_subj = bar_subject, .direct = true });
    defer last_bar.deinit();

    try testing.expectEqualStrings(bar_subject, last_bar.subject);
    try testing.expectEqualStrings("Direct bar 2", last_bar.data);
}

test "get next message by subject with direct API" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subjects  
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    
    const base_subject = try std.fmt.allocPrint(testing.allocator, "{s}.direct", .{stream_name});
    defer testing.allocator.free(base_subject);
    
    const target_subject = try std.fmt.allocPrint(testing.allocator, "{s}.target", .{base_subject});
    defer testing.allocator.free(target_subject);
    
    const other_subject = try std.fmt.allocPrint(testing.allocator, "{s}.other", .{base_subject});
    defer testing.allocator.free(other_subject);

    const wildcard_subject = try std.fmt.allocPrint(testing.allocator, "{s}.*", .{base_subject});
    defer testing.allocator.free(wildcard_subject);

    // Create stream with allow_direct enabled
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{wildcard_subject},
        .allow_direct = true,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish interleaved messages
    try conn.publish(other_subject, "other 1");      // seq 1
    try conn.publish(target_subject, "direct 1");    // seq 2
    try conn.publish(other_subject, "other 2");      // seq 3
    try conn.publish(target_subject, "direct 2");    // seq 4
    try conn.flush();

    // Get first target message at or after sequence 1 (direct API)
    const next_target = try js.getMsg(stream_name, .{ .seq = 1, .next_by_subj = target_subject, .direct = true });
    defer next_target.deinit();

    try testing.expectEqualStrings(target_subject, next_target.subject);
    try testing.expectEqualStrings("direct 1", next_target.data);

    // Get first target message at or after sequence 3 (direct API)
    const next_target_3 = try js.getMsg(stream_name, .{ .seq = 3, .next_by_subj = target_subject, .direct = true });
    defer next_target_3.deinit();

    try testing.expectEqualStrings(target_subject, next_target_3.subject);
    try testing.expectEqualStrings("direct 2", next_target_3.data);
}

test "get message with headers using direct API" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream with allow_direct enabled
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .allow_direct = true,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create message with headers
    var msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject(subject);
    try msg.setPayload("Direct message with headers");
    try msg.headerSet("X-Direct-Header", "direct-value");
    try msg.headerSet("X-Test-Mode", "direct");

    // Publish the message
    _ = try js.publishMsg(msg, .{});

    // Get the message back using direct API
    const retrieved = try js.getMsg(stream_name, .{ .seq = 1, .direct = true });
    defer retrieved.deinit();

    try testing.expectEqualStrings(subject, retrieved.subject);
    try testing.expectEqualStrings("Direct message with headers", retrieved.data);

    // Check original headers
    const direct_header = try retrieved.headerGet("X-Direct-Header");
    try testing.expect(direct_header != null);
    try testing.expectEqualStrings("direct-value", direct_header.?);

    const test_mode = try retrieved.headerGet("X-Test-Mode");
    try testing.expect(test_mode != null);
    try testing.expectEqualStrings("direct", test_mode.?);

    // Check JetStream headers added by direct get
    const nats_stream = try retrieved.headerGet("Nats-Stream");
    try testing.expect(nats_stream != null);
    try testing.expectEqualStrings(stream_name, nats_stream.?);

    const nats_sequence = try retrieved.headerGet("Nats-Sequence");
    try testing.expect(nats_sequence != null);
    try testing.expectEqualStrings("1", nats_sequence.?);

    const nats_subject = try retrieved.headerGet("Nats-Subject");
    try testing.expect(nats_subject != null);
    try testing.expectEqualStrings(subject, nats_subject.?);
}

test "direct API without allow_direct should fail" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream WITHOUT allow_direct
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .allow_direct = false,  // Explicitly disable
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish test message
    try conn.publish(subject, "Test message");
    try conn.flush();

    // Try to use direct API - should get no responders since no direct get responder is listening
    const result = js.getMsg(stream_name, .{ .seq = 1, .direct = true });
    try testing.expectError(error.NoResponders, result);
}

test "direct API error cases" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique stream name and subject
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream with allow_direct enabled
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .allow_direct = true,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish one message
    try conn.publish(subject, "Test message");
    try conn.flush();

    // Test getting non-existent message by sequence (direct API)
    const get_nonexistent_msg = js.getMsg(stream_name, .{ .seq = 999, .direct = true });
    try testing.expectError(error.MessageNotFound, get_nonexistent_msg);

    // Test getting non-existent message by subject (direct API)
    const get_nonexistent_subject = js.getMsg(stream_name, .{ .last_by_subj = "non.existent.subject", .direct = true });
    try testing.expectError(error.MessageNotFound, get_nonexistent_subject);
}