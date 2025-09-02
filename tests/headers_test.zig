const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "publish and receive message with headers" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.headers");
    defer sub.deinit();

    var msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject("test.headers");
    try msg.setPayload("Hello with headers!");
    try msg.headerSet("X-Test-Key", "test-value");
    try msg.headerSet("X-Another-Key", "another-value");

    // Publish the message with headers
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    var received_msg = sub.nextMsg(1000) catch return error.NoMessageReceived;
    defer received_msg.deinit();

    // Verify basic message properties
    try std.testing.expectEqualStrings("test.headers", received_msg.subject);
    try std.testing.expectEqualStrings("Hello with headers!", received_msg.data);

    // Verify headers
    const test_key_value = try received_msg.headerGet("X-Test-Key");
    try std.testing.expect(test_key_value != null);
    try std.testing.expectEqualStrings("test-value", test_key_value.?);

    const another_key_value = try received_msg.headerGet("X-Another-Key");
    try std.testing.expect(another_key_value != null);
    try std.testing.expectEqualStrings("another-value", another_key_value.?);

    log.info("Headers test passed - received message with headers", .{});
}

test "publish message without headers using publishMsg" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.no-headers");
    defer sub.deinit();

    var msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject("test.no-headers");
    try msg.setPayload("Hello without headers!");

    // Publish the message (should use regular PUB, not HPUB)
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    var received_msg = sub.nextMsg(1000) catch return error.NoMessageReceived;
    defer received_msg.deinit();

    // Verify basic message properties
    try std.testing.expectEqualStrings("test.no-headers", received_msg.subject);
    try std.testing.expectEqualStrings("Hello without headers!", received_msg.data);

    log.info("No headers test passed - received message without headers", .{});
}

test "header manipulation API" {
    // Test header manipulation on a message
    var msg = nats.Message.init(std.testing.allocator);
    defer msg.deinit();

    // Set headers
    try msg.headerSet("Content-Type", "application/json");
    try msg.headerSet("X-Custom-Header", "custom-value");

    // Get headers
    const content_type = try msg.headerGet("Content-Type");
    try std.testing.expect(content_type != null);
    try std.testing.expectEqualStrings("application/json", content_type.?);

    const custom_header = try msg.headerGet("X-Custom-Header");
    try std.testing.expect(custom_header != null);
    try std.testing.expectEqualStrings("custom-value", custom_header.?);

    // Test non-existent header
    const non_existent = try msg.headerGet("Non-Existent-Header");
    try std.testing.expect(non_existent == null);

    // Delete a header
    try msg.headerDelete("X-Custom-Header");
    const deleted_header = try msg.headerGet("X-Custom-Header");
    try std.testing.expect(deleted_header == null);

    // Content-Type should still exist
    const still_exists = try msg.headerGet("Content-Type");
    try std.testing.expect(still_exists != null);
    try std.testing.expectEqualStrings("application/json", still_exists.?);

    log.info("Header manipulation test passed", .{});
}

test "message with reply and headers" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.reply-headers");
    defer sub.deinit();

    var msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject("test.reply-headers");
    try msg.setReply("reply.subject");
    try msg.setPayload("Request with headers");
    try msg.headerSet("Request-ID", "12345");
    try msg.headerSet("Content-Type", "text/plain");

    // Publish the message
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    var received_msg = sub.nextMsg(1000) catch return error.NoMessageReceived;
    defer received_msg.deinit();

    // Verify message properties
    try std.testing.expectEqualStrings("test.reply-headers", received_msg.subject);
    try std.testing.expectEqualStrings("reply.subject", received_msg.reply.?);
    try std.testing.expectEqualStrings("Request with headers", received_msg.data);

    // Verify headers
    const request_id = try received_msg.headerGet("Request-ID");
    try std.testing.expect(request_id != null);
    try std.testing.expectEqualStrings("12345", request_id.?);

    const content_type = try received_msg.headerGet("Content-Type");
    try std.testing.expect(content_type != null);
    try std.testing.expectEqualStrings("text/plain", content_type.?);

    log.info("Reply with headers test passed", .{});
}

test "no responders header detection" {
    // Create a message that simulates "no responders" response
    var msg = nats.Message.init(std.testing.allocator);
    defer msg.deinit();

    try msg.setRawHeaders("NATS/1.0 503\r\n");

    // Test no responders detection
    const is_no_responders = msg.isNoResponders();
    try std.testing.expect(is_no_responders);

    log.info("No responders detection test passed", .{});
}
