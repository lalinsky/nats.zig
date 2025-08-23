const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.testing);

test "publish and receive message with headers" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.headers");
    defer sub.deinit();

    // Create message with headers
    const msg = try nats.Message.initWithHeaders(std.testing.allocator, "test.headers", null, "Hello with headers!", "NATS/1.0\r\nX-Test-Key: test-value\r\nX-Another-Key: another-value\r\n\r\n");
    defer msg.deinit();

    // Publish the message with headers
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    if (sub.nextMsg(1000)) |received_msg| {
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
    } else {
        return error.NoMessageReceived;
    }
}

test "publish message without headers using publishMsg" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create a subscription
    const sub = try conn.subscribeSync("test.no-headers");
    defer sub.deinit();

    // Create message without headers
    const msg = try nats.Message.init(std.testing.allocator, "test.no-headers", null, "Hello without headers!");
    defer msg.deinit();

    // Publish the message (should use regular PUB, not HPUB)
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    if (sub.nextMsg(1000)) |received_msg| {
        defer received_msg.deinit();

        // Verify basic message properties
        try std.testing.expectEqualStrings("test.no-headers", received_msg.subject);
        try std.testing.expectEqualStrings("Hello without headers!", received_msg.data);

        log.info("No headers test passed - received message without headers", .{});
    } else {
        return error.NoMessageReceived;
    }
}

test "header manipulation API" {
    // Test header manipulation on a message
    const msg = try nats.Message.init(std.testing.allocator, "test.subject", null, "test data");
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

    // Create message with reply and headers
    const msg = try nats.Message.initWithHeaders(std.testing.allocator, "test.reply-headers", "reply.subject", "Request with headers", "NATS/1.0\r\nRequest-ID: 12345\r\nContent-Type: text/plain\r\n\r\n");
    defer msg.deinit();

    // Publish the message
    try conn.publishMsg(msg);
    try conn.flush();

    // Receive the message
    if (sub.nextMsg(1000)) |received_msg| {
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
    } else {
        return error.NoMessageReceived;
    }
}

test "no responders header detection" {
    // Create a message that simulates "no responders" response
    const msg = try nats.Message.initWithHeaders(std.testing.allocator, "test.subject", null, "", // Empty data for no responders
        "NATS/1.0\r\nStatus: 503 No Responders Available\r\n\r\n");
    defer msg.deinit();

    // Test no responders detection
    const is_no_responders = try msg.isNoResponders();
    try std.testing.expect(is_no_responders);

    log.info("No responders detection test passed", .{});
}
