const std = @import("std");
const testing = std.testing;
const Message = @import("message_simple.zig").Message;

test "Message owned data lifecycle" {
    const allocator = testing.allocator;
    
    // Create message that owns its data
    const msg = try Message.init(allocator, "test.subject", "reply.to", "hello world");
    defer msg.deinit();
    
    // Verify data
    try testing.expectEqualStrings("test.subject", msg.subject);
    try testing.expectEqualStrings("reply.to", msg.reply.?);
    try testing.expectEqualStrings("hello world", msg.data);
    try testing.expectEqual(@as(usize, 11), msg.data.len);
}

test "Message with headers" {
    const allocator = testing.allocator;
    
    // Raw headers as they would come from network
    const raw_headers = "NATS/1.0\r\nContent-Type: application/json\r\nX-Custom: test-value\r\n\r\n";
    
    // Create message with headers
    const msg = try Message.initWithHeaders(allocator, "test.subject", "reply.to", "hello world", raw_headers);
    defer msg.deinit();
    
    // Verify data was copied (not same pointers)
    try testing.expectEqualStrings("test.subject", msg.subject);
    try testing.expectEqualStrings("reply.to", msg.reply.?);
    try testing.expectEqualStrings("hello world", msg.data);
    
    // Verify headers
    const content_type = try msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);
}

test "Message header management" {
    const allocator = testing.allocator;
    
    const msg = try Message.init(allocator, "test", null, "data");
    defer msg.deinit();
    
    // Set headers
    try msg.headerSet("Content-Type", "application/json");
    try msg.headerSet("Custom-Header", "value1");
    
    // Get headers
    const content_type = try msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);
    
    const custom = try msg.headerGet("Custom-Header");
    try testing.expectEqualStrings("value1", custom.?);
    
    // Non-existent header
    const missing = try msg.headerGet("Missing");
    try testing.expectEqual(@as(?[]const u8, null), missing);
    
    // Delete header
    try msg.headerDelete("Content-Type");
    const deleted = try msg.headerGet("Content-Type");
    try testing.expectEqual(@as(?[]const u8, null), deleted);
}

test "Message lazy header parsing" {
    const allocator = testing.allocator;
    
    // Raw headers as they would come from network
    const raw_headers = "NATS/1.0\r\nContent-Type: application/json\r\nX-Custom: test-value\r\n\r\n";
    
    const msg = try Message.initWithHeaders(allocator, "test.subject", null, "hello world", raw_headers);
    defer msg.deinit();
    
    // Headers should need parsing initially
    try testing.expect(msg.needs_header_parsing);
    
    // First access should parse headers
    const content_type = try msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);
    
    // Should no longer need parsing
    try testing.expect(!msg.needs_header_parsing);
    
    // Verify other headers were parsed
    const custom = try msg.headerGet("X-Custom");
    try testing.expectEqualStrings("test-value", custom.?);
}

test "Message no responders detection" {
    const allocator = testing.allocator;
    
    const msg = try Message.init(allocator, "test", null, "");
    defer msg.deinit();
    
    // Set 503 status
    try msg.headerSet("Status", "503");
    
    // Should be detected as no responders
    try testing.expect(try msg.isNoResponders());
    
    // Change status
    try msg.headerSet("Status", "200");
    try testing.expect(!try msg.isNoResponders());
}

test "Message header encoding" {
    const allocator = testing.allocator;
    
    const msg = try Message.init(allocator, "test", null, "data");
    defer msg.deinit();
    
    try msg.headerSet("Content-Type", "application/json");
    try msg.headerSet("X-Custom", "value");
    
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();
    
    try msg.encodeHeaders(buf.writer());
    
    // Should start with NATS/1.0
    try testing.expect(std.mem.startsWith(u8, buf.items, "NATS/1.0\r\n"));
    
    // Should contain headers
    try testing.expect(std.mem.indexOf(u8, buf.items, "Content-Type: application/json\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, buf.items, "X-Custom: value\r\n") != null);
    
    // Should end with double CRLF
    try testing.expect(std.mem.endsWith(u8, buf.items, "\r\n\r\n"));
}


test "Message memory patterns" {
    const allocator = testing.allocator;
    
    // Test that all messages copy their data (using arena)
    const msg1 = try Message.init(allocator, "test", "reply", "data");
    defer msg1.deinit();
    
    // Verify data was copied into arena
    try testing.expectEqualStrings("test", msg1.subject);
    try testing.expectEqualStrings("reply", msg1.reply.?);
    try testing.expectEqualStrings("data", msg1.data);
}

test "Message error handling" {
    const allocator = testing.allocator;
    
    // These should work fine
    const msg = try Message.init(allocator, "", null, "");
    defer msg.deinit();
    
    // Empty header operations should work
    try msg.headerSet("", "value");
    try msg.headerDelete("nonexistent");
    
    const result = try msg.headerGet("");
    try testing.expectEqualStrings("value", result.?);
}