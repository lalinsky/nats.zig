// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const testing = std.testing;
const Message = @import("message.zig").Message;

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
    const content_type = msg.headerGet("Content-Type");
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
    const content_type = msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);

    const custom = msg.headerGet("Custom-Header");
    try testing.expectEqualStrings("value1", custom.?);

    // Non-existent header
    const missing = try msg.headerGet("Missing");
    try testing.expectEqual(@as(?[]const u8, null), missing);

    // Delete header
    msg.headerDelete("Content-Type");
    const deleted = msg.headerGet("Content-Type");
    try testing.expectEqual(@as(?[]const u8, null), deleted);
}

test "Message header parsing" {
    const allocator = testing.allocator;

    // Raw headers as they would come from network
    const raw_headers = "NATS/1.0\r\nContent-Type: application/json\r\nX-Custom: test-value\r\n\r\n";

    const msg = try Message.initWithHeaders(allocator, "test.subject", null, "hello world", raw_headers);
    defer msg.deinit();

    // Parse headers explicitly
    try msg.parseHeaders();

    // Verify headers were parsed
    const content_type = msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);

    // Verify other headers were parsed
    const custom = msg.headerGet("X-Custom");
    try testing.expectEqualStrings("test-value", custom.?);
}

test "Message no responders detection" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setSubject("test");
    try msg.setPayload("");

    // Set 503 status
    msg.status_code = STATUS_NO_RESPONSE;

    // Should be detected as no responders
    try testing.expect(msg.isNoResponders());

    // Change status
    msg.status_code = 200;
    try testing.expect(!msg.isNoResponders());

    // Test with data - should not be no responders even with 503
    try msg.setPayload("some data");
    msg.status_code = STATUS_NO_RESPONSE;
    try testing.expect(!msg.isNoResponders());
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

test "Message status field and header parsing" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setSubject("test");
    try msg.setPayload("test data");

    // Test parsing inline status with description
    const raw_headers = "NATS/1.0 503 No Responders\r\nX-Custom: test\r\n\r\n";
    try msg.setRawHeaders(raw_headers);
    try msg.parseHeaders();

    // Status field should be set
    try testing.expectEqual(@as(u16, 503), msg.status_code);

    // Full status line should be in Status header
    const status_header = msg.headerGet("Status");
    try testing.expect(status_header != null);
    try testing.expectEqualStrings("NATS/1.0 503 No Responders", status_header.?);

    // Other headers should still be parsed
    const custom_header = msg.headerGet("X-Custom");
    try testing.expect(custom_header != null);
    try testing.expectEqualStrings("test", custom_header.?);

    // Test encoding - Status header should be first line
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    try msg.encodeHeaders(buf.writer());

    // Should start with the full status line
    try testing.expect(std.mem.startsWith(u8, buf.items, "NATS/1.0 503 No Responders\r\n"));

    // Should contain other headers
    try testing.expect(std.mem.indexOf(u8, buf.items, "X-Custom: test\r\n") != null);

    // Should NOT contain Status as a regular header
    try testing.expect(std.mem.indexOf(u8, buf.items, "Status: NATS/1.0 503 No Responders\r\n") == null);
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
