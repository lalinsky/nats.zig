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
const message = @import("message.zig");
const Message = message.Message;
const STATUS_NO_RESPONSE = message.STATUS_NO_RESPONSE;

test "Message owned data lifecycle" {
    const allocator = testing.allocator;

    // Create message that owns its data
    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("test.subject", true);
    try msg.setReply("reply.to", true);
    try msg.setPayload("hello world", true);

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
    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("test.subject", true);
    try msg.setReply("reply.to", true);
    try msg.setPayload("hello world", true);
    try msg.setRawHeaders(raw_headers, true);

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

    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("test", true);
    try msg.setPayload("data", true);

    // Set headers
    try msg.headerSet("Content-Type", "application/json");
    try msg.headerSet("Custom-Header", "value1");

    // Get headers
    const content_type = msg.headerGet("Content-Type");
    try testing.expectEqualStrings("application/json", content_type.?);

    const custom = msg.headerGet("Custom-Header");
    try testing.expectEqualStrings("value1", custom.?);

    // Non-existent header
    const missing = msg.headerGet("Missing");
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

    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("test.subject", true);
    try msg.setPayload("hello world", true);
    try msg.setRawHeaders(raw_headers, true);

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

    try msg.setSubject("test", true);
    try msg.setPayload("", false);

    // Set 503 status
    msg.status_code = STATUS_NO_RESPONSE;

    // Should be detected as no responders
    try testing.expect(msg.isNoResponders());

    // Change status
    msg.status_code = 200;
    try testing.expect(!msg.isNoResponders());

    // Test with data - should not be no responders even with 503
    try msg.setPayload("some data", true);
    msg.status_code = STATUS_NO_RESPONSE;
    try testing.expect(!msg.isNoResponders());
}

test "Message header encoding" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("test", true);
    try msg.setPayload("data", true);

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

    try msg.setSubject("test", true);
    try msg.setPayload("test data", true);

    // Test parsing inline status with description
    const raw_headers = "NATS/1.0 503 No Responders\r\nX-Custom: test\r\n\r\n";
    try msg.setRawHeaders(raw_headers, true);
    try msg.parseHeaders();

    // Status field should be set
    try testing.expectEqual(@as(u16, 503), msg.status_code);

    // Status header should contain just the status code
    const status_header = msg.headerGet("Status");
    try testing.expect(status_header != null);
    try testing.expectEqualStrings("503", status_header.?);

    // Description header should contain the description text
    const description_header = msg.headerGet("Description");
    try testing.expect(description_header != null);
    try testing.expectEqualStrings("No Responders", description_header.?);

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

    // Should NOT contain Status or Description as regular headers
    try testing.expect(std.mem.indexOf(u8, buf.items, "Status: 503\r\n") == null);
    try testing.expect(std.mem.indexOf(u8, buf.items, "Description: No Responders\r\n") == null);
}

test "Message memory patterns" {
    const allocator = testing.allocator;

    // Test that all messages copy their data (using arena)
    var msg1 = Message.init(allocator);
    defer msg1.deinit();
    try msg1.setSubject("test", true);
    try msg1.setReply("reply", true);
    try msg1.setPayload("data", true);

    // Verify data was copied into arena
    try testing.expectEqualStrings("test", msg1.subject);
    try testing.expectEqualStrings("reply", msg1.reply.?);
    try testing.expectEqualStrings("data", msg1.data);
}

test "Message error handling" {
    const allocator = testing.allocator;

    // These should work fine
    var msg = Message.init(allocator);
    defer msg.deinit();
    try msg.setSubject("", false);
    try msg.setPayload("", false);

    // Empty header operations should work
    try msg.headerSet("", "value");
    msg.headerDelete("nonexistent");

    const result = msg.headerGet("");
    try testing.expectEqualStrings("value", result.?);
}
