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
const Message = @import("nats").Message;

test "Message owned data lifecycle" {
    const allocator = testing.allocator;

    // Create message that owns its data
    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setSubject("test.subject");
    try msg.setReply("reply.to");
    try msg.setPayload("hello world");

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
    var msg = try Message.initWithHeaders(allocator, "test.subject", "reply.to", "hello world", raw_headers);
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

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setSubject("test");
    try msg.setPayload("data");

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

test "Message eager header parsing" {
    const allocator = testing.allocator;

    // Raw headers as they would come from network
    const raw_headers = "NATS/1.0\r\nContent-Type: application/json\r\nX-Custom: test-value\r\n\r\n";

    var msg = try Message.initWithHeaders(allocator, "test.subject", null, "hello world", raw_headers);
    defer msg.deinit();

    // Headers are already parsed during initialization
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
    try msg.headerSet("Status", "503");

    // Should be detected as no responders
    try testing.expect(msg.isNoResponders());

    // Change status
    try msg.headerSet("Status", "200");
    try testing.expect(!msg.isNoResponders());
}

test "Message header encoding" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setSubject("test");
    try msg.setPayload("data");

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
    var msg1 = Message.init(allocator);
    defer msg1.deinit();

    try msg1.setSubject("test");
    try msg1.setReply("reply");
    try msg1.setPayload("data");

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

    try msg.setSubject("");
    try msg.setPayload("");

    // Empty header operations should work
    try msg.headerSet("", "value");
    msg.headerDelete("nonexistent");

    const result = msg.headerGet("");
    try testing.expectEqualStrings("value", result.?);
}
