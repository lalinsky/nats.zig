const std = @import("std");
const nats = @import("root.zig");
const Connection = nats.Connection;
const ConnectionError = nats.ConnectionError;
const Message = nats.Message;
const Subscription = nats.Subscription;
const inbox = nats.inbox;

test "request reply basic functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test that request fails when disconnected
    const result = conn.request("test.subject", "request data", 1000);
    try std.testing.expectError(ConnectionError.ConnectionClosed, result);
}

test "inbox generation uniqueness" {
    const allocator = std.testing.allocator;
    
    // Generate multiple inboxes and verify they are unique
    var inboxes: [10][]u8 = undefined;
    defer {
        for (inboxes) |inbox_str| {
            allocator.free(inbox_str);
        }
    }
    
    for (0..10) |i| {
        inboxes[i] = try inbox.newInbox(allocator);
        try std.testing.expect(std.mem.startsWith(u8, inboxes[i], "_INBOX."));
        try std.testing.expectEqual(@as(usize, 29), inboxes[i].len);
    }
    
    // Check that all are unique
    for (0..9) |i| {
        for (i+1..10) |j| {
            try std.testing.expect(!std.mem.eql(u8, inboxes[i], inboxes[j]));
        }
    }
}

test "subscription timeout" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const sub = try Subscription.init(allocator, 1, "test.subject", null);
    defer sub.deinit();
    
    // Test immediate timeout (should return null)
    const start = std.time.nanoTimestamp();
    const result = sub.nextMsg(1); // 1ms
    const duration = std.time.nanoTimestamp() - start;
    
    try std.testing.expectEqual(@as(?*Message, null), result);
    try std.testing.expect(duration >= 1_000_000); // At least 1ms passed
}

test "publish request format" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test that publishRequest fails when disconnected
    const result = conn.publishRequest("test.subject", "reply.inbox", "test data");
    try std.testing.expectError(ConnectionError.ConnectionClosed, result);
}

test "unsubscribe functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Create a subscription (but we're not connected, so this should work in memory)
    const sub = try Subscription.init(allocator, 1, "test.subject", null);
    defer sub.deinit();
    
    // Test that unsubscribe fails when disconnected
    const result = conn.unsubscribe(sub);
    try std.testing.expectError(ConnectionError.ConnectionClosed, result);
}