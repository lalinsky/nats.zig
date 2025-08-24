const std = @import("std");
const nuid = @import("nuid.zig");

/// Default inbox prefix used by NATS clients
const INBOX_PREFIX = "_INBOX.";

/// Generate a unique inbox subject for request/reply pattern
/// Format: _INBOX.<22-char-nuid>
pub fn newInbox(allocator: std.mem.Allocator) ![]u8 {
    const nuid_bytes = nuid.next();
    return try std.fmt.allocPrint(allocator, "{s}{s}", .{ INBOX_PREFIX, nuid_bytes });
}

test "inbox generation" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const inbox1 = try newInbox(allocator);
    defer allocator.free(inbox1);
    
    const inbox2 = try newInbox(allocator);
    defer allocator.free(inbox2);

    // Should start with _INBOX.
    try testing.expect(std.mem.startsWith(u8, inbox1, INBOX_PREFIX));
    try testing.expect(std.mem.startsWith(u8, inbox2, INBOX_PREFIX));
    
    // Should be 29 characters total (7 + 22)
    try testing.expectEqual(@as(usize, 29), inbox1.len);
    try testing.expectEqual(@as(usize, 29), inbox2.len);
    
    // Should be unique
    try testing.expect(!std.mem.eql(u8, inbox1, inbox2));
}

test "nuid generation" {
    const nuid1 = nuid.next();
    const nuid2 = nuid.next();
    
    // Should be 22 characters
    try std.testing.expectEqual(@as(usize, 22), nuid1.len);
    try std.testing.expectEqual(@as(usize, 22), nuid2.len);
    
    // Should be unique (very high probability)
    try std.testing.expect(!std.mem.eql(u8, &nuid1, &nuid2));
}