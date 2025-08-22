const std = @import("std");
const testing = std.testing;
const Connection = @import("connection.zig").Connection;
const Message = @import("message.zig").Message;

test "subscription basic setup" {
    const allocator = testing.allocator;
    
    // Test connection creation (basic state test)
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test initial state
    try testing.expect(conn.getStatus() == .disconnected);
    
    // Test subscription counter initialization
    try testing.expect(conn.next_sid.load(.monotonic) == 1);
}