const std = @import("std");

// Re-export key types and functions
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionOptions = @import("connection.zig").ConnectionOptions;
pub const ConnectionStatus = @import("connection.zig").ConnectionStatus;
pub const ConnectionError = @import("connection.zig").ConnectionError;
pub const Message = @import("connection.zig").Message;
pub const Subscription = @import("connection.zig").Subscription;
pub const inbox = @import("inbox.zig");

// Removed top-level connect functions - use Connection.init() and Connection.connect() directly

// Test basic functionality
test "basic connection lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Test connection creation
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test initial state
    try std.testing.expect(conn.getStatus() == .disconnected);
}