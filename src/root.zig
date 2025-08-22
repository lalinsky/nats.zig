const std = @import("std");

// Re-export key types and functions
pub const Connection = @import("connection2.zig").Connection;
pub const ConnectionOptions = @import("connection2.zig").ConnectionOptions;
pub const ConnectionStatus = @import("connection2.zig").ConnectionStatus;
pub const ConnectionError = @import("connection2.zig").ConnectionError;
pub const Message = @import("connection2.zig").Message;
pub const Subscription = @import("connection2.zig").Subscription;

/// Create a new NATS connection
pub fn connect(allocator: std.mem.Allocator, url: []const u8, options: ConnectionOptions) !Connection {
    var conn = Connection.init(allocator, options);
    try conn.connect(url);
    return conn;
}

/// Create a connection to localhost:4222 with default options
pub fn connectDefault(allocator: std.mem.Allocator) !Connection {
    return connect(allocator, "nats://localhost:4222", .{});
}

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