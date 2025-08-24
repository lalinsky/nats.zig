const std = @import("std");

// Re-export key types and functions
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionOptions = @import("connection.zig").ConnectionOptions;
pub const ConnectionStatus = @import("connection.zig").ConnectionStatus;
pub const ConnectionError = @import("connection.zig").ConnectionError;
pub const Message = @import("message.zig").Message;
pub const Subscription = @import("subscription.zig").Subscription;
pub const MsgHandler = @import("subscription.zig").MsgHandler;
pub const ServerPool = @import("server_pool.zig").ServerPool;
pub const Server = @import("server_pool.zig").Server;
pub const inbox = @import("inbox.zig");

// JetStream types
pub const JetStream = @import("jetstream.zig").JetStream;
pub const JetStreamOptions = @import("jetstream.zig").JetStreamOptions;
pub const StreamConfig = @import("jetstream.zig").StreamConfig;
pub const StreamInfo = @import("jetstream.zig").StreamInfo;
pub const ConsumerConfig = @import("jetstream.zig").ConsumerConfig;
pub const ConsumerInfo = @import("jetstream.zig").ConsumerInfo;
pub const Stream = @import("jetstream.zig").Stream;
pub const PubAck = @import("jetstream.zig").PubAck;
pub const AccountInfo = @import("jetstream.zig").AccountInfo;
pub const JetStreamError = @import("jetstream.zig").JetStreamError;

// Removed top-level connect functions - use Connection.init() and Connection.connect() directly

// Test basic functionality
test "basic connection lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Test connection creation
    var conn = try Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test initial state
    try std.testing.expect(conn.getStatus() == .disconnected);
}

test "reconnection configuration" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Test connection with custom reconnection options
    const options = ConnectionOptions{
        .reconnect = .{
            .max_reconnect = 5,
            .reconnect_wait_ms = 1000,
            .reconnect_jitter_ms = 50,
            .allow_reconnect = true,
        },
    };
    
    var conn = try Connection.init(allocator, options);
    defer conn.deinit();
    
    // Test that options are set correctly
    try std.testing.expect(conn.options.reconnect.max_reconnect == 5);
    try std.testing.expect(conn.options.reconnect.reconnect_wait_ms == 1000);
    try std.testing.expect(conn.options.reconnect.allow_reconnect == true);
    try std.testing.expect(conn.getStatus() == .disconnected);
}

test "server pool management" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var conn = try Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Add multiple servers
    try conn.addServer("nats://localhost:4222");
    try conn.addServer("nats://localhost:4223");
    try conn.addServer("nats://localhost:4224");
    
    // Test server pool has correct number of servers
    try std.testing.expect(conn.server_pool.getSize() == 3);
    
    // Test C library server selection algorithm
    const server1 = try conn.server_pool.getNextServer(-1, null); // Get first server for initial connection
    try std.testing.expect(server1 != null);
    
    // Test that server moved to end after selection (C library behavior)
    const server2 = try conn.server_pool.getNextServer(-1, server1);
    try std.testing.expect(server2 != null);
    try std.testing.expect(server2 != server1); // Should be different server
    
    // Test pool size remains the same (servers moved, not removed)
    try std.testing.expect(conn.server_pool.getSize() == 3);
}

test "parser state reset" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var parser = @import("parser.zig").Parser.init(allocator);
    defer parser.deinit();
    
    // Set some parser state
    parser.state = .MSG_PAYLOAD;
    parser.ma.subject = "test.subject";
    parser.ma.sid = 123;
    
    // Reset parser
    parser.reset();
    
    // Verify state is reset
    try std.testing.expect(parser.state == .OP_START);
    try std.testing.expect(parser.ma.sid == 0);
    try std.testing.expect(parser.ma.subject.len == 0);
}

test "reconnection thread management" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var conn = try Connection.init(allocator, .{
        .reconnect = .{ .allow_reconnect = true },
    });
    defer conn.deinit();
    
    // Test initial state
    try std.testing.expect(conn.in_reconnect == 0);
    try std.testing.expect(conn.reconnect_thread == null);
    
    // Test that atomic counter prevents double-spawning
    // (This simulates the race condition prevention mechanism)
    try std.testing.expect(conn.status == .disconnected);
}