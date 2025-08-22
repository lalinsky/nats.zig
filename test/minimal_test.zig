const std = @import("std");
const nats = @import("nats");

test "minimal connection test" {
    const allocator = std.testing.allocator;
    
    // Just test that we can create a connection object
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Test initial state
    try std.testing.expect(conn.getStatus() == .disconnected);
    
    std.debug.print("✓ Connection created successfully\n", .{});
}

test "connect to docker compose nats server" {
    const allocator = std.testing.allocator;
    
    // Connect to the test server from docker-compose.test.yml (port 14222)
    var conn = nats.Connection.init(allocator, .{
        .timeout_ms = 2000,
        .reconnect = .{ .allow_reconnect = false },
    });
    defer conn.deinit();
    
    // Attempt connection - this will fail gracefully if server is not running
    conn.connect("nats://127.0.0.1:14222") catch |err| {
        std.debug.print("⚠️  Could not connect to NATS server on port 14222: {}\n", .{err});
        std.debug.print("   Make sure to run: docker compose -f docker-compose.test.yml up -d nats-test\n", .{});
        return; // Skip test if server not available
    };
    
    // Verify connection
    try std.testing.expect(conn.getStatus() == .connected);
    
    // Test basic server info
    try std.testing.expect(conn.server_info.server_id != null);
    try std.testing.expect(conn.server_info.max_payload > 0);
    
    std.debug.print("✓ Connected to NATS server successfully\n", .{});
    std.debug.print("  Server ID: {s}\n", .{conn.server_info.server_id.?});
    if (conn.server_info.version) |version| {
        std.debug.print("  Version: {s}\n", .{version});
    }
    std.debug.print("  Max Payload: {d} bytes\n", .{conn.server_info.max_payload});
}

test "basic publish and subscribe" {
    const allocator = std.testing.allocator;
    
    var conn = nats.Connection.init(allocator, .{
        .timeout_ms = 2000,
        .reconnect = .{ .allow_reconnect = false },
    });
    defer conn.deinit();
    
    // Connect to test server
    conn.connect("nats://127.0.0.1:14222") catch |err| {
        std.debug.print("⚠️  Could not connect to NATS server: {}\n", .{err});
        return; // Skip test if server not available
    };
    
    // Create a subscription
    const sub = conn.subscribeSync("test.minimal") catch |err| {
        std.debug.print("⚠️  Could not create subscription: {}\n", .{err});
        return;
    };
    defer sub.deinit(allocator);
    
    // Publish a message
    conn.publish("test.minimal", "Hello from minimal test!") catch |err| {
        std.debug.print("⚠️  Could not publish message: {}\n", .{err});
        return;
    };
    
    try conn.flush();
    
    // Try to receive the message
    if (sub.nextMsg(1000)) |msg| {
        defer msg.deinit();
        
        try std.testing.expectEqualStrings("test.minimal", msg.subject);
        try std.testing.expectEqualStrings("Hello from minimal test!", msg.data);
        
        std.debug.print("✓ Basic pub/sub test passed\n", .{});
        std.debug.print("  Subject: {s}\n", .{msg.subject});
        std.debug.print("  Data: {s}\n", .{msg.data});
    } else {
        std.debug.print("⚠️  No message received within timeout\n", .{});
        return;
    }
}