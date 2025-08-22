const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create connection with basic options
    const options = nats.ConnectionOptions{
        .name = "basic-test",
        .timeout_ms = 5000,
        .allow_reconnect = false, // Disable reconnection for simpler testing
    };
    
    // Connect to NATS server
    std.log.info("Connecting to NATS server...", .{});
    var conn = nats.connect(allocator, "nats://localhost:4222", options) catch |err| {
        std.log.err("Failed to connect: {}", .{err});
        return;
    };
    defer conn.deinit();
    
    std.log.info("Connected! Status: {}", .{conn.getStatus()});
    
    // Test flush (ping/pong)
    std.log.info("Testing flush/ping...", .{});
    conn.flush() catch |err| {
        std.log.err("Flush failed: {}", .{err});
        return;
    };
    
    std.log.info("Flush successful - ping/pong working", .{});
    
    // Wait a bit then close cleanly
    std.time.sleep(1000 * std.time.ns_per_ms);
    std.log.info("Basic connection test completed successfully!", .{});
}