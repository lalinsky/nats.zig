const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create connection with custom options
    const options = nats.ConnectionOptions{
        .name = "example-client",
        .timeout_ms = 5000,
        .verbose = false,
        .send_asap = false, // Use flusher thread for batched writes
    };
    
    // Connect to NATS server
    std.log.info("Connecting to NATS server...", .{});
    var conn = nats.connect(allocator, "nats://localhost:4222", options) catch |err| {
        std.log.err("Failed to connect: {}", .{err});
        return;
    };
    defer conn.deinit();
    
    std.log.info("Connected! Status: {}", .{conn.getStatus()});
    
    // Subscribe to a subject
    std.log.info("Subscribing to 'test.subject'...", .{});
    const sub = conn.subscribe("test.subject") catch |err| {
        std.log.err("Failed to subscribe: {}", .{err});
        return;
    };
    
    // Publish a message
    std.log.info("Publishing message...", .{});
    conn.publish("test.subject", "Hello NATS from Zig!") catch |err| {
        std.log.err("Failed to publish: {}", .{err});
        return;
    };
    
    // Wait for and receive the message
    std.log.info("Waiting for message...", .{});
    std.time.sleep(500 * std.time.ns_per_ms); // Give more time for message to arrive
    
    if (sub.nextMessage()) |msg| {
        defer {
            msg.deinit(allocator);
            allocator.destroy(msg);
        }
        std.log.info("Received message on '{s}': {s}", .{ msg.subject, msg.data });
    } else {
        std.log.info("No message received", .{});
    }
    
    std.log.info("NATS pub/sub test completed successfully!", .{});
}