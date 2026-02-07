// Simple publisher example - sends a single message
const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Publishing message to subject 'foo'", .{});

    // Initialize zio runtime
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Connect to NATS server
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Publish message
    try conn.publish("foo", "Hello World!");
    try conn.flush();

    std.log.info("Message published", .{});
}
