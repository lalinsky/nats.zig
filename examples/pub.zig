// Simple publisher example - sends a single message
const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    std.log.info("Publishing message to subject 'foo'", .{});

    // Connect to NATS server
    var conn = nats.Connection.init(init.gpa, init.io, .{});
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Publish message
    try conn.publish("foo", "Hello World!");
    try conn.flush();

    std.log.info("Message published", .{});
}
