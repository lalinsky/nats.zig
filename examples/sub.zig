// Simple subscriber example - listens for a single message
const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    std.log.info("Listening for message on subject 'foo'", .{});

    // Connect to NATS server
    var conn = nats.Connection.init(init.gpa, init.io, .{});
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Subscribe to subject (sync)
    const sub = try conn.subscribeSync("foo");
    defer sub.deinit();

    std.log.info("Waiting for messages...", .{});

    while (true) {
        var msg = sub.nextMsgTimeout(.fromSeconds(1)) catch |err| {
            if (err == error.Timeout) continue;
            return err;
        };
        defer msg.deinit();

        std.log.info("Received message: {s} - {s}", .{ msg.subject, msg.data });
        return;
    }
}
