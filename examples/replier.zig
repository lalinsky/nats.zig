const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Listening for requests on subject 'help'\n", .{});

    // Creates a connection to the default NATS URL
    var conn = try nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        std.process.exit(2);
    };

    // Creates a synchronous subscription on subject "help",
    // waiting for a request. When a message arrives on this
    // subject, we will send a reply.
    const sub = conn.subscribeSync("help") catch |err| {
        std.debug.print("Failed to subscribe: {}\n", .{err});
        std.process.exit(2);
    };
    defer {
        conn.unsubscribe(sub) catch {};
        sub.deinit(allocator);
    }

    std.debug.print("Waiting for requests...\n", .{});

    // Wait for messages in a loop
    while (true) {
        // Wait for the next message (blocks until one arrives)
        if (sub.nextMsg(1000)) |msg| {
            defer msg.deinit();

            std.debug.print("Received msg: {s} - {s}\n", .{ msg.subject, msg.data });

            // Send a reply if there's a reply subject
            if (msg.reply) |reply_subject| {
                conn.publish(reply_subject, "here's some help") catch |err| {
                    std.debug.print("Failed to send reply: {}\n", .{err});
                };
                std.debug.print("Sent reply to: {s}\n", .{reply_subject});
            }
        } else {
            // Sleep a bit if no message
            std.time.sleep(100 * std.time.ns_per_ms);
        }
    }
}