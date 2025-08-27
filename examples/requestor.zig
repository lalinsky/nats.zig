const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Publishes a message on subject 'help'\n", .{});

    // Creates a connection to the default NATS URL
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();

    conn.connect("nats://localhost:4222") catch |err| {
        std.debug.print("Failed to connect to NATS server: {}\n", .{err});
        std.debug.print("Make sure NATS server is running at nats://localhost:4222\n", .{});
        std.process.exit(2);
    };

    // Sends a request on "help" and expects a reply.
    // Will wait only for a given number of milliseconds,
    // say for 5 seconds in this example.
    const reply = conn.request("help", "really need some", std.time.ns_per_s * 5) catch |err| {
        std.debug.print("Request failed: {}\n", .{err});
        std.process.exit(2);
    };
    defer reply.deinit();

    // If we are here, we should have received the reply
    std.debug.print("Received reply: {s}\n", .{reply.data});

    std.debug.print("Request/reply completed successfully!\n", .{});
}
