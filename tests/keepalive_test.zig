const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const testing = std.testing;

test "idle connection sends keep-alive pings" {
    const io = std.testing.io;

    const nc = try utils.createConnection(io, .node1, .{
        .ping_interval = .fromMilliseconds(100),
    });
    defer utils.closeConnection(nc);

    // Stay completely idle: no publishes, no inbound traffic. The
    // keep-alive timer alone must produce PINGs.
    const start = std.Io.Timestamp.now(io, .awake);
    while (true) {
        if (start.untilNow(io, .awake).nanoseconds > 5 * std.time.ns_per_s) {
            return error.NoKeepAlivePings;
        }
        try io.sleep(.fromMilliseconds(50), .awake);

        nc.mutex.lockUncancelable(io);
        const pings = nc.outgoing_pings;
        nc.mutex.unlock(io);
        if (pings >= 3) break;
    }

    // The server answered them with PONGs, so the connection is still
    // healthy and usable.
    try nc.publish("test.keepalive", "still alive");
    try nc.flush();
}
