const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const testing = std.testing;

// Port for the in-test fake server; outside the range used by the
// docker-compose servers (14222-14229).
const fake_server_port = 14239;

/// Accepts one connection, completes the NATS handshake (INFO, then PONG
/// for the client's handshake PING), and then never responds again while
/// keeping the socket open and draining input.
const SilentServer = struct {
    fn run(io: std.Io, listener: *std.Io.net.Server) void {
        var stream = listener.accept(io) catch return;
        defer stream.close(io);

        var read_buf: [4096]u8 = undefined;
        var write_buf: [256]u8 = undefined;
        var stream_reader = stream.reader(io, &read_buf);
        var stream_writer = stream.writer(io, &write_buf);
        const reader = &stream_reader.interface;
        const writer = &stream_writer.interface;

        writer.writeAll("INFO {\"max_payload\":1048576}\r\n") catch return;
        writer.flush() catch return;

        // Answer only the handshake PING, so the client connects fully.
        handshake: while (true) {
            const data = reader.buffered();
            if (std.mem.indexOfScalar(u8, data, '\n')) |i| {
                const is_ping = std.mem.startsWith(u8, data[0..i], "PING");
                reader.toss(i + 1);
                if (is_ping) {
                    writer.writeAll("PONG\r\n") catch return;
                    writer.flush() catch return;
                    break :handshake;
                }
                continue;
            }
            reader.fillMore() catch return;
        }

        // Stay silent: drain keep-alive PINGs without ever answering.
        // (fillMore/toss rather than line reads: takeDelimiterExclusive
        // keeps returning a delimiter-less tail after EOF instead of
        // erroring, which would spin here.)
        while (true) {
            reader.fillMore() catch return;
            reader.toss(reader.buffered().len);
        }
    }
};

test "stale connection detected on unresponsive server" {
    const io = std.testing.io;

    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(fake_server_port) };
    var listener = try address.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    var server_task = try io.concurrent(SilentServer.run, .{ io, &listener });
    defer server_task.cancel(io);

    const url = std.fmt.comptimePrint("nats://127.0.0.1:{d}", .{fake_server_port});
    const nc = try utils.createConnectionWithUrl(io, url, .{
        .ping_interval = .fromMilliseconds(100),
        .max_pings_out = 2,
    });
    defer utils.closeConnection(nc);

    // With a 100ms interval and max_pings_out=2, the third unanswered PING
    // (~300ms) must mark the connection stale and tear it down.
    const start = std.Io.Timestamp.now(io, .awake);
    while (nc.status == .connected) {
        if (start.untilNow(io, .awake).nanoseconds > 5 * std.time.ns_per_s) {
            return error.StaleNotDetected;
        }
        try io.sleep(.fromMilliseconds(50), .awake);
    }
}

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
