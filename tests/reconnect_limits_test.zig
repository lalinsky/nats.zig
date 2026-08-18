const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const testing = std.testing;

// Port for the in-test fake server; outside the range used by the
// docker-compose servers (14222-14229) and the keepalive test (14239).
const fake_server_port = 14238;

/// Serves one successful NATS handshake and then drops the connection;
/// every later accepted connection is closed immediately, so each
/// reconnection attempt fails its handshake after TCP succeeds.
const FlakyServer = struct {
    var accepts: std.atomic.Value(u32) = .init(0);

    fn run(io: std.Io, listener: *std.Io.net.Server) void {
        var first = true;
        while (true) {
            var stream = listener.accept(io) catch return;
            _ = accepts.fetchAdd(1, .monotonic);

            if (!first) {
                // Accept TCP, then refuse the handshake.
                stream.close(io);
                continue;
            }
            first = false;
            serveHandshakeThenDrop(io, &stream);
        }
    }

    fn serveHandshakeThenDrop(io: std.Io, stream: *std.Io.net.Stream) void {
        defer stream.close(io);

        var read_buf: [4096]u8 = undefined;
        var write_buf: [256]u8 = undefined;
        var stream_reader = stream.reader(io, &read_buf);
        var stream_writer = stream.writer(io, &write_buf);
        const reader = &stream_reader.interface;
        const writer = &stream_writer.interface;

        writer.writeAll("INFO {\"max_payload\":1048576}\r\n") catch return;
        writer.flush() catch return;

        // Answer the handshake PING, completing the connection, then
        // return, which closes the socket and drops the client.
        while (true) {
            const data = reader.buffered();
            if (std.mem.indexOfScalar(u8, data, '\n')) |i| {
                const is_ping = std.mem.startsWith(u8, data[0..i], "PING");
                reader.toss(i + 1);
                if (is_ping) {
                    writer.writeAll("PONG\r\n") catch return;
                    writer.flush() catch return;
                    return;
                }
                continue;
            }
            reader.fillMore() catch return;
        }
    }
};

const ClosedTracker = struct {
    var closed_called: std.atomic.Value(u32) = .init(0);
    var reconnected_called: std.atomic.Value(u32) = .init(0);

    fn closedCallback(_: *nats.Connection) void {
        _ = closed_called.fetchAdd(1, .monotonic);
    }

    fn reconnectedCallback(_: *nats.Connection) void {
        _ = reconnected_called.fetchAdd(1, .monotonic);
    }
};

test "handshake failures count against max_reconnect" {
    const io = std.testing.io;

    FlakyServer.accepts.store(0, .monotonic);
    ClosedTracker.closed_called.store(0, .monotonic);
    ClosedTracker.reconnected_called.store(0, .monotonic);

    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(fake_server_port) };
    var listener = try address.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    var server_task = try io.concurrent(FlakyServer.run, .{ io, &listener });
    defer server_task.cancel(io);

    const url = std.fmt.comptimePrint("nats://127.0.0.1:{d}", .{fake_server_port});
    const nc = try utils.createConnectionWithUrl(io, url, .{
        // Keep failed handshakes quick: the flaky server closes the socket
        // without answering, and the handshake only fails via this timeout.
        .timeout = .{ .duration = .{ .raw = .fromMilliseconds(250), .clock = .awake } },
        .reconnect = .{
            .allow_reconnect = true,
            .max_reconnect = 2,
            .reconnect_wait = .fromMilliseconds(50),
            .reconnect_jitter = .fromMilliseconds(10),
        },
        .callbacks = .{
            .closed_cb = ClosedTracker.closedCallback,
            .reconnected_cb = ClosedTracker.reconnectedCallback,
        },
    });
    defer utils.closeConnection(nc);

    // The server drops us right after the handshake. Every reconnection
    // attempt connects at the TCP level but fails the handshake; those
    // failures must count against max_reconnect, so after 2 attempts the
    // server is removed from the pool and the connection goes terminal.
    const start = std.Io.Timestamp.now(io, .awake);
    while (nc.status != .connection_failed) {
        if (start.untilNow(io, .awake).nanoseconds >= 10 * std.time.ns_per_s) {
            return error.NeverWentTerminal;
        }
        try io.sleep(.fromMilliseconds(50), .awake);
    }

    // Initial connection plus exactly max_reconnect failed attempts.
    try testing.expectEqual(@as(u32, 3), FlakyServer.accepts.load(.monotonic));
    try testing.expectEqual(@as(u32, 1), ClosedTracker.closed_called.load(.monotonic));
    try testing.expectEqual(@as(u32, 0), ClosedTracker.reconnected_called.load(.monotonic));
}
