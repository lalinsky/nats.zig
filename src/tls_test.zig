// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TLS tests against an in-process fake NATS server speaking real TLS via
//! tls.zig's blocking API, over a loopback socket. The fake server uses the
//! blocking Connection while the client under test uses the nonblock split
//! enc/dec copies, so these tests also pin the interop between the two.

const std = @import("std");
const testing = std.testing;
const tls = @import("tls");
const xsync = @import("xsync");
const Io = std.Io;
const net = Io.net;

const connection_mod = @import("connection.zig");
const Connection = connection_mod.Connection;
const ProtocolError = connection_mod.ProtocolError;

const test_cert = @embedFile("testdata/test_cert.pem");
const test_key = @embedFile("testdata/test_key.pem");
const client_cert = @embedFile("testdata/client_cert.pem");

/// Paths for the client under test; the test runner's working directory
/// is the project root, like the paths used by the e2e tests.
const client_cert_file = "src/testdata/client_cert.pem";
const client_key_file = "src/testdata/client_key.pem";

const ServerBehavior = enum {
    /// Plaintext INFO, TLS upgrade, NATS handshake, then a scripted
    /// SUB/MSG/PUB exchange.
    serve,
    /// Like serve, but with a server-initiated TLS key update (both the
    /// plain and the update-requested variant) between the exchanges.
    serve_key_update,
    /// TLS handshake first, INFO over TLS, then the scripted exchange.
    serve_handshake_first,
    /// Like serve, but the handshake requires a valid client certificate
    /// (the self-signed testdata client certificate is the trust root).
    serve_mtls,
    /// INFO gossiping another server via connect_urls, TLS upgrade, NATS
    /// handshake, then close - so the client reconnects to the gossiped
    /// server.
    serve_gossip_then_close,
    /// Plaintext INFO, then read the ClientHello and never respond.
    stall_in_tls_handshake,
    /// Plaintext INFO, then respond to the ClientHello with garbage.
    garbage_instead_of_tls,
    /// Plaintext INFO, then a handshake flight larger than the client's
    /// read buffer: a ServerHello claiming a 100000-byte body, streamed
    /// as well-formed TLS records that never complete the message.
    oversized_handshake_flight,
};

const FakeServer = struct {
    io: Io,
    allocator: std.mem.Allocator,
    tcp_server: net.Server,
    behavior: ServerBehavior,
    /// Advertised in INFO connect_urls for .serve_gossip_then_close.
    gossip_port: u16 = 0,
    /// Payload received in the client's PUB frame, for the test to assert.
    received_pub: [64]u8 = undefined,
    received_pub_len: usize = 0,

    fn init(io: Io, allocator: std.mem.Allocator, behavior: ServerBehavior) !FakeServer {
        const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
        const tcp_server = try listen_address.listen(io, .{});
        return .{
            .io = io,
            .allocator = allocator,
            .tcp_server = tcp_server,
            .behavior = behavior,
        };
    }

    fn deinit(self: *FakeServer) void {
        self.tcp_server.deinit(self.io);
    }

    fn port(self: *FakeServer) u16 {
        return self.tcp_server.socket.address.getPort();
    }

    /// Read one protocol line, consuming and stripping the "\r\n".
    fn readLine(reader: *Io.Reader) ![]const u8 {
        const line = try reader.takeDelimiterInclusive('\n');
        return std.mem.trimEnd(u8, line, "\r\n");
    }

    fn run(self: *FakeServer) !void {
        const io = self.io;

        var stream = try self.tcp_server.accept(io);
        defer stream.close(io);

        var read_buf: [tls.input_buffer_len]u8 = undefined;
        var write_buf: [tls.output_buffer_len]u8 = undefined;
        var tcp_reader = stream.reader(io, &read_buf);
        var tcp_writer = stream.writer(io, &write_buf);

        if (self.behavior != .serve_handshake_first) {
            var info_buf: [256]u8 = undefined;
            const info = if (self.behavior == .serve_gossip_then_close)
                try std.fmt.bufPrint(&info_buf, "INFO {{\"tls_required\":true,\"headers\":true,\"max_payload\":1048576,\"connect_urls\":[\"127.0.0.1:{d}\"]}}\r\n", .{self.gossip_port})
            else
                "INFO {\"tls_required\":true,\"headers\":true,\"max_payload\":1048576}\r\n";
            try tcp_writer.interface.writeAll(info);
            try tcp_writer.interface.flush();
        }

        switch (self.behavior) {
            .stall_in_tls_handshake => {
                // Consume the ClientHello and go silent; the client stays
                // blocked in the handshake until it is torn down.
                _ = tcp_reader.interface.fillMore() catch return;
                _ = tcp_reader.interface.fillMore() catch return;
                return;
            },
            .garbage_instead_of_tls => {
                _ = tcp_reader.interface.fillMore() catch return;
                try tcp_writer.interface.writeAll("THIS IS NOT A TLS SERVER\r\n");
                try tcp_writer.interface.flush();
                return;
            },
            .oversized_handshake_flight => {
                _ = tcp_reader.interface.fillMore() catch return; // ClientHello
                // Handshake records of 16 KiB each; the first opens a
                // ServerHello with a 100000-byte length (0x0186A0), so the
                // flight never completes within the client's read buffer.
                var record: [5 + 16384]u8 = @splat(0);
                record[0] = 0x16; // handshake
                record[1] = 0x03;
                record[2] = 0x03; // TLS 1.2 record version
                record[3] = 0x40; // length 16384
                record[4] = 0x00;
                record[5] = 0x02; // ServerHello
                record[6] = 0x01;
                record[7] = 0x86;
                record[8] = 0xA0; // handshake length 100000
                var sent: usize = 0;
                while (sent < 128 * 1024) : (sent += 16384) {
                    tcp_writer.interface.writeAll(&record) catch return;
                    tcp_writer.interface.flush() catch return;
                    @memset(record[5..], 0); // only the first record opens the message
                }
                return;
            },
            else => {},
        }

        var auth = try tls.config.CertKeyPair.fromSlice(self.allocator, io, test_cert, test_key);
        defer auth.deinit(self.allocator);

        // For mTLS, the self-signed client certificate is its own trust
        // root, mirroring how the client trusts the self-signed server
        // certificate via insecure_skip_verify.
        var client_root_ca: std.crypto.Certificate.Bundle = .empty;
        defer client_root_ca.deinit(self.allocator);
        if (self.behavior == .serve_mtls) {
            client_root_ca = try tls.config.cert.fromSlice(self.allocator, io, client_cert);
        }

        var rng: std.Random.IoSource = .{ .io = io };
        var conn = try tls.server(&tcp_reader.interface, &tcp_writer.interface, .{
            .auth = &auth,
            .client_auth = if (self.behavior == .serve_mtls) .{
                .root_ca = client_root_ca,
                .auth_type = .require,
            } else null,
            .rng = rng.interface(),
            .now = Io.Clock.real.now(io),
        });

        var cleartext_read_buf: [4096]u8 = undefined;
        var cleartext_write_buf: [4096]u8 = undefined;
        var tls_reader = conn.reader(&cleartext_read_buf);
        var tls_writer = conn.writer(&cleartext_write_buf);
        const reader = &tls_reader.interface;
        const writer = &tls_writer.interface;

        if (self.behavior == .serve_handshake_first) {
            try writer.writeAll("INFO {\"tls_required\":true,\"headers\":true,\"max_payload\":1048576}\r\n");
            try writer.flush();
        }

        // CONNECT, then PING.
        _ = try readLine(reader); // CONNECT {...}
        _ = try readLine(reader); // PING
        try writer.writeAll("PONG\r\n");
        try writer.flush();

        if (self.behavior == .serve_gossip_then_close) {
            // The client is now connected and knows about the gossiped
            // server; closing the stream sends it there.
            return;
        }

        // SUB <subject> <sid>
        const sub_line = try readLine(reader);
        var sub_it = std.mem.tokenizeAny(u8, sub_line, " \r");
        _ = sub_it.next(); // SUB
        const subject = sub_it.next().?;
        const sid = sub_it.next().?;

        var msg_buf: [128]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "MSG {s} {s} 5\r\nhello\r\n", .{ subject, sid });
        try writer.writeAll(msg);
        try writer.flush();

        if (self.behavior == .serve_key_update) {
            // Plain key update: we rekey our encrypt side and tell the
            // client, which must rekey its decrypt side (and only that).
            @atomicStore(bool, &conn.key_update_requested, true, .monotonic);

            // update_requested key update: hand-crafted, because tls.zig
            // only ever sends update_not_requested. The client must rekey
            // its decrypt side and its encrypt copy must respond with a
            // KeyUpdate of its own before its next application data.
            const key_update_requested_msg = [_]u8{ 0x18, 0x00, 0x00, 0x01, 0x01 };
            var rec_buf: [64]u8 = undefined;
            const rec = try conn.cipher.encrypt(&rec_buf, .handshake, &key_update_requested_msg);
            try conn.cipher.keyUpdateEncrypt();
            try tcp_writer.interface.writeAll(rec);
            try tcp_writer.interface.flush();
        }

        // Second MSG; after .serve_key_update this is encrypted with
        // updated keys, so receiving it proves the client rekeyed.
        const msg2 = try std.fmt.bufPrint(&msg_buf, "MSG {s} {s} 5\r\nworld\r\n", .{ subject, sid });
        try writer.writeAll(msg2);
        try writer.flush();

        // PUB <subject> <size>, then the payload line. After the
        // update-requested key update above, the blocking connection
        // processes the client's KeyUpdate response transparently;
        // receiving this PUB proves the client's encrypt copy sent it.
        const pub_line = try readLine(reader);
        var pub_it = std.mem.tokenizeAny(u8, pub_line, " \r");
        _ = pub_it.next(); // PUB
        _ = pub_it.next(); // subject
        const size_str = pub_it.next().?;
        const size = try std.fmt.parseInt(usize, size_str, 10);
        const payload = try reader.take(size + 2);
        @memcpy(self.received_pub[0..size], payload[0..size]);
        self.received_pub_len = size;
    }
};

fn serverTask(server: *FakeServer, err_out: *?anyerror) Io.Cancelable!void {
    server.run() catch |err| {
        if (err == error.Canceled) return error.Canceled;
        err_out.* = err;
    };
}

const RoundTripOptions = struct {
    behavior: ServerBehavior,
    handshake_first: bool = false,
    cert_file: ?[]const u8 = null,
    key_file: ?[]const u8 = null,
    insecure_skip_verify: bool = true,
    ca_file: ?[]const u8 = null,
    server_name: ?[]const u8 = null,
};

fn testTlsRoundTrip(opts: RoundTripOptions) !void {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, opts.behavior);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{
            .insecure_skip_verify = opts.insecure_skip_verify,
            .ca_file = opts.ca_file,
            .server_name = opts.server_name,
            .handshake_first = opts.handshake_first,
            .cert_file = opts.cert_file,
            .key_file = opts.key_file,
        },
    });
    defer nc.deinit();

    try nc.connect(url);

    const sub = try nc.subscribeSync("foo");
    defer sub.deinit();

    var msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer msg.deinit();
    try testing.expectEqualStrings("hello", msg.data);

    var msg2 = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer msg2.deinit();
    try testing.expectEqualStrings("world", msg2.data);

    try nc.publish("bar", "reply");

    try server_task.await(io);
    if (server_err) |err| return err;

    try testing.expectEqualStrings("reply", server.received_pub[0..server.received_pub_len]);
}

test "tls: connect, subscribe, publish round-trip" {
    try testTlsRoundTrip(.{ .behavior = .serve });
}

test "tls: server key updates are handled and answered" {
    try testTlsRoundTrip(.{ .behavior = .serve_key_update });
}

test "tls: handshake-first mode" {
    try testTlsRoundTrip(.{ .behavior = .serve_handshake_first, .handshake_first = true });
}

test "tls: mtls client certificate round-trip" {
    try testTlsRoundTrip(.{
        .behavior = .serve_mtls,
        .cert_file = client_cert_file,
        .key_file = client_key_file,
    });
}

test "tls: mtls server rejects a client without a certificate" {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, .serve_mtls);
    defer server.deinit();

    // The server-side handshake fails too; that error is expected and
    // deliberately not asserted on.
    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{ .insecure_skip_verify = true },
    });
    defer nc.deinit();

    const result = nc.connect(url);
    try testing.expect(result != error.Timeout);
    try testing.expectError(error.TlsAlertCertificateRequired, result);
}

test "tls: server_name overrides the URL host for verification" {
    // The URL dials the IP, but verification runs against server_name,
    // which matches the certificate's DNS SAN (localhost). The
    // self-signed server certificate acts as its own trust root.
    try testTlsRoundTrip(.{
        .behavior = .serve,
        .insecure_skip_verify = false,
        .ca_file = "src/testdata/test_cert.pem",
        .server_name = "localhost",
    });
}

test "tls: server_name mismatch fails verification" {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, .serve);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{
            .ca_file = "src/testdata/test_cert.pem",
            .server_name = "wrong.example.com",
        },
    });
    defer nc.deinit();

    const result = nc.connect(url);
    try testing.expect(result != error.Timeout);
    try testing.expectError(error.CertificateHostMismatch, result);
}

test "tls: reconnect to a discovered server stays on tls" {
    const io = testing.io;
    const allocator = testing.allocator;

    // server1 gossips server2 through INFO connect_urls and closes right
    // after the NATS handshake; the client must reconnect to server2 -
    // discovered as bare host:port - over TLS.
    var server2 = try FakeServer.init(io, allocator, .serve);
    defer server2.deinit();

    var server1 = try FakeServer.init(io, allocator, .serve_gossip_then_close);
    defer server1.deinit();
    server1.gossip_port = server2.port();

    var server1_err: ?anyerror = null;
    var server1_task = try io.concurrent(serverTask, .{ &server1, &server1_err });
    defer server1_task.cancel(io) catch {};

    var server2_err: ?anyerror = null;
    var server2_task = try io.concurrent(serverTask, .{ &server2, &server2_err });
    defer server2_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server1.port()});

    reconnect_tracker = .{};
    var nc = Connection.init(allocator, io, .{
        .tls = .{ .insecure_skip_verify = true },
        .callbacks = .{ .reconnected_cb = ReconnectTracker.reconnectedCallback },
    });
    defer nc.deinit();

    try nc.connect(url);

    try reconnect_tracker.reconnected.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });

    // The reconnected connection runs against server2, over a fresh TLS
    // session; run the scripted exchange to prove it carries traffic.
    const sub = try nc.subscribeSync("foo");
    defer sub.deinit();

    var msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer msg.deinit();
    try testing.expectEqualStrings("hello", msg.data);

    var msg2 = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer msg2.deinit();
    try testing.expectEqualStrings("world", msg2.data);

    try nc.publish("bar", "reply");

    try server2_task.await(io);
    if (server2_err) |err| return err;
    try testing.expectEqualStrings("reply", server2.received_pub[0..server2.received_pub_len]);
}

const ReconnectTracker = struct {
    reconnected: xsync.Event = .init,

    fn reconnectedCallback(_: *Connection) void {
        reconnect_tracker.reconnected.set(testing.io);
    }
};

var reconnect_tracker: ReconnectTracker = .{};

test "tls: an oversized handshake flight fails cleanly" {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, .oversized_handshake_flight);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{ .insecure_skip_verify = true },
    });
    defer nc.deinit();

    // A handshake message spilling past its record is rejected by
    // tls.zig before the flight can outgrow the read buffer; a flight
    // of many unfragmented messages would hit the read-buffer guard in
    // performTlsHandshake instead. Either way this must surface as an
    // error from connect(), never a stall or a full-buffer assertion.
    const result = nc.connect(url);
    try testing.expect(result != error.Timeout);
    try testing.expectError(error.TlsUnsupportedFragmentedHandshakeMessage, result);
}

test "tls: a client certificate without its key is rejected upfront" {
    const io = testing.io;

    var nc = Connection.init(testing.allocator, io, .{
        .tls = .{ .cert_file = client_cert_file },
    });
    defer nc.deinit();

    try testing.expectError(error.MissingTlsKeyPair, nc.connect("tls://127.0.0.1:4222"));
}

test "tls: close during a stalled handshake does not hang" {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, .stall_in_tls_handshake);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{ .insecure_skip_verify = true },
    });
    defer nc.deinit();

    const Task = struct {
        fn connect(conn: *Connection, target: []const u8, err_out: *?anyerror) Io.Cancelable!void {
            conn.connect(target) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                err_out.* = err;
            };
        }
    };

    var connect_err: ?anyerror = null;
    var connect_task = try io.concurrent(Task.connect, .{ &nc, url, &connect_err });
    defer connect_task.cancel(io) catch {};

    // Give the client time to get blocked inside the TLS handshake, then
    // close. The close must interrupt the handshake promptly; a hang here
    // fails the test via the harness timeout.
    try io.sleep(.fromMilliseconds(200), .awake);
    nc.close();

    try connect_task.await(io);
    try testing.expect(connect_err != null);
    try testing.expectEqual(error.ConnectionClosed, connect_err.?);
}

test "tls: handshake failure reports the real error, not a timeout" {
    const io = testing.io;
    const allocator = testing.allocator;

    var server = try FakeServer.init(io, allocator, .garbage_instead_of_tls);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "tls://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{
        .tls = .{ .insecure_skip_verify = true },
    });
    defer nc.deinit();

    // "THIS is..." parses as a TLS record header with an absurd length.
    // The point: the real TLS error must surface through connect(),
    // instead of the attempt timing out with the cause swallowed.
    const result = nc.connect(url);
    try testing.expect(result != error.Timeout);
    try testing.expectError(error.TlsRecordOverflow, result);
}

test "tls: plaintext client fails against a tls-required server" {
    const io = testing.io;
    const allocator = testing.allocator;

    // The fake server never gets past the INFO for a plaintext client;
    // the client itself must detect tls_required and fail the handshake.
    var server = try FakeServer.init(io, allocator, .stall_in_tls_handshake);
    defer server.deinit();

    var server_err: ?anyerror = null;
    var server_task = try io.concurrent(serverTask, .{ &server, &server_err });
    defer server_task.cancel(io) catch {};

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{server.port()});

    var nc = Connection.init(allocator, io, .{});
    defer nc.deinit();

    try testing.expectError(ProtocolError.SecureConnectionRequired, nc.connect(url));
}
