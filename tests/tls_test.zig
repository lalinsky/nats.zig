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

//! TLS end-to-end tests against real nats-server instances (see
//! docker-compose.test.yml: nats-tls and nats-tls-first). The servers use
//! a certificate signed by the test CA in tests/configs/certs/ca.pem with
//! SAN localhost, so verified connections go through tls://localhost.

const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const xsync = @import("xsync");
const utils = @import("utils.zig");

const log = std.log.default;

const ca_file = "tests/configs/certs/ca.pem";

fn tlsUrl(buf: []u8, node: utils.Node) ![]const u8 {
    return std.fmt.bufPrint(buf, "tls://localhost:{d}", .{@intFromEnum(node)});
}

/// Subscribe, publish, and receive one message over the connection.
fn expectRoundTrip(nc: *nats.Connection, subject: []const u8) !void {
    const sub = try nc.subscribeSync(subject);
    defer sub.deinit();

    // Make sure the SUB reached the server before publishing.
    try nc.flush();

    try nc.publish(subject, "over tls");

    var msg = try sub.nextMsgTimeout(utils.ioTimeout(.fromSeconds(5)));
    defer msg.deinit();
    try testing.expectEqualStrings("over tls", msg.data);
}

test "tls e2e: verified connect with CA file" {
    const io = std.testing.io;

    var url_buf: [64]u8 = undefined;
    const url = try tlsUrl(&url_buf, .tls);

    const nc = try utils.createConnectionWithUrl(io, url, .{
        .tls = .{ .ca_file = ca_file },
    });
    defer utils.closeConnection(nc);

    try expectRoundTrip(nc, "test.tls.verified");
}

test "tls e2e: certificate verification failure without the CA" {
    const io = std.testing.io;

    var url_buf: [64]u8 = undefined;
    const url = try tlsUrl(&url_buf, .tls);

    // The system trust store does not contain the test CA, so the
    // handshake must fail with a certificate error - and connect() must
    // report that error, not a timeout.
    const result = utils.createConnectionWithUrl(io, url, .{
        .tls = .{},
    });
    try testing.expect(result != error.Timeout);
    if (result) |nc| {
        utils.closeConnection(nc);
        return error.TestUnexpectedResult;
    } else |err| {
        try testing.expectEqual(error.CertificateIssuerNotFound, err);
    }
}

test "tls e2e: insecure skip verify" {
    const io = std.testing.io;

    var url_buf: [64]u8 = undefined;
    const url = try tlsUrl(&url_buf, .tls);

    const nc = try utils.createConnectionWithUrl(io, url, .{
        .tls = .{ .insecure_skip_verify = true },
    });
    defer utils.closeConnection(nc);

    try expectRoundTrip(nc, "test.tls.insecure");
}

test "tls e2e: handshake-first server" {
    const io = std.testing.io;

    var url_buf: [64]u8 = undefined;
    const url = try tlsUrl(&url_buf, .tls_first);

    const nc = try utils.createConnectionWithUrl(io, url, .{
        .tls = .{ .ca_file = ca_file, .handshake_first = true },
    });
    defer utils.closeConnection(nc);

    try expectRoundTrip(nc, "test.tls.first");
}

const ReconnectTracker = struct {
    reconnected: xsync.Event = .init,

    fn reconnectedCallback(_: *nats.Connection) void {
        reconnect_tracker.reconnected.set(std.testing.io);
    }
};

var reconnect_tracker: ReconnectTracker = .{};

test "tls e2e: reconnect over tls" {
    const io = std.testing.io;

    reconnect_tracker = .{};

    var url_buf: [64]u8 = undefined;
    const url = try tlsUrl(&url_buf, .tls);

    const nc = try utils.createConnectionWithUrl(io, url, .{
        .tls = .{ .ca_file = ca_file },
        .reconnect = .{ .allow_reconnect = true },
        .callbacks = .{
            .reconnected_cb = ReconnectTracker.reconnectedCallback,
        },
    });
    defer utils.closeConnection(nc);

    try nc.publish("test.tls.before", "before restart");

    log.debug("Restarting nats-tls", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-tls" });

    reconnect_tracker.reconnected.waitTimeout(io, utils.ioTimeout(.fromSeconds(15))) catch {
        return error.ReconnectTimedOut;
    };

    // The reconnected connection performed a fresh TLS handshake; verify
    // it carries traffic.
    try expectRoundTrip(nc, "test.tls.after");
}
