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

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const net = Io.net;

pub const ConnectError = net.HostName.ValidateError || net.HostName.ConnectError || Io.ConcurrentError;
pub const SetKeepAliveError = std.posix.SetSockOptError;

const connect_options: net.IpAddress.ConnectOptions = .{ .mode = .stream, .protocol = .tcp };

/// Open a TCP connection to `host:port`. `host` may be an IP literal or a
/// host name; host names are resolved and the addresses dialed one at a
/// time, in resolver order, until one connects.
///
/// `std.Io.net.HostName.connect` is deliberately not used: it dials every
/// resolved address concurrently and keeps only the winner (and, in Zig
/// 0.16.0, prints a stray debug line per attempt).
pub fn tcpConnectToHost(io: Io, host: []const u8, port: u16) ConnectError!net.Stream {
    if (net.IpAddress.parse(host, port)) |address| {
        return address.connect(io, connect_options);
    } else |_| {}

    const host_name = try net.HostName.init(host);

    var canonical_name_buffer: [net.HostName.max_len]u8 = undefined;
    var lookup_buffer: [16]net.HostName.LookupResult = undefined;
    var lookup_queue: Io.Queue(net.HostName.LookupResult) = .init(&lookup_buffer);

    // The queue is consumed while the lookup fills it, so the lookup must run
    // concurrently; `lookup` guarantees it will not outgrow a queue with
    // capacity of at least 16 as long as someone keeps consuming it.
    var lookup_task = try io.concurrent(net.HostName.lookup, .{ host_name, io, &lookup_queue, .{
        .port = port,
        .canonical_name_buffer = &canonical_name_buffer,
    } });
    defer lookup_task.cancel(io) catch {};

    var connect_err: ?net.IpAddress.ConnectError = null;

    while (lookup_queue.getOne(io)) |result| switch (result) {
        .address => |address| {
            if (address.connect(io, connect_options)) |stream| {
                return stream;
            } else |err| switch (err) {
                error.Canceled => |e| return e,
                else => |e| connect_err = e,
            }
        },
        .canonical_name => continue,
    } else |err| switch (err) {
        error.Canceled => |e| return e,
        error.Closed => {},
    }

    // Nothing connected: report a lookup failure, else the last failed dial,
    // else the lookup produced no addresses at all.
    try lookup_task.await(io);
    return connect_err orelse error.UnknownHostName;
}

/// Vectored write straight through the `Io` vtable. `net.Stream` only
/// writes through the `Io.Writer` interface, which erases the real error
/// behind `error.WriteFailed` and stashes it on the writer; the vtable
/// call keeps the typed error set - including cancellation - intact.
/// Returns the number of bytes written; short writes are possible.
pub fn writeVec(io: Io, stream: net.Stream, data: []const []const u8) net.Stream.Writer.Error!usize {
    return io.vtable.netWrite(io.userdata, stream.socket.handle, &.{}, data, 1);
}

/// Write all of `bytes` to the stream, looping over short writes.
pub fn writeAll(io: Io, stream: net.Stream, bytes: []const u8) net.Stream.Writer.Error!void {
    var pos: usize = 0;
    while (pos < bytes.len) {
        pos += try writeVec(io, stream, &.{bytes[pos..]});
    }
}

/// Enable or disable SO_KEEPALIVE. There is no socket-options API in std.Io,
/// so this goes through the raw socket handle.
pub fn setKeepAlive(socket: net.Socket, enabled: bool) SetKeepAliveError!void {
    if (builtin.os.tag == .windows) return;

    const value: c_int = @intFromBool(enabled);
    try std.posix.setsockopt(socket.handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&value));
}

test "tcpConnectToHost with an IP literal" {
    const io = std.testing.io;

    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);

    const stream = try tcpConnectToHost(io, "127.0.0.1", server.socket.address.getPort());
    defer stream.close(io);

    try setKeepAlive(stream.socket, true);
}

test "tcpConnectToHost with a host name" {
    const io = std.testing.io;

    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);

    const stream = try tcpConnectToHost(io, "localhost", server.socket.address.getPort());
    defer stream.close(io);
}

test "tcpConnectToHost with an invalid host name" {
    const io = std.testing.io;

    try std.testing.expectError(error.InvalidHostName, tcpConnectToHost(io, "invalid_host!", 4222));
}

test "tcpConnectToHost with nothing listening" {
    const io = std.testing.io;

    // Grab an ephemeral port and close the listener so the port is dead.
    const listen_address: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try listen_address.listen(io, .{});
    const port = server.socket.address.getPort();
    server.deinit(io);

    try std.testing.expectError(error.ConnectionRefused, tcpConnectToHost(io, "127.0.0.1", port));
}
