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
const assert = std.debug.assert;

const log = @import("log.zig").log;

pub const ConnectError = std.net.TcpConnectToHostError;
pub const ReadError = std.net.Stream.ReadError;
pub const WriteError = std.net.Stream.WriteError;
pub const ShutdownHow = std.posix.ShutdownHow;
pub const ShutdownError = std.posix.ShutdownError;

pub const Socket = struct {
    stream: std.net.Stream,

    pub fn connect(allocator: std.mem.Allocator, host: []const u8, port: u16) ConnectError!Socket {
        const stream = try std.net.tcpConnectToHost(allocator, host, port);
        return .{ .stream = stream };
    }

    /// Closes the socket.
    pub fn close(s: Socket) void {
        s.stream.close();
    }

    /// Shuts down the socket, unblocking any pending reads or writes.
    pub fn shutdown(s: Socket, how: ShutdownHow) ShutdownError!void {
        try std.posix.shutdown(s.stream.handle, how);
    }

    /// Sets the socket to non-blocking mode.
    pub fn setNonBlocking(s: Socket) !void {
        const current_flags = try std.posix.fcntl(s.stream.handle, std.posix.F.GETFL, 0);
        const new_flags = current_flags | 0o4000; // O_NONBLOCK
        _ = try std.posix.fcntl(s.stream.handle, std.posix.F.SETFL, new_flags);
    }

    /// Sets the socket read timeout.
    pub fn setReadTimeout(s: Socket, timeout_ms: u64) !void {
        const timeout = std.posix.timeval{
            .sec = @intCast(@divFloor(timeout_ms, std.time.ms_per_s)),
            .usec = @intCast(@mod(timeout_ms, std.time.ms_per_s) * 1000), // ms -> µs
        };
        try std.posix.setsockopt(s.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout));
    }

    /// Sets the socket write timeout.
    pub fn setWriteTimeout(s: Socket, timeout_ms: u64) !void {
        const timeout = std.posix.timeval{
            .sec = @intCast(@divFloor(timeout_ms, std.time.ms_per_s)),
            .usec = @intCast(@mod(timeout_ms, std.time.ms_per_s) * 1000), // ms -> µs
        };
        try std.posix.setsockopt(s.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&timeout));
    }

    /// Enables TCP keep-alive on the socket.
    pub fn setKeepAlive(s: Socket, enable: bool) !void {
        const value: c_int = if (enable) 1 else 0;
        try std.posix.setsockopt(s.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&value));
    }

    /// Reads data from the socket and returns the number of bytes read.
    pub fn read(s: Socket, buf: []u8) ReadError!usize {
        return s.stream.read(buf);
    }

    /// Writes data to the socket and returns the number of bytes written.
    /// The operation is atomic, if it returns an error, no data was written.
    pub fn write(s: Socket, buf: []const u8) WriteError!usize {
        return s.stream.write(buf);
    }

    /// Writes data to the socket using multiple buffers and returns the number of bytes written.
    /// The operation is atomic, if it returns an error, no data was written.
    pub fn writev(s: Socket, iovecs: []std.posix.iovec_const) WriteError!usize {
        return s.stream.writev(iovecs);
    }
};
