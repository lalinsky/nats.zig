const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.socket);

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

    pub fn write(s: Socket, buf: []const u8) WriteError!usize {
        return s.stream.write(buf);
    }

    pub fn writeAll(s: Socket, buf: []const u8) WriteError!void {
        return s.stream.writeAll(buf);
    }

    pub fn read(s: Socket, buf: []u8) ReadError!usize {
        return s.stream.read(buf);
    }

    pub fn readAtLeast(s: Socket, buffer: []u8, len: usize) ReadError!usize {
        return s.stream.readAtLeast(buffer, len);
    }

    pub fn readAll(s: Socket, buf: []u8) ReadError!void {
        return s.stream.readAll(buf);
    }

    pub fn shutdown(s: Socket, how: ShutdownHow) ShutdownError!void {
        try std.posix.shutdown(s.stream.handle, how);
    }

    pub fn close(s: Socket) void {
        s.stream.close();
    }

    pub fn setNonBlocking(s: Socket) !void {
        const current_flags = try std.posix.fcntl(s.stream.handle, std.posix.F.GETFL, 0);

        const new_flags = current_flags | std.posix.SOCK.NONBLOCK;
        _ = try std.posix.fcntl(s.stream.handle, std.posix.F.SETFL, new_flags);
    }

    pub fn reader(s: Socket) std.net.Stream.Reader {
        return s.stream.reader();
    }

    pub fn writevAll(s: Socket, iovecs: []std.posix.iovec_const) WriteError!void {
        return s.stream.writevAll(iovecs);
    }

    pub fn readUntilDelimiterOrEof(s: Socket, buf: []u8, delimiter: u8) (ReadError || error{StreamTooLong})!?[]u8 {
        return s.stream.reader().readUntilDelimiterOrEof(buf, delimiter);
    }
};
