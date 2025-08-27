const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.conn);

const ServerPool = @import("server_pool.zig").ServerPool;
const Server = @import("server_pool.zig").Server;

const Parser = @import("parser.zig").Parser;

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

    pub fn write(s: *Socket, buf: []const u8) WriteError!usize {
        return s.stream.write(buf);
    }

    pub fn writeAll(s: *Socket, buf: []const u8) WriteError!void {
        return s.stream.writeAll(buf);
    }

    pub fn read(s: *Socket, buf: []u8) ReadError!usize {
        return s.stream.read(buf);
    }

    pub fn readAtLeast(s: *Socket, buffer: []u8, len: usize) ReadError!usize {
        return s.stream.readAtLeast(buffer, len);
    }

    pub fn readAll(s: *Socket, buf: []u8) ReadError!void {
        return s.stream.readAll(buf);
    }

    pub fn shutdown(s: *Socket, how: ShutdownHow) ShutdownError!void {
        try std.posix.shutdown(s.stream.handle, how);
    }

    pub fn close(s: *Socket) void {
        s.stream.close();
    }

    pub fn setNonBlocking(s: *Socket) !void {
        const current_flags = try std.posix.fcntl(s.stream.handle, std.posix.F.GETFL, 0);

        const new_flags = current_flags | std.posix.SOCK.NONBLOCK;
        _ = try std.posix.fcntl(s.stream.handle, std.posix.F.SETFL, new_flags);
    }
};

/// Internal NATS connection handler, responsible for maintaining the connection to the NATS server.
pub const Conn = struct {
    allocator: std.mem.Allocator,

    server_pool: ServerPool,
    current_server: ?*Server = null,

    max_reconnects_per_server: i32 = 5,

    parser: Parser,

    mutex: std.Thread.Mutex = .{},
    status_changed: std.Thread.Condition = .{},

    // change to any of these is controlled by the condition variable
    sock: ?Socket = null,
    last_error: ?anyerror = null,
    reconnecting: bool = false,
    reconnect_attempts: usize = 0,
    should_stop: bool = false,
    read_thread_stopped: bool = false,
    read_thread: ?std.Thread = null,

    pub fn init(allocator: std.mem.Allocator) Conn {
        return Conn{
            .allocator = allocator,
            .server_pool = ServerPool.init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Conn) void {
        self.stop();
        self.server_pool.deinit();
    }

    // NOTE: runs under the mutex locked
    fn connectAndRun(self: *Conn) !void {
        assert(self.sock == null);

        const server = try self.server_pool.getNextServer(self.max_reconnects_per_server, null);
        if (self.reconnecting) {
            server.reconnects += 1;
        }

        var sock = blk: {
            self.mutex.unlock(); // unlock the mutex before connecting to the server
            defer self.mutex.lock();

            const sock = try Socket.connect(self.allocator, server.parsed_url.host, server.parsed_url.port);
            break :blk sock;
        };
        defer sock.close();

        self.reconnecting = false;
        self.reconnect_attempts = 0;
        self.sock = sock;
        self.status_changed.broadcast();

        defer {
            self.sock = null;
            self.status_changed.broadcast();
        }

        self.parser.reset();

        while (!self.should_stop) {
            self.mutex.unlock(); // unlock the mutex before reading from the socket
            defer self.mutex.lock();

            var buffer: [1024]u8 = undefined;

            const bytes_read = sock.read(&buffer) catch |err| {
                log.err("Socket read error: {}", .{err});
                return err;
            };

            if (bytes_read == 0) {
                log.info("Connection closed by peer", .{});
                return error.ConnectionClosed;
            }

            self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
                log.err("Parser error: {}", .{err});
                return err;
            };
        }
    }

    pub fn readLoop(self: *Conn) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        defer {
            self.read_thread_stopped = true;
            self.status_changed.broadcast();
        }

        const reconnect_base_delay_ms: u64 = 100;

        while (!self.should_stop) {
            self.connectAndRun() catch |err| {
                switch (err) {
                    error.NoServerAvailable => {
                        log.info("No more servers available, stopping connection loop", .{});

                        self.last_error = err;
                        self.status_changed.broadcast();

                        break;
                    },
                    else => {
                        log.err("Unexpected error in connection thread: {}", .{err});

                        self.last_error = err;
                        self.reconnecting = true;
                        self.reconnect_attempts += 1;
                        self.status_changed.broadcast();

                        self.mutex.unlock();
                        defer self.mutex.lock();

                        const delay_ms = reconnect_base_delay_ms * (@as(u64, 1) << @min(self.reconnect_attempts - 1, 11)); // Exponential backoff, cap at 2^11

                        log.info("Retrying connection in {}ms", .{delay_ms});
                        std.time.sleep(std.time.ns_per_ms * delay_ms);
                    },
                }
            };
        }
    }

    fn start(self: *Conn) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.read_thread != null) {
            return;
        }

        self.read_thread = try std.Thread.spawn(.{}, readLoop, .{self});
        self.status_changed.broadcast();
    }

    fn prepareShutdown(self: *Conn) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.sock) |*sock| {
            sock.shutdown(.both) catch |err| {
                log.warn("Error shutting down socket: {}", .{err});
                // it will stop anyway, just a bit later
            };
        }

        self.should_stop = true;
        self.status_changed.broadcast();

        // wait for the read thread to stop
        while (!self.read_thread_stopped) {
            self.status_changed.wait(&self.mutex);
        }
    }

    pub fn stop(self: *Conn) void {
        self.prepareShutdown();

        if (self.read_thread) |*thread| {
            thread.join();
            self.read_thread = null;
        }
    }

    pub fn addServer(self: *Conn, url: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.server_pool.addServer(url, false);
    }

    pub fn connect(self: *Conn, url: []const u8) !void {
        try self.addServer(url);
        try self.start();
        try self.waitForConnected();
    }

    pub fn waitForConnected(self: *Conn) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.sock == null) {
            if (self.last_error) |err| {
                return err;
            }
            self.status_changed.wait(&self.mutex);
        }
    }

    pub fn wait(self: *Conn) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (!self.read_thread_stopped) {
            self.status_changed.wait(&self.mutex);
        }
    }

    pub fn processMsg(self: *Conn, data: []const u8) !void {
        _ = self;
        _ = data;
    }

    pub fn processInfo(self: *Conn, info_json: []const u8) !void {
        _ = self;
        _ = info_json;
    }

    pub fn processErr(self: *Conn, err_msg: []const u8) !void {
        _ = self;
        _ = err_msg;
    }

    pub fn processOK(self: *Conn) !void {
        _ = self;
    }

    pub fn processPing(self: *Conn) !void {
        _ = self;
    }

    pub fn processPong(self: *Conn) !void {
        _ = self;
    }
};
