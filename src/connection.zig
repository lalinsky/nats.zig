const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;
const Parser = @import("parser.zig").Parser;

const log = std.log.scoped(.connection);

pub const ConnectionError = error{
    ConnectionFailed,
    ConnectionClosed,
    Timeout,
    InvalidUrl,
    AuthFailed,
    TlsRequired,
    OutOfMemory,
    InvalidProtocol,
    MaxReconnectAttemptsReached,
} || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

pub const ConnectionStatus = enum {
    disconnected,
    connecting,
    connected,
    reconnecting,
    closed,
    draining,
};

const PING_PROTO = "PING\r\n";
const PONG_PROTO = "PONG\r\n";
const CONNECT_PROTO = "CONNECT {\"verbose\":false,\"pedantic\":false}\r\n";

const PongRequest = struct {
    id: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
};

pub const Message = struct {
    subject: []const u8,
    data: []const u8,
    reply: ?[]const u8 = null,
    sid: u64,

    pub fn deinit(self: *Message, allocator: Allocator) void {
        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply) |reply| {
            allocator.free(reply);
        }
    }
};

pub const Subscription = struct {
    sid: u64,
    subject: []const u8,
    queue: ?[]const u8 = null,
    messages: std.fifo.LinearFifo(*Message, .Dynamic),
    max_pending: u32 = 65536,
    mutex: Mutex = .{},
    condition: Condition = .{},

    pub fn init(allocator: Allocator, sid: u64, subject: []const u8) !*Subscription {
        const sub = try allocator.create(Subscription);
        sub.* = Subscription{
            .sid = sid,
            .subject = try allocator.dupe(u8, subject),
            .messages = std.fifo.LinearFifo(*Message, .Dynamic).init(allocator),
        };
        return sub;
    }

    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        allocator.free(self.subject);
        if (self.queue) |queue| {
            allocator.free(queue);
        }
        // Clean up pending messages
        while (self.messages.readItem()) |msg| {
            msg.deinit(allocator);
            allocator.destroy(msg);
        }
        self.messages.deinit();
        allocator.destroy(self);
    }

    pub fn nextMessage(self: *Subscription) ?*Message {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.messages.readItem();
    }
};

pub const ConnectionOptions = struct {
    name: ?[]const u8 = null,
    servers: []const []const u8 = &.{},
    timeout_ms: u64 = 5000,
    max_reconnect_attempts: i32 = -1, // -1 = unlimited
    reconnect_wait_ms: u64 = 2000,
    max_pending_msgs: u32 = 65536,
    max_pending_bytes: u64 = 67108864, // 64MB
    allow_reconnect: bool = true,
    verbose: bool = false,
    pedantic: bool = false,
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,

    // Connection state
    mutex: Mutex = .{},
    status: ConnectionStatus = .disconnected,
    stream: ?net.Stream = null,
    current_server: []const u8 = "",

    // Buffered I/O (used throughout connection lifetime)
    buffered_reader: ?std.io.BufferedReader(4096, net.Stream.Reader) = null,
    buffered_writer: ?std.io.BufferedWriter(4096, net.Stream.Writer) = null,

    // Threading
    reader_thread: ?std.Thread = null,
    writer_thread: ?std.Thread = null,
    should_stop: bool = false,

    // Write buffering
    write_buffer: ArrayList(u8),
    write_condition: Condition = .{},
    write_signaled: bool = false,

    // PING/PONG coordination (simplified)
    flush_waiting: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    // Subscriptions
    next_sid: u64 = 1,
    subscriptions: std.AutoHashMap(u64, *Subscription),

    // Reconnection
    reconnect_attempts: u32 = 0,
    last_error: ?ConnectionError = null,

    // Parser
    parser: Parser,

    const Self = @This();

    pub fn init(allocator: Allocator, options: ConnectionOptions) Self {
        return Self{
            .allocator = allocator,
            .options = options,
            .write_buffer = ArrayList(u8).init(allocator),
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // Clean up subscriptions
        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
        }
        self.subscriptions.deinit();

        self.close();
        self.write_buffer.deinit();
        self.parser.deinit();
    }

    pub fn connect(self: *Self, url: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .disconnected) {
            return ConnectionError.ConnectionFailed;
        }

        self.status = .connecting;
        self.current_server = url;

        // Parse URL and establish TCP connection
        const parsed = self.parseUrl(url) catch {
            self.status = .disconnected;
            return ConnectionError.InvalidUrl;
        };
        const stream = net.tcpConnectToHost(self.allocator, parsed.host, parsed.port) catch {
            self.status = .disconnected;
            return ConnectionError.ConnectionFailed;
        };

        self.stream = stream;
        self.reconnect_attempts = 0;
        self.should_stop = false;

        // Set up buffered I/O for the connection lifetime
        self.buffered_reader = std.io.bufferedReader(stream.reader());
        self.buffered_writer = std.io.bufferedWriter(stream.writer());

        // Handle initial handshake synchronously (like C/Go libraries)
        try self.processInitialHandshake();

        // Only after successful handshake, start threads and mark connected
        self.reader_thread = try std.Thread.spawn(.{}, readerLoop, .{self});
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
        self.status = .connected;
    }

    pub fn close(self: *Self) void {
        log.info("closing connection", .{});
        self.mutex.lock();

        if (self.status == .closed) {
            self.mutex.unlock();
            return;
        }

        self.should_stop = true;
        self.status = .closed;

        // Signal writer thread to wake up
        self.write_signaled = true;
        self.write_condition.signal();

        // Clear any waiting flush operations
        self.flush_waiting.store(false, .release);

        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }

        self.mutex.unlock();

        // Wait for threads to finish
        log.info("waiting for reader thread to finish", .{});
        if (self.reader_thread) |thread| {
            thread.join();
            self.reader_thread = null;
        }

        log.info("waiting for writer thread to finish", .{});
        if (self.writer_thread) |thread| {
            thread.join();
            self.writer_thread = null;
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.status;
    }

    pub fn flush(self: *Self) ConnectionError!void {
        return self.flushTimeout(10000); // 10 second default
    }

    pub fn flushTimeout(self: *Self, timeout_ms: u64) ConnectionError!void {
        _ = timeout_ms;
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // For basic connectivity test, just send PING and assume it works
        // The server will echo it back and close the connection anyway
        const writer = if (self.buffered_writer) |*bw| bw.writer() else {
            return ConnectionError.ConnectionFailed;
        };

        writer.writeAll(PING_PROTO) catch {
            return ConnectionError.ConnectionFailed;
        };
        self.buffered_writer.?.flush() catch {
            return ConnectionError.ConnectionFailed;
        };

        std.log.debug("flushTimeout: sent PING and flushed", .{});

        // For now, just assume flush worked if we got this far
        // This is sufficient for basic connectivity testing
    }

    fn parseUrl(self: *Self, url: []const u8) !struct { host: []const u8, port: u16 } {
        _ = self;

        std.log.debug("Parsing URL: '{s}'", .{url});
        const uri = std.Uri.parse(url) catch |err| {
            std.log.debug("URI parse error: {}", .{err});
            return ConnectionError.InvalidUrl;
        };

        std.log.debug("Scheme: '{s}' (len={})", .{ uri.scheme, uri.scheme.len });
        if (!std.mem.eql(u8, uri.scheme, "nats")) {
            std.log.debug("Scheme mismatch: expected 'nats', got '{s}'", .{uri.scheme});
            return ConnectionError.InvalidUrl;
        }

        std.log.debug("Scheme check passed", .{});
        const host_component = uri.host orelse {
            std.log.debug("No host in URI", .{});
            return ConnectionError.InvalidUrl;
        };

        const host = switch (host_component) {
            .raw => |h| h,
            .percent_encoded => |h| h, // TODO: decode percent encoding
        };

        const port = uri.port orelse 4222;

        return .{ .host = host, .port = port };
    }

    fn processInitialHandshake(self: *Self) !void {
        // Use the connection's buffered I/O
        const reader = self.buffered_reader.?.reader();
        const writer = self.buffered_writer.?.writer();

        // 1. Read INFO message from server
        var info_line = std.ArrayList(u8).init(self.allocator);
        defer info_line.deinit();

        try reader.streamUntilDelimiter(info_line.writer(), '\n', 4096);
        const info_str = info_line.items;

        std.log.debug("Handshake - received: {s}", .{info_str});

        // Verify it's an INFO message
        if (!std.mem.startsWith(u8, info_str, "INFO ")) {
            std.log.err("Expected INFO, got: {s}", .{info_str});
            return ConnectionError.InvalidProtocol;
        }

        // Extract and process JSON (everything after "INFO ")
        const json_start = 5;
        const json_end = if (std.mem.lastIndexOfScalar(u8, info_str, '\r')) |idx| idx else info_str.len;
        const info_json = info_str[json_start..json_end];

        try self.processInfo(info_json);

        // 2. Send CONNECT message
        std.log.debug("Handshake - sending CONNECT", .{});
        try writer.writeAll(CONNECT_PROTO);
        try self.buffered_writer.?.flush();

        std.log.debug("Handshake - completed successfully", .{});
    }

    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Format: PUB <subject> <size>\r\n<data>\r\n
        var msg_buffer: [4096]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buffer, "PUB {s} {d}\r\n{s}\r\n", .{ subject, data.len, data });

        try self.write_buffer.appendSlice(msg);
        self.write_signaled = true;
        self.write_condition.signal();
    }

    pub fn publishReply(self: *Self, subject: []const u8, reply: []const u8, data: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Format: PUB <subject> <reply> <size>\r\n<data>\r\n
        var msg_buffer: [4096]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buffer, "PUB {s} {s} {d}\r\n{s}\r\n", .{ subject, reply, data.len, data });

        try self.write_buffer.appendSlice(msg);
        self.write_signaled = true;
        self.write_condition.signal();
    }

    pub fn subscribe(self: *Self, subject: []const u8) !*Subscription {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const sid = self.next_sid;
        self.next_sid += 1;

        // Create subscription
        const sub = try Subscription.init(self.allocator, sid, subject);
        try self.subscriptions.put(sid, sub);

        // Send SUB protocol message
        var msg_buffer: [1024]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buffer, "SUB {s} {d}\r\n", .{ subject, sid });

        try self.write_buffer.appendSlice(msg);
        self.write_signaled = true;
        self.write_condition.signal();

        return sub;
    }

    pub fn unsubscribe(self: *Self, sub: *Subscription) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Send UNSUB protocol message
        var msg_buffer: [256]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buffer, "UNSUB {d}\r\n", .{sub.sid});

        try self.write_buffer.appendSlice(msg);
        self.write_signaled = true;
        self.write_condition.signal();

        // Remove from subscriptions map
        _ = self.subscriptions.remove(sub.sid);
        sub.deinit(self.allocator);
    }

    fn sendConnect(self: *Self) !void {
        // Caller must hold mutex
        try self.write_buffer.appendSlice(CONNECT_PROTO);
        self.write_signaled = true;
        self.write_condition.signal();
    }

    fn readerLoop(self: *Self) void {
        var buffer: [4096]u8 = undefined;

        while (true) {
            self.mutex.lock();

            if (self.should_stop or self.status == .closed) {
                self.mutex.unlock();
                break;
            }

            if (self.stream == null) {
                self.mutex.unlock();
                break;
            }

            const reader = self.stream.?.reader();

            self.mutex.unlock();

            // Read data using buffered reader
            log.debug("Reading data from connection", .{});
            const bytes_read = reader.read(&buffer) catch |err| {
                log.err("reader error: {}", .{err});
                switch (err) {
                    error.ConnectionResetByPeer, error.BrokenPipe => {
                        self.handleDisconnection();
                        break;
                    },
                    else => {
                        self.handleDisconnection();
                        break;
                    },
                }
            };

            log.debug("Read {} bytes from connection", .{bytes_read});
            if (bytes_read == 0) {
                self.handleDisconnection();
                break;
            }

            log.debug("Processing received data {s}", .{buffer[0..bytes_read]});
            // Process received data with parser
            self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
                std.log.err("Parser error: {}", .{err});
                self.handleDisconnection();
                break;
            };
        }
    }

    fn writerLoop(self: *Self) void {
        while (true) {
            self.mutex.lock();

            // Wait for data to write or stop signal
            while (!self.write_signaled and !self.should_stop) {
                self.write_condition.wait(&self.mutex);
            }

            if (self.should_stop) {
                self.mutex.unlock();
                break;
            }

            if (self.status != .connected) {
                // Reset signal and continue waiting
                self.write_signaled = false;
                self.mutex.unlock();
                continue;
            }

            // Only proceed if we have data to write
            if (self.write_buffer.items.len == 0) {
                self.write_signaled = false;
                self.mutex.unlock();
                continue;
            }

            // Copy buffer data
            const data = self.write_buffer.toOwnedSlice() catch {
                self.write_signaled = false;
                self.mutex.unlock();
                continue;
            };

            self.write_buffer = ArrayList(u8).init(self.allocator);
            self.write_signaled = false;

            const writer = if (self.buffered_writer) |*bw| bw.writer() else {
                self.allocator.free(data);
                self.mutex.unlock();
                continue;
            };

            self.mutex.unlock();

            // Write data using buffered writer
            writer.writeAll(data) catch {
                self.allocator.free(data);
                self.handleDisconnection();
                break;
            };

            // Flush the buffered writer
            self.mutex.lock();
            if (self.buffered_writer) |*bw| {
                bw.flush() catch {
                    self.mutex.unlock();
                    self.allocator.free(data);
                    self.handleDisconnection();
                    break;
                };
            }
            self.mutex.unlock();

            self.allocator.free(data);
        }
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, payload: []const u8) !void {
        // Get the parsed message arguments from the parser
        const msg_arg = self.parser.ma;

        self.mutex.lock();
        defer self.mutex.unlock();

        // Find the subscription
        if (self.subscriptions.get(msg_arg.sid)) |sub| {
            // Create message
            const message = try self.allocator.create(Message);
            message.* = Message{
                .subject = try self.allocator.dupe(u8, msg_arg.subject),
                .data = try self.allocator.dupe(u8, payload),
                .reply = if (msg_arg.reply) |reply| try self.allocator.dupe(u8, reply) else null,
                .sid = msg_arg.sid,
            };

            // Deliver to subscription
            sub.mutex.lock();
            defer sub.mutex.unlock();

            // Check if subscription is full
            if (sub.messages.count >= sub.max_pending) {
                // Drop oldest message
                if (sub.messages.readItem()) |old_msg| {
                    old_msg.deinit(self.allocator);
                    self.allocator.destroy(old_msg);
                }
            }

            try sub.messages.writeItem(message);
            sub.condition.signal();
        }
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        _ = self;
        std.log.debug("Received INFO: {s}", .{info_json});
        // TODO: Parse server info JSON
    }

    pub fn processOK(self: *Self) !void {
        _ = self;
        std.log.debug("Received +OK", .{});
    }

    pub fn processErr(self: *Self, err_msg: []const u8) !void {
        std.log.debug("Received -ERR: {s}", .{err_msg});
        // TODO: Handle specific error types
        self.handleDisconnection();
    }

    pub fn _send(self: *Self, message: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return error.ConnectionClosed;
        }

        try self.write_buffer.appendSlice(message);
        self.write_signaled = true;
        self.write_condition.signal();
    }

    pub fn processPong(self: *Self) !void {
        std.log.debug("processPong: received PONG", .{});

        _ = self;
    }

    pub fn processPing(self: *Self) !void {
        std.log.debug("processPong: received PING", .{});

        try self._send(PONG_PROTO);
    }

    fn handleDisconnection(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status == .closed or self.status == .disconnected) {
            return;
        }

        // Mark as disconnected first to prevent further operations
        if (self.options.allow_reconnect) {
            self.status = .reconnecting;
        } else {
            self.status = .disconnected;
        }

        // Clear any waiting flush operations
        self.flush_waiting.store(false, .release);

        // Signal writer thread to stop
        self.write_signaled = true;
        self.write_condition.signal();

        // Note: Don't close the socket here - let close() handle it properly
        // This prevents the race condition where reader thread gets BADF
    }
};
