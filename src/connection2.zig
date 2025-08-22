const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Parser = @import("parser.zig").Parser;

const log = std.log.scoped(.connection2);

pub const ConnectionError = error{
    ConnectionFailed,
    ConnectionClosed,
    Timeout,
    InvalidUrl,
    AuthFailed,
    InvalidProtocol,
    OutOfMemory,
} || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

pub const ConnectionStatus = enum {
    disconnected,
    connecting,
    connected,
    closed,
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
    messages: std.fifo.LinearFifo(*Message, .Dynamic),
    mutex: std.Thread.Mutex = .{},

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
    timeout_ms: u64 = 5000,
    verbose: bool = false,
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,
    
    // Network
    stream: ?net.Stream = null,
    status: ConnectionStatus = .disconnected,
    
    // Threading
    reader_thread: ?std.Thread = null,
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    
    // Subscriptions
    next_sid: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    subscriptions: std.AutoHashMap(u64, *Subscription),
    subs_mutex: std.Thread.Mutex = .{},
    
    // Parser
    parser: Parser,

    const Self = @This();

    pub fn init(allocator: Allocator, options: ConnectionOptions) Self {
        return Self{
            .allocator = allocator,
            .options = options,
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();
        
        // Clean up subscriptions
        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
        }
        self.subscriptions.deinit();
        
        self.parser.deinit();
    }

    pub fn connect(self: *Self, url: []const u8) !void {
        if (self.status != .disconnected) {
            return ConnectionError.ConnectionFailed;
        }

        self.status = .connecting;
        
        // Parse URL and establish TCP connection
        const parsed = try self.parseUrl(url);
        const stream = net.tcpConnectToHost(self.allocator, parsed.host, parsed.port) catch {
            self.status = .disconnected;
            return ConnectionError.ConnectionFailed;
        };

        self.stream = stream;
        self.should_stop.store(false, .monotonic);

        // Handle initial handshake
        try self.processInitialHandshake();

        // Start reader thread
        self.reader_thread = try std.Thread.spawn(.{}, readerLoop, .{self});
        self.status = .connected;
        
        log.info("Connected successfully", .{});
    }

    pub fn close(self: *Self) void {
        if (self.status == .closed) return;
        
        log.info("Closing connection", .{});
        self.should_stop.store(true, .release);
        self.status = .closed;

        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }

        if (self.reader_thread) |thread| {
            thread.join();
            self.reader_thread = null;
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        return self.status;
    }

    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const stream = self.stream orelse return ConnectionError.ConnectionClosed;
        const writer = stream.writer();

        // Send PUB command directly - no buffering, no threading complications
        try writer.print("PUB {s} {d}\r\n", .{ subject, data.len });
        try writer.writeAll(data);
        try writer.writeAll("\r\n");
        
        log.debug("Published to {s}: {s}", .{ subject, data });
    }

    pub fn subscribe(self: *Self, subject: []const u8) !*Subscription {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject);

        // Add to subscriptions map
        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();
        try self.subscriptions.put(sid, sub);

        // Send SUB command
        const stream = self.stream orelse return ConnectionError.ConnectionClosed;
        const writer = stream.writer();
        try writer.print("SUB {s} {d}\r\n", .{ subject, sid });
        
        log.debug("Subscribed to {s} with sid {d}", .{ subject, sid });
        return sub;
    }

    pub fn flush(self: *Self) !void {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const stream = self.stream orelse return ConnectionError.ConnectionClosed;
        const writer = stream.writer();
        try writer.writeAll("PING\r\n");
        
        log.debug("Sent PING for flush", .{});
    }

    fn parseUrl(self: *Self, url: []const u8) !struct { host: []const u8, port: u16 } {
        _ = self;
        
        const uri = std.Uri.parse(url) catch return ConnectionError.InvalidUrl;
        
        if (!std.mem.eql(u8, uri.scheme, "nats")) {
            return ConnectionError.InvalidUrl;
        }

        const host_component = uri.host orelse return ConnectionError.InvalidUrl;
        const host = switch (host_component) {
            .raw => |h| h,
            .percent_encoded => |h| h,
        };

        const port = uri.port orelse 4222;
        return .{ .host = host, .port = port };
    }

    fn processInitialHandshake(self: *Self) !void {
        const stream = self.stream orelse return ConnectionError.ConnectionClosed;
        const reader = stream.reader();
        const writer = stream.writer();

        // Read INFO message
        var info_buffer: [4096]u8 = undefined;
        if (try reader.readUntilDelimiterOrEof(info_buffer[0..], '\n')) |info_line| {
            log.debug("Received: {s}", .{info_line});
            
            if (!std.mem.startsWith(u8, info_line, "INFO ")) {
                return ConnectionError.InvalidProtocol;
            }
        } else {
            return ConnectionError.ConnectionClosed;
        }

        // Send CONNECT + PING
        const connect_msg = if (self.options.verbose) 
            "CONNECT {\"verbose\":true,\"pedantic\":false}\r\n"
        else 
            "CONNECT {\"verbose\":false,\"pedantic\":false}\r\n";
            
        try writer.writeAll(connect_msg);
        try writer.writeAll("PING\r\n");

        // Wait for PONG (or +OK then PONG if verbose)
        var response_buffer: [256]u8 = undefined;
        if (try reader.readUntilDelimiterOrEof(response_buffer[0..], '\n')) |response| {
            log.debug("Handshake response: {s}", .{response});
            
            // Handle +OK if verbose
            if (std.mem.startsWith(u8, response, "+OK")) {
                // Read next line for PONG
                if (try reader.readUntilDelimiterOrEof(response_buffer[0..], '\n')) |pong_line| {
                    if (!std.mem.startsWith(u8, pong_line, "PONG")) {
                        return ConnectionError.InvalidProtocol;
                    }
                } else {
                    return ConnectionError.ConnectionClosed;
                }
            } else if (!std.mem.startsWith(u8, response, "PONG")) {
                if (std.mem.startsWith(u8, response, "-ERR")) {
                    return ConnectionError.AuthFailed;
                }
                return ConnectionError.InvalidProtocol;
            }
        } else {
            return ConnectionError.ConnectionClosed;
        }

        log.debug("Handshake completed successfully", .{});
    }

    fn readerLoop(self: *Self) void {
        var buffer: [4096]u8 = undefined;
        const stream = self.stream orelse return;
        const reader = stream.reader();

        log.debug("Reader loop started", .{});

        while (!self.should_stop.load(.acquire)) {
            const bytes_read = reader.read(&buffer) catch |err| {
                log.err("Read error: {}", .{err});
                break;
            };

            if (bytes_read == 0) {
                log.debug("Connection closed by server", .{});
                break;
            }

            log.debug("Read {} bytes: {s}", .{ bytes_read, buffer[0..bytes_read] });

            // Parse the received data
            self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
                log.err("Parser error: {}", .{err});
                break;
            };
        }

        log.debug("Reader loop exited", .{});
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, payload: []const u8) !void {
        const msg_arg = self.parser.ma;

        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();

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
            try sub.messages.writeItem(message);
            
            log.debug("Delivered message to subscription {d}: {s}", .{ msg_arg.sid, payload });
        }
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        _ = self;
        log.debug("Received INFO: {s}", .{info_json});
    }

    pub fn processOK(self: *Self) !void {
        _ = self;
        log.debug("Received +OK", .{});
    }

    pub fn processErr(self: *Self, err_msg: []const u8) !void {
        _ = self;
        log.err("Received -ERR: {s}", .{err_msg});
    }

    pub fn processPong(self: *Self) !void {
        _ = self;
        log.debug("Received PONG", .{});
    }

    pub fn processPing(self: *Self) !void {
        if (self.status == .connected and self.stream != null) {
            const writer = self.stream.?.writer();
            writer.writeAll("PONG\r\n") catch |err| {
                log.err("Failed to send PONG: {}", .{err});
            };
            log.debug("Sent PONG in response to PING", .{});
        }
    }
};