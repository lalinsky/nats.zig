const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Parser = @import("parser.zig").Parser;
const inbox = @import("inbox.zig");
const Message = @import("message.zig").Message;
const subscription_mod = @import("subscription.zig");
const Subscription = subscription_mod.Subscription;
const MsgHandler = subscription_mod.MsgHandler;

const log = std.log.scoped(.connection);

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



pub const ConnectionOptions = struct {
    name: ?[]const u8 = null,
    timeout_ms: u64 = 5000,
    verbose: bool = false,
    send_asap: bool = false,
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,

    // Network
    stream: ?net.Stream = null,
    status: ConnectionStatus = .disconnected,

    // Threading
    reader_thread: ?std.Thread = null,
    flusher_thread: ?std.Thread = null,
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    // Main connection mutex (protects most fields)
    mutex: std.Thread.Mutex = .{},

    // Flusher synchronization (protected by main mutex)
    flusher_stop: bool = false,
    flusher_signaled: bool = false,
    flusher_condition: std.Thread.Condition = .{},

    // Write buffer (protected by main mutex)
    write_buffer: std.ArrayListUnmanaged(u8) = .{},

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

        // Clean up write buffer
        self.write_buffer.deinit(self.allocator);

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

        // Start flusher thread if not send_asap mode
        if (!self.options.send_asap) {
            self.flusher_thread = try std.Thread.spawn(.{}, flusherLoop, .{self});
        }

        self.status = .connected;

        log.info("Connected successfully", .{});
    }

    pub fn close(self: *Self) void {
        if (self.status == .closed) return;

        log.info("Closing connection", .{});
        self.should_stop.store(true, .release);
        self.status = .closed;

        // Stop flusher thread
        if (self.flusher_thread) |thread| {
            self.mutex.lock();
            self.flusher_stop = true;
            self.flusher_condition.signal();
            self.mutex.unlock();

            thread.join();
            self.flusher_thread = null;
        }

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

        // Format the complete PUB message
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        try buffer.writer().print("PUB {s} {d}\r\n", .{ subject, data.len });
        try buffer.appendSlice(data);
        try buffer.appendSlice("\r\n");

        // Send via buffer (either immediate or flusher thread)
        try self.bufferWrite(buffer.items);

        log.debug("Published to {s}: {s}", .{ subject, data });
    }

    pub fn subscribeSync(self: *Self, subject: []const u8) !*Subscription {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.initSync(self.allocator, sid, subject);

        // Add to subscriptions map
        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();
        try self.subscriptions.put(sid, sub);

        // Send SUB command via buffer
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();
        try buffer.writer().print("SUB {s} {d}\r\n", .{ subject, sid });
        try self.bufferWrite(buffer.items);

        log.debug("Subscribed to {s} with sid {d} (sync)", .{ subject, sid });
        return sub;
    }
    
    pub fn subscribe(self: *Self, subject: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Create type-erased message handler
        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.initAsync(self.allocator, sid, subject, handler);

        // Add to subscriptions map
        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();
        try self.subscriptions.put(sid, sub);

        // Send SUB command via buffer
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();
        try buffer.writer().print("SUB {s} {d}\r\n", .{ subject, sid });
        try self.bufferWrite(buffer.items);

        log.debug("Subscribed to {s} with sid {d} (async)", .{ subject, sid });
        return sub;
    }
    
    pub fn unsubscribe(self: *Self, sub: *Subscription) !void {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Remove from subscriptions map
        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();
        _ = self.subscriptions.remove(sub.sid);

        // Send UNSUB command
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();
        try buffer.writer().print("UNSUB {d}\r\n", .{sub.sid});
        try self.bufferWrite(buffer.items);

        log.debug("Unsubscribed from {s} with sid {d}", .{ sub.subject, sub.sid });
    }

    pub fn flush(self: *Self) !void {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        if (self.options.send_asap) {
            // In send_asap mode, everything is already sent
            return;
        }

        // Force flush any pending writes
        self.mutex.lock();
        const has_pending = self.write_buffer.items.len > 0;
        if (has_pending) {
            self.flusher_signaled = true;
            self.flusher_condition.signal();
        }
        self.mutex.unlock();

        // Send PING to ensure server acknowledges all previous messages
        try self.bufferWrite("PING\r\n");

        log.debug("Sent PING for flush", .{});
    }
    
    pub fn publishRequest(self: *Self, subject: []const u8, reply: []const u8, data: []const u8) !void {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Format: PUB <subject> <reply> <size>\r\n<data>\r\n
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        try buffer.writer().print("PUB {s} {s} {d}\r\n", .{ subject, reply, data.len });
        try buffer.appendSlice(data);
        try buffer.appendSlice("\r\n");

        // Send via buffer
        try self.bufferWrite(buffer.items);

        log.debug("Published request to {s} with reply {s}: {s}", .{ subject, reply, data });
    }
    
    pub fn request(self: *Self, subject: []const u8, data: []const u8, timeout_ns: u64) !?*Message {
        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // 1. Create unique inbox
        const reply_subject = try inbox.newInbox(self.allocator);
        defer self.allocator.free(reply_subject);

        // 2. Subscribe to inbox
        const sub = try self.subscribeSync(reply_subject);
        defer {
            self.unsubscribe(sub) catch |err| {
                log.warn("Failed to unsubscribe from inbox: {}", .{err});
            };
            sub.deinit(self.allocator);
        }

        // 3. Publish with reply-to
        try self.publishRequest(subject, reply_subject, data);

        // 4. Wait for response with timeout
        return sub.nextMessageTimeout(timeout_ns);
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

    fn flusherLoop(self: *Self) void {
        log.debug("Flusher loop started", .{});

        while (true) {
            self.mutex.lock();

            // Wait for signal or stop condition
            while (!self.flusher_signaled and !self.flusher_stop) {
                self.flusher_condition.wait(&self.mutex);
            }

            if (self.flusher_stop) {
                log.debug("Flusher stopping...", .{});
                self.mutex.unlock();
                break;
            }

            // Give a chance to accumulate more requests (like C implementation)
            self.flusher_condition.timedWait(&self.mutex, 1_000_000) catch {}; // 1ms in nanoseconds

            self.flusher_signaled = false;

            // Check if we should flush
            const should_flush = (self.status == .connected) and
                                (self.stream != null) and
                                (self.write_buffer.items.len > 0);

            if (should_flush) {
                const stream = self.stream.?;
                const writer = stream.writer();

                // Write all buffered data
                if (writer.writeAll(self.write_buffer.items)) {
                    log.debug("Flushed {} bytes", .{self.write_buffer.items.len});
                    self.write_buffer.clearRetainingCapacity();
                } else |err| {
                    log.err("Flush error: {}", .{err});
                    // Keep the data in buffer for retry
                }
            }

            self.mutex.unlock();
        }

        log.debug("Flusher loop exited", .{});
    }

    fn bufferWrite(self: *Self, data: []const u8) !void {
        if (self.options.send_asap) {
            // Send immediately
            const stream = self.stream orelse return ConnectionError.ConnectionClosed;
            const writer = stream.writer();
            try writer.writeAll(data);
        } else {
            // Buffer and signal flusher
            self.mutex.lock();
            defer self.mutex.unlock();

            try self.write_buffer.appendSlice(self.allocator, data);
            
            if (!self.flusher_signaled) {
                self.flusher_signaled = true;
                self.flusher_condition.signal();
            }
        }
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, payload: []const u8) !void {
        const msg_arg = self.parser.ma;

        // Create message copying data from parser buffers (like C implementation)
        // This is necessary because parser buffers get reused on next read
        const message = try Message.init(
            self.allocator,
            msg_arg.subject,
            msg_arg.reply,
            payload
        );
        message.sid = msg_arg.sid;
        // TODO: handle headers when parser supports them

        // Retain subscription while holding lock, then release lock
        self.subs_mutex.lock();
        const sub = self.subscriptions.get(msg_arg.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock();

        if (sub) |s| {
            defer s.release(self.allocator); // Release when done
            
            if (s.handler) |handler| {
                // Execute callback without holding locks
                handler.call(message);
            } else {
                // Sync subscription - queue message
                s.mutex.lock();
                defer s.mutex.unlock();
                try s.messages.writeItem(message);
            }

            log.debug("Delivered message to subscription {d}: {s}", .{ msg_arg.sid, payload });
        } else {
            // No subscription found, clean up message
            message.deinit();
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
