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
const net = std.net;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Parser = @import("parser.zig").Parser;
const inbox = @import("inbox.zig");
const Message = @import("message.zig").Message;
const MessageList = @import("message.zig").MessageList;
const subscription_mod = @import("subscription.zig");
const Subscription = subscription_mod.Subscription;
const MsgHandler = subscription_mod.MsgHandler;
const dispatcher_mod = @import("dispatcher.zig");
const DispatcherPool = dispatcher_mod.DispatcherPool;
const server_pool_mod = @import("server_pool.zig");
const ServerPool = server_pool_mod.ServerPool;
const Server = server_pool_mod.Server;
const jetstream_mod = @import("jetstream.zig");
const JetStream = jetstream_mod.JetStream;
const JetStreamOptions = jetstream_mod.JetStreamOptions;
const build_options = @import("build_options");
const ConcurrentWriteBuffer = @import("queue.zig").ConcurrentWriteBuffer;
const ResponseManager = @import("response_manager.zig").ResponseManager;
const Socket = @import("socket.zig").Socket;
const MAX_CONTROL_LINE_SIZE = @import("parser.zig").MAX_CONTROL_LINE_SIZE;

const log = std.log.scoped(.connection);

pub const ServerVersion = struct {
    major: u32 = 0,
    minor: u32 = 0,
    update: u32 = 0,

    pub fn isAtLeast(self: ServerVersion, major: u32, minor: u32, update: u32) bool {
        return (self.major > major) or
            (self.major == major and self.minor > minor) or
            (self.major == major and self.minor == minor and self.update >= update);
    }

    pub fn parse(version_str: ?[]const u8) ServerVersion {
        var result = ServerVersion{};

        if (version_str) |str| {
            var iter = std.mem.splitSequence(u8, str, ".");

            if (iter.next()) |major_str| {
                result.major = std.fmt.parseInt(u32, major_str, 10) catch 0;
            }
            if (iter.next()) |minor_str| {
                result.minor = std.fmt.parseInt(u32, minor_str, 10) catch 0;
            }
            if (iter.next()) |update_str| {
                result.update = std.fmt.parseInt(u32, update_str, 10) catch 0;
            }
        }

        return result;
    }
};

pub const ServerInfo = struct {
    server_id: ?[]const u8 = null,
    server_name: ?[]const u8 = null,
    version: ?[]const u8 = null,
    host: ?[]const u8 = null,
    port: i32 = 4222,
    auth_required: bool = false,
    tls_required: bool = false,
    tls_available: bool = false,
    max_payload: i64 = 1048576,
    connect_urls: ?[][]const u8 = null,
    proto: i32 = 1,
    client_id: u64 = 0,
    nonce: ?[]const u8 = null,
    client_ip: ?[]const u8 = null,
    ldm: bool = false, // lame_duck_mode
    headers: bool = false,

    // Parsed version for easy comparison (like C implementation's srvVersion)
    parsed_version: ServerVersion = .{},
};

pub const ConnectionClosedError = error{
    ConnectionClosed,
};

pub const PublishError = error{
    MaxPayload,
    InvalidSubject,
} || ConnectionClosedError || std.mem.Allocator.Error;

pub const ConnectionError = error{
    ConnectionFailed,
    ConnectionClosed,
    Timeout,
    InvalidUrl,
    AuthFailed,
    InvalidProtocol,
    OutOfMemory,
    NoResponders,
} || PublishError || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

pub const ConnectionStatus = enum {
    disconnected,
    connecting,
    connected,
    closed, // explicitly closed, can't be automatically reconnected
};

pub const ReconnectOptions = struct {
    max_reconnect: i32 = 60, // -1 = unlimited
    reconnect_wait_ms: u64 = 2000, // milliseconds
    reconnect_jitter_ms: u64 = 100,
    reconnect_jitter_tls_ms: u64 = 1000,
    reconnect_buf_size: usize = 8 * 1024 * 1024,
    allow_reconnect: bool = true,
    custom_reconnect_delay_cb: ?*const fn (attempts: u32) u64 = null,
};

pub const ConnectionCallbacks = struct {
    disconnected_cb: ?*const fn (*Connection) void = null,
    reconnected_cb: ?*const fn (*Connection) void = null,
    closed_cb: ?*const fn (*Connection) void = null,
    error_cb: ?*const fn (*Connection, []const u8) void = null,
};

pub const ConnectionOptions = struct {
    name: ?[]const u8 = null,
    timeout_ms: u64 = 5000,
    verbose: bool = false,
    send_asap: bool = false,
    reconnect: ReconnectOptions = .{},
    callbacks: ConnectionCallbacks = .{},
    trace: bool = false,
    no_responders: bool = true,
    // Internal wait time in connection threads
    idle_wait_ms: u64 = 60000,
    max_scratch_size: usize = 1024 * 1024 * 10,
};

const HandshakeState = packed struct(u4) {
    pending_info: bool = true,
    pending_connect: bool = true,
    pending_ping: bool = true,
    pending_pong: bool = true,

    pub fn reset(self: *HandshakeState) void {
        self.* = .{};
    }

    // Returns true if we are not longer waiting for any operation
    pub fn isComplete(self: HandshakeState) bool {
        return !(self.pending_info or self.pending_connect or self.pending_ping or self.pending_pong);
    }
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,

    // Main connection mutex (protects most fields)
    mutex: std.Thread.Mutex = .{},

    // Network
    socket: ?Socket = null,
    status: ConnectionStatus = .disconnected,
    status_changed: std.Thread.Condition = .{},

    // Server management
    server_pool: ServerPool,
    current_server: ?*Server = null, // Track current server like C library
    server_info: ServerInfo = .{}, // Current server info from INFO message
    server_info_arena: std.heap.ArenaAllocator, // Arena for server_info strings

    last_received_error: ?[]const u8 = null,

    // Handshake state
    in_handshake: bool = false,
    handshake: HandshakeState = .{},

    // Reconnection state
    reconnect_thread: ?std.Thread = null,
    in_reconnect: i32 = 0, // Regular int like C library, protected by mutex
    abort_reconnect: bool = false, // Like C library's nc->ar flag, protected by mutex
    pending_buffer: WriteBuffer,

    // Reconnection coordination
    reconnect_condition: std.Thread.Condition = .{},

    // Stop flag for both threads
    should_stop: bool = false,

    // Reader thread
    reader_thread: ?std.Thread = null,
    reader_condition: std.Thread.Condition = .{},
    reader_waiting_for_socket: bool = false,
    reader_waiting_for_socket_cond: std.Thread.Condition = .{},

    // Flusher thread
    flusher_thread: ?std.Thread = null,
    flusher_signaled: bool = false,
    flusher_asap: bool = false,
    flusher_condition: std.Thread.Condition = .{},
    flusher_waiting_for_socket: bool = false,
    flusher_waiting_for_socket_cond: std.Thread.Condition = .{},

    // PING/PONG flush tracking (simplified counter approach)
    outgoing_pings: u64 = 0,
    incoming_pongs: u64 = 0,
    pong_condition: std.Thread.Condition = .{},

    // Write buffer (thread-safe, 64KB chunk size)
    write_buffer: WriteBuffer,

    // Subscriptions
    next_sid: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    subscriptions: std.AutoHashMap(u64, *Subscription),
    subs_mutex: std.Thread.Mutex = .{},

    // Message dispatching
    dispatcher_pool: ?*DispatcherPool = null,

    // Response management (shared subscription for request/reply)
    response_manager: ResponseManager,

    scratch: std.heap.ArenaAllocator,

    // Parser
    parser: Parser,

    const Self = @This();
    const WriteBuffer = ConcurrentWriteBuffer(65536); // 64KB chunk size

    pub fn init(allocator: Allocator, options: ConnectionOptions) Self {
        return Self{
            .allocator = allocator,
            .options = options,
            .server_pool = ServerPool.init(allocator),
            .server_info_arena = std.heap.ArenaAllocator.init(allocator),
            .pending_buffer = WriteBuffer.init(allocator, .{ .max_size = options.reconnect.reconnect_buf_size }),
            .write_buffer = WriteBuffer.init(allocator, .{}),
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .response_manager = ResponseManager.init(allocator),
            .parser = Parser.init(allocator),
            .scratch = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();

        // Release global dispatcher pool
        if (self.dispatcher_pool != null) {
            dispatcher_mod.releaseGlobalPool();
            self.dispatcher_pool = null;
        }

        // Clean up subscriptions - release connection's references first
        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.release(); // Release connection's ownership reference
        }
        self.subscriptions.deinit();

        // Clean up the buffers
        self.pending_buffer.deinit();
        self.write_buffer.deinit();

        // Clean up response manager
        self.response_manager.deinit();

        // Clean up server pool
        self.server_pool.deinit();

        // Clean up server info arena
        self.server_info_arena.deinit();

        self.parser.deinit();
        self.scratch.deinit();

        if (self.last_received_error) |err_msg| {
            self.allocator.free(err_msg);
        }
    }

    fn resetScratch(self: *Self) void {
        _ = self.scratch.reset(.{ .retain_with_limit = self.options.max_scratch_size });
    }

    pub fn newMsg(self: *Self) !*Message {
        return self.parser.msg_pool.acquire();
    }

    /// Ensure dispatcher pool is initialized (lazy initialization)
    fn ensureDispatcherPool(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.dispatcher_pool != null) return; // Already initialized

        self.dispatcher_pool = try dispatcher_mod.acquireGlobalPool(self.allocator);

        log.debug("Acquired global dispatcher pool", .{});
    }

    pub fn isConnected(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.status == .connected;
    }

    pub fn connect(self: *Self, url: []const u8) !void {
        try self.addServer(url);

        errdefer {
            // wait for the threads to finish
            self.joinThreads();
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.socket != null) {
            return error.AlreadyConnected;
        }

        self.updateStatus(.connecting);

        errdefer {
            // this will wake up the threads if they are in IO wait
            self.closeSocket();

            // this will wake them up if they are in condition wait
            self.should_stop = true;
            self.reader_condition.signal();
            self.flusher_condition.signal();

            // announce that we are closed
            self.updateStatus(.closed);
        }

        try self.openSocket();
        try self.startThreads();
        try self.waitForHandshake();
    }

    pub fn addServer(self: *Self, url_str: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = try self.server_pool.addServer(url_str, false); // Explicit server
    }

    fn closeSocket(self: *Self) void {
        if (self.socket) |sock| {
            sock.shutdown(.both) catch |err| {
                log.err("Socket shutdown failed: {}", .{err});
            };
            sock.close();
            self.socket = null;
        }
    }

    fn openSocket(self: *Self) !void {
        std.debug.assert(self.socket == null);

        const server = try self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) orelse {
            log.err("No servers available", .{});
            return error.NoServersAvailable;
        };

        self.current_server = server;

        const host = server.parsed_url.host;
        const port = server.parsed_url.port;

        log.debug("Connecting to server {s}:{d}", .{ host, port });
        self.socket = Socket.connect(self.allocator, host, port) catch |err| {
            log.debug("Failed to connect to server {s}:{d}: {}", .{ host, port, err });
            return err;
        };

        log.info("Connected successfully to server {s}:{d}", .{ host, port });
    }

    fn startReaderThread(self: *Self) !void {
        std.debug.assert(self.reader_thread == null);
        log.debug("Starting reader thread", .{});
        self.reader_thread = std.Thread.spawn(.{}, readerLoop, .{self}) catch |err| {
            log.err("Failed to start reader thread: {}", .{err});
            return err;
        };
    }

    fn startFlusherThread(self: *Self) !void {
        std.debug.assert(self.flusher_thread == null);
        log.debug("Starting flusher thread", .{});
        self.flusher_thread = std.Thread.spawn(.{}, flusherLoop, .{self}) catch |err| {
            log.err("Failed to start flusher thread: {}", .{err});
            return err;
        };
    }

    fn joinAndClearReaderThread(self: *Self) void {
        if (self.reader_thread) |thread| {
            log.debug("Waiting for reader thread to finish", .{});
            thread.join();
            self.reader_thread = null;
        }
    }

    fn joinAndClearFlusherThread(self: *Self) void {
        if (self.flusher_thread) |thread| {
            log.debug("Waiting for flusher thread to finish", .{});
            thread.join();
            self.flusher_thread = null;
        }
    }

    fn joinThreads(self: *Self) void {
        self.joinAndClearReaderThread();
        self.joinAndClearFlusherThread();
    }

    fn wakeUpThreads(self: *Self) void {
        self.reader_condition.signal();
        self.flusher_signaled = true;
        self.flusher_condition.signal();
    }

    fn stopThreads(self: *Self) void {
        self.should_stop = true;
        self.wakeUpThreads();
    }

    fn startThreads(self: *Self) !void {
        self.should_stop = false;
        try self.startReaderThread();
        try self.startFlusherThread();
    }

    fn waitForHandshake(self: *Self) !void {
        log.debug("Starting handshake", .{});
        self.in_handshake = true;
        self.handshake.reset();

        const timeout_ns = self.options.timeout_ms * std.time.ns_per_ms;
        var timer = std.time.Timer.start() catch unreachable;
        while (true) {
            if (self.last_received_error) |err_msg| {
                log.err("Server error: {s}", .{err_msg});
                return error.ConnectionFailed;
            }

            if (self.handshake.isComplete()) {
                log.info("Handshake completed successfully", .{});
                self.in_handshake = false;
                self.status = .connected;
                self.status_changed.broadcast();
                return;
            }

            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }
            try self.status_changed.timedWait(&self.mutex, timeout_ns - elapsed_ns);
        }
    }

    pub fn close(self: *Self) void {
        defer self.joinThreads();

        defer if (self.reconnect_thread) |thread| {
            log.debug("Waiting for reconnection thread to complete", .{});
            thread.join();
            self.reconnect_thread = null;
        };

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status == .closed) {
            return;
        }

        log.info("Closing connection", .{});

        self.closeSocket();
        self.stopThreads();

        self.status = .closed;

        // Clear pending flushes (wake up any waiting flush() calls)
        self.clearPendingFlushes();

        // Get reconnection thread reference and wake it up
        self.abort_reconnect = true;
        self.reconnect_condition.signal();

        // Reset the buffers
        self.pending_buffer.reset();
        self.write_buffer.reset();
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        // Lock like C library status getter
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.status;
    }

    /// Publishes data on a subject.
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined, // we don't need a fully constructed arena for this
        };
        return self.publishMsgInternal(&msg, null, false);
    }

    /// Publishes a message on a subject.
    pub fn publishMsg(self: *Self, msg: *Message) !void {
        return self.publishMsgInternal(msg, null, false);
    }

    /// Publishes data on a subject, with a reply subject.
    pub fn publishRequest(self: *Self, subject: []const u8, reply: []const u8, data: []const u8) !void {
        var msg = Message{
            .subject = subject,
            .reply = reply,
            .data = data,
            .pool = null,
            .arena = undefined, // we don't need a fully constructed arena for this
        };
        return self.publishMsgInternal(&msg, null, true);
    }

    /// Publishes a message on a subject, with a reply subject.
    pub fn publishRequestMsg(self: *Self, msg: *Message, reply: []const u8) !void {
        return self.publishMsgInternal(msg, reply, true);
    }

    fn publishMsgInternal(self: *Self, msg: *Message, reply_override: ?[]const u8, asap: bool) !void {
        if (msg.subject.len == 0) {
            return error.InvalidSubject;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        // TODO pre-allocate headers_buffer
        var headers_buffer = ArrayList(u8).init(allocator);
        defer headers_buffer.deinit();

        try msg.encodeHeaders(headers_buffer.writer());
        const headers_len = headers_buffer.items.len;

        const total_payload = headers_len + msg.data.len;

        if (self.server_info.max_payload > 0 and total_payload > @as(usize, @intCast(self.server_info.max_payload))) {
            return PublishError.MaxPayload;
        }

        const reply_to_use = reply_override orelse msg.reply;

        var buffer = try std.ArrayListUnmanaged(u8).initCapacity(allocator, MAX_CONTROL_LINE_SIZE + total_payload);
        defer buffer.deinit(allocator);

        var buffer_writer = buffer.fixedWriter();

        if (headers_len > 0) {
            // HPUB <subject> [reply] <headers_len> <total_len>\r\n<headers><data>\r\n
            if (reply_to_use) |reply| {
                try buffer_writer.print("HPUB {s} {s} {d} {d}\r\n", .{ msg.subject, reply, headers_len, total_payload });
            } else {
                try buffer_writer.print("HPUB {s} {d} {d}\r\n", .{ msg.subject, headers_len, total_payload });
            }
            try buffer_writer.writeAll(headers_buffer.items);
            try buffer_writer.writeAll(msg.data);
            try buffer_writer.writeAll("\r\n");
        } else {
            // PUB <subject> [reply] <size>\r\n<data>\r\n
            if (reply_to_use) |reply| {
                try buffer_writer.print("PUB {s} {s} {d}\r\n", .{ msg.subject, reply, msg.data.len });
            } else {
                try buffer_writer.print("PUB {s} {d}\r\n", .{ msg.subject, msg.data.len });
            }
            try buffer_writer.writeAll(msg.data);
            try buffer_writer.writeAll("\r\n");
        }

        // Allow publishes when connected or reconnecting (buffered).
        // Reject when not usable for sending.
        if (self.status == .closed) {
            return ConnectionError.ConnectionClosed;
        }

        try self.bufferWrite(buffer.items, asap);

        if (reply_to_use) |reply| {
            log.debug("Published message to {s} with reply {s}", .{ msg.subject, reply });
        } else {
            log.debug("Published message to {s}", .{msg.subject});
        }
    }

    fn subscribeInternal(self: *Self, sub: *Subscription) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();

        try self.subscriptions.put(sub.sid, sub);
        sub.retain(); // Connection takes ownership reference

        errdefer {
            if (self.subscriptions.remove(sub.sid)) {
                sub.release();
            }
        }

        // Send SUB command via buffer
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();
        if (sub.queue_group) |group| {
            try buffer.writer().print("SUB {s} {s} {d}\r\n", .{ sub.subject, group, sub.sid });
        } else {
            try buffer.writer().print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
        }
        try self.bufferWrite(buffer.items, false);
    }

    pub fn subscribe(self: *Self, subject: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, null, handler);
        errdefer sub.deinit();

        try self.ensureDispatcherPool();
        sub.dispatcher = self.dispatcher_pool.?.assignDispatcher();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with sid {d} (async)", .{ sub.subject, sub.sid });
        return sub;
    }

    /// Subscribe to a subject, the code is responsible for handling the fetching
    pub fn subscribeSync(self: *Self, subject: []const u8) !*Subscription {
        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, null, null);
        errdefer sub.deinit();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with sid {d} (sync)", .{ sub.subject, sub.sid });
        return sub;
    }

    pub fn queueSubscribe(self: *Self, subject: []const u8, queue_group: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        if (queue_group.len == 0) return error.EmptyQueueGroupName;

        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, queue_group, handler);
        errdefer sub.deinit();

        try self.ensureDispatcherPool();
        sub.dispatcher = self.dispatcher_pool.?.assignDispatcher();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with queue group '{s}' and sid {d} (async)", .{ sub.subject, queue_group, sub.sid });
        return sub;
    }

    /// Subscribe to a subject, the code is responsible for handling the fetching
    pub fn queueSubscribeSync(self: *Self, subject: []const u8, queue_group: []const u8) !*Subscription {
        if (queue_group.len == 0) return error.EmptyQueueGroupName;

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, queue_group, null);
        errdefer sub.deinit();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with queue group '{s}' and sid {d} (sync)", .{ sub.subject, queue_group, sub.sid });
        return sub;
    }

    pub fn unsubscribe(self: *Self, sub: *Subscription) !void {
        // Lock asaply like C library
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Remove from subscriptions map
        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();
        if (self.subscriptions.remove(sub.sid)) {
            sub.release(); // Release connection's ownership reference
        }

        // Send UNSUB command
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();
        try buffer.writer().print("UNSUB {d}\r\n", .{sub.sid});
        try self.bufferWrite(buffer.items, false);

        log.debug("Unsubscribed from {s} with sid {d}", .{ sub.subject, sub.sid });
    }

    pub fn flush(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        switch (self.status) {
            .closed => return error.ConnectionClosed,
            .connecting => return error.ConnectionDisconnected,
            else => {},
        }

        // Buffer the PING first (can fail on allocation)
        try self.bufferWrite("PING\r\n", true); // ASAP=true for immediate flush

        // Only increment counter after successful buffering
        self.outgoing_pings += 1;
        const our_ping_id = self.outgoing_pings;

        log.debug("Sent PING with ping_id={}, waiting for PONG", .{our_ping_id});

        const timeout_ns = self.options.timeout_ms * std.time.ns_per_ms;
        var timer = try std.time.Timer.start();

        while (self.incoming_pongs < our_ping_id) {
            switch (self.status) {
                .closed => return error.ConnectionClosed,
                .connecting => return error.ConnectionDisconnected,
                else => {},
            }

            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                log.warn("Flush timeout waiting for PONG", .{});
                return ConnectionError.Timeout;
            }

            const remaining_ns = timeout_ns - elapsed_ns;
            self.pong_condition.timedWait(&self.mutex, remaining_ns) catch {};
        }

        log.debug("Flush completed, received PONG for ping_id={}", .{our_ping_id});
    }

    pub fn request(self: *Self, subject: []const u8, data: []const u8, timeout_ms: u64) !*Message {
        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined,
        };
        return self.requestMsg(&msg, timeout_ms);
    }

    pub fn requestMsg(self: *Self, msg: *Message, timeout_ms: u64) !*Message {
        if (self.options.trace) {
            log.debug("Sending request message to {s} with timeout {d}ms", .{ msg.subject, timeout_ms });
        }

        // Ensure response system is initialized (without mutex held)
        try self.response_manager.ensureInitialized(self);

        // Create request handle
        const handle = try self.response_manager.createRequest();
        defer self.response_manager.cleanupRequest(handle);

        // Get reply subject for the request (like C library)
        const reply_subject = try self.response_manager.getReplySubject(self.allocator, handle);
        defer self.allocator.free(reply_subject);

        // Publish the request message
        try self.publishRequestMsg(msg, reply_subject);

        // Wait for response
        const reply_msg = try self.response_manager.waitForResponse(handle, timeout_ms * std.time.ns_per_ms);

        // Check for "no responders" like C library
        if (reply_msg.isNoResponders()) {
            reply_msg.deinit();
            return ConnectionError.NoResponders;
        }

        return reply_msg;
    }

    pub const RequestManyOptions = ResponseManager.WaitForMultiResponseOptions;

    pub fn requestMany(self: *Self, subject: []const u8, data: []const u8, timeout_ms: u64, options: RequestManyOptions) !MessageList {
        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined,
        };
        return self.requestManyMsg(&msg, timeout_ms, options);
    }

    pub fn requestManyMsg(self: *Self, msg: *Message, timeout_ms: u64, options: RequestManyOptions) !MessageList {
        if (self.options.trace) {
            log.debug("Sending request-many message to {s} with timeout {d}ms", .{ msg.subject, timeout_ms });
        }

        // Ensure response system is initialized (without mutex held)
        try self.response_manager.ensureInitialized(self);

        // Create multi-request handle
        const handle = try self.response_manager.createMultiRequest();
        defer self.response_manager.cleanupRequest(handle);

        // Get reply subject for the request
        const reply_subject = try self.response_manager.getReplySubject(self.allocator, handle);
        defer self.allocator.free(reply_subject);

        // Publish the request message
        try self.publishRequestMsg(msg, reply_subject);

        // Wait for multiple responses
        const messages = try self.response_manager.waitForMultiResponse(handle, timeout_ms * std.time.ns_per_ms, options);

        if (self.options.trace) {
            log.debug("Received {} responses for request-many to {s}", .{ messages.len, msg.subject });
        }

        return messages;
    }
    fn readerLoop(self: *Self) void {
        log.debug("Reader thread started", .{});

        self.mutex.lock();
        defer self.mutex.unlock();

        var buffer: [4096]u8 = undefined;

        while (!self.should_stop) {
            const socket = self.socket orelse {
                // If we don't have a socket, wait
                log.info("[reader] Waiting for socket", .{});
                self.reader_waiting_for_socket = true;
                self.reader_waiting_for_socket_cond.broadcast();
                self.reader_condition.wait(&self.mutex);
                continue;
            };
            self.reader_waiting_for_socket = false;

            // Unlock before I/O
            self.mutex.unlock();
            defer self.mutex.lock(); // Re-lock at end of iteration

            // Simple blocking read - shutdown() will wake us up
            const bytes_read = socket.read(&buffer) catch |err| {
                log.err("Read error: {}", .{err});
                self.processConnectionError(err);
                continue;
            };

            if (bytes_read == 0) {
                log.debug("Empty read", .{});
                self.processConnectionError(error.EndOfStream);
                continue;
            }

            log.debug("Read {} bytes: {s}", .{ bytes_read, buffer[0..bytes_read] });

            // Parse the received data
            self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
                log.err("Parser error: {}", .{err});
                self.processConnectionError(err);
                continue;
            };
        }

        log.debug("Reader thread exited", .{});
    }

    fn getSocket(self: *Self) ?*std.os.socket.Socket {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.socket;
    }

    fn flusherLoop(self: *Self) void {
        log.debug("Flusher thread started", .{});

        self.mutex.lock();
        defer self.mutex.unlock();

        while (!self.should_stop) {
            const socket = self.socket orelse {
                // If we don't have a socket, wait
                log.info("[flusher] Waiting for socket", .{});
                self.flusher_waiting_for_socket = true;
                self.flusher_waiting_for_socket_cond.broadcast();
                self.flusher_condition.wait(&self.mutex);
                continue;
            };
            self.flusher_waiting_for_socket = false;

            if (!self.flusher_signaled) {
                // If we don't have any data to write, wait
                log.info("[flusher] Waiting for data", .{});
                self.flusher_condition.wait(&self.mutex);
                continue;
            }

            if (!self.flusher_asap and !self.options.send_asap) {
                // Give a chance to accumulate more requests
                log.info("[flusher] 1ms delay", .{});
                self.flusher_condition.timedWait(&self.mutex, 1 * std.time.ns_per_ms) catch {};
            }

            self.flusher_signaled = false;
            self.flusher_asap = false;

            log.info("[flusher] waiting for mutex", .{});

            // Unlock before I/O
            self.mutex.unlock();
            defer self.mutex.lock(); // Re-lock at end of iteration

            var iovecs: [16]std.posix.iovec_const = undefined;
            const iovec_count = self.write_buffer.gatherReadVectors(&iovecs);
            if (iovec_count == 0) {
                // If we don't have any data to write, try again
                continue;
            }

            log.info("[flusher] writing", .{});

            // Write all buffered data using vectored I/O
            socket.writevAll(iovecs[0..iovec_count]) catch |err| {
                log.err("Flush error: {}", .{err});
                self.processConnectionError(err);
                continue;
            };

            var total_size: usize = 0;
            for (iovecs[0..iovec_count]) |iov| {
                log.debug("Writing {} bytes: {s}", .{ iov.len, iov.base[0..iov.len] });
                total_size += iov.len;
            }
            self.write_buffer.consumeBytesMultiple(total_size);

            log.info("[flusher] wrote {} bytes", .{total_size});
        }

        log.debug("Flusher thread exited", .{});
    }

    fn bufferWrite(self: *Self, data: []const u8, asap: bool) !void {
        // Assume mutex is already held by caller

        // If we're reconnecting, buffer the message for later
        if (!asap and self.status == .connecting and self.options.reconnect.allow_reconnect) {
            return self.pending_buffer.append(data);
        }

        // Buffer and signal flusher (mutex already held)
        try self.write_buffer.append(data);

        // Wake up the flusher thread, in the ASAP mode we signal() multiple times
        // to wake it up from the 1ms delay, if needed
        if (!self.flusher_signaled or asap) {
            self.flusher_signaled = true;
            self.flusher_asap = asap;
            log.debug("flusher signaled", .{});
            self.flusher_condition.signal();
        }
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, message: *Message) !void {

        // Retain subscription while holding lock, then release lock
        self.subs_mutex.lock();
        const sub = self.subscriptions.get(message.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock();

        if (sub) |s| {
            defer s.release(); // Release when done

            // Log before consuming message (to avoid use-after-free)
            log.debug("Delivering message to subscription {d}: {s}", .{ message.sid, message.data });

            if (s.handler) |_| {
                // Async subscription - dispatch to assigned dispatcher
                if (s.dispatcher) |dispatcher| {
                    dispatcher.enqueue(s, message) catch |err| {
                        log.err("Failed to dispatch message for sid {d}: {}", .{ message.sid, err });
                        message.deinit();
                        return;
                    };
                } else {
                    log.err("Async subscription {} has no assigned dispatcher", .{message.sid});
                    message.deinit();
                    return;
                }
            } else {
                // Sync subscription - queue message
                s.messages.push(message) catch |err| {
                    switch (err) {
                        error.QueueClosed => {
                            // Queue is closed; drop gracefully.
                            log.debug("Queue closed for sid {d}; dropping message", .{message.sid});
                            message.deinit();
                            return;
                        },
                        else => {
                            // Allocation or unexpected push failure; log and tear down the connection.
                            log.err("Failed to enqueue message for sid {d}: {}", .{ message.sid, err });
                            message.deinit();
                            return err;
                        },
                    }
                };
            }
        } else {
            // No subscription found, discard message
            log.debug("No subscription found for sid {d}; dropping message", .{message.sid});
            message.deinit();
        }
    }

    // Process INFO commands
    // Called from the parser in the reader thread
    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        log.debug("Received INFO: {s}", .{info_json});

        self.mutex.lock();
        defer self.mutex.unlock();

        // Reset arena to clear any previous server info strings
        _ = self.server_info_arena.reset(.retain_capacity);
        const arena = self.server_info_arena.allocator();

        // Parse JSON directly into ServerInfo struct using leaky parser
        self.server_info = std.json.parseFromSliceLeaky(ServerInfo, arena, info_json, .{ .ignore_unknown_fields = true }) catch |err| {
            log.err("Failed to parse INFO JSON: {}", .{err});
            return;
        };

        // Parse version string into components (like C implementation's _unpackSrvVersion)
        self.server_info.parsed_version = ServerVersion.parse(self.server_info.version);

        log.debug("Parsed server info: name={?s}, version={}.{}.{}, max_payload={}, headers={}", .{
            self.server_info.server_name,
            self.server_info.parsed_version.major,
            self.server_info.parsed_version.minor,
            self.server_info.parsed_version.update,
            self.server_info.max_payload,
            self.server_info.headers,
        });

        // Add discovered servers to pool if any connect_urls were provided
        if (self.server_info.connect_urls) |urls| {
            for (urls) |url| {
                // Add as implicit server (discovered, not explicitly configured)
                const was_added = self.server_pool.addServer(url, true) catch |err| {
                    log.warn("Failed to add discovered server {s}: {}", .{ url, err });
                    continue;
                };
                if (was_added) {
                    log.info("Discovered new server: {s}", .{url});
                }
            }
        }

        if (self.in_handshake and self.handshake.pending_info) {
            self.handshake.pending_info = false;
            self.status_changed.broadcast();
            try self.sendConnect();
        }
    }

    pub fn processOK(self: *Self) !void {
        _ = self;
        log.debug("Received +OK", .{});
    }

    pub fn processErr(self: *Self, original_msg: []const u8) !void {
        const trimmed_msg = std.mem.trim(u8, original_msg, "'");

        const msg = try self.allocator.alloc(u8, trimmed_msg.len);
        errdefer self.allocator.free(msg);

        _ = std.ascii.lowerString(msg, trimmed_msg);

        log.err("Received -ERR: {s}", .{msg});

        self.mutex.lock();
        defer self.mutex.unlock();

        self.last_received_error = msg;
        self.status_changed.broadcast();
    }

    pub fn processPong(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.incoming_pongs += 1;
        self.pong_condition.broadcast();

        if (self.in_handshake and self.handshake.pending_pong) {
            self.handshake.pending_pong = false;
            self.status_changed.broadcast();
        }

        log.debug("Received PONG for ping_id={}", .{self.incoming_pongs});
    }

    pub fn processPing(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.bufferWrite("PONG\r\n", true);
    }

    fn waitForStatus(self: *Self, status: ConnectionStatus, timeout_ms: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const timeout_ns = timeout_ms * std.time.ns_per_ms;
        var timer = try std.time.Timer.start();
        while (self.status != status) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }
            try self.status_changed.timedWait(&self.mutex, timeout_ns - elapsed_ns);
        }
    }

    fn updateStatus(self: *Self, status: ConnectionStatus) void {
        log.debug("Status changed to {}", .{status});
        self.status = status;
        self.status_changed.broadcast();
    }

    fn sendConnect(self: *Self) !void {
        std.debug.assert(self.in_handshake);
        std.debug.assert(self.handshake.pending_connect);
        std.debug.assert(self.handshake.pending_ping);

        const connect_obj = .{
            .verbose = self.options.verbose,
            .pedantic = false,
            .headers = true,
            .no_responders = self.options.no_responders and self.server_info.headers,
            .name = self.options.name orelse build_options.name,
            .lang = build_options.lang,
            .version = build_options.version,
            .protocol = 1,
        };

        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        var writer = buffer.writer();
        try writer.writeAll("CONNECT ");
        try std.json.stringify(connect_obj, .{}, writer);
        try writer.writeAll("\r\n");

        self.outgoing_pings += 1;
        try writer.writeAll("PING\r\n");

        log.debug("Sending CONNECT", .{});
        log.debug("Sending PING #{}", .{self.outgoing_pings});
        try self.bufferWrite(buffer.items, true);

        self.handshake.pending_connect = false;
        self.handshake.pending_ping = false;
        self.status_changed.broadcast();
    }

    // Reconnection Logic

    fn clearPendingFlushes(self: *Self) void {
        // Reset counters on reconnection/close
        self.outgoing_pings = 0;
        self.incoming_pongs = 0;
        self.pong_condition.broadcast();
    }

    // This will be called from the reader/flusher threads while the mutex is
    // unlocked, in the I/O code path. We will just shutdown and close the socket,
    // to unblock reads/writes. The rest needs to be done in a separate thread.
    fn processConnectionError(self: *Self, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.in_reconnect > 0) return; // Already reconnecting

        // Mark current server as having an error (already under connection mutex)
        if (self.current_server) |server| {
            server.last_error = err;
            server.reconnects += 1;
        }

        log.info("Connection error ({}), starting reconnection", .{err});

        self.status = .connecting;
        self.in_reconnect += 1;
        self.abort_reconnect = false;
        self.status_changed.broadcast();

        // Shutdown socket to interrupt any ongoing reads/writes
        self.closeSocket();
        self.wakeUpThreads();

        // Clear pending flushes (wake up any waiting flush calls)
        self.clearPendingFlushes();

        // Start reconnection thread (protected by mutex like C library)
        const thread = std.Thread.spawn(.{}, doReconnect, .{self}) catch |spawn_err| {
            log.err("Failed to start reconnection thread: {}", .{spawn_err});
            self.status = .closed;
            return;
        };

        // Successfully created thread - update state (under mutex like C library)
        self.reconnect_thread = thread;
    }

    fn doReconnect(self: *Self) void {
        log.info("Reconnection thread started", .{});
        defer log.info("Reconnection thread finished", .{});

        var successful = false;

        defer {
            if (!successful) {
                self.joinThreads();
            }
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        std.debug.assert(self.status == .connecting);

        defer {
            if (successful) {
                self.updateStatus(.connected);
            } else {
                self.updateStatus(.closed);
                self.closeSocket();
                self.stopThreads();
            }
        }

        var attempts: u32 = 0;

        while (true) {
            const delay_ms = self.calculateReconnectDelay(attempts);
            self.reconnect_condition.timedWait(&self.mutex, delay_ms * std.time.ns_per_ms) catch {};

            if (self.abort_reconnect) {
                log.debug("Reconnection aborted", .{});
                return;
            }

            std.debug.assert(self.socket == null);
            self.wakeUpThreads();

            log.info("Waiting for flusher thread to pause", .{});
            while (!self.flusher_waiting_for_socket) {
                self.flusher_waiting_for_socket_cond.wait(&self.mutex);
            }

            log.info("Waiting for reader thread to pause", .{});
            while (!self.reader_waiting_for_socket) {
                self.reader_waiting_for_socket_cond.wait(&self.mutex);
            }

            log.info("Clearing connection state", .{});
            self.parser.reset();
            self.write_buffer.reset();

            attempts += 1;

            self.openSocket() catch |err| {
                switch (err) {
                    error.NoServersAvailable => {
                        log.err("Ran out of servers to try", .{});
                        break;
                    },
                    else => {
                        log.err("Failed to connect: {}", .{err});
                        continue;
                    },
                }
            };

            self.wakeUpThreads();

            self.waitForHandshake() catch |err| {
                log.err("Failed to handshake: {}", .{err});
                continue;
            };

            successful = true;
            return;
        }
    }

    fn doReconnectOld(self: *Self) void {
        log.info("Reconnection loop started", .{});

        // Follow C library: pattern: main loop under mutex with selective releases
        self.mutex.lock();

        var total_attempts: u32 = 0;
        var server_cycle_count: u32 = 0;

        // Main reconnection loop (like C library: continue while status OK and servers available)
        while (self.status != .closed and !self.abort_reconnect and self.server_pool.getSize() > 0) {

            // Check if we've exhausted our reconnection attempts (under mutex)
            if (self.options.reconnect.max_reconnect >= 0 and
                total_attempts >= self.options.reconnect.max_reconnect)
            {
                log.err("Max reconnection attempts ({}) reached", .{self.options.reconnect.max_reconnect});
                break;
            }

            // Sleep after trying all servers once (under mutex like C library)
            if (server_cycle_count >= self.server_pool.getSize() and total_attempts > 0) {
                server_cycle_count = 0;

                // Check if connection closed before sleep (like C library)
                if (self.status == .closed) break;

                const delay_ms = self.calculateReconnectDelay(total_attempts);
                log.debug("Waiting {}ms before next reconnection attempt", .{delay_ms});

                // Exception: Custom delay callback outside mutex (like C library)
                if (self.options.reconnect.custom_reconnect_delay_cb) |callback| {
                    self.mutex.unlock(); // Release for callback
                    const custom_delay = callback(total_attempts);
                    self.mutex.lock(); // Re-acquire after callback

                    const timeout_ns = custom_delay * std.time.ns_per_ms;
                    self.reconnect_condition.timedWait(&self.mutex, timeout_ns) catch {};
                } else {
                    // Regular sleep with interruptible wait (under mutex)
                    const timeout_ns = delay_ms * std.time.ns_per_ms;
                    self.reconnect_condition.timedWait(&self.mutex, timeout_ns) catch {};
                }
            }

            // Check if connection closed after potential sleep (like C library)
            if (self.status == .closed) break;

            // Get next server using C library algorithm (under mutex)
            const server = self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) catch |err| {
                log.err("Server pool error: {}", .{err});
                break;
            } orelse {
                log.err("No servers available for reconnection", .{});
                break;
            };

            // Update current server and track attempt (under mutex)
            self.current_server = server;
            server.reconnects += 1;
            total_attempts += 1;
            server_cycle_count += 1;

            self.status = .connecting;

            // Exception: Release mutex for actual TCP connection (like C library)
            self.mutex.unlock();

            // Establish TCP connection (outside mutex)
            log.debug("Connecting to server: {s}:{d} (reconnect attempt {})", .{ server.parsed_url.host, server.parsed_url.port, total_attempts });
            const socket = Socket.connect(self.allocator, server.parsed_url.host, server.parsed_url.port) catch |err| {
                self.mutex.lock(); // Re-acquire for error handling
                server.last_error = err;
                log.debug("Reconnection attempt {} failed: {}", .{ total_attempts, err });
                continue; // Continue loop (mutex still held)
            };

            self.socket = socket;
            self.should_stop = false;

            // Handle initial handshake (outside mutex)
            const handshake_result = self.processInitialHandshake();

            self.mutex.lock(); // Re-acquire mutex

            if (handshake_result) |_| {
                // Success! Update connection state (under mutex)
                server.did_connect = true;
                server.reconnects = 0;
                self.status = .connected;
                self.abort_reconnect = false;

                // Clean up thread state
                self.in_reconnect -= 1;
                const thread = self.reconnect_thread;
                self.reconnect_thread = null;

                self.mutex.unlock(); // Release before operations that don't need mutex

                // Restart reader thread for the new connection
                self.reader_thread = std.Thread.spawn(.{}, readerLoop, .{self}) catch |err| {
                    log.err("Failed to restart reader thread: {}", .{err});
                    self.processConnectionError(err);
                    continue; // Try next server
                };

                // Restart flusher thread for the new connection
                self.mutex.lock();
                self.should_stop = false;
                self.flusher_signaled = false;
                self.mutex.unlock();

                self.flusher_thread = std.Thread.spawn(.{}, flusherLoop, .{self}) catch |err| {
                    log.err("Failed to restart flusher thread: {}", .{err});
                    self.processConnectionError(err);
                    continue; // Try next server
                };

                // Re-establish subscriptions (outside mutex like C library)
                self.resendSubscriptions() catch |err| {
                    log.err("Failed to re-establish subscriptions: {}", .{err});
                    // Continue anyway, connection is established
                };

                // Flush pending messages (outside mutex like C library)
                self.pending_buffer.moveToBuffer(&self.write_buffer) catch |err| {
                    log.warn("Failed to flush pending messages: {}", .{err});
                    // Continue anyway, connection is established
                };

                // Exception: Invoke callback outside mutex (like C library)
                if (self.options.callbacks.reconnected_cb) |callback| {
                    callback(self);
                }

                log.info("Reconnection successful after {} attempts", .{total_attempts});

                // Detach thread for cleanup (like C library)
                if (thread) |t| {
                    t.detach();
                }
                return;
            } else |err| {
                // Handshake failed, clean up and continue
                self.socket = null;
                socket.close();
                server.last_error = err;
                self.status = .connecting;
                log.debug("Handshake failed for reconnection attempt {}: {}", .{ total_attempts, err });

                // Check if connection closed or should abort after error (like C library)
                if (self.status == .closed or self.abort_reconnect) {
                    break;
                }
                // Continue loop (mutex still held)
            }
        }

        // Failed to reconnect - cleanup (under mutex like C library)
        log.err("Reconnection failed after {} attempts", .{total_attempts});

        self.in_reconnect -= 1; // Always decrement
        self.reconnect_thread = null;
        self.status = .closed;
        self.abort_reconnect = true; // Mark as aborted

        self.mutex.unlock(); // Final release

        // Exception: Invoke callback outside mutex (like C library)
        if (self.options.callbacks.closed_cb) |callback| {
            callback(self);
        }
    }

    fn calculateReconnectDelay(self: *Self, attempts: u32) u64 {
        if (self.options.reconnect.custom_reconnect_delay_cb) |callback| {
            return callback(attempts);
        }

        if (attempts == 0) {
            return 0;
        }

        var base_wait = self.options.reconnect.reconnect_wait_ms;
        const jitter = self.options.reconnect.reconnect_jitter_ms;

        if (jitter > 0) {
            var rng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
            const random_jitter = rng.random().uintLessThan(u64, jitter);
            base_wait += random_jitter;
        }

        return base_wait;
    }

    fn resendSubscriptions(self: *Self) !void {
        log.debug("Re-establishing subscriptions", .{});

        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();

        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            const sub = entry.value_ptr.*;

            // Send SUB command
            if (sub.queue_group) |queue_group| {
                try buffer.writer().print("SUB {s} {s} {d}\r\n", .{ sub.subject, queue_group, sub.sid });
            } else {
                try buffer.writer().print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
            }

            // Send directly (bypass buffering since we're reconnecting)
            const socket = self.socket orelse return ConnectionError.ConnectionClosed;
            try socket.writeAll(buffer.items);

            log.debug("Re-subscribed to {s} with sid {d}", .{ sub.subject, sub.sid });
            buffer.clearRetainingCapacity();
        }
    }

    // JetStream support
    pub fn jetstream(self: *Self, options: JetStreamOptions) JetStream {
        return JetStream.init(self.allocator, self, options);
    }
};
