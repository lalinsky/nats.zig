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
const MAX_CONTROL_LINE_SIZE = @import("parser.zig").MAX_CONTROL_LINE_SIZE;
const Socket = @import("socket.zig").Socket;
const validation = @import("validation.zig");

const log = @import("log.zig").log;

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
    ReconnectDisabled,
    AlreadyReconnecting,
    NotConnected,
    ManualReconnect,
    StaleConnection,
} || PublishError || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

pub const ConnectionStatus = enum {
    closed,
    connecting,
    connected,
    reconnecting,
};

pub const HandshakeState = enum {
    not_started,
    waiting_for_info,
    waiting_for_pong,
    completed,
    failed,

    /// Returns true if handshake is currently in progress
    pub fn isInProgress(self: HandshakeState) bool {
        return switch (self) {
            .waiting_for_info, .waiting_for_pong => true,
            .not_started, .completed, .failed => false,
        };
    }

    /// Returns true if handshake is waiting for any server response
    pub fn isWaiting(self: HandshakeState) bool {
        return switch (self) {
            .waiting_for_info, .waiting_for_pong => true,
            .not_started, .completed, .failed => false,
        };
    }

    /// Returns true if handshake has finished (either success or failure)
    pub fn isFinished(self: HandshakeState) bool {
        return switch (self) {
            .completed, .failed => true,
            .not_started, .waiting_for_info, .waiting_for_pong => false,
        };
    }
};

pub const ReconnectOptions = struct {
    max_reconnect: u32 = 60,
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
    max_scratch_size: usize = 1024 * 1024 * 10,
    ping_interval_ms: u64 = 120000, // 2 minutes default, 0 = disabled
    max_pings_out: u32 = 2, // max unanswered keep-alive PINGs
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,

    status: ConnectionStatus = .closed,

    // Network
    socket: ?Socket = null,
    socket_refs: u64 = 0,
    socket_available_cond: std.Thread.Condition = .{}, // Signals when socket becomes available
    socket_unused_cond: std.Thread.Condition = .{}, // Signals when socket is no longer in use

    // Server management
    server_pool: ServerPool,
    current_server: ?*Server = null, // Track current server like C library
    server_info: ServerInfo = .{}, // Current server info from INFO message
    server_info_arena: std.heap.ArenaAllocator, // Arena for server_info strings

    // Reconnection state
    pending_buffer: WriteBuffer,

    // Handshake state
    handshake_state: HandshakeState = .not_started,
    handshake_error: ?anyerror = null,
    handshake_cond: std.Thread.Condition = .{},

    // Threading
    reader_thread: ?std.Thread = null,
    flusher_thread: ?std.Thread = null,
    reconnect_thread: ?std.Thread = null,
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    should_stop_cond: std.Thread.Condition = .{}, // Signals when should_stop becomes true

    // Main connection mutex (protects most fields)
    mutex: std.Thread.Mutex = .{},

    // PING/PONG flush tracking (simplified counter approach)
    outgoing_pings: u64 = 0,
    incoming_pongs: u64 = 0,
    pong_condition: std.Thread.Condition = .{},

    // PING/PONG keep-alive tracking
    ping_timer: std.time.Timer, // Timer for tracking ping intervals
    pings_out: std.atomic.Value(u32) = std.atomic.Value(u32).init(0), // Outstanding keep-alive pings (atomic)

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
            .ping_timer = std.time.Timer.start() catch unreachable,
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();

        // Join threads (they should have been signaled to stop by close())
        if (self.flusher_thread) |thread| {
            thread.join();
            self.flusher_thread = null;
        }
        if (self.reader_thread) |thread| {
            thread.join();
            self.reader_thread = null;
        }
        if (self.reconnect_thread) |thread| {
            thread.join();
            self.reconnect_thread = null;
        }

        // Release global dispatcher pool
        if (self.dispatcher_pool != null) {
            dispatcher_mod.releaseGlobalPool();
            self.dispatcher_pool = null;
        }

        // Clean up response manager
        self.response_manager.deinit();

        // Clean up subscriptions - release connection's references first
        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.release(); // Release connection's ownership reference
        }
        self.subscriptions.deinit();

        // Clean up the buffers
        self.pending_buffer.deinit();
        self.write_buffer.deinit();

        // Clean up server pool
        self.server_pool.deinit();

        // Clean up server info arena
        self.server_info_arena.deinit();

        self.parser.deinit();
        self.scratch.deinit();
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

    pub fn connect(self: *Self, url: []const u8) !void {
        if (self.status != .closed) {
            return ConnectionError.ConnectionFailed;
        }

        // Add server to pool if it's not already there
        if (self.server_pool.getSize() == 0) {
            _ = try self.server_pool.addServer(url, false); // Explicit server
        }

        return self.connectToServer();
    }

    pub fn addServer(self: *Self, url_str: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return try self.server_pool.addServer(url_str, false); // Explicit server
    }

    fn connectToServer(self: *Self) !void {
        // This is called from initial connect() - needs to manage its own mutex
        self.mutex.lock();
        defer self.mutex.unlock();

        self.status = .connecting;

        // Get server using C library's GetNextServer algorithm
        const selected_server = try self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) orelse {
            self.status = .closed;
            return ConnectionError.ConnectionFailed;
        };

        // Update current server and track reconnection attempt (like C library)
        self.current_server = selected_server;
        selected_server.reconnects += 1;

        // Establish connection (under mutex for consistent state management)
        self.establishConnection(selected_server) catch |err| {
            self.status = .closed;
            self.cleanupFailedConnection(err, true);
            return err;
        };

        // Socket is now established and connection state is set up
        self.should_stop.store(false, .monotonic);

        // Start reader thread - it will handle the handshake
        self.reader_thread = try std.Thread.spawn(.{}, readerLoop, .{self});

        // Start flusher thread
        self.flusher_thread = try std.Thread.spawn(.{}, flusherLoop, .{self});

        // Wait for handshake completion
        self.waitForHandshakeCompletion() catch |err| {
            // Clean up failed handshake state and close socket
            self.status = .closed;
            self.cleanupFailedConnection(err, true);
            return err;
        };

        // Handshake completed successfully
        self.status = .connected;

        log.info("Connected successfully to {s}", .{selected_server.parsed_url.full_url});
    }

    /// Close the connection
    pub fn close(self: *Self) void {
        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.closed_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status == .closed) {
            return;
        }

        log.info("Closing connection", .{});

        // Mark the connection as permanently closed
        self.status = .closed;
        self.should_stop.store(true, .release);
        self.should_stop_cond.broadcast(); // Wake up any threads waiting on should_stop

        // Detach socket immediately and wake any waiting threads
        const old_socket = self.socket;
        self.socket = null;
        self.socket_available_cond.broadcast();

        // Shutdown socket to interrupt ongoing I/O (but don't close yet)
        if (old_socket) |socket| {
            socket.shutdown(.both) catch |shutdown_err| {
                log.err("Socket shutdown failed: {}", .{shutdown_err});
            };
        }

        // Close write buffer - this wakes up the flusher thread
        self.pending_buffer.close();
        self.write_buffer.close();

        // Wait for both threads to release the socket with timeout
        self.waitForSocketUnused(self.options.timeout_ms * 2) catch {
            log.warn("Timeout waiting for socket references to be released during close", .{});
        };

        // Now safe to close socket since threads are done with it
        if (old_socket) |socket| {
            socket.close();
        }

        // Wake up any waiting flush() calls
        self.pong_condition.broadcast();

        // Make sure we invoke the closed callback
        if (self.options.callbacks.closed_cb) |cb| {
            callback = cb;
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.status;
    }

    pub fn isConnected(self: *Self) bool {
        return self.getStatus() == .connected;
    }

    pub fn isConnecting(self: *Self) bool {
        const status = self.getStatus();
        return status == .connecting or status == .reconnecting;
    }

    /// Force a reconnection to the NATS server
    /// This allows users to manually trigger reconnection for scenarios like:
    /// - Refreshing authentication credentials
    /// - Rebalancing client connections
    /// - Testing reconnection behavior
    /// Returns error if reconnection cannot be initiated
    pub fn reconnect(self: *Self) !void {
        var needs_close = false;
        defer if (needs_close) self.close();
        var callback: @TypeOf(self.options.callbacks.disconnected_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if reconnection is allowed
        if (!self.options.reconnect.allow_reconnect) {
            log.warn("Manual reconnect requested but reconnection is disabled", .{});
            return ConnectionError.ReconnectDisabled;
        }

        // Check current status
        switch (self.status) {
            .reconnecting => {
                log.info("Already reconnecting, ignoring manual reconnect request", .{});
                return ConnectionError.AlreadyReconnecting;
            },
            .closed => {
                log.warn("Cannot reconnect: connection is closed", .{});
                return ConnectionError.ConnectionClosed;
            },
            .connecting => {
                log.warn("Cannot reconnect: initial connection not yet established", .{});
                return ConnectionError.NotConnected;
            },
            .connected => {
                // OK to proceed
            },
        }

        log.info("Manual reconnection requested", .{});

        // Use a synthetic error to trigger the reconnection
        const synthetic_error = ConnectionError.ManualReconnect;

        // Perform connection cleanup (don't close socket - let reconnect handle it)
        self.status = .reconnecting;
        self.cleanupFailedConnection(synthetic_error, false);

        // Spawn reconnect thread if not already running
        if (self.reconnect_thread == null) {
            self.reconnect_thread = std.Thread.spawn(.{}, doReconnectThread, .{self}) catch |spawn_err| {
                log.err("Failed to spawn reconnect thread: {}", .{spawn_err});
                // Fall back to closing connection
                needs_close = true;
                return spawn_err;
            };
        }

        // Invoke disconnected callback
        if (self.options.callbacks.disconnected_cb) |cb| {
            callback = cb;
        }
    }

    /// Publishes data on a subject.
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        try validation.validateSubject(subject);

        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined, // we don't need a fully constructed arena for this
        };
        return self.publishMsgInternal(&msg, null);
    }

    /// Publishes a message on a subject.
    pub fn publishMsg(self: *Self, msg: *Message) !void {
        try validation.validateSubject(msg.subject);
        return self.publishMsgInternal(msg, null);
    }

    /// Publishes data on a subject, with a reply subject.
    pub fn publishRequest(self: *Self, subject: []const u8, reply: []const u8, data: []const u8) !void {
        try validation.validateSubject(subject);
        try validation.validateSubject(reply);

        var msg = Message{
            .subject = subject,
            .reply = reply,
            .data = data,
            .pool = null,
            .arena = undefined, // we don't need a fully constructed arena for this
        };
        return self.publishMsgInternal(&msg, null);
    }

    /// Publishes a message on a subject, with a reply subject.
    pub fn publishRequestMsg(self: *Self, msg: *Message, reply: []const u8) !void {
        try validation.validateSubject(msg.subject);
        try validation.validateSubject(reply);
        return self.publishMsgInternal(msg, reply);
    }

    fn publishMsgInternal(self: *Self, msg: *Message, reply_override: ?[]const u8) !void {
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
        switch (self.status) {
            .connected, .reconnecting => {},
            else => {
                return ConnectionError.ConnectionClosed;
            },
        }

        // Published messages go to pending_buffer during reconnection, otherwise write_buffer
        if (self.status == .reconnecting and self.options.reconnect.allow_reconnect) {
            try self.pending_buffer.append(buffer.items);
        } else {
            try self.write_buffer.append(buffer.items);
        }

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
        errdefer _ = self.subscriptions.remove(sub.sid);

        // Send SUB command via buffer
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        var buffer = ArrayList(u8).init(allocator);
        if (sub.queue) |group| {
            try buffer.writer().print("SUB {s} {s} {d}\r\n", .{ sub.subject, group, sub.sid });
        } else {
            try buffer.writer().print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
        }
        try self.write_buffer.append(buffer.items);
    }

    pub fn subscribe(self: *Self, subject: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        try validation.validateSubject(subject);

        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.create(self, sid, subject, null, handler);
        errdefer sub.release();

        try self.ensureDispatcherPool();
        sub.dispatcher = self.dispatcher_pool.?.assignDispatcher();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with sid {d} (async)", .{ sub.subject, sub.sid });
        return sub;
    }

    /// Subscribe to a subject, the code is responsible for handling the fetching
    pub fn subscribeSync(self: *Self, subject: []const u8) !*Subscription {
        try validation.validateSubject(subject);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.create(self, sid, subject, null, null);
        errdefer sub.release();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with sid {d} (sync)", .{ sub.subject, sub.sid });
        return sub;
    }

    pub fn queueSubscribe(self: *Self, subject: []const u8, queue: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        try validation.validateSubject(subject);
        try validation.validateQueueName(queue);

        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.create(self, sid, subject, queue, handler);
        errdefer sub.release();

        try self.ensureDispatcherPool();
        sub.dispatcher = self.dispatcher_pool.?.assignDispatcher();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with queue group '{s}' and sid {d} (async)", .{ sub.subject, queue, sub.sid });
        return sub;
    }

    /// Subscribe to a subject, the code is responsible for handling the fetching
    pub fn queueSubscribeSync(self: *Self, subject: []const u8, queue: []const u8) !*Subscription {
        try validation.validateSubject(subject);
        try validation.validateQueueName(queue);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.create(self, sid, subject, queue, null);
        errdefer sub.release();

        try self.subscribeInternal(sub);

        log.debug("Subscribed to {s} with queue group '{s}' and sid {d} (sync)", .{ sub.subject, queue, sub.sid });
        return sub;
    }

    pub fn unsubscribeInternal(self: *Self, sid: u64) void {
        var buffer: [256]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        var writer = stream.writer();

        writer.print("UNSUB {d}\r\n", .{sid}) catch unreachable; // Will always fit

        self.write_buffer.append(stream.getWritten()) catch |err| {
            log.err("Failed to enqueue UNSUB message for sid {d}: {}", .{ sid, err });
        };
    }

    pub fn unsubscribe(self: *Self, sub: *Subscription) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.subs_mutex.lock();
        defer self.subs_mutex.unlock();

        if (!self.subscriptions.remove(sub.sid)) {
            // Nothing to do, already unsubscribed
            return;
        }

        // Try to send UNSUB command. Even if it fails internally,
        // processMsg will keep sending UNSUB commands once
        // it receives a message with unknown sid.
        self.unsubscribeInternal(sub.sid);

        log.debug("Unsubscribed from {s} with sid {d}", .{ sub.subject, sub.sid });

        sub.release();
    }

    pub fn flush(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Buffer the PING first (can fail on allocation)
        try self.write_buffer.append("PING\r\n");

        // Only increment counter after successful buffering
        self.outgoing_pings += 1;
        const our_ping_id = self.outgoing_pings;

        log.debug("Sent PING with ping_id={}, waiting for PONG", .{our_ping_id});

        const timeout_ns = self.options.timeout_ms * std.time.ns_per_ms;
        var timer = try std.time.Timer.start();

        while (self.incoming_pongs < our_ping_id) {
            if (self.status != .connected) {
                return ConnectionError.ConnectionClosed;
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

    fn establishConnection(self: *Self, server: *Server) !void {
        // Establish TCP connection
        log.debug("Connecting to server: {s}:{d}", .{ server.parsed_url.host, server.parsed_url.port });
        const socket = Socket.connect(self.allocator, server.parsed_url.host, server.parsed_url.port) catch |err| {
            log.err("Failed to connect to server: {}", .{err});
            return err;
        };
        errdefer socket.close();

        // Configure socket options (fail hard if these don't work)
        socket.setKeepAlive(true) catch |err| {
            log.err("Failed to set keep alive: {}", .{err});
            return err;
        };
        socket.setReadTimeout(self.options.timeout_ms) catch |err| {
            log.err("Failed to set read timeout: {}", .{err});
            return err;
        };
        socket.setWriteTimeout(self.options.timeout_ms) catch |err| {
            log.err("Failed to set write timeout: {}", .{err});
            return err;
        };

        if (self.options.trace) {
            log.debug("Connected, starting handshake...", .{});
        }

        // Setup connection state
        self.socket = socket;
        self.socket_available_cond.broadcast();

        server.did_connect = true;
        server.reconnects = 0;

        // Reset parser for clean state
        self.parser.reset();

        // Reset ping/pong counters for fresh connection
        self.outgoing_pings = 0;
        self.incoming_pongs = 0;

        // Initialize handshake state
        self.handshake_state = .waiting_for_info;
        self.handshake_error = null;

        // Unfreeze write buffer now that we have a working socket
        self.write_buffer.unfreeze();
    }

    fn readerLoop(self: *Self) void {
        log.debug("Reader loop started", .{});

        while (!self.should_stop.load(.acquire)) {
            self.readerIteration() catch |err| {
                // Handle errors from readerIteration
                switch (err) {
                    error.ShouldStop => break,
                    else => {
                        // Mark that reconnection is needed
                        self.markNeedsReconnect(err);
                        continue;
                    },
                }
            };

            // Check if we need to send a PING
            self.checkAndSendPing() catch |err| {
                self.markNeedsReconnect(err);
                continue;
            };
        }

        log.debug("Reader loop exited", .{});
    }

    fn readerIteration(self: *Self) !void {
        var buffer: [4096]u8 = undefined;

        const socket = try self.acquireSocket();
        defer self.releaseSocket();

        // Simple blocking read - shutdown() will wake us up
        const bytes_read = socket.read(&buffer) catch |err| switch (err) {
            error.WouldBlock => {
                // Read timeout from SO_RCVTIMEO - this is expected, continue reading
                return;
            },
            else => {
                log.err("Read error: {}", .{err});
                return err;
            },
        };

        if (bytes_read == 0) {
            log.debug("Connection closed by server (EOF)", .{});
            return error.EndOfStream;
        }

        log.debug("Read {} bytes: {s}", .{ bytes_read, buffer[0..bytes_read] });

        // Parse the received data
        self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
            switch (err) {
                error.ShouldStop => return err,
                else => {
                    log.err("Parser error: {}", .{err});
                    return err;
                },
            }
        };
    }

    fn flusherLoop(self: *Self) void {
        log.debug("Flusher loop started", .{});

        while (!self.should_stop.load(.acquire)) {
            self.flusherIteration() catch |err| {
                // Handle errors from flusherIteration
                switch (err) {
                    error.ShouldStop => break,
                    else => {
                        // Mark that reconnection is needed
                        self.markNeedsReconnect(err);
                        continue;
                    },
                }
            };
        }

        log.debug("Flusher loop exited", .{});
    }

    fn flusherIteration(self: *Self) !void {
        // Try to gather data from buffer first
        var iovecs: [16]std.posix.iovec_const = undefined;
        const gather = self.write_buffer.gatherReadVectors(&iovecs, self.options.timeout_ms) catch |err| switch (err) {
            error.QueueEmpty, error.BufferFrozen => {
                // No data to write or buffer frozen during reconnection
                return;
            },
            error.QueueClosed => return error.ShouldStop,
        };

        if (gather.iovecs.len == 0) {
            // No data to write
            return;
        }

        // Now try to get a socket - blocks until available
        const socket = try self.acquireSocket();
        defer self.releaseSocket();

        const bytes_written = socket.writev(gather.iovecs) catch |err| switch (err) {
            error.WouldBlock => {
                // Write timeout from SO_SNDTIMEO - this is expected, continue writing
                return;
            },
            else => {
                log.err("Write error: {}", .{err});
                return err;
            },
        };

        gather.consume(bytes_written) catch |err| switch (err) {
            error.BufferReset => {
                // This can only happen during reconnection, we can assume
                // it's safe to continue, since we will have a new buffer
                return;
            },
            error.ConcurrentConsumer => {
                // This is a bug, we can't handle it
                std.debug.panic("Concurrent consumer detected", .{});
            },
        };
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, message: *Message) !void {
        var owns_message = true;
        defer if (owns_message) message.deinit();

        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        // Retain subscription while holding lock, then release lock
        self.subs_mutex.lock();
        const sub = self.subscriptions.get(message.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock();

        if (sub) |s| {
            defer s.release(); // Release when done

            // Check if subscription is draining - drop message if so
            if (s.isDraining()) {
                log.debug("Dropping message for draining subscription {d}", .{message.sid});
                return;
            }

            // Increment pending message count and bytes for this subscription
            subscription_mod.incrementPending(s, message.data.len);

            // Log before consuming message (to avoid use-after-free)
            log.debug("Delivering message to subscription {d}: {s}", .{ message.sid, message.data });

            if (s.handler) |_| {
                // Async subscription - dispatch to assigned dispatcher
                if (s.dispatcher) |dispatcher| {
                    dispatcher.enqueue(s, message) catch |err| {
                        log.err("Failed to dispatch message for sid {d}: {}", .{ message.sid, err });
                        // Undo the pending counters since we failed to enqueue
                        subscription_mod.decrementPending(s, message.data.len);
                        return;
                    };
                    owns_message = false;
                } else {
                    log.err("Async subscription {} has no assigned dispatcher", .{message.sid});
                    // Undo the pending counters since we can't process
                    subscription_mod.decrementPending(s, message.data.len);
                    return;
                }
            } else {
                // Sync subscription - queue message
                s.messages.push(message) catch |err| {
                    switch (err) {
                        error.QueueClosed => {
                            // Queue is closed; drop gracefully.
                            log.debug("Queue closed for sid {d}; dropping message", .{message.sid});
                            // Undo the pending counters since queue is closed
                            subscription_mod.decrementPending(s, message.data.len);
                            return;
                        },
                        else => {
                            // Allocation or unexpected push failure; log and tear down the connection.
                            log.err("Failed to enqueue message for sid {d}: {}", .{ message.sid, err });
                            // Undo the pending counters since we failed to enqueue
                            subscription_mod.decrementPending(s, message.data.len);
                            return err;
                        },
                    }
                };
                owns_message = false;
            }
        } else {
            // No sub subscription found, try to send UNSUB command
            self.unsubscribeInternal(message.sid);
        }
    }

    /// Sends CONNECT and PING during handshake (assumes mutex is held)
    fn sendConnectAndPing(self: *Self) !void {
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        // Build CONNECT message with all options
        var buffer = ArrayList(u8).init(allocator);

        // Calculate effective no_responders: enable if server supports headers
        const no_responders = self.options.no_responders and self.server_info.headers;

        // Get client name from options or use default
        const client_name = self.options.name orelse build_options.name;

        // Create CONNECT JSON object
        const connect_obj = .{
            .verbose = self.options.verbose,
            .pedantic = false,
            .headers = true,
            .no_responders = no_responders,
            .name = client_name,
            .lang = build_options.lang,
            .version = build_options.version,
            .protocol = 1,
        };

        try buffer.writer().writeAll("CONNECT ");
        try std.json.stringify(connect_obj, .{}, buffer.writer());
        try buffer.writer().writeAll("\r\n");
        try buffer.writer().writeAll("PING\r\n");

        // Send via buffer (mutex already held)
        try self.write_buffer.append(buffer.items);

        log.debug("Sent CONNECT+PING during handshake", .{});
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

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

        log.debug("Parsed server info: id={?s}, version={?s} ({}.{}.{}), max_payload={}, headers={}", .{ self.server_info.server_id, self.server_info.version, self.server_info.parsed_version.major, self.server_info.parsed_version.minor, self.server_info.parsed_version.update, self.server_info.max_payload, self.server_info.headers });

        // Handle handshake if we're waiting for INFO
        if (self.handshake_state == .waiting_for_info) {
            self.sendConnectAndPing() catch |err| {
                log.err("Failed to send CONNECT+PING: {}", .{err});
                self.handshake_error = err;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast();
                return;
            };

            self.handshake_state = .waiting_for_pong;
            self.handshake_cond.broadcast(); // Signal state change
            log.debug("Handshake: sent CONNECT+PING, waiting for PONG", .{});
        }

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
    }

    pub fn processOK(self: *Self) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        log.debug("Received +OK", .{});

        // Handle verbose handshake mode - +OK is expected before PONG
        if (self.handshake_state == .waiting_for_pong and self.options.verbose) {
            log.debug("Received +OK during verbose handshake, waiting for PONG", .{});
            // Continue waiting for PONG - no state change needed
            return;
        }

        // Regular +OK handling (acknowledgment of successful command)
        // No action needed for now
    }

    pub fn processErr(self: *Self, err_msg: []const u8) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.error_cb) = null;
        defer if (callback) |cb| cb(self, err_msg);

        self.mutex.lock();
        defer self.mutex.unlock();

        log.err("Received -ERR: {s}", .{err_msg});

        // Handle handshake failure
        if (self.handshake_state.isWaiting()) {
            self.handshake_error = ConnectionError.AuthFailed;
            self.handshake_state = .failed;
            self.handshake_cond.broadcast(); // Signal handshake failure
            log.debug("Handshake failed due to server error: {s}", .{err_msg});
            return;
        }

        // Invoke error callback (in defer outside of mutex)
        if (self.options.callbacks.error_cb) |cb| {
            callback = cb;
        }
    }

    fn checkAndSendPing(self: *Self) !void {
        if (self.options.ping_interval_ms == 0) return;

        const interval_ns = self.options.ping_interval_ms * std.time.ns_per_ms;
        const elapsed_ns = self.ping_timer.read();
        if (elapsed_ns >= interval_ns) {
            try self.write_buffer.append("PING\r\n");
            const current_pings = self.pings_out.fetchAdd(1, .monotonic) + 1;
            if (self.options.max_pings_out > 0 and current_pings > self.options.max_pings_out) {
                log.warn("Stale connection: {} unanswered PINGs", .{current_pings});
                return error.StaleConnection;
            }
            self.ping_timer.reset();
        }
    }

    pub fn processPong(self: *Self) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Handle handshake completion
        if (self.handshake_state == .waiting_for_pong) {
            self.handshake_state = .completed;
            self.handshake_cond.broadcast(); // Signal handshake completion
            log.debug("Handshake completed successfully", .{});
            return;
        }

        // Regular PONG handling for flush() calls
        self.incoming_pongs += 1;
        self.pong_condition.broadcast();

        // Reset keep-alive ping counter - ANY PONG proves connection is alive
        self.pings_out.store(0, .monotonic);

        log.debug("Received PONG for ping_id={}", .{self.incoming_pongs});
    }

    pub fn processPing(self: *Self) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        try self.write_buffer.append("PONG\r\n");
    }

    // Reconnection Logic

    /// Performs cleanup after connection failure while keeping the Connection
    /// object intact for potential reconnection attempts.
    /// This should be called with mutex already held.
    ///
    /// @param err The error that caused the connection failure
    /// @param close_socket Whether to close the socket (false for markNeedsReconnect)
    fn cleanupFailedConnection(self: *Self, err: anyerror, close_socket: bool) void {
        // Update server tracking if we have a current server
        if (self.current_server) |server| {
            server.did_connect = false;
            server.last_error = err;

            // Only increment reconnects for active connection loss
            if (self.status == .reconnecting) {
                server.reconnects += 1;
            }
        }

        // Detach socket and wake any threads waiting for it
        const old_socket = self.socket;
        self.socket = null;
        self.socket_available_cond.broadcast();

        // Always freeze write buffer (idempotent operation)
        self.write_buffer.freeze();

        // Wake up any waiting flush() calls
        self.pong_condition.broadcast();

        // Reset handshake state
        self.handshake_state = .not_started;
        self.handshake_error = null;
        self.handshake_cond.broadcast();

        // Handle socket cleanup
        if (old_socket) |socket| {
            // Always shutdown first to interrupt ongoing I/O
            socket.shutdown(.both) catch |shutdown_err| {
                log.debug("Socket shutdown failed: {}", .{shutdown_err});
            };

            // Close socket if requested
            if (close_socket) {
                socket.close();
            }
        }
    }

    fn markNeedsReconnect(self: *Self, err: anyerror) void {
        var needs_close = false;
        defer if (needs_close) self.close();

        var callback: @TypeOf(self.options.callbacks.disconnected_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lock();
        defer self.mutex.unlock();

        log.info("markNeedsReconnect", .{});

        if (self.status != .connected) {
            log.info("Connection not in connected state", .{});
            return;
        }

        // Check if reconnection is allowed
        if (!self.options.reconnect.allow_reconnect) {
            log.info("Connection lost: {} (reconnect disabled)", .{err});
            needs_close = true;
            return;
        }

        log.info("Connection lost: {}", .{err});

        // Perform connection cleanup (don't close socket - let reconnect handle it)
        self.status = .reconnecting;
        self.cleanupFailedConnection(err, false);

        // Spawn reconnect thread if not already running
        if (self.reconnect_thread == null) {
            self.reconnect_thread = std.Thread.spawn(.{}, doReconnectThread, .{self}) catch |spawn_err| {
                log.err("Failed to spawn reconnect thread: {}", .{spawn_err});
                // Fall back to closing connection
                needs_close = true;
                return;
            };
        }

        // Invoke disconnected callback
        if (self.options.callbacks.disconnected_cb) |cb| {
            callback = cb;
        }
    }

    fn doReconnectThread(self: *Self) void {
        defer {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Detach thread and mark as null
            if (self.reconnect_thread) |thread| {
                thread.detach();
                self.reconnect_thread = null;
            }
        }

        // Run existing doReconnect logic
        self.doReconnect();
    }

    fn doReconnect(self: *Self) void {
        var needs_close = false;
        defer if (needs_close) self.close();

        var callback: @TypeOf(self.options.callbacks.reconnected_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if we should still reconnect (could have been closed)
        if (self.status != .reconnecting) {
            return;
        }

        log.debug("Starting reconnection", .{});

        // Close old socket if still attached (reader thread owns socket lifecycle)
        const old_socket = self.socket;
        self.socket = null;
        self.socket_available_cond.broadcast(); // Wake any threads waiting for socket

        if (old_socket) |socket| {
            socket.close();
        }

        var total_attempts: u32 = 0;
        var server_cycle_count: u32 = 0;
        const max_attempts = self.options.reconnect.max_reconnect;

        // Main reconnection loop (under mutex for state consistency)
        while (total_attempts < max_attempts and self.status == .reconnecting and !self.should_stop.load(.acquire)) {
            const server_count = self.server_pool.getSize();
            if (server_count == 0) {
                log.err("No servers available for reconnection", .{});
                break;
            }

            // Sleep after trying all servers once (using condition variable for responsive shutdown)
            if (server_cycle_count >= server_count and total_attempts > 0) {
                server_cycle_count = 0;
                const delay_ms = self.calculateReconnectDelay(total_attempts);
                log.debug("Waiting {}ms before next reconnection attempt", .{delay_ms});

                const timeout_ns = delay_ms * std.time.ns_per_ms;
                self.should_stop_cond.timedWait(&self.mutex, timeout_ns) catch {};

                // Re-check status after wait (either timeout or signaled)
                if (self.status != .reconnecting or self.should_stop.load(.acquire)) break;
            }

            // Get next server
            const server = self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) catch |err| {
                log.err("Server pool error: {}", .{err});
                break;
            } orelse {
                log.err("No servers available for reconnection", .{});
                break;
            };

            self.current_server = server;
            server.reconnects += 1;
            total_attempts += 1;
            server_cycle_count += 1;

            log.debug("Reconnection attempt {} to {s}:{d}", .{ total_attempts, server.parsed_url.host, server.parsed_url.port });

            // Try to establish connection
            self.establishConnection(server) catch |err| {
                log.debug("Reconnection attempt {} failed: {}", .{ total_attempts, err });
                self.cleanupFailedConnection(err, true);
                continue;
            };

            // Try to execute the handshake procedure
            self.waitForHandshakeCompletion() catch |err| {
                log.debug("Reconnect handshake failed: {}", .{err});
                self.cleanupFailedConnection(err, true);
                continue;
            };

            // Handshake completed successfully!
            self.status = .connected;

            // Re-establish subscriptions
            self.resendSubscriptions() catch |err| {
                log.err("Failed to re-establish subscriptions: {}", .{err});
            };

            // Flush pending messages
            self.pending_buffer.moveToBuffer(&self.write_buffer) catch |err| {
                log.warn("Failed to flush pending messages: {}", .{err});
            };

            // Invoke callback (in defer outside of mutex)
            if (self.options.callbacks.reconnected_cb) |cb| {
                callback = cb;
            }

            log.info("Reconnection successful after {} attempts", .{total_attempts});
            return;
        }

        // Reconnection ended (either failed or closed)
        if (self.should_stop.load(.acquire)) {
            log.debug("Reconnection aborted due to connection close", .{});
        } else {
            log.err("Reconnection failed after {} attempts", .{total_attempts});
        }

        // Will call close() in defer (outside of the mutex)
        needs_close = true;
    }

    fn calculateReconnectDelay(self: *Self, attempts: u32) u64 {
        if (self.options.reconnect.custom_reconnect_delay_cb) |callback| {
            return callback(attempts);
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

        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        var buffer = ArrayList(u8).init(allocator);

        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            const sub = entry.value_ptr.*;

            // Send SUB command
            if (sub.queue) |queue| {
                try buffer.writer().print("SUB {s} {s} {d}\r\n", .{ sub.subject, queue, sub.sid });
            } else {
                try buffer.writer().print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
            }

            log.debug("Re-subscribed to {s} with sid {d}", .{ sub.subject, sub.sid });
        }

        // Send all subscription commands via write buffer
        if (buffer.items.len > 0) {
            try self.write_buffer.append(buffer.items);
        }
    }

    /// Acquires a reference to the socket for safe concurrent use.
    /// Blocks until a socket becomes available.
    /// The caller MUST call releaseSocket() when done with the socket.
    fn acquireSocket(self: *Self) !Socket {
        // Check should_stop flag before acquiring the mutex
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Wait until socket is available
        while (self.socket == null) {
            if (self.should_stop.load(.acquire)) {
                return error.ShouldStop;
            }
            self.socket_available_cond.wait(&self.mutex);
        }

        if (self.socket) |socket| {
            self.socket_refs += 1;
            return socket;
        }

        unreachable;
    }

    /// Releases a reference to the socket obtained via acquireSocket().
    /// This must be called for every successful acquireSocket() call.
    fn releaseSocket(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.socket_refs > 0) {
            self.socket_refs -= 1;
            // Only broadcast when all references are released
            if (self.socket_refs == 0) {
                self.socket_unused_cond.broadcast();
            }
        }
    }

    /// Waits for handshake completion with timeout (assumes mutex is held)
    /// Returns error if handshake fails or times out
    fn waitForHandshakeCompletion(self: *Self) !void {
        const timeout_ns = self.options.timeout_ms * std.time.ns_per_ms;
        var timer = std.time.Timer.start() catch {
            log.err("Failed to start timer for handshake", .{});
            return ConnectionError.ConnectionFailed;
        };

        while (!self.handshake_state.isFinished()) {
            log.debug("Handshake state: {}", .{self.handshake_state});

            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                log.err("Handshake timeout", .{});
                self.handshake_error = ConnectionError.Timeout;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast(); // Signal the state change
                break;
            }

            const remaining_ns = timeout_ns - elapsed_ns;
            self.handshake_cond.timedWait(&self.mutex, remaining_ns) catch {};

            // Check for early termination conditions
            if (self.should_stop.load(.acquire)) {
                self.handshake_error = ConnectionError.ConnectionClosed;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast(); // Signal the state change
                break;
            }
        }

        // Return the handshake error if it failed, or void if successful
        if (self.handshake_state == .completed) {
            return;
        } else {
            return self.handshake_error orelse ConnectionError.ConnectionFailed;
        }
    }

    /// Internal helper for waiting for socket to become unused (assumes mutex is held)
    fn waitForSocketUnused(self: *Self, timeout_ms: u64) !void {
        if (self.socket_refs == 0) {
            return; // Already unused
        }

        if (timeout_ms == 0) {
            return error.Timeout; // Non-blocking, socket still in use
        }

        var timer = std.time.Timer.start() catch return error.Timeout;
        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        while (self.socket_refs > 0) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }

            const remaining_ns = timeout_ns - elapsed_ns;
            self.socket_unused_cond.timedWait(&self.mutex, remaining_ns) catch {};
        }

        // Socket became unused
    }

    // JetStream support
    pub fn jetstream(self: *Self, options: JetStreamOptions) JetStream {
        return JetStream.init(self, options);
    }
};
