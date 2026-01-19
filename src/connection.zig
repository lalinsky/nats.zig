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
const validation = @import("validation.zig");
const zio = @import("zio");

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
    DrainInProgress,
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
} || PublishError || ProtocolError || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

// Protocol-specific errors from server -ERR messages (matching nats.go approach)
pub const ProtocolError = error{
    // Authentication/Authorization errors
    AuthorizationViolation, // "Authorization Violation"
    AuthExpired, // "User Authentication Expired"
    AuthRevoked, // "User Authentication Revoked"
    AccountAuthExpired, // "Account Authentication Expired"
    PermissionViolation, // "Permissions Violation"

    // Connection/Limit errors
    MaxConnectionsExceeded, // "maximum connections exceeded"
    ConnectionThrottling, // "Connection throttling is active"
    MaxPayloadViolation, // "Maximum Payload Violation"
    MaxSubscriptionsExceeded, // "maximum subscriptions exceeded"

    // Protocol errors
    SecureConnectionRequired, // "Secure Connection - TLS Required"
    InvalidClientProtocol, // "invalid client protocol"
    UnknownProtocolOperation, // "Unknown Protocol Operation"
    InvalidPublishSubject, // "Invalid Publish Subject"
    NoRespondersRequiresHeaders, // "no responders requires headers support"

    // Account errors
    FailedAccountRegistration, // "Failed Account Registration"

    // Generic fallback
    UnknownServerError, // For unrecognized -ERR messages
};

pub const ConnectionStatus = enum {
    closed,
    connecting,
    connected,
    reconnecting,
};

pub const DrainState = enum(u8) {
    not_draining = 0,
    draining_subs = 1,
    draining_pubs = 2,
    drain_complete = 3,
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

    // Authentication
    token: ?[]const u8 = null,
    token_handler: ?*const fn () []const u8 = null,
};

pub const Connection = struct {
    allocator: Allocator,
    rt: *zio.Runtime,
    options: ConnectionOptions,

    status: ConnectionStatus = .closed,

    // Network
    stream: ?zio.net.Stream = null,
    stream_refs: u64 = 0,
    stream_available_cond: zio.Condition = .{},
    stream_unused_cond: zio.Condition = .{},

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
    handshake_cond: zio.Condition = .{},

    // Fibers
    reader_task: ?zio.JoinHandle(void) = null,
    flusher_task: ?zio.JoinHandle(void) = null,
    reconnect_group: zio.Group = .init,
    reconnect_running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    should_stop_cond: zio.Condition = .{},

    // Main connection mutex (protects most fields)
    mutex: zio.Mutex = .{},

    // PING/PONG flush tracking (simplified counter approach)
    outgoing_pings: u64 = 0,
    incoming_pongs: u64 = 0,
    pong_condition: zio.Condition = .{},

    // PING/PONG keep-alive tracking
    ping_timer: std.time.Timer, // Timer for tracking ping intervals
    pings_out: std.atomic.Value(u32) = std.atomic.Value(u32).init(0), // Outstanding keep-alive pings (atomic)

    // Write buffer (thread-safe, 64KB chunk size)
    write_buffer: WriteBuffer,

    // Subscriptions
    next_sid: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    subscriptions: std.AutoHashMap(u64, *Subscription),
    subs_mutex: zio.Mutex = .{},

    // Response management (shared subscription for request/reply)
    response_manager: ResponseManager,

    // Connection draining
    drain_state: std.atomic.Value(DrainState) = std.atomic.Value(DrainState).init(.not_draining),
    drain_completion: zio.ResetEvent = .init,
    drain_subscription_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    drain_ping_id: u64 = 0,

    scratch: std.heap.ArenaAllocator,

    // Parser
    parser: Parser,

    const Self = @This();
    const WriteBuffer = ConcurrentWriteBuffer(65536); // 64KB chunk size

    pub fn init(allocator: Allocator, rt: *zio.Runtime, options: ConnectionOptions) Self {
        return Self{
            .allocator = allocator,
            .rt = rt,
            .options = options,
            .server_pool = ServerPool.init(allocator),
            .server_info_arena = std.heap.ArenaAllocator.init(allocator),
            .pending_buffer = WriteBuffer.init(allocator, .{ .max_size = options.reconnect.reconnect_buf_size }),
            .write_buffer = WriteBuffer.init(allocator, .{}),
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .response_manager = ResponseManager.init(allocator, rt),
            .parser = Parser.init(allocator, rt),
            .scratch = std.heap.ArenaAllocator.init(allocator),
            .ping_timer = std.time.Timer.start() catch unreachable,
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();

        // Cancel and wait for fibers (they should have been signaled to stop by close())
        if (self.flusher_task) |*task| {
            task.cancel(self.rt);
            self.flusher_task = null;
        }
        if (self.reader_task) |*task| {
            task.cancel(self.rt);
            self.reader_task = null;
        }
        self.reconnect_group.cancel(self.rt);

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
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);
        return try self.server_pool.addServer(url_str, false); // Explicit server
    }

    fn connectToServer(self: *Self) !void {
        errdefer self.close();

        // This is called from initial connect() - needs to manage its own mutex
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        self.status = .connecting;

        // Get server using C library's GetNextServer algorithm
        const selected_server = try self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) orelse {
            return ConnectionError.ConnectionFailed;
        };

        // Update current server and track reconnection attempt (like C library)
        self.current_server = selected_server;
        selected_server.reconnects += 1;

        // Establish connection (under mutex for consistent state management)
        try self.establishConnection(selected_server);

        // Stream is now established and connection state is set up
        self.should_stop.store(false, .monotonic);

        // Start reader fiber - it will handle the handshake
        self.reader_task = try self.rt.spawn(readerLoop, .{self});

        // Start flusher fiber
        self.flusher_task = try self.rt.spawn(flusherLoop, .{self});

        // Wait for handshake completion
        try self.waitForHandshakeCompletion();

        // Handshake completed successfully
        self.status = .connected;

        log.info("Connected successfully to {s}", .{selected_server.parsed_url.full_url});
    }

    /// Close the connection
    pub fn close(self: *Self) void {
        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.closed_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.status == .closed) {
            return;
        }

        log.info("Closing connection", .{});

        // Mark the connection as permanently closed
        self.status = .closed;
        self.should_stop.store(true, .release);
        self.should_stop_cond.broadcast(self.rt); // Wake up any fibers waiting on should_stop

        // Detach stream immediately and wake any waiting fibers
        const old_stream = self.stream;
        self.stream = null;
        self.stream_available_cond.broadcast(self.rt);

        // Shutdown stream to interrupt ongoing I/O (but don't close yet)
        if (old_stream) |stream| {
            stream.shutdown(self.rt, .both) catch |shutdown_err| {
                log.err("Stream shutdown failed: {}", .{shutdown_err});
            };
        }

        // Close write buffer - this wakes up the flusher fiber
        self.pending_buffer.close(self.rt);
        self.write_buffer.close(self.rt);

        // Wait for both fibers to release the stream with timeout
        self.waitForStreamUnused(self.options.timeout_ms * 2) catch {
            log.warn("Timeout waiting for stream references to be released during close", .{});
        };

        // Now safe to close stream since fibers are done with it
        if (old_stream) |stream| {
            stream.close(self.rt);
        }

        // Wake up any waiting flush() calls
        self.pong_condition.broadcast(self.rt);

        // Make sure we invoke the closed callback
        if (self.options.callbacks.closed_cb) |cb| {
            callback = cb;
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);
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

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

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

        // Spawn reconnect fiber if not already running
        if (!self.reconnect_running.swap(true, .acq_rel)) {
            self.reconnect_group.spawn(self.rt, doReconnectLoop, .{self}) catch |spawn_err| {
                log.err("Failed to spawn reconnect fiber: {}", .{spawn_err});
                self.reconnect_running.store(false, .release);
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
        if (self.drain_state.load(.acquire) == .draining_pubs) {
            return error.DrainInProgress;
        }

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        // TODO pre-allocate headers_buffer
        var headers_buffer = ArrayList(u8){};
        defer headers_buffer.deinit(allocator);

        try msg.encodeHeaders(headers_buffer.writer(allocator));
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
            try self.pending_buffer.append(self.rt, buffer.items);
        } else {
            try self.write_buffer.append(self.rt, buffer.items);
        }

        if (reply_to_use) |reply| {
            log.debug("Published message to {s} with reply {s}", .{ msg.subject, reply });
        } else {
            log.debug("Published message to {s}", .{msg.subject});
        }
    }

    fn subscribeInternal(self: *Self, sub: *Subscription) !void {
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        self.subs_mutex.lockUncancelable(self.rt);
        defer self.subs_mutex.unlock(self.rt);

        try self.subscriptions.put(sub.sid, sub);
        errdefer _ = self.subscriptions.remove(sub.sid);

        // Send SUB command via buffer
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        var buffer = ArrayList(u8){};
        defer buffer.deinit(allocator);
        if (sub.queue) |group| {
            try buffer.writer(allocator).print("SUB {s} {s} {d}\r\n", .{ sub.subject, group, sub.sid });
        } else {
            try buffer.writer(allocator).print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
        }
        try self.write_buffer.append(self.rt, buffer.items);
    }

    pub fn subscribe(self: *Self, subject: []const u8, comptime handlerFn: anytype, args: anytype) !*Subscription {
        try validation.validateSubject(subject);

        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.create(self, sid, subject, null, handler);
        errdefer sub.release();

        try self.subscribeInternal(sub);
        try sub.startHandler();

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

        try self.subscribeInternal(sub);
        try sub.startHandler();

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

    pub fn unsubscribeInternal(self: *Self, sid: u64, max: ?u64) !void {
        var buffer: [256]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        var writer = stream.writer();

        if (max) |m| {
            writer.print("UNSUB {d} {d}\r\n", .{ sid, m }) catch unreachable; // Will always fit
        } else {
            writer.print("UNSUB {d}\r\n", .{sid}) catch unreachable; // Will always fit
        }

        try self.write_buffer.append(self.rt, stream.getWritten());
    }

    pub fn unsubscribe(self: *Self, sub: *Subscription) void {
        // Remove from subscription table first
        {
            self.mutex.lockUncancelable(self.rt);
            defer self.mutex.unlock(self.rt);

            self.subs_mutex.lockUncancelable(self.rt);
            defer self.subs_mutex.unlock(self.rt);

            if (!self.subscriptions.remove(sub.sid)) {
                // Nothing to do, already unsubscribed
                return;
            }
        }

        // Try to send UNSUB command. Even if it fails internally,
        // processMsg will keep sending UNSUB commands once
        // it receives a message with unknown sid.
        self.unsubscribeInternal(sub.sid, null) catch |err| {
            log.err("Failed to send UNSUB for sid {d}: {}", .{ sub.sid, err });
        };

        log.debug("Unsubscribed from {s} with sid {d}", .{ sub.subject, sub.sid });

        // Release connection's reference to the subscription
        sub.release();
    }

    /// Remove subscription from connection's subscription table
    /// This does not send UNSUB to server - that should be done separately
    pub fn removeSubscriptionInternal(self: *Self, sid: u64) void {
        self.subs_mutex.lockUncancelable(self.rt);
        defer self.subs_mutex.unlock(self.rt);

        if (self.subscriptions.fetchRemove(sid)) |kv| {
            log.debug("Removed subscription {d} ({s}) from connection", .{ sid, kv.value.subject });
            // Release connection's reference to the subscription
            kv.value.release();
        }
    }

    pub fn flush(self: *Self) !void {
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const our_ping_id = try self.sendPing(false);

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
            self.pong_condition.timedWait(self.rt, &self.mutex, .fromNanoseconds(remaining_ns)) catch {};
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
        const stream = zio.net.tcpConnectToHost(self.rt, server.parsed_url.host, server.parsed_url.port) catch |err| {
            log.err("Failed to connect to server: {}", .{err});
            return err;
        };
        errdefer stream.close(self.rt);

        // Configure socket options
        stream.socket.setKeepAlive(true) catch |err| {
            log.err("Failed to set keep alive: {}", .{err});
            return err;
        };

        if (self.options.trace) {
            log.debug("Connected, starting handshake...", .{});
        }

        // Setup connection state
        self.stream = stream;
        self.stream_available_cond.broadcast(self.rt);

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
        self.write_buffer.unfreeze(self.rt);
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

        const stream = try self.acquireStream();
        defer self.releaseStream();

        // Async read - will yield fiber until data is available
        const bytes_read = stream.read(self.rt, &buffer) catch |err| {
            log.err("Read error: {}", .{err});
            return err;
        };

        if (bytes_read == 0) {
            log.debug("Connection closed by server (EOF)", .{});
            return error.EndOfStream;
        }

        log.debug("Read {} bytes: {s}", .{ bytes_read, buffer[0..bytes_read] });

        // Parse the received data
        self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
            switch (err) {
                error.ShouldStop, error.ShouldClose => return err,
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

            if (self.drain_state.load(.acquire) == .draining_pubs) {
                self.sendDrainPing();
            }
        }

        log.debug("Flusher loop exited", .{});
    }

    fn flusherIteration(self: *Self) !void {
        // Try to gather data from buffer first
        var slices: [16][]const u8 = undefined;
        const gather = self.write_buffer.gatherReadSlices(self.rt, &slices, self.options.timeout_ms) catch |err| switch (err) {
            error.QueueEmpty => {
                // No data to write
                return;
            },
            error.BufferFrozen => {
                // Buffer frozen during reconnection
                return;
            },
            error.QueueClosed => return error.ShouldStop,
            error.Canceled => return error.Canceled,
        };

        if (gather.slices.len == 0) {
            // No data to write
            return;
        }

        // Now try to get a stream - blocks until available
        const stream = try self.acquireStream();
        defer self.releaseStream();

        // Write each buffer slice
        var bytes_written: usize = 0;
        for (gather.slices) |slice| {
            stream.writeAll(self.rt, slice) catch |err| {
                log.err("Write error: {}", .{err});
                // Consume what we wrote so far before returning error
                if (bytes_written > 0) {
                    gather.consume(self.rt, bytes_written) catch {};
                }
                return err;
            };
            bytes_written += slice.len;
        }

        try gather.consume(self.rt, bytes_written);
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, message: *Message) !void {
        var owns_message = true;
        defer if (owns_message) message.deinit();

        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        // Retain subscription while holding lock, then release lock
        self.subs_mutex.lockUncancelable(self.rt);
        const sub = self.subscriptions.get(message.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock(self.rt);

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

            // Queue message for subscription (both sync and async use the queue)
            // For async subscriptions, the handler fiber will pick it up
            s.messages.push(self.rt, message) catch |err| {
                switch (err) {
                    error.QueueClosed => {
                        // Queue is closed; drop gracefully.
                        log.debug("Queue closed for sid {d}; dropping message", .{message.sid});
                        // Undo the pending counters since queue is closed
                        subscription_mod.decrementPending(s, message.data.len);
                        return;
                    },
                    error.Canceled => return error.Canceled,
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
        } else {
            // No sub subscription found, try to send UNSUB command
            self.unsubscribeInternal(message.sid, null) catch |err| {
                log.err("Failed to send UNSUB for unknown sid {d}: {}", .{ message.sid, err });
            };
        }
    }

    /// Sends CONNECT and PING during handshake (assumes mutex is held)
    fn sendConnectAndPing(self: *Self) !void {
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        // Build CONNECT message with all options
        var buffer = ArrayList(u8){};
        defer buffer.deinit(allocator);

        // Calculate effective no_responders: enable if server supports headers
        const no_responders = self.options.no_responders and self.server_info.headers;

        // Get client name from options or use default
        const client_name = self.options.name orelse build_options.name;

        // Get authentication token (dynamic handler takes precedence)
        const auth_token = if (self.options.token_handler) |handler|
            handler()
        else
            self.options.token;

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
            .auth_token = auth_token,
        };

        try buffer.writer(allocator).writeAll("CONNECT ");
        try std.fmt.format(buffer.writer(allocator), "{f}", .{std.json.fmt(connect_obj, .{})});
        try buffer.writer(allocator).writeAll("\r\n");
        try buffer.writer(allocator).writeAll("PING\r\n");

        // Send via buffer (mutex already held)
        try self.write_buffer.append(self.rt, buffer.items);

        log.debug("Sent CONNECT+PING during handshake", .{});
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        log.debug("Received INFO: {s}", .{info_json});

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

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
                self.handshake_cond.broadcast(self.rt);
                return;
            };

            self.handshake_state = .waiting_for_pong;
            self.handshake_cond.broadcast(self.rt); // Signal state change
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

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

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

    /// Maps -ERR message to specific ProtocolError (similar to nats.go approach)
    fn parseProtocolError(err_msg: []const u8, allocator: std.mem.Allocator) ProtocolError {
        const lower_err = std.ascii.allocLowerString(allocator, err_msg) catch return ProtocolError.UnknownServerError;
        defer allocator.free(lower_err);

        // Authentication/Authorization errors
        if (std.mem.containsAtLeast(u8, lower_err, 1, "authorization violation")) {
            return ProtocolError.AuthorizationViolation;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "user authentication expired")) {
            return ProtocolError.AuthExpired;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "user authentication revoked")) {
            return ProtocolError.AuthRevoked;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "account authentication expired")) {
            return ProtocolError.AccountAuthExpired;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "permissions violation")) {
            return ProtocolError.PermissionViolation;
        }
        // Connection/Limit errors
        else if (std.mem.containsAtLeast(u8, lower_err, 1, "maximum connections exceeded")) {
            return ProtocolError.MaxConnectionsExceeded;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "connection throttling")) {
            return ProtocolError.ConnectionThrottling;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "maximum payload violation")) {
            return ProtocolError.MaxPayloadViolation;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "maximum subscriptions exceeded")) {
            return ProtocolError.MaxSubscriptionsExceeded;
        }
        // Protocol errors
        else if (std.mem.containsAtLeast(u8, lower_err, 1, "secure connection") and
            std.mem.containsAtLeast(u8, lower_err, 1, "tls required"))
        {
            return ProtocolError.SecureConnectionRequired;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "invalid client protocol")) {
            return ProtocolError.InvalidClientProtocol;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "unknown protocol operation")) {
            return ProtocolError.UnknownProtocolOperation;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "invalid publish subject")) {
            return ProtocolError.InvalidPublishSubject;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "no responders requires headers")) {
            return ProtocolError.NoRespondersRequiresHeaders;
        } else if (std.mem.containsAtLeast(u8, lower_err, 1, "failed account registration")) {
            return ProtocolError.FailedAccountRegistration;
        }

        return ProtocolError.UnknownServerError; // Unrecognized error
    }

    pub fn processErr(self: *Self, err_msg: []const u8) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.error_cb) = null;
        defer if (callback) |cb| cb(self, err_msg);

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        // Parse the protocol error once
        const protocol_err = parseProtocolError(err_msg, self.allocator);

        log.err("Server protocol error: {} - {s}", .{ protocol_err, err_msg });

        // Handle handshake failure
        if (self.handshake_state.isWaiting()) {
            // Propagate specific protocol errors to client
            self.handshake_error = protocol_err;
            self.handshake_state = .failed;
            self.handshake_cond.broadcast(self.rt); // Signal handshake failure
            log.debug("Handshake failed: {}", .{protocol_err});
            return;
        }

        // Invoke error callback (in defer outside of mutex)
        if (self.options.callbacks.error_cb) |cb| {
            callback = cb;
        }
    }

    fn sendPing(self: *Self, comptime lock: bool) !u64 {
        try self.write_buffer.append(self.rt, "PING\r\n");

        if (lock) self.mutex.lockUncancelable(self.rt);
        defer if (lock) self.mutex.unlock(self.rt);

        self.outgoing_pings += 1;
        return self.outgoing_pings;
    }

    fn checkAndSendPing(self: *Self) !void {
        if (self.options.ping_interval_ms == 0) return;

        const interval_ns = self.options.ping_interval_ms * std.time.ns_per_ms;
        const elapsed_ns = self.ping_timer.read();
        if (elapsed_ns >= interval_ns) {
            _ = try self.sendPing(true);
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

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        // Handle handshake completion
        if (self.handshake_state == .waiting_for_pong) {
            self.handshake_state = .completed;
            self.handshake_cond.broadcast(self.rt); // Signal handshake completion
            log.debug("Handshake completed successfully", .{});
            return;
        }

        // Regular PONG handling for flush() calls
        self.incoming_pongs += 1;
        self.pong_condition.broadcast(self.rt);

        log.debug("Received PONG for ping_id={}", .{self.incoming_pongs});

        // Reset keep-alive ping counter - ANY PONG proves connection is alive
        self.pings_out.store(0, .monotonic);

        if (self.drain_ping_id == self.incoming_pongs) {
            try self.notifyPublishDrainComplete();
        }
    }

    pub fn processPing(self: *Self) !void {
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        try self.write_buffer.append(self.rt, "PONG\r\n");
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

        // Detach stream and wake any threads waiting for it
        const old_stream = self.stream;
        self.stream = null;
        self.stream_available_cond.broadcast(self.rt);

        // Always freeze write buffer (idempotent operation)
        self.write_buffer.freeze(self.rt);

        // Wake up any waiting flush() calls
        self.pong_condition.broadcast(self.rt);

        // Reset handshake state
        self.handshake_state = .not_started;
        self.handshake_error = null;
        self.handshake_cond.broadcast(self.rt);

        // Handle stream cleanup
        if (old_stream) |s| {
            // Always shutdown first to interrupt ongoing I/O
            s.shutdown(self.rt, .both) catch |shutdown_err| {
                log.debug("Stream shutdown failed: {}", .{shutdown_err});
            };

            // Close stream if requested
            if (close_socket) {
                s.close(self.rt);
            }
        }
    }

    fn markNeedsReconnect(self: *Self, err: anyerror) void {
        var needs_close = false;
        defer if (needs_close) self.close();

        var callback: @TypeOf(self.options.callbacks.disconnected_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.status != .connected) {
            log.info("Connection not in connected state", .{});
            return;
        }

        // Handle explicit close request
        if (err == error.ShouldClose) {
            needs_close = true;
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

        // Spawn reconnect fiber if not already running
        if (!self.reconnect_running.swap(true, .acq_rel)) {
            self.reconnect_group.spawn(self.rt, doReconnectLoop, .{self}) catch |spawn_err| {
                log.err("Failed to spawn reconnect fiber: {}", .{spawn_err});
                self.reconnect_running.store(false, .release);
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

    fn doReconnectLoop(self: *Self) void {
        defer self.reconnect_running.store(false, .release);
        self.doReconnect();
    }

    fn doReconnect(self: *Self) void {
        var needs_close = false;
        defer if (needs_close) self.close();

        var callback: @TypeOf(self.options.callbacks.reconnected_cb) = null;
        defer if (callback) |cb| cb(self);

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        // Check if we should still reconnect (could have been closed)
        if (self.status != .reconnecting) {
            return;
        }

        log.debug("Starting reconnection", .{});

        // Close old stream if still attached (reader thread owns stream lifecycle)
        const old_stream = self.stream;
        self.stream = null;
        self.stream_available_cond.broadcast(self.rt); // Wake any threads waiting for stream

        if (old_stream) |s| {
            s.close(self.rt);
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

                self.should_stop_cond.timedWait(self.rt, &self.mutex, .fromMilliseconds(delay_ms)) catch {};

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
            self.pending_buffer.moveToBuffer(self.rt, &self.write_buffer) catch |err| {
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

        // Track SIDs that shouldn't be re-subscribed and must be removed
        var to_remove = ArrayList(u64){};
        defer to_remove.deinit(self.allocator);

        {
            self.subs_mutex.lockUncancelable(self.rt);
            defer self.subs_mutex.unlock(self.rt);

            const allocator = self.scratch.allocator();
            defer self.resetScratch();

            var buffer = ArrayList(u8){};
            defer buffer.deinit(allocator);

            var iter = self.subscriptions.iterator();
            while (iter.next()) |entry| {
                const sub = entry.value_ptr.*;

                // Check autounsubscribe state
                const max = sub.max_msgs.load(.acquire);
                const delivered = sub.delivered_msgs.load(.acquire);

                var adjusted_max: ?u64 = null;
                if (max > 0) {
                    if (delivered < max) {
                        adjusted_max = max - delivered; // Remaining messages
                    } else {
                        // Already reached limit - don't re-subscribe; remove after unlock
                        log.debug("Subscription {d} ({s}) already reached limit; will remove during reconnect", .{ sub.sid, sub.subject });
                        try to_remove.append(self.allocator, sub.sid);
                        continue;
                    }
                }

                // Send SUB command
                if (sub.queue) |queue| {
                    try buffer.writer(allocator).print("SUB {s} {s} {d}\r\n", .{ sub.subject, queue, sub.sid });
                } else {
                    try buffer.writer(allocator).print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
                }

                // Send UNSUB with remaining limit if needed
                if (adjusted_max) |remaining| {
                    try buffer.writer(allocator).print("UNSUB {d} {d}\r\n", .{ sub.sid, remaining });
                    log.debug("Re-subscribed to {s} with sid {d} and autounsubscribe limit {d} (delivered: {d})", .{ sub.subject, sub.sid, remaining, delivered });
                } else {
                    log.debug("Re-subscribed to {s} with sid {d}", .{ sub.subject, sub.sid });
                }
            }

            // Send all subscription commands via write buffer
            if (buffer.items.len > 0) {
                try self.write_buffer.append(self.rt, buffer.items);
            }
        }

        // Now remove stale subs outside the subs_mutex
        for (to_remove.items) |sid| {
            self.removeSubscriptionInternal(sid);
        }
    }

    /// Acquires a reference to the stream for safe concurrent use.
    /// Blocks until a stream becomes available.
    /// The caller MUST call releaseStream() when done with the stream.
    fn acquireStream(self: *Self) !zio.net.Stream {
        // Check should_stop flag before acquiring the mutex
        if (self.should_stop.load(.acquire)) {
            return error.ShouldStop;
        }

        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        // Wait until stream is available
        while (self.stream == null) {
            if (self.should_stop.load(.acquire)) {
                return error.ShouldStop;
            }
            self.stream_available_cond.waitUncancelable(self.rt, &self.mutex);
        }

        if (self.stream) |stream| {
            self.stream_refs += 1;
            return stream;
        }

        unreachable;
    }

    /// Releases a reference to the stream obtained via acquireStream().
    /// This must be called for every successful acquireStream() call.
    fn releaseStream(self: *Self) void {
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.stream_refs > 0) {
            self.stream_refs -= 1;
            // Only broadcast when all references are released
            if (self.stream_refs == 0) {
                self.stream_unused_cond.broadcast(self.rt);
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
                self.handshake_cond.broadcast(self.rt); // Signal the state change
                break;
            }

            const remaining_ns = timeout_ns - elapsed_ns;
            self.handshake_cond.timedWait(self.rt, &self.mutex, .fromNanoseconds(remaining_ns)) catch {};

            // Check for early termination conditions
            if (self.should_stop.load(.acquire)) {
                self.handshake_error = ConnectionError.ConnectionClosed;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast(self.rt); // Signal the state change
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
    fn waitForStreamUnused(self: *Self, timeout_ms: u64) !void {
        if (self.stream_refs == 0) {
            return; // Already unused
        }

        if (timeout_ms == 0) {
            return error.Timeout; // Non-blocking, socket still in use
        }

        var timer = std.time.Timer.start() catch return error.Timeout;
        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        while (self.stream_refs > 0) {
            const elapsed_ns = timer.read();
            if (elapsed_ns >= timeout_ns) {
                return error.Timeout;
            }

            const remaining_ns = timeout_ns - elapsed_ns;
            self.stream_unused_cond.timedWait(self.rt, &self.mutex, .fromNanoseconds(remaining_ns)) catch {};
        }

        // Stream became unused
    }

    // JetStream support
    pub fn jetstream(self: *Self, options: JetStreamOptions) JetStream {
        return JetStream.init(self, options);
    }

    // Connection draining
    pub fn drain(self: *Self) !void {
        const prev_state = self.drain_state.cmpxchgStrong(.not_draining, .draining_subs, .acq_rel, .acquire);
        if (prev_state != null) return; // Already draining

        // Add one count as a blocker, to avoid early switch to the draining_pubs phase
        _ = self.drain_subscription_count.fetchAdd(1, .release);

        // Start draining subscriptions
        self.subs_mutex.lockUncancelable(self.rt);
        var iter = self.subscriptions.valueIterator();
        while (iter.next()) |sub_ptr| {
            const sub = sub_ptr.*;
            _ = self.drain_subscription_count.fetchAdd(1, .release);
            sub.drain(); // Drain the subscription
        }
        self.subs_mutex.unlock(self.rt);

        // Release the blocker
        self.notifySubscriptionDrainComplete();
    }

    pub fn isDraining(self: *Self) bool {
        const state = self.drain_state.load(.acquire);
        switch (state) {
            .draining_subs, .draining_pubs => return true,
            else => return false,
        }
    }

    pub fn isDrainComplete(self: *Self) bool {
        const state = self.drain_state.load(.acquire);
        switch (state) {
            .drain_complete => return true,
            else => return false,
        }
    }

    pub fn waitForDrainCompletion(self: *Self, timeout_ms: ?u64) !void {
        const state = self.drain_state.load(.acquire);
        switch (state) {
            .not_draining => return error.NotDraining,
            .drain_complete => return,
            else => {},
        }

        if (timeout_ms) |timeout| {
            try self.drain_completion.timedWait(self.rt, .fromMilliseconds(timeout));
        } else {
            try self.drain_completion.wait(self.rt);
        }
    }

    pub fn notifySubscriptionDrainComplete(self: *Self) void {
        // Only decrement if we're in the right state
        const state = self.drain_state.load(.acquire);
        if (state == .draining_subs) {
            const remaining = self.drain_subscription_count.fetchSub(1, .acq_rel);
            std.debug.assert(remaining > 0); // Catch atomic underflow during development
            if (remaining == 1) { // This was the last one
                self.startPublicationDrain();
            }
        }
    }

    fn sendDrainPing(self: *Self) void {
        self.mutex.lockUncancelable(self.rt);
        defer self.mutex.unlock(self.rt);

        if (self.drain_ping_id > 0) return; // Already sent the last ping

        self.drain_ping_id = self.sendPing(false) catch |err| {
            log.err("Failed to send drain ping: {}", .{err});
            return;
        };
    }

    fn notifyPublishDrainComplete(self: *Self) !void {
        const prev_state = self.drain_state.cmpxchgStrong(.draining_pubs, .drain_complete, .acq_rel, .acquire);
        if (prev_state != null) return; // Already completed

        self.drain_completion.set();

        return error.ShouldClose;
    }

    fn startPublicationDrain(self: *Self) void {
        const prev_state = self.drain_state.cmpxchgStrong(.draining_subs, .draining_pubs, .acq_rel, .acquire);
        if (prev_state != null) return; // Already draining pubs

        self.sendDrainPing();
    }
};
