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
const net = std.Io.net;
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
const nkeys = @import("nkeys.zig");
const creds_mod = @import("creds.zig");
const xsync = @import("xsync");
const Io = std.Io;
const io_util = @import("io_util.zig");
const net_util = @import("net_util.zig");

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
} || PublishError || ProtocolError || std.Thread.SpawnError || net.Stream.Reader.Error || net.Stream.Writer.Error;

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
    connection_failed,
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
    reconnect_wait: Io.Duration = .fromSeconds(2),
    reconnect_jitter: Io.Duration = .fromMilliseconds(100),
    reconnect_jitter_tls: Io.Duration = .fromSeconds(1),
    reconnect_buf_size: usize = 8 * 1024 * 1024,
    allow_reconnect: bool = true,
    custom_reconnect_delay_cb: ?*const fn (attempts: u32) Io.Duration = null,
};

pub const ConnectionCallbacks = struct {
    disconnected_cb: ?*const fn (*Connection) void = null,
    reconnected_cb: ?*const fn (*Connection) void = null,
    closed_cb: ?*const fn (*Connection) void = null,
    error_cb: ?*const fn (*Connection, []const u8) void = null,
    /// Called once per slow-consumer episode with the subscription that
    /// dropped a message and its cumulative dropped count. Invoked outside
    /// connection and queue locks; the subscription is retained for the
    /// duration of the call.
    slow_consumer_cb: ?*const fn (*Connection, *Subscription, u64) void = null,
};

const ExitReason = enum(u8) {
    none,
    reader_done,
    flusher_done,
    reconnect_requested,
};

/// One-shot exit signal owned by a single `runConnection` iteration. The
/// reader loop, the flusher loop, and `reconnect()` all funnel into it; the
/// first signaler decides the exit reason and everyone else is a no-op.
const ExitSignal = struct {
    event: xsync.Event = .init,
    reason: std.atomic.Value(ExitReason) = .init(.none),
    /// Written by the winning signaler before the event is set.
    err: ?anyerror = null,

    fn signal(self: *ExitSignal, io: Io, reason: ExitReason, err: ?anyerror) void {
        if (self.reason.cmpxchgStrong(.none, reason, .acq_rel, .acquire) != null) {
            // Someone else already decided the exit reason and will set the
            // event; setting it here could wake the waiter before the winner
            // has published its error.
            return;
        }
        self.err = err;
        self.event.set(io);
    }
};

pub const ConnectionOptions = struct {
    name: ?[]const u8 = null,
    timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } },
    verbose: bool = false,
    send_asap: bool = false,
    reconnect: ReconnectOptions = .{},
    callbacks: ConnectionCallbacks = .{},
    trace: bool = false,
    no_responders: bool = true,
    max_scratch_size: usize = 1024 * 1024 * 10,
    ping_interval: Io.Duration = .fromSeconds(120), // .zero = disabled
    max_pings_out: u32 = 2, // max unanswered keep-alive PINGs

    /// High-water mark for bytes buffered on a live connection
    /// (0 = unlimited). When exceeded, publishers wait (cancelably,
    /// without holding connection locks) for the flusher to catch up,
    /// like the buffered writers in nats.go and nats.c. A single message
    /// larger than the limit is still accepted into an empty buffer, and
    /// small control frames (PING, SUB, ...) always go through.
    write_buffer_limit: usize = 8 * 1024 * 1024,

    // Authentication
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
    token: ?[]const u8 = null,
    token_handler: ?*const fn () []const u8 = null,
    /// NKey seed ("SU..."); the client signs the server nonce with it and
    /// sends the derived public key and signature in CONNECT.
    nkey_seed: ?[]const u8 = null,
    /// Path to a credentials (.creds) file containing the user JWT and the
    /// NKey seed. The file is re-read on every (re)connect, so rotated
    /// credentials are picked up automatically. Takes precedence over
    /// `user_jwt` and `nkey_seed`.
    user_creds: ?[]const u8 = null,
    /// User JWT to present in CONNECT. Requires `nkey_seed` or
    /// `nkey_sign_cb` for signing the server nonce.
    user_jwt: ?[]const u8 = null,
    /// Public NKey ("U..."), for use with `nkey_sign_cb` so the seed never
    /// has to be in this process. Ignored when a seed or JWT is configured.
    nkey: ?[]const u8 = null,
    /// Custom signer: receives the server nonce and returns the raw Ed25519
    /// signature. Used with `nkey` (or `user_jwt`) instead of `nkey_seed`;
    /// a configured seed takes precedence.
    nkey_sign_cb: ?*const fn (nonce: []const u8) anyerror![64]u8 = null,
};

/// Maximum accepted size of a credentials file.
const max_creds_file_size = 1024 * 1024;

pub const Connection = struct {
    allocator: Allocator,

    /// The `std.Io` instance used for blocking and waking
    io: Io,

    options: ConnectionOptions,

    status: ConnectionStatus = .closed,
    status_cond: xsync.Condition = .init,

    // Exit signal of the current runConnection iteration (protected by mutex)
    exit_signal: ?*ExitSignal = null,

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
    handshake_cond: xsync.Condition = .init,

    // Connection manager task (owns reader/flusher tasks)
    manager_task: Io.Group = .init,

    // Main connection mutex (protects most fields)
    mutex: xsync.Mutex = .init,

    // PING/PONG flush tracking (simplified counter approach)
    outgoing_pings: u64 = 0,
    incoming_pongs: u64 = 0,
    pong_condition: xsync.Condition = .init,

    // PING/PONG keep-alive tracking
    ping_time: Io.Timestamp, // Time of the last keep-alive PING interval mark
    pings_out: std.atomic.Value(u32) = std.atomic.Value(u32).init(0), // Outstanding keep-alive pings (atomic)

    // Write buffer (thread-safe, 64KB chunk size)
    write_buffer: WriteBuffer,

    // Subscriptions
    next_sid: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    subscriptions: std.AutoHashMap(u64, *Subscription),
    subs_mutex: xsync.Mutex = .init,

    // Response management (shared subscription for request/reply)
    response_manager: ResponseManager,

    // Connection draining
    drain_state: std.atomic.Value(DrainState) = std.atomic.Value(DrainState).init(.not_draining),
    drain_completion: xsync.Event = .init,
    drain_subscription_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    drain_ping_id: u64 = 0,

    scratch: std.heap.ArenaAllocator,

    // Parser
    parser: Parser,

    const Self = @This();
    const WriteBuffer = ConcurrentWriteBuffer(65536); // 64KB chunk size

    pub fn init(allocator: Allocator, io: Io, options: ConnectionOptions) Self {
        return Self{
            .allocator = allocator,
            .io = io,

            .options = options,
            .server_pool = ServerPool.init(allocator),
            .server_info_arena = std.heap.ArenaAllocator.init(allocator),
            .pending_buffer = WriteBuffer.init(allocator, io, .{ .max_size = options.reconnect.reconnect_buf_size }),
            .write_buffer = WriteBuffer.init(allocator, io, .{ .max_size = options.write_buffer_limit, .soft_limit = true }),
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .response_manager = ResponseManager.init(allocator, io),
            .parser = Parser.init(allocator, io),
            .scratch = std.heap.ArenaAllocator.init(allocator),
            .ping_time = io_util.now(io),
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();

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
        errdefer self.close();

        try io_util.ensureAwakeClock(self.io);

        // Validate credential options up front so a bad seed or an
        // unreadable credentials file fails immediately instead of
        // surfacing as a handshake error on every server.
        if (self.options.user_creds) |path| {
            const content = try Io.Dir.cwd().readFileAlloc(self.io, path, self.allocator, .limited(max_creds_file_size));
            defer {
                std.crypto.secureZero(u8, content);
                self.allocator.free(content);
            }
            const credentials = try creds_mod.parse(content);
            var seed_kp = try nkeys.SeedKeyPair.fromSeed(credentials.seed);
            seed_kp.wipe();
        } else {
            if (self.options.user_jwt != null and self.options.nkey_seed == null and self.options.nkey_sign_cb == null) {
                // A JWT alone cannot authenticate; the nonce must be signed.
                return error.MissingNKeySeed;
            }
            if (self.options.nkey_seed) |seed| {
                var seed_kp = try nkeys.SeedKeyPair.fromSeed(seed);
                seed_kp.wipe();
            } else if (self.options.nkey_sign_cb != null and self.options.user_jwt == null and self.options.nkey == null) {
                // A signer without a JWT needs the public key to present.
                return error.MissingNKey;
            }
        }

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        if (self.status != .closed) {
            log.err("Already connected", .{});
            return error.AlreadyConnected;
        }

        self.status = .connecting;
        self.status_cond.broadcast(self.io);

        // The URL may be a comma-separated list of servers; the initial
        // connection tries them in order until one succeeds.
        var added: usize = 0;
        var url_it = std.mem.splitScalar(u8, url, ',');
        while (url_it.next()) |part| {
            const trimmed = std.mem.trim(u8, part, " \t");
            if (trimmed.len == 0) continue;
            _ = try self.server_pool.addServer(trimmed, false);
            added += 1;
        }
        if (added == 0) return error.InvalidUrl;

        try self.manager_task.concurrent(self.io, managerTask, .{self});
        errdefer self.manager_task.cancel(self.io);

        while (true) {
            switch (self.status) {
                .closed => return error.ConnectionClosed,
                .connection_failed => {
                    if (self.handshake_state == .failed) {
                        if (self.handshake_error) |err| {
                            return err;
                        }
                    }
                    return error.ConnectionFailed;
                },
                .connected => {
                    log.info("Connected successfully", .{});
                    return;
                },
                else => {
                    try self.status_cond.wait(self.io, &self.mutex);
                },
            }
        }
    }

    pub fn addServer(self: *Self, url: []const u8) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        _ = try self.server_pool.addServer(url, false);
    }

    /// Close the connection
    pub fn close(self: *Self) void {
        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.closed_cb) = null;
        defer if (callback) |cb| cb(self);

        log.info("Closing connection", .{});

        self.manager_task.cancel(self.io);

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.status == .closed) {
            return;
        }

        // Mark the connection as permanently closed
        self.status = .closed;
        self.status_cond.broadcast(self.io);

        // Close write buffers to wake up any waiting fibers
        self.pending_buffer.close();
        self.write_buffer.close();

        // Wake up any waiting flush() calls
        self.pong_condition.broadcast(self.io);

        // Make sure we invoke the closed callback
        if (self.options.callbacks.closed_cb) |cb| {
            callback = cb;
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
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
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

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
            .connecting, .connection_failed => {
                log.warn("Cannot reconnect: initial connection not yet established", .{});
                return ConnectionError.NotConnected;
            },
            .connected => {
                // OK to proceed
            },
        }

        log.info("Manual reconnection requested", .{});

        // Signal the current connection's exit signal; if it is already gone,
        // the connection is being torn down and a reconnect is underway anyway.
        if (self.exit_signal) |sig| {
            sig.signal(self.io, .reconnect_requested, null);
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
        var frame_size: usize = 0;
        while (true) {
            return self.publishMsgAttempt(msg, reply_override, &frame_size) catch |err| switch (err) {
                error.NoSpace => {
                    // The live write buffer is over its high-water mark.
                    // Wait for the flusher outside of Connection.mutex, so
                    // reconnect and close can always proceed (both wake the
                    // waiters), then retry with the connection state
                    // re-checked (it may route to the pending buffer now).
                    // The failed attempt reported the exact frame size, so
                    // the wait cannot return before the frame actually fits.
                    self.write_buffer.queue.waitForSpace(frame_size) catch |wait_err| switch (wait_err) {
                        error.Closed => return ConnectionError.ConnectionClosed,
                        error.Canceled => |e| return e,
                    };
                    continue;
                },
                else => |other| return other,
            };
        }
    }

    fn publishMsgAttempt(self: *Self, msg: *Message, reply_override: ?[]const u8, frame_size: *usize) !void {
        if (self.drain_state.load(.acquire) == .draining_pubs) {
            return error.DrainInProgress;
        }

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        var headers_buffer: std.Io.Writer.Allocating = .init(allocator);
        defer headers_buffer.deinit();

        try msg.encodeHeaders(&headers_buffer.writer);
        const headers = headers_buffer.written();
        const headers_len = headers.len;

        const total_payload = headers_len + msg.data.len;

        if (self.server_info.max_payload > 0 and total_payload > @as(usize, @intCast(self.server_info.max_payload))) {
            return PublishError.MaxPayload;
        }

        const reply_to_use = reply_override orelse msg.reply;

        // Build control line + headers (without copying msg.data)
        const control_buf = try allocator.alloc(u8, MAX_CONTROL_LINE_SIZE + headers_len);
        defer allocator.free(control_buf);

        var buffer_writer = std.Io.Writer.fixed(control_buf);

        if (headers_len > 0) {
            // HPUB <subject> [reply] <headers_len> <total_len>\r\n<headers>
            if (reply_to_use) |reply| {
                try buffer_writer.print("HPUB {s} {s} {d} {d}\r\n", .{ msg.subject, reply, headers_len, total_payload });
            } else {
                try buffer_writer.print("HPUB {s} {d} {d}\r\n", .{ msg.subject, headers_len, total_payload });
            }
            try buffer_writer.writeAll(headers);
        } else {
            // PUB <subject> [reply] <size>\r\n
            if (reply_to_use) |reply| {
                try buffer_writer.print("PUB {s} {s} {d}\r\n", .{ msg.subject, reply, msg.data.len });
            } else {
                try buffer_writer.print("PUB {s} {d}\r\n", .{ msg.subject, msg.data.len });
            }
        }

        // Allow publishes when connected or reconnecting (buffered).
        // Reject when not usable for sending.
        switch (self.status) {
            .connected, .reconnecting => {},
            else => {
                return ConnectionError.ConnectionClosed;
            },
        }

        // Append control+headers, data, and trailer without copying msg.data
        const slices = &[_][]const u8{ buffer_writer.buffered(), msg.data, "\r\n" };
        frame_size.* = buffer_writer.buffered().len + msg.data.len + 2;

        // Published messages go to pending_buffer during reconnection, otherwise write_buffer
        if (self.status == .reconnecting and self.options.reconnect.allow_reconnect) {
            try self.pending_buffer.appendMany(slices);
        } else {
            try self.write_buffer.appendMany(slices);
        }

        if (reply_to_use) |reply| {
            log.debug("Published message to {s} with reply {s}", .{ msg.subject, reply });
        } else {
            log.debug("Published message to {s}", .{msg.subject});
        }
    }

    fn subscribeInternal(self: *Self, sub: *Subscription) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        try self.subs_mutex.lock(self.io);
        defer self.subs_mutex.unlock(self.io);

        try self.subscriptions.put(sub.sid, sub);
        errdefer _ = self.subscriptions.remove(sub.sid);

        // Send SUB command via buffer
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        var buffer: std.Io.Writer.Allocating = .init(allocator);
        defer buffer.deinit();
        if (sub.queue) |group| {
            try buffer.writer.print("SUB {s} {s} {d}\r\n", .{ sub.subject, group, sub.sid });
        } else {
            try buffer.writer.print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
        }
        try self.write_buffer.appendUnmetered(buffer.written());
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
        var writer = std.Io.Writer.fixed(&buffer);

        if (max) |m| {
            writer.print("UNSUB {d} {d}\r\n", .{ sid, m }) catch unreachable; // Will always fit
        } else {
            writer.print("UNSUB {d}\r\n", .{sid}) catch unreachable; // Will always fit
        }

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        // Route like publishes: while reconnecting the command goes to
        // pending_buffer so it survives the connection switch (the live
        // write buffer is per-connection) and is applied after the
        // restored subscriptions.
        if (self.status == .reconnecting and self.options.reconnect.allow_reconnect) {
            try self.pending_buffer.append(writer.buffered());
        } else {
            try self.write_buffer.appendUnmetered(writer.buffered());
        }
    }

    pub fn unsubscribe(self: *Self, sub: *Subscription) void {
        // Remove from subscription table first
        {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            self.subs_mutex.lockUncancelable(self.io);
            defer self.subs_mutex.unlock(self.io);

            if (!self.subscriptions.remove(sub.sid)) {
                // Nothing to do, already unsubscribed
                return;
            }
        }

        // Try to send UNSUB command. Even if it fails internally,
        // processMsg will keep sending UNSUB commands once
        // it receives a message with unknown sid.
        self.unsubscribeInternal(sub.sid, null) catch |err| {
            if (err == error.Canceled) {
                // This is a void-returning cleanup path; re-arm the
                // cancelation so the caller's next cancelation point
                // still observes it.
                self.io.recancel();
            } else {
                log.err("Failed to send UNSUB for sid {d}: {}", .{ sub.sid, err });
            }
        };

        log.debug("Unsubscribed from {s} with sid {d}", .{ sub.subject, sub.sid });

        // Release connection's reference to the subscription
        sub.release();
    }

    /// Remove subscription from connection's subscription table
    /// This does not send UNSUB to server - that should be done separately
    pub fn removeSubscriptionInternal(self: *Self, sid: u64) void {
        self.subs_mutex.lockUncancelable(self.io);
        defer self.subs_mutex.unlock(self.io);

        if (self.subscriptions.fetchRemove(sid)) |kv| {
            log.debug("Removed subscription {d} ({s}) from connection", .{ sid, kv.value.subject });
            // Release connection's reference to the subscription
            kv.value.release();
        }
    }

    pub fn flush(self: *Self) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        while (self.status != .connected) {
            if (self.status == .closed) {
                log.debug("Flush skipped, no longer connected", .{});
                return error.ConnectionClosed;
            }
            try self.status_cond.wait(self.io, &self.mutex);
        }

        const our_ping_id = try self.sendPing(false);

        log.debug("Sent PING with ping_id={}, waiting for PONG", .{our_ping_id});

        const deadline = io_util.deadline(self.io, self.options.timeout);

        while (self.incoming_pongs < our_ping_id) {
            if (self.status != .connected) {
                log.debug("Flush interrupted, no longer connected", .{});
                return ConnectionError.ConnectionClosed;
            }

            if (io_util.expired(self.io, deadline)) {
                log.warn("Flush timeout waiting for PONG", .{});
                return ConnectionError.Timeout;
            }

            self.pong_condition.waitTimeout(self.io, &self.mutex, deadline) catch |err| switch (err) {
                error.Canceled => |e| return e,
                error.Timeout => {}, // Continue loop to check conditions
            };
        }

        log.debug("Flush completed, received PONG for ping_id={}", .{our_ping_id});
    }

    pub fn request(self: *Self, subject: []const u8, data: []const u8, timeout: Io.Timeout) !*Message {
        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined,
        };
        return self.requestMsg(&msg, timeout);
    }

    pub fn requestMsg(self: *Self, msg: *Message, timeout: Io.Timeout) !*Message {
        if (self.options.trace) {
            log.debug("Sending request message to {s} with timeout {any}", .{ msg.subject, timeout });
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
        const reply_msg = try self.response_manager.waitForResponse(handle, timeout);

        // Check for "no responders" like C library
        if (reply_msg.isNoResponders()) {
            reply_msg.deinit();
            return ConnectionError.NoResponders;
        }

        return reply_msg;
    }

    pub const RequestManyOptions = ResponseManager.WaitForMultiResponseOptions;

    pub fn requestMany(self: *Self, subject: []const u8, data: []const u8, timeout: Io.Timeout, options: RequestManyOptions) !MessageList {
        var msg = Message{
            .subject = subject,
            .data = data,
            .pool = null,
            .arena = undefined,
        };
        return self.requestManyMsg(&msg, timeout, options);
    }

    pub fn requestManyMsg(self: *Self, msg: *Message, timeout: Io.Timeout, options: RequestManyOptions) !MessageList {
        if (self.options.trace) {
            log.debug("Sending request-many message to {s} with timeout {any}", .{ msg.subject, timeout });
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
        const messages = try self.response_manager.waitForMultiResponse(handle, timeout, options);

        if (self.options.trace) {
            log.debug("Received {} responses for request-many to {s}", .{ messages.len, msg.subject });
        }

        return messages;
    }

    /// Group task wrapper: `Io.Group` task return types must coerce to
    /// `Cancelable!void`, so any other error is logged here.
    fn managerTask(self: *Self) Io.Cancelable!void {
        self.managerLoop() catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.err("Manager loop failed: {}", .{err});
        };
    }

    fn managerLoop(self: *Self) anyerror!void {
        log.debug("Manager loop started", .{});
        defer log.debug("Manager loop exited", .{});

        defer {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.status != .closed) {
                self.status = .connection_failed;
                self.status_cond.broadcast(self.io);
            }
        }

        var attempts: u32 = 0;

        while (true) {
            var conn_err: ?anyerror = null;
            self.runConnection(&attempts) catch |err| {
                if (err == error.Canceled) {
                    return err;
                }
                if (err == error.ShouldClose) {
                    // Deliberate give-up (e.g. repeated auth failures):
                    // terminal, the disconnected callback already fired
                    // when reconnection started.
                    self.failTerminally(false);
                    return;
                }
                conn_err = err;
            };

            const pool_exhausted = if (conn_err) |err| err == error.NoServerAvailable else false;

            var callback: @TypeOf(self.options.callbacks.disconnected_cb) = null;

            {
                try self.mutex.lock(self.io);
                defer self.mutex.unlock(self.io);

                if (conn_err) |err| {
                    log.info("Connection failed: {}", .{err});
                } else {
                    log.info("Disconnected", .{});
                }

                if (self.status == .connecting) {
                    // Initial connection: try each server in the pool once
                    // (selectNextServer rotates through it), then report the
                    // failure through connect(). No callbacks fire here; the
                    // connection was never established.
                    attempts += 1;
                    if (pool_exhausted or attempts >= self.server_pool.getSize()) {
                        return;
                    }
                    continue;
                }

                if (pool_exhausted or !self.options.reconnect.allow_reconnect) {
                    // Terminal; handled outside the mutex below.
                } else {
                    self.status = .reconnecting;
                    self.status_cond.broadcast(self.io);

                    // Discard whatever was buffered for the dead socket. This
                    // also wakes publishers blocked in waitForSpace, so they
                    // re-check the status and reroute into pending_buffer
                    // instead of waiting out the whole reconnect. The reader
                    // and flusher tasks are already joined at this point.
                    self.write_buffer.reset();

                    if (self.options.callbacks.disconnected_cb) |cb| {
                        callback = cb;
                    }
                }
            }

            if (pool_exhausted) {
                // Every server exceeded max_reconnect and was removed from
                // the pool. The disconnected callback fired when
                // reconnection started.
                log.warn("No servers left to reconnect to, giving up", .{});
                self.failTerminally(false);
                return;
            }
            if (!self.options.reconnect.allow_reconnect) {
                // Fresh connection loss with reconnection disabled: report
                // both the disconnect and the final closed state.
                log.info("Reconnection is disabled, closing connection", .{});
                self.failTerminally(true);
                return;
            }

            if (callback) |cb| cb(self);

            // Wait before the next attempt; the first retry after losing an
            // established connection is immediate.
            attempts += 1;
            if (attempts > 1) {
                const delay = self.calculateReconnectDelay(attempts - 1);
                log.debug("Waiting {f} before reconnection attempt {}", .{ delay, attempts });
                try self.io.sleep(delay, .awake);
            }
        }
    }

    /// Take the connection to the terminal connection_failed state: no
    /// further reconnection attempts will be made. Closes the buffers so
    /// blocked publishers and drain waiters wake up, and fires the
    /// disconnected (optionally) and closed callbacks, unless the user
    /// already closed the connection.
    fn failTerminally(self: *Self, fire_disconnected: bool) void {
        var disconnected_cb: @TypeOf(self.options.callbacks.disconnected_cb) = null;
        var closed_cb: @TypeOf(self.options.callbacks.closed_cb) = null;

        {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.status == .closed) return;

            self.status = .connection_failed;
            self.status_cond.broadcast(self.io);

            if (fire_disconnected) {
                disconnected_cb = self.options.callbacks.disconnected_cb;
            }
            closed_cb = self.options.callbacks.closed_cb;
        }

        // Wake anything blocked on the buffers (publishers, flush waiters).
        self.pending_buffer.close();
        self.write_buffer.close();

        if (disconnected_cb) |cb| cb(self);
        if (closed_cb) |cb| cb(self);
    }

    fn selectNextServer(self: *Self) !*Server {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        const server = try self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) orelse return error.NoServerAvailable;
        server.reconnects += 1;
        self.current_server = server;
        return server;
    }

    fn establishConnection(self: *Self, server: *Server) !net.Stream {
        log.debug("Connecting to server: {s}:{d} ({d} retries)", .{ server.parsed_url.host, server.parsed_url.port, server.reconnects });
        const stream = try net_util.tcpConnectToHost(self.io, server.parsed_url.host, server.parsed_url.port);
        errdefer stream.close(self.io);

        try net_util.setKeepAlive(stream.socket, true);

        if (self.options.trace) {
            log.debug("Connected, starting handshake...", .{});
        }

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        std.debug.assert(self.status == .connecting or self.status == .reconnecting);

        // Setup connection state. The reconnect counter is deliberately not
        // reset here: a server that accepts TCP but fails the NATS handshake
        // must still count against max_reconnect (the counter is reset after
        // a successful handshake).
        server.did_connect = true;

        // Reset parser for clean state
        self.parser.reset();

        // Discard whatever the previous connection left unsent: the head may
        // even be the tail of a partially written frame (the flusher consumes
        // byte-wise), so replaying it would corrupt the new protocol stream,
        // and any stale complete frames would precede our CONNECT. Dropping
        // unflushed frames on connection loss matches the at-most-once
        // semantics of the official clients. Publishes made while
        // reconnecting are preserved separately in pending_buffer.
        self.write_buffer.reset();

        // Reset ping/pong counters for fresh connection
        self.outgoing_pings = 0;
        self.incoming_pongs = 0;
        self.pings_out.store(0, .monotonic);
        self.ping_time = io_util.now(self.io);

        // Initialize handshake state
        self.handshake_state = .waiting_for_info;
        self.handshake_error = null;
        self.handshake_cond.broadcast(self.io);

        return stream;
    }

    fn runConnection(self: *Self, attempts: *u32) !void {
        const server = try self.selectNextServer();

        var stream = try self.establishConnection(server);
        defer stream.close(self.io);

        var exit_signal: ExitSignal = .{};

        {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            self.exit_signal = &exit_signal;
        }
        defer {
            // Runs after both tasks below are joined, so nothing can signal
            // through a stale pointer.
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.exit_signal = null;
        }

        var reader_task = try self.io.concurrent(readerLoop, .{ self, &stream, &exit_signal });
        defer reader_task.cancel(self.io);

        var flusher_task = try self.io.concurrent(flusherLoop, .{ self, &stream, &exit_signal });
        defer flusher_task.cancel(self.io);

        var was_reconnect = false;

        try self.mutex.lock(self.io);
        self.waitForHandshakeCompletion() catch |err| {
            // Track authentication errors per server: a server that rejects
            // the same credentials twice in a row will keep rejecting them,
            // so stop burning reconnect attempts on it (nats.go behavior).
            if (isAuthError(err)) {
                if (server.last_auth_error) |last| {
                    if (last == err) {
                        self.mutex.unlock(self.io);
                        log.err("Same authentication error twice from {s}, giving up: {}", .{ server.parsed_url.full_url, err });
                        return error.ShouldClose;
                    }
                }
                server.last_auth_error = err;
            }
            self.mutex.unlock(self.io);
            return err;
        };
        server.last_auth_error = null;
        server.reconnects = 0;
        if (self.status == .reconnecting) {
            was_reconnect = true;
        }
        self.mutex.unlock(self.io);

        // Restore subscriptions while the status is still not `connected`:
        // publishes keep going to pending_buffer, so the server sees our
        // interest (including the request/reply inbox) before any buffered
        // publish. Only then move the pending commands into the live buffer
        // and flip the status - atomically with respect to publishers, so a
        // new publish can never overtake a buffered one.
        try self.resendSubscriptions();

        {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            try self.pending_buffer.moveToBuffer(&self.write_buffer);
            self.status = .connected;
            self.status_cond.broadcast(self.io);
        }

        attempts.* = 0;

        log.info("Connected successfully to {s}", .{server.parsed_url.full_url});

        // Invoke reconnected callback last, so it observes a connection with
        // subscriptions restored and buffered publishes already queued ahead
        // of anything it sends (the flusher writes them out asynchronously).
        if (was_reconnect) {
            if (self.options.callbacks.reconnected_cb) |cb| {
                cb(self);
            }
        }

        // Wait for the connection to end, waking up on the keep-alive
        // interval to send PINGs. The manager task is otherwise idle while
        // the connection is up, so it doubles as the heartbeat timer; this
        // is the only path that sends keep-alive PINGs, so an idle or
        // half-open connection is detected even when nothing is received.
        if (self.options.ping_interval.nanoseconds == 0) {
            try exit_signal.event.wait(self.io);
        } else {
            while (true) {
                exit_signal.event.waitTimeout(self.io, .{ .duration = .{ .raw = self.options.ping_interval, .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {
                        try self.checkAndSendPing();
                        continue;
                    },
                    error.Canceled => |e| return e,
                };
                break;
            }
        }

        switch (exit_signal.reason.load(.acquire)) {
            .none => unreachable, // the event is only set by signal()
            .reader_done => {
                if (exit_signal.err) |err| {
                    log.err("Error in reader loop: {}", .{err});
                    return err;
                }
                return;
            },
            .flusher_done => {
                if (exit_signal.err) |err| {
                    log.err("Error in flusher loop: {}", .{err});
                    return err;
                }
                return;
            },
            .reconnect_requested => {
                log.info("Reconnect requested", .{});
            },
        }
    }

    fn readerLoop(self: *Self, stream: *net.Stream, exit: *ExitSignal) void {
        if (self.runReader(stream)) |_| {
            exit.signal(self.io, .reader_done, null);
        } else |err| {
            exit.signal(self.io, .reader_done, err);
        }
    }

    fn runReader(self: *Self, stream: *net.Stream) !void {
        log.debug("Reader loop started", .{});
        defer {
            log.debug("Reader loop exited", .{});
        }

        var buffer: [4096]u8 = undefined;
        var stream_reader = stream.reader(self.io, &buffer);
        const reader = &stream_reader.interface;

        while (true) {
            reader.fillMore() catch |err| switch (err) {
                error.EndOfStream => {
                    log.debug("Connection closed by server (EOF)", .{});
                    break;
                },
                // The `Io.Reader` interface erases the real error; it is
                // stashed on the stream reader (including cancellation).
                error.ReadFailed => return stream_reader.err.?,
            };

            const data = reader.buffered();
            if (data.len == 0) continue;

            log.debug("Read {} bytes: {s}", .{ data.len, data });
            try self.parser.parse(self, data);
            reader.toss(data.len);
        }
    }

    fn flusherLoop(self: *Self, stream: *net.Stream, exit: *ExitSignal) void {
        if (self.runFlusher(stream)) |_| {
            exit.signal(self.io, .flusher_done, null);
        } else |err| {
            exit.signal(self.io, .flusher_done, err);
        }
    }

    fn runFlusher(self: *Self, stream: *net.Stream) !void {
        log.debug("Flusher loop started", .{});
        defer {
            log.debug("Flusher loop stopped", .{});
        }

        // Unbuffered: every writeVec goes straight to the socket.
        var stream_writer = stream.writer(self.io, &.{});

        while (true) {
            try self.flusherIteration(&stream_writer);

            if (self.drain_state.load(.acquire) == .draining_pubs) {
                try self.sendDrainPing();
            }
        }
    }

    fn flusherIteration(self: *Self, stream_writer: *net.Stream.Writer) !void {
        // Try to gather data from buffer first
        var slices: [16][]const u8 = undefined;
        const gather = self.write_buffer.gatherReadSlices(&slices, self.options.timeout) catch |err| switch (err) {
            error.Timeout => {
                // No data to write
                return;
            },
            error.Closed => return error.Closed,
            error.Canceled => return error.Canceled,
        };

        if (gather.slices.len == 0) {
            // No data to write
            return;
        }

        // The `Io.Writer` interface erases the real error; it is stashed on
        // the stream writer (including cancellation). Partial writes are
        // fine, whatever was written is consumed from the buffer.
        const bytes_written = stream_writer.interface.writeVec(gather.slices) catch {
            return stream_writer.err.?;
        };
        try gather.consume(bytes_written);
    }

    /// Count a dropped message and report the start of a slow-consumer
    /// episode (once per episode, like nats.go). Called from processMsg
    /// with no locks held and the subscription retained.
    fn handleSlowConsumer(self: *Self, sub: *Subscription) void {
        const total_dropped = sub.dropped_msgs.fetchAdd(1, .monotonic) + 1;
        if (sub.slow_consumer.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
            // Arm the in-band error once when a synchronous subscription
            // enters a slow-consumer episode. Further drops in the same
            // episode must not prevent the receiver from draining queued
            // messages; a successful enqueue ends the episode below.
            if (sub.handler == null) {
                sub.sc_error_pending.store(true, .release);
            }
            log.warn("Slow consumer, messages dropped for subscription {d} ({s})", .{ sub.sid, sub.subject });
            if (self.options.callbacks.slow_consumer_cb) |cb| {
                cb(self, sub, total_dropped);
            }
        }
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, message: *Message) !void {
        var owns_message = true;
        defer if (owns_message) message.deinit();

        // Retain subscription while holding lock, then release lock
        try self.subs_mutex.lock(self.io);
        const sub = self.subscriptions.get(message.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock(self.io);

        if (sub) |s| {
            defer s.release(); // Release when done

            // Check if subscription is draining - drop message if so
            if (s.isDraining()) {
                log.debug("Dropping message for draining subscription {d}", .{message.sid});
                return;
            }

            // Slow-consumer protection: drop the message when a pending
            // limit is exceeded. Limits are measured on message counts and
            // payload bytes, never on queue storage.
            const msgs_limit = s.pending_msgs_limit.load(.acquire);
            const bytes_limit = s.pending_bytes_limit.load(.acquire);
            if ((msgs_limit > 0 and s.pending_msgs.load(.acquire) >= msgs_limit) or
                (bytes_limit > 0 and s.pending_bytes.load(.acquire) + message.data.len > bytes_limit))
            {
                self.handleSlowConsumer(s);
                return;
            }

            // Increment pending message count and bytes for this subscription
            subscription_mod.incrementPending(s, message.data.len);

            // Log before consuming message (to avoid use-after-free)
            log.debug("Delivering message to subscription {d}: {s}", .{ message.sid, message.data });

            // Queue message for subscription (both sync and async use the queue)
            // For async subscriptions, the handler fiber will pick it up
            s.messages.push(message) catch |err| {
                switch (err) {
                    error.Closed => {
                        // Queue is closed; drop gracefully.
                        log.debug("Queue closed for sid {d}; dropping message", .{message.sid});
                        // Undo the pending counters since queue is closed
                        subscription_mod.decrementPending(s, message.data.len);
                        return;
                    },
                    error.Canceled => return error.Canceled,
                    else => {
                        // Real allocation failure: this remains a connection
                        // error (unlike a configured pending-limit drop).
                        log.err("Failed to enqueue message for sid {d}: {}", .{ message.sid, err });
                        subscription_mod.decrementPending(s, message.data.len);
                        return err;
                    },
                }
            };
            owns_message = false;
            // A successful delivery ends a slow-consumer episode.
            s.slow_consumer.store(false, .release);
        } else {
            // No sub subscription found, try to send UNSUB command
            self.unsubscribeInternal(message.sid, null) catch |err| {
                if (err == error.Canceled) return err;
                log.err("Failed to send UNSUB for unknown sid {d}: {}", .{ message.sid, err });
            };
        }
    }

    /// Sends CONNECT and PING during handshake (assumes mutex is held)
    fn sendConnectAndPing(self: *Self) !void {
        const allocator = self.scratch.allocator();
        defer self.resetScratch();

        // Build CONNECT message with all options
        var buffer: std.Io.Writer.Allocating = .init(allocator);
        defer buffer.deinit();

        // Calculate effective no_responders: enable if server supports headers
        const no_responders = self.options.no_responders and self.server_info.headers;

        // Get client name from options or use default
        const client_name = self.options.name orelse build_options.name;

        // Determine credentials with the same precedence as the C client:
        // the current server's URL first, then the connection options, then
        // credentials saved from the first explicit URL in the server pool
        // (for implicitly discovered servers). A username without a password
        // is treated as a token.
        var user: ?[]const u8 = null;
        var password: ?[]const u8 = null;
        var auth_token: ?[]const u8 = null;

        if (self.current_server) |server| {
            user = server.parsed_url.username;
            password = server.parsed_url.password;
        }
        if (user != null and password == null) {
            auth_token = user;
            user = null;
        }
        if (user == null and auth_token == null) {
            user = self.options.user;
            password = self.options.password;
            // Dynamic token handler takes precedence over the static token
            auth_token = if (self.options.token_handler) |handler|
                handler()
            else
                self.options.token;

            if (user == null and auth_token == null) {
                user = self.server_pool.default_user;
                password = self.server_pool.default_pwd;
                if (user != null and password == null) {
                    auth_token = user;
                    user = null;
                }
            }
        }

        // JWT and NKey authentication. Credentials files are re-read on
        // every handshake so rotated credentials are picked up across
        // reconnects. With a JWT the public key is omitted from CONNECT;
        // the server takes it from the JWT itself.
        var nkey: ?[]const u8 = null;
        var sig: ?[]const u8 = null;
        var jwt: ?[]const u8 = null;
        var signing_seed: ?[]const u8 = null;

        var creds_content: ?[]u8 = null;
        defer if (creds_content) |content| std.crypto.secureZero(u8, content);

        if (self.options.user_creds) |path| {
            const content = try Io.Dir.cwd().readFileAlloc(self.io, path, allocator, .limited(max_creds_file_size));
            creds_content = content;
            const credentials = try creds_mod.parse(content);
            jwt = credentials.jwt;
            signing_seed = credentials.seed;
        } else if (self.options.user_jwt) |user_jwt| {
            jwt = user_jwt;
            signing_seed = self.options.nkey_seed;
            if (signing_seed == null and self.options.nkey_sign_cb == null) return error.MissingNKeySeed;
        } else if (self.options.nkey_seed) |seed| {
            signing_seed = seed;
        }

        if (self.server_info.nonce) |nonce| {
            var raw_sig: ?[64]u8 = null;
            if (signing_seed) |seed| {
                var seed_kp = try nkeys.SeedKeyPair.fromSeed(seed);
                defer seed_kp.wipe();

                if (jwt == null) {
                    const nkey_buf = try allocator.create([nkeys.public_key_text_len]u8);
                    nkey = seed_kp.publicKeyText(nkey_buf);
                }
                raw_sig = try seed_kp.sign(nonce);
            } else if (self.options.nkey_sign_cb) |sign_cb| {
                if (jwt == null) {
                    nkey = self.options.nkey orelse return error.MissingNKey;
                }
                raw_sig = try sign_cb(nonce);
            }

            if (raw_sig) |*signature| {
                const Base64Encoder = std.base64.url_safe_no_pad.Encoder;
                const sig_buf = try allocator.alloc(u8, Base64Encoder.calcSize(signature.len));
                sig = Base64Encoder.encode(sig_buf, signature);
            }
        }

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
            .user = user,
            .pass = password,
            .auth_token = auth_token,
            .nkey = nkey,
            .sig = sig,
            .jwt = jwt,
        };

        try buffer.writer.writeAll("CONNECT ");
        try buffer.writer.print("{f}", .{std.json.fmt(connect_obj, .{ .emit_null_optional_fields = false })});
        try buffer.writer.writeAll("\r\n");
        try buffer.writer.writeAll("PING\r\n");

        // Send via buffer (mutex already held)
        try self.write_buffer.appendUnmetered(buffer.written());

        log.debug("Sent CONNECT+PING during handshake", .{});
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        log.debug("Received INFO: {s}", .{info_json});

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

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
                if (err == error.Canceled) return err;
                log.err("Failed to send CONNECT+PING: {}", .{err});
                self.handshake_error = err;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast(self.io);
                return;
            };

            self.handshake_state = .waiting_for_pong;
            self.handshake_cond.broadcast(self.io); // Signal state change
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
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

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

    /// Whether an error reported by the server means the credentials were
    /// rejected (as opposed to a transient or protocol problem).
    fn isAuthError(err: anyerror) bool {
        return switch (err) {
            ProtocolError.AuthorizationViolation,
            ProtocolError.AuthExpired,
            ProtocolError.AuthRevoked,
            ProtocolError.AccountAuthExpired,
            => true,
            else => false,
        };
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
        // Call the callback outside of mutex, if provided
        var callback: @TypeOf(self.options.callbacks.error_cb) = null;
        defer if (callback) |cb| cb(self, err_msg);

        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        // Parse the protocol error once
        const protocol_err = parseProtocolError(err_msg, self.allocator);

        log.err("Server protocol error: {} - {s}", .{ protocol_err, err_msg });

        // Handle handshake failure
        if (self.handshake_state.isWaiting()) {
            // Propagate specific protocol errors to client
            self.handshake_error = protocol_err;
            self.handshake_state = .failed;
            self.handshake_cond.broadcast(self.io); // Signal handshake failure
            log.debug("Handshake failed: {}", .{protocol_err});
            // During reconnection there is no caller waiting on connect()
            // to see the error, so report it through the error callback
            // (in the defer, outside the mutex).
            if (self.status == .reconnecting) {
                callback = self.options.callbacks.error_cb;
            }
            return;
        }

        // Invoke error callback (in defer outside of mutex)
        if (self.options.callbacks.error_cb) |cb| {
            callback = cb;
        }
    }

    fn sendPing(self: *Self, comptime lock: bool) !u64 {
        try self.write_buffer.appendUnmetered("PING\r\n");

        if (lock) try self.mutex.lock(self.io);
        defer if (lock) self.mutex.unlock(self.io);

        self.outgoing_pings += 1;
        return self.outgoing_pings;
    }

    fn checkAndSendPing(self: *Self) !void {
        if (self.options.ping_interval.nanoseconds == 0) return;

        const current_time = io_util.now(self.io);
        if (self.ping_time.durationTo(current_time).nanoseconds >= self.options.ping_interval.nanoseconds) {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            // Count the PING as outstanding before it can be written:
            // processPong resets the counter under the same mutex, so the
            // PONG can never be processed ahead of the increment. Checking
            // first also avoids queueing another PING onto a connection
            // already deemed stale.
            const current_pings = self.pings_out.fetchAdd(1, .monotonic) + 1;
            if (self.options.max_pings_out > 0 and current_pings > self.options.max_pings_out) {
                log.warn("Stale connection: {} unanswered PINGs", .{current_pings});
                return error.StaleConnection;
            }
            _ = try self.sendPing(false);
            self.ping_time = current_time;
        }
    }

    pub fn processPong(self: *Self) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        // Handle handshake completion
        if (self.handshake_state == .waiting_for_pong) {
            self.handshake_state = .completed;
            self.handshake_cond.broadcast(self.io); // Signal handshake completion
            log.debug("Handshake completed successfully", .{});
            return;
        }

        // Regular PONG handling for flush() calls
        self.incoming_pongs += 1;
        self.pong_condition.broadcast(self.io);

        log.debug("Received PONG for ping_id={}", .{self.incoming_pongs});

        // Reset keep-alive ping counter - ANY PONG proves connection is alive
        self.pings_out.store(0, .monotonic);

        if (self.drain_ping_id == self.incoming_pongs) {
            try self.notifyPublishDrainComplete();
        }
    }

    pub fn processPing(self: *Self) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        try self.write_buffer.appendUnmetered("PONG\r\n");
    }

    fn calculateReconnectDelay(self: *Self, attempts: u32) Io.Duration {
        if (self.options.reconnect.custom_reconnect_delay_cb) |callback| {
            return callback(attempts);
        }

        var delay = self.options.reconnect.reconnect_wait;
        const jitter = self.options.reconnect.reconnect_jitter;

        if (jitter.nanoseconds > 0) {
            var rng = std.Random.DefaultPrng.init(@intCast(io_util.now(self.io).nanoseconds));
            const random_jitter = rng.random().uintLessThan(u64, @intCast(jitter.nanoseconds));
            delay.nanoseconds += random_jitter;
        }

        return delay;
    }

    fn resendSubscriptions(self: *Self) !void {
        log.debug("Re-establishing subscriptions", .{});

        // Track SIDs that shouldn't be re-subscribed and must be removed
        var to_remove = ArrayList(u64).empty;
        defer to_remove.deinit(self.allocator);

        // Use a local arena rather than the shared scratch arena: scratch is
        // guarded by the connection mutex, which is not held here, and
        // publishes may use it concurrently during reconnection.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        {
            self.subs_mutex.lockUncancelable(self.io);
            defer self.subs_mutex.unlock(self.io);

            var buffer: std.Io.Writer.Allocating = .init(arena.allocator());
            defer buffer.deinit();

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
                    try buffer.writer.print("SUB {s} {s} {d}\r\n", .{ sub.subject, queue, sub.sid });
                } else {
                    try buffer.writer.print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });
                }

                // Send UNSUB with remaining limit if needed
                if (adjusted_max) |remaining| {
                    try buffer.writer.print("UNSUB {d} {d}\r\n", .{ sub.sid, remaining });
                    log.debug("Re-subscribed to {s} with sid {d} and autounsubscribe limit {d} (delivered: {d})", .{ sub.subject, sub.sid, remaining, delivered });
                } else {
                    log.debug("Re-subscribed to {s} with sid {d}", .{ sub.subject, sub.sid });
                }
            }

            // Send all subscription commands via write buffer
            if (buffer.written().len > 0) {
                try self.write_buffer.appendUnmetered(buffer.written());
            }
        }

        // Now remove stale subs outside the subs_mutex
        for (to_remove.items) |sid| {
            self.removeSubscriptionInternal(sid);
        }
    }

    /// Waits for handshake completion with timeout (assumes mutex is held)
    /// Returns error if handshake fails or times out
    fn waitForHandshakeCompletion(self: *Self) !void {
        const deadline = io_util.deadline(self.io, self.options.timeout);

        while (!self.handshake_state.isFinished()) {
            log.debug("Handshake state: {}", .{self.handshake_state});

            if (io_util.expired(self.io, deadline)) {
                log.err("Handshake timeout", .{});
                self.handshake_error = ConnectionError.Timeout;
                self.handshake_state = .failed;
                self.handshake_cond.broadcast(self.io); // Signal the state change
                break;
            }

            self.handshake_cond.waitTimeout(self.io, &self.mutex, deadline) catch |err| switch (err) {
                error.Canceled => |e| return e,
                error.Timeout => {}, // Continue loop to check conditions
            };
        }

        // Return the handshake error if it failed, or void if successful
        if (self.handshake_state == .completed) {
            return;
        } else {
            return self.handshake_error orelse ConnectionError.ConnectionFailed;
        }
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
        self.subs_mutex.lockUncancelable(self.io);
        var iter = self.subscriptions.valueIterator();
        while (iter.next()) |sub_ptr| {
            const sub = sub_ptr.*;
            _ = self.drain_subscription_count.fetchAdd(1, .release);
            sub.drain(); // Drain the subscription
        }
        self.subs_mutex.unlock(self.io);

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

    /// Wait for the connection drain to finish.
    pub fn waitForDrainCompletion(self: *Self, timeout: Io.Timeout) !void {
        const state = self.drain_state.load(.acquire);
        switch (state) {
            .not_draining => return error.NotDraining,
            .drain_complete => return,
            else => {},
        }

        if (timeout == .none) {
            try self.drain_completion.wait(self.io);
        } else {
            try self.drain_completion.waitTimeout(self.io, timeout);
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

    fn sendDrainPing(self: *Self) Io.Cancelable!void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.drain_ping_id > 0) return; // Already sent the last ping

        self.drain_ping_id = self.sendPing(false) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            log.err("Failed to send drain ping: {}", .{err});
            return;
        };
    }

    fn notifyPublishDrainComplete(self: *Self) !void {
        const prev_state = self.drain_state.cmpxchgStrong(.draining_pubs, .drain_complete, .acq_rel, .acquire);
        if (prev_state != null) return; // Already completed

        self.drain_completion.set(self.io);

        return error.ShouldClose;
    }

    fn startPublicationDrain(self: *Self) void {
        const prev_state = self.drain_state.cmpxchgStrong(.draining_subs, .draining_pubs, .acq_rel, .acquire);
        if (prev_state != null) return; // Already draining pubs

        self.sendDrainPing() catch |err| switch (err) {
            // This is a void-returning path; re-arm the cancelation so the
            // caller's next cancelation point still observes it. The drain
            // ping is retried by the flusher loop while draining pubs.
            error.Canceled => self.io.recancel(),
        };
    }
};
