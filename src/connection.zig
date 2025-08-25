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
const dispatcher_mod = @import("dispatcher.zig");
const DispatcherPool = dispatcher_mod.DispatcherPool;
const server_pool_mod = @import("server_pool.zig");
const ServerPool = server_pool_mod.ServerPool;
const Server = server_pool_mod.Server;
const net_utils = @import("net_utils.zig");
const jetstream_mod = @import("jetstream.zig");
const JetStream = jetstream_mod.JetStream;
const JetStreamOptions = jetstream_mod.JetStreamOptions;
const build_options = @import("build_options");
const ConcurrentWriteBuffer = @import("queue.zig").ConcurrentWriteBuffer;
const ResponseManager = @import("response_manager.zig").ResponseManager;

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

pub const PendingBuffer = struct {
    buffer: ArrayList(u8),
    max_size: usize,

    pub fn init(allocator: Allocator, max_size: usize) PendingBuffer {
        return PendingBuffer{
            .buffer = ArrayList(u8).init(allocator),
            .max_size = max_size,
        };
    }

    pub fn deinit(self: *PendingBuffer) void {
        self.buffer.deinit();
    }

    pub fn addMessage(self: *PendingBuffer, data: []const u8) !void {
        if (self.buffer.items.len + data.len > self.max_size) {
            return ConnectionError.OutOfMemory;
        }
        try self.buffer.appendSlice(data);
    }

    pub fn flush(self: *PendingBuffer, stream: net.Stream) !void {
        if (self.buffer.items.len > 0) {
            try stream.writeAll(self.buffer.items);
            self.buffer.clearRetainingCapacity();
        }
    }

    pub fn clear(self: *PendingBuffer) void {
        self.buffer.clearRetainingCapacity();
    }
};

pub const ConnectionError = error{
    ConnectionFailed,
    ConnectionClosed,
    Timeout,
    InvalidUrl,
    AuthFailed,
    InvalidProtocol,
    OutOfMemory,
    NoResponders,
} || std.Thread.SpawnError || std.posix.WriteError || std.posix.ReadError;

pub const ConnectionStatus = enum {
    disconnected,
    connecting,
    connected,
    reconnecting,
    closed,
    draining_subs,
    draining_pubs,
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
};

pub const Connection = struct {
    allocator: Allocator,
    options: ConnectionOptions,

    // Network
    stream: ?net.Stream = null,
    status: ConnectionStatus = .disconnected,

    // Server management
    server_pool: ServerPool,
    current_server: ?*Server = null, // Track current server like C library
    server_info: ServerInfo = .{}, // Current server info from INFO message
    server_info_arena: std.heap.ArenaAllocator, // Arena for server_info strings

    // Reconnection state
    reconnect_thread: ?std.Thread = null,
    in_reconnect: i32 = 0, // Regular int like C library, protected by mutex
    abort_reconnect: bool = false, // Like C library's nc->ar flag, protected by mutex
    pending_buffer: PendingBuffer,

    // Reconnection coordination
    reconnect_condition: std.Thread.Condition = .{},

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

    // PONG waiting (protected by main mutex)
    pending_pongs: u32 = 0,
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
            .pending_buffer = PendingBuffer.init(allocator, options.reconnect.reconnect_buf_size),
            .write_buffer = WriteBuffer.init(allocator, .{}),
            .subscriptions = std.AutoHashMap(u64, *Subscription).init(allocator),
            .response_manager = ResponseManager.init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.close();

        // Release global dispatcher pool
        if (self.dispatcher_pool != null) {
            dispatcher_mod.releaseGlobalPool();
            self.dispatcher_pool = null;
        }

        // Clean up subscriptions
        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit();
        }
        self.subscriptions.deinit();

        // Clean up write buffer
        self.write_buffer.deinit();

        // Clean up response manager
        self.response_manager.deinit();

        // Clean up server pool and pending buffer
        self.server_pool.deinit();
        self.pending_buffer.deinit();

        // Clean up server info arena
        self.server_info_arena.deinit();

        self.parser.deinit();
    }

    /// Ensure dispatcher pool is initialized (lazy initialization)
    fn ensureDispatcherPool(self: *Self) !void {
        if (self.dispatcher_pool != null) return; // Already initialized

        self.dispatcher_pool = try dispatcher_mod.acquireGlobalPool(self.allocator);

        log.debug("Acquired global dispatcher pool", .{});
    }


    pub fn connect(self: *Self, url: []const u8) !void {
        if (self.status != .disconnected) {
            return ConnectionError.ConnectionFailed;
        }

        // Add server to pool if it's not already there
        if (self.server_pool.getSize() == 0) {
            try self.server_pool.addServer(url, false); // Explicit server
        }

        return self.connectToServer();
    }

    pub fn addServer(self: *Self, url_str: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.server_pool.addServer(url_str, false); // Explicit server
    }

    fn connectToServer(self: *Self) !void {
        // This is called from initial connect() - needs to manage its own mutex
        self.mutex.lock();

        self.status = .connecting;

        // Get server using C library's GetNextServer algorithm
        const selected_server = try self.server_pool.getNextServer(self.options.reconnect.max_reconnect, self.current_server) orelse {
            self.status = .disconnected;
            self.mutex.unlock();
            return ConnectionError.ConnectionFailed;
        };

        // Update current server and track reconnection attempt (like C library)
        self.current_server = selected_server;
        selected_server.reconnects += 1;

        self.mutex.unlock();

        // Establish TCP connection (outside mutex like C library)
        log.debug("Connecting to server: {s}:{d}", .{ selected_server.parsed_url.host, selected_server.parsed_url.port });
        const stream = net.tcpConnectToHost(self.allocator, selected_server.parsed_url.host, selected_server.parsed_url.port) catch |err| {
            self.mutex.lock();
            selected_server.last_error = err;
            self.status = .disconnected;
            self.mutex.unlock();
            return ConnectionError.ConnectionFailed;
        };

        self.stream = stream;
        self.should_stop.store(false, .monotonic);

        // Handle initial handshake (outside mutex) - before setting non-blocking
        self.processInitialHandshake() catch |err| {
            self.stream = null;
            stream.close();
            self.mutex.lock();
            selected_server.last_error = err;
            self.status = .disconnected;
            self.mutex.unlock();
            return err;
        };

        // Mark successful connection (back under mutex)
        self.mutex.lock();
        selected_server.did_connect = true;
        selected_server.reconnects = 0; // Reset on success
        self.status = .connected;

        // Reset reconnection state (under mutex)
        self.abort_reconnect = false;
        self.mutex.unlock();

        // Socket will be blocking - shutdown() will wake up reader

        // Start reader thread
        self.reader_thread = try std.Thread.spawn(.{}, readerLoop, .{self});

        // Start flusher thread
        self.flusher_thread = try std.Thread.spawn(.{}, flusherLoop, .{self});

        log.info("Connected successfully to {s}", .{selected_server.parsed_url.full_url});
    }

    pub fn close(self: *Self) void {
        self.mutex.lock();
        if (self.status == .closed) {
            self.mutex.unlock();
            return;
        }

        log.info("Closing connection", .{});
        self.should_stop.store(true, .release);
        self.status = .closed;

        // Shutdown socket to wake up reader thread (like C library)
        if (self.stream) |stream| {
            net_utils.shutdown(stream, .both) catch |shutdown_err| {
                log.debug("Socket shutdown failed: {}", .{shutdown_err});
                // Continue anyway, not critical
            };
        }

        // Get reconnection thread reference and wake it up
        const reconnect_thread = self.reconnect_thread;
        self.reconnect_condition.signal();
        self.mutex.unlock();

        // Wait for reconnection thread to complete
        if (reconnect_thread) |thread| {
            thread.join();
        }

        // Stop flusher thread
        if (self.flusher_thread) |thread| {
            self.mutex.lock();
            self.flusher_stop = true;
            self.flusher_condition.signal();
            self.mutex.unlock();

            thread.join();
            self.flusher_thread = null;
        }

        // Wait for reader thread to complete (it will handle stream cleanup)
        if (self.reader_thread) |thread| {
            thread.join();
            self.reader_thread = null;
        }

        // Clear pending buffer
        self.pending_buffer.clear();

        // Invoke closed callback
        if (self.options.callbacks.closed_cb) |callback| {
            callback(self);
        }
    }

    pub fn getStatus(self: *Self) ConnectionStatus {
        // Lock like C library status getter
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.status;
    }

    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        // Lock immediately like C natsConn_publish
        self.mutex.lock();
        defer self.mutex.unlock();

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

    pub fn publishMsg(self: *Self, msg: *Message) !void {
        // Lock immediately like other publish methods
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Check if message has headers
        try msg.ensureHeadersParsed();
        const has_headers = msg.headers.count() > 0;

        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        if (has_headers) {
            // Format headers
            var headers_buffer = ArrayList(u8).init(self.allocator);
            defer headers_buffer.deinit();
            try msg.encodeHeaders(headers_buffer.writer());

            const headers_len = headers_buffer.items.len;
            const total_len = headers_len + msg.data.len;

            // HPUB format: HPUB <subject> [reply] <headers_len> <total_len>\r\n<headers><data>\r\n
            if (msg.reply) |reply| {
                try buffer.writer().print("HPUB {s} {s} {d} {d}\r\n", .{ msg.subject, reply, headers_len, total_len });
            } else {
                try buffer.writer().print("HPUB {s} {d} {d}\r\n", .{ msg.subject, headers_len, total_len });
            }
            try buffer.appendSlice(headers_buffer.items);
            try buffer.appendSlice(msg.data);
            try buffer.appendSlice("\r\n");
        } else {
            // Regular PUB format: PUB <subject> [reply] <size>\r\n<data>\r\n
            if (msg.reply) |reply| {
                try buffer.writer().print("PUB {s} {s} {d}\r\n", .{ msg.subject, reply, msg.data.len });
            } else {
                try buffer.writer().print("PUB {s} {d}\r\n", .{ msg.subject, msg.data.len });
            }
            try buffer.appendSlice(msg.data);
            try buffer.appendSlice("\r\n");
        }

        // Send via buffer
        try self.bufferWrite(buffer.items);

        log.debug("Published message to {s}: has_headers={}, data_len={d}", .{ msg.subject, has_headers, msg.data.len });
    }

    pub fn subscribeSync(self: *Self, subject: []const u8) !*Subscription {
        // Lock immediately like C library
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, null);
        errdefer sub.deinit();

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
        // Lock immediately like C library
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Create type-erased message handler
        const handler = try subscription_mod.createMsgHandler(self.allocator, handlerFn, args);
        errdefer handler.cleanup(self.allocator);

        const sid = self.next_sid.fetchAdd(1, .monotonic);
        const sub = try Subscription.init(self.allocator, sid, subject, handler);
        errdefer sub.deinit();

        // Assign dispatcher for async subscription (round-robin like C library)
        try self.ensureDispatcherPool();
        sub.dispatcher = self.dispatcher_pool.?.assignDispatcher();

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
        // Lock immediately like C library
        self.mutex.lock();
        defer self.mutex.unlock();

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
        // Lock immediately like C library
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Force flush any pending writes (mutex already held)
        const has_pending = self.write_buffer.hasData();
        if (has_pending) {
            self.flusher_signaled = true;
            self.flusher_condition.signal();
        }

        // Increment pending PONG counter before sending PING
        self.pending_pongs += 1;

        // Send PING to ensure server acknowledges all previous messages (mutex held)
        try self.bufferWrite("PING\r\n");

        log.debug("Sent PING for flush, waiting for PONG", .{});

        // Wait for PONG response with timeout (mutex already held)

        const timeout_ns = self.options.timeout_ms * std.time.ns_per_ms;
        const start_time = std.time.nanoTimestamp();

        while (self.pending_pongs > 0) {
            const elapsed = std.time.nanoTimestamp() - start_time;
            if (elapsed >= timeout_ns) {
                log.warn("Flush timeout waiting for PONG", .{});
                return ConnectionError.Timeout;
            }

            const remaining_ns = @as(u64, @intCast(timeout_ns - elapsed));
            self.pong_condition.timedWait(&self.mutex, remaining_ns) catch {};
        }

        log.debug("Flush completed, received PONG", .{});
    }

    pub fn publishRequest(self: *Self, subject: []const u8, reply: []const u8, data: []const u8) !void {
        // Lock immediately like C library
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.status != .connected) {
            return ConnectionError.ConnectionClosed;
        }

        // Format: PUB <subject> <reply> <size>\r\n<data>\r\n
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        try buffer.writer().print("PUB {s} {s} {d}\r\n", .{ subject, reply, data.len });
        try buffer.appendSlice(data);
        try buffer.appendSlice("\r\n");

        // Send via buffer (mutex held)
        try self.bufferWrite(buffer.items);

        log.debug("Published request to {s} with reply {s}: {s}", .{ subject, reply, data });
    }

    pub fn request(self: *Self, subject: []const u8, data: []const u8, timeout_ms: u64) !*Message {
        if (self.options.trace) {
            log.debug("Sending request to {s} with timeout {d}ms", .{ subject, timeout_ms });
        }

        // Ensure response system is initialized (without mutex held)
        try self.response_manager.ensureInitialized(self);

        // Create request
        const request_info = try self.response_manager.createRequest(subject, data);

        defer {
            self.allocator.free(request_info.reply_subject);
            // Remove from token map (this also frees the token memory)
            self.response_manager.cleanupRequest(request_info.token);
            self.allocator.destroy(request_info.response_info);
        }

        // Send request (publishRequest will acquire its own mutex)
        try self.publishRequest(subject, request_info.reply_subject, data);

        // Wait for response
        return request_info.response_info.timedWait(timeout_ms * std.time.ns_per_ms);
    }

    fn processInitialHandshake(self: *Self) !void {
        const stream = self.stream orelse return ConnectionError.ConnectionClosed;
        const reader = stream.reader();

        // Read INFO message
        var info_buffer: [4096]u8 = undefined;
        if (try reader.readUntilDelimiterOrEof(info_buffer[0..], '\n')) |info_line| {
            log.debug("Received: {s}", .{info_line});

            if (!std.mem.startsWith(u8, info_line, "INFO ")) {
                return ConnectionError.InvalidProtocol;
            }

            // Extract and parse the JSON part after "INFO "
            const json_start = "INFO ".len;
            if (info_line.len > json_start) {
                const info_json = std.mem.trim(u8, info_line[json_start..], " \r\n");
                try self.processInfo(info_json);
            }
        } else {
            return ConnectionError.ConnectionClosed;
        }

        // Build CONNECT message with all options
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

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
        };

        try buffer.writer().writeAll("CONNECT ");
        try std.json.stringify(connect_obj, .{}, buffer.writer());
        try buffer.writer().writeAll("\r\n");
        const connect_msg = buffer.items;

        try stream.writeAll(connect_msg);
        try stream.writeAll("PING\r\n");

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

        log.debug("Reader loop started", .{});

        // Lock at start like C _readLoop
        self.mutex.lock();
        defer self.mutex.unlock(); // Final cleanup

        // Simple while condition - check status inside loop under lock
        while (!self.should_stop.load(.acquire)) {

            // Check status and stream under lock
            if (self.status == .closed or self.status == .reconnecting) {
                break;
            }

            const stream = self.stream orelse break;

            // Unlock before I/O like C _readLoop
            self.mutex.unlock();
            defer self.mutex.lock(); // Re-lock at end of iteration

            // Simple blocking read - shutdown() will wake us up
            const bytes_read = stream.read(&buffer) catch |err| {
                log.err("Read error: {}", .{err});
                self.triggerReconnect(err); // Handles its own locking
                break;
            };

            if (bytes_read == 0) {
                log.debug("Connection closed by server", .{});
                self.triggerReconnect(ConnectionError.ConnectionClosed); // Handles its own locking
                break;
            }

            log.debug("Read {} bytes: {s}", .{ bytes_read, buffer[0..bytes_read] });

            // Parse the received data
            self.parser.parse(self, buffer[0..bytes_read]) catch |err| {
                log.err("Parser error: {}", .{err});
                // Reset parser state on error to prevent corruption
                self.parser.reset();
                self.triggerReconnect(err); // Handles its own locking
                break;
            };
        }

        // Cleanup stream when reader exits (like C _readLoop)
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }

        log.debug("Reader loop exited", .{});
    }

    fn flusherLoop(self: *Self) void {
        log.debug("Flusher loop started", .{});

        self.mutex.lock();
        defer self.mutex.unlock();

        while (true) {
            // Wait for signal or stop condition
            while (!self.flusher_signaled and !self.flusher_stop) {
                self.flusher_condition.wait(&self.mutex);
            }

            if (self.flusher_stop) {
                log.debug("Flusher stopping...", .{});
                break;
            }

            // Give a chance to accumulate more requests (like C implementation)
            self.flusher_condition.timedWait(&self.mutex, 1_000_000) catch {}; // 1ms in nanoseconds

            self.flusher_signaled = false;

            if (self.status != .connected) {
                // No need to flush if not connected
                continue;
            }

            const stream = self.stream orelse break;

            // Unlock before I/O like C _readLoop
            self.mutex.unlock();
            defer self.mutex.lock(); // Re-lock at end of iteration

            // Write all buffered data using vectored I/O

            var iovecs: [16]std.posix.iovec_const = undefined;
            const iovec_count = self.write_buffer.gatherReadVectors(&iovecs);
            if (iovec_count == 0) {
                // No data to write
                continue;
            }

            var total_size: usize = 0;
            for (iovecs[0..iovec_count]) |iov| {
                total_size += iov.len;
            }

            if (stream.writev(iovecs[0..iovec_count])) |total_written| {
                log.debug("Flushed {} bytes", .{total_written});
                self.write_buffer.consumeBytesMultiple(total_written);
                if (total_written < total_size) {
                    self.mutex.lock();
                    self.flusher_signaled = true;
                    // don't need to signal, since we're already in the flusher loop
                    self.mutex.unlock();
                }
            } else |err| {
                log.err("Flush error: {}", .{err});
                self.triggerReconnect(err);
                break;
            }
        }

        log.debug("Flusher loop exited", .{});
    }

    fn bufferWrite(self: *Self, data: []const u8) !void {
        // Assume mutex is already held by caller

        // If we're reconnecting, buffer the message for later
        if (self.status == .reconnecting and self.options.reconnect.allow_reconnect) {
            return self.pending_buffer.addMessage(data);
        }

        // Buffer and signal flusher (mutex already held)
        try self.write_buffer.append(data);

        if (!self.flusher_signaled) {
            self.flusher_signaled = true;
            self.flusher_condition.signal();
        }
    }

    // Parser callback methods
    pub fn processMsg(self: *Self, message_buffer: []const u8) !void {
        const msg_arg = self.parser.ma;

        // Handle full message buffer like C parser (splits headers internally)
        const message = if (msg_arg.hdr >= 0) blk: {
            // HMSG - message_buffer contains headers + payload
            const hdr_len = @as(usize, @intCast(msg_arg.hdr));
            const headers_data = message_buffer[0..hdr_len];
            const msg_data = message_buffer[hdr_len..];
            break :blk try Message.initWithHeaders(self.allocator, msg_arg.subject, msg_arg.reply, msg_data, headers_data);
        } else blk: {
            // Regular MSG - message_buffer is just payload
            break :blk try Message.init(self.allocator, msg_arg.subject, msg_arg.reply, message_buffer);
        };

        message.sid = msg_arg.sid;

        // Retain subscription while holding lock, then release lock
        self.subs_mutex.lock();
        const sub = self.subscriptions.get(msg_arg.sid);
        if (sub) |s| {
            s.retain(); // Keep subscription alive
        }
        self.subs_mutex.unlock();

        if (sub) |s| {
            defer s.release(); // Release when done

            // Log before consuming message (to avoid use-after-free)
            log.debug("Delivering message to subscription {d}: {s}", .{ msg_arg.sid, message.data });

            if (s.handler) |_| {
                // Async subscription - dispatch to assigned dispatcher
                if (s.dispatcher) |dispatcher| {
                    dispatcher.enqueue(s, message) catch |err| {
                        log.err("Failed to dispatch message for sid {d}: {}", .{ msg_arg.sid, err });
                        message.deinit();
                        return;
                    };
                } else {
                    log.err("Async subscription {} has no assigned dispatcher", .{msg_arg.sid});
                    message.deinit();
                    return;
                }
            } else {
                // Sync subscription - queue message
                s.messages.push(message) catch |err| {
                    switch (err) {
                        error.QueueClosed => {
                            // Queue is closed; drop gracefully.
                            log.debug("Queue closed for sid {d}; dropping message", .{msg_arg.sid});
                            message.deinit();
                            return;
                        },
                        else => {
                            // Allocation or unexpected push failure; log and tear down the connection.
                            log.err("Failed to enqueue message for sid {d}: {}", .{ msg_arg.sid, err });
                            message.deinit();
                            return err;
                        },
                    }
                };
            }
        } else {
            // No subscription found, clean up message
            message.deinit();
        }
    }

    pub fn processInfo(self: *Self, info_json: []const u8) !void {
        log.debug("Received INFO: {s}", .{info_json});

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

        // Add discovered servers to pool if any connect_urls were provided
        if (self.server_info.connect_urls) |urls| {
            log.debug("Discovered {} servers: {s}", .{ urls.len, urls });
            self.addDiscoveredServers(urls) catch |err| {
                log.warn("Failed to add discovered servers: {}", .{err});
            };
        }
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
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.pending_pongs > 0) {
            self.pending_pongs -= 1;
            self.pong_condition.signal();
            log.debug("Received PONG for flush, pending_pongs: {}", .{self.pending_pongs});
        } else {
            log.warn("Received PONG (no pending flush), ignoring", .{});
        }
    }

    pub fn processPing(self: *Self) !void {
        std.debug.print("processPing: status={}, stream={}\n", .{ self.status, self.stream != null });
        const ping_start = std.time.nanoTimestamp();

        if (self.status == .connected) {
            const stream = self.stream orelse return ConnectionError.ConnectionClosed;
            std.debug.print("processPing: about to writeAll PONG\n", .{});
            stream.writeAll("PONG\r\n") catch |err| {
                std.debug.print("processPing: writeAll failed: {}\n", .{err});
                log.err("Failed to send PONG: {}", .{err});
            };
            std.debug.print("processPing: PONG write took {d}ms\n", .{@divTrunc(std.time.nanoTimestamp() - ping_start, std.time.ns_per_ms)});
            log.debug("Sent PONG in response to PING", .{});
        } else {
            std.debug.print("processPing: skipped (not connected or no stream)\n", .{});
        }
    }

    // Reconnection Logic

    fn triggerReconnect(self: *Self, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if we should start reconnection (like C library's _processOpError)
        if (!self.options.reconnect.allow_reconnect or
            self.status != .connected or // Only trigger if we were connected
            self.in_reconnect > 0)
        {
            return; // Already reconnecting or not connected
        }

        log.info("Connection lost ({}), starting reconnection", .{err});

        // Mark current server as having an error (already under connection mutex)
        if (self.current_server) |server| {
            server.last_error = err;
            server.reconnects += 1;
        }

        self.status = .reconnecting;
        self.abort_reconnect = false; // Reset abort flag when starting reconnection

        // Shutdown socket to interrupt any ongoing reads (like C natsSock_Shutdown)
        if (self.stream) |stream| {
            net_utils.shutdown(stream, .both) catch |shutdown_err| {
                log.debug("Socket shutdown failed: {}", .{shutdown_err});
                // Continue anyway, not critical
            };
        }

        // Reset parser state for clean reconnection
        self.parser.reset();

        // Invoke disconnected callback
        if (self.options.callbacks.disconnected_cb) |callback| {
            callback(self);
        }

        // Start reconnection thread (protected by mutex like C library)
        const thread = std.Thread.spawn(.{}, doReconnect, .{self}) catch |spawn_err| {
            log.err("Failed to start reconnection thread: {}", .{spawn_err});
            self.status = .closed;
            return;
        };

        // Successfully created thread - update state (under mutex like C library)
        self.reconnect_thread = thread;
        self.in_reconnect += 1; // Increment like C library
    }

    fn doReconnect(self: *Self) void {
        log.info("Reconnection loop started", .{});

        // Follow C library pattern: main loop under mutex with selective releases
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
            const stream = net.tcpConnectToHost(self.allocator, server.parsed_url.host, server.parsed_url.port) catch |err| {
                self.mutex.lock(); // Re-acquire for error handling
                server.last_error = err;
                log.debug("Reconnection attempt {} failed: {}", .{ total_attempts, err });
                continue; // Continue loop (mutex still held)
            };

            self.stream = stream;
            self.should_stop.store(false, .monotonic);

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
                    self.triggerReconnect(err);
                    continue; // Try next server
                };

                // Restart flusher thread for the new connection
                self.mutex.lock();
                self.flusher_stop = false;
                self.flusher_signaled = false;
                self.mutex.unlock();

                self.flusher_thread = std.Thread.spawn(.{}, flusherLoop, .{self}) catch |err| {
                    log.err("Failed to restart flusher thread: {}", .{err});
                    self.triggerReconnect(err);
                    continue; // Try next server
                };

                // Re-establish subscriptions (outside mutex like C library)
                self.resendSubscriptions() catch |err| {
                    log.err("Failed to re-establish subscriptions: {}", .{err});
                    // Continue anyway, connection is established
                };

                // Flush pending messages (outside mutex like C library)
                self.pending_buffer.flush(self.stream.?) catch |err| {
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
                self.stream = null;
                stream.close();
                server.last_error = err;
                self.status = .reconnecting;
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

        var iter = self.subscriptions.iterator();
        while (iter.next()) |entry| {
            const sub = entry.value_ptr.*;

            // Send SUB command
            var buffer = ArrayList(u8).init(self.allocator);
            defer buffer.deinit();

            try buffer.writer().print("SUB {s} {d}\r\n", .{ sub.subject, sub.sid });

            // Send directly (bypass buffering since we're reconnecting)
            const stream = self.stream orelse return ConnectionError.ConnectionClosed;
            try stream.writeAll(buffer.items);

            log.debug("Re-subscribed to {s} with sid {d}", .{ sub.subject, sub.sid });
        }
    }

    fn addDiscoveredServers(self: *Self, urls: [][]const u8) !void {
        log.debug("Adding {} discovered servers to pool", .{urls.len});

        self.mutex.lock();
        defer self.mutex.unlock();

        for (urls) |url| {
            // Add as implicit server (discovered, not explicitly configured)
            self.server_pool.addServer(url, true) catch |err| {
                log.warn("Failed to add discovered server {s}: {}", .{ url, err });
                continue;
            };
            log.debug("Added discovered server: {s}", .{url});
        }
    }

    // JetStream support
    pub fn jetstream(self: *Self, options: JetStreamOptions) JetStream {
        return JetStream.init(self.allocator, self, options);
    }
};
