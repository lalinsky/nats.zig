const std = @import("std");

const errors = @import("errors.zig");

pub const ConnectOptions = struct {
    // Basic connection
    name: ?[]const u8 = null,
    verbose: bool = false,
    pedantic: bool = false,

    // Authentication
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
    token: ?[]const u8 = null,

    // Reconnection
    allow_reconnect: bool = true,
    max_reconnect: u32 = 60,
    reconnect_wait: u64 = 2000, // milliseconds

    // Keep-alive
    ping_interval: u64 = 120000, // milliseconds
    max_pings_out: u32 = 2,

    // Buffering
    read_buffer_size: usize = 16384,
    write_buffer_size: usize = 16384,

    // Callbacks
    on_connected: ?*const fn (*Connection) void = null,
    on_disconnected: ?*const fn (*Connection) void = null,
    on_error: ?*const fn (*Connection, errors.NatsError) void = null,
};

pub const ConnectionStatus = enum {
    disconnected,
    connecting,
    connected,
    closed,
    reconnecting,
};

const HostAndPort = struct {
    host: []const u8,
    port: u16,
};

fn parseUrl(url: []const u8) errors.NatsError!HostAndPort {
    const parsed_url = std.Uri.parse(url) catch return errors.NatsError.InvalidUrl;

    if (!std.mem.eql(u8, parsed_url.scheme, "nats")) {
        return errors.NatsError.InvalidScheme;
    }

    const host = parsed_url.host orelse return errors.NatsError.InvalidUrl;
    const port = parsed_url.port orelse return errors.NatsError.InvalidUrl;

    return .{ .host = host, .port = port };
}

pub const Connection = struct {
    allocator: std.mem.Allocator,

    url: []const u8,
    options: ConnectOptions = .{},

    // Network
    stream: ?std.net.Stream = null,
    read_buffer: [16384]u8 = undefined, // 16KB read buffer
    write_buffer: [16384]u8 = undefined, // 16KB write buffer

    status: std.atomic.Value(ConnectionStatus) = std.atomic.Value(ConnectionStatus).init(.disconnected),

    pub fn init(allocator: std.mem.Allocator, url: []const u8, options: ConnectOptions) !Connection {
        return .{
            .allocator = allocator,
            .url = url,
            .options = options,
        };
    }

    pub fn deinit(self: *Connection) void {
        if (self.stream) |stream| {
            stream.close();
        }
    }

    pub fn connect(self: *Connection) !void {
        const address = try parseUrl(self.url);

        // Connect to the server
        self.status.store(.connecting, .release);
        self.stream = try std.net.tcpConnectToHost(self.allocator, address.host, address.port);

        self.status.store(.connected, .release);
    }

    // ...
};
