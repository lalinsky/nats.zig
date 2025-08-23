const std = @import("std");
const nats = @import("nats");

const log = std.log.scoped(.testing);

pub fn createConnection() !*nats.Connection {
    const default_url = "nats://localhost:14222";
    const url = std.process.getEnvVarOwned(std.testing.allocator, "TEST_NATS_URL") catch default_url;
    defer if (url.ptr != default_url.ptr) std.testing.allocator.free(url);

    var conn = try std.testing.allocator.create(nats.Connection);
    conn.* = nats.Connection.init(std.testing.allocator, .{});
    try conn.connect(url);
    return conn;
}

pub fn closeConnection(conn: *nats.Connection) void {
    conn.deinit();
    std.testing.allocator.destroy(conn);
}
