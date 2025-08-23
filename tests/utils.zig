const std = @import("std");
const nats = @import("nats");

const log = std.log.scoped(.testing);

pub fn createConnection(port: u16) !*nats.Connection {
    const url = try std.fmt.allocPrint(std.testing.allocator, "nats://localhost:{d}", .{port});
    defer std.testing.allocator.free(url);

    var conn = try std.testing.allocator.create(nats.Connection);
    errdefer std.testing.allocator.destroy(conn);

    conn.* = nats.Connection.init(std.testing.allocator, .{});
    errdefer conn.deinit();

    try conn.connect(url);

    return conn;
}

pub fn createDefaultConnection() !*nats.Connection {
    return createConnection(14222);
}

pub fn createConnectionWrongPort() !*nats.Connection {
    return createConnection(14225);
}

pub fn closeConnection(conn: *nats.Connection) void {
    conn.deinit();
    std.testing.allocator.destroy(conn);
}
