const std = @import("std");

pub const Connection = @import("connection.zig").Connection;
pub const ConnectionOptions = @import("connection.zig").ConnectionOptions;

pub fn connect(allocator: std.mem.Allocator, url: []const u8, options: Connection.Options) !*Connection {
    _ = allocator;
    _ = url;
    _ = options;
    return error.NotImplemented;
}
