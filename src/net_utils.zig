const std = @import("std");
const net = std.net;
const posix = std.posix;

/// Shutdown a network stream to interrupt blocking I/O operations.
/// This is useful for waking up reader threads that are blocked on read().
pub fn shutdown(stream: net.Stream, how: posix.ShutdownHow) !void {
    try posix.shutdown(stream.handle, how);
}