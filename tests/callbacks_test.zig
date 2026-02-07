const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

const Counters = struct {
    closed_cb_called: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn reset(self: *Counters) void {
        self.closed_cb_called.store(0, .monotonic);
    }
};

var counts: Counters = Counters{};

test "close callback" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    counts.reset();

    const conn = try utils.createConnection(.node1, .{
        .callbacks = .{
            .closed_cb = struct {
                fn close(_: *nats.Connection) void {
                    log.info("close callback", .{});
                    _ = counts.closed_cb_called.fetchAdd(1, .monotonic);
                }
            }.close,
        },
    });

    utils.closeConnection(conn);
    try std.testing.expectEqual(@as(usize, 1), counts.closed_cb_called.load(.monotonic));
}
