const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

const Counters = struct {
    closed_cb_called: usize = 0,

    pub fn reset(self: *Counters) void {
        self.closed_cb_called = 0;
    }
};

var counts: Counters = Counters{};

test "close callback" {
    counts.reset();

    const conn = try utils.createConnectionOption(.node1, .{
        .callbacks = .{
            .closed_cb = struct {
                fn close(_: *nats.Connection) void {
                    log.info("close callback", .{});
                    counts.closed_cb_called += 1;
                }
            }.close,
        },
    });

    utils.closeConnection(conn);
    try std.testing.expectEqual(1, counts.closed_cb_called);
}
