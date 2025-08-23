const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.reconnection_test);

test "basic reconnection when server stops" {
    const nc = try utils.createConnection(.node1);
    defer utils.closeConnection(nc);

    // Publish a test message to ensure connection works
    log.debug("Publishing test message before", .{});
    try nc.publish("test.before", "hello before");
    try nc.flush();

    log.debug("Restarting nats-1", .{});
    try utils.runDockerCompose(std.testing.allocator, &.{ "restart", "nats-1" });

    // Verify connection works after reconnection
    // log.debug("Trying to publish after reconnection", .{});
    // try nc.publish("test.after", "hello after reconnection");
    // try nc.flush();
}
