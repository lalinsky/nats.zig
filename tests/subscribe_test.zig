const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");
const Message = nats.Message;

const log = std.log.scoped(.testing);

test "subscribeSync smoke test" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const result = sub.nextMsg(1000);
    defer if (result) |msg| msg.deinit();

    if (result) |msg| {
        try std.testing.expectEqualStrings("test", msg.subject);
        try std.testing.expectEqualStrings("Hello world!", msg.data);
    } else {
        try std.testing.expect(false);
    }
}

test "queueSubscribeSync smoke test" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.queueSubscribeSync("test", "workers");
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    const result = sub.nextMsg(1000);
    defer if (result) |msg| msg.deinit();

    if (result) |msg| {
        try std.testing.expectEqualStrings("test", msg.subject);
        try std.testing.expectEqualStrings("Hello world!", msg.data);
    } else {
        try std.testing.expect(false);
    }
}

fn collectMsg(msg: *Message, result: *?*Message) !void {
    result.* = msg;
}

test "subscribe smoke test" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var result: ?*Message = null;
    defer if (result) |msg| msg.deinit();

    const sub = try conn.subscribe("test", collectMsg, .{&result});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    if (result) |msg| {
        try std.testing.expectEqualStrings("test", msg.subject);
        try std.testing.expectEqualStrings("Hello world!", msg.data);
    } else {
        try std.testing.expect(false);
    }
}

test "queueSubscribe smoke test" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var result: ?*Message = null;
    defer if (result) |msg| msg.deinit();

    const sub = try conn.queueSubscribe("test", "workers", collectMsg, .{&result});
    defer sub.deinit();

    try conn.publish("test", "Hello world!");
    try conn.flush();

    if (result) |msg| {
        try std.testing.expectEqualStrings("test", msg.subject);
        try std.testing.expectEqualStrings("Hello world!", msg.data);
    } else {
        try std.testing.expect(false);
    }
}
