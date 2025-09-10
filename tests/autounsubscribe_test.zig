const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");
const Message = nats.Message;

test "autounsubscribe sync basic functionality" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("auto.test");
    defer sub.deinit();

    // Set autounsubscribe limit to 3 messages
    try sub.autoUnsubscribe(3);
    try conn.flush();

    // Publish 5 messages
    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const msg_data = try std.fmt.bufPrint(&buf, "message_{d}", .{i});
        try conn.publish("auto.test", msg_data);
    }
    try conn.flush();

    // Should receive exactly 3 messages
    var received_count: u32 = 0;
    for (0..3) |_| {
        const msg = try sub.nextMsg(1000);
        defer msg.deinit();
        received_count += 1;
    }

    try std.testing.expectEqual(@as(u32, 3), received_count);

    // Fourth message should timeout (subscription auto-unsubscribed)
    try std.testing.expectError(error.Timeout, sub.nextMsg(100));
}

test "autounsubscribe async basic functionality" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var messages_received = std.ArrayList(*Message).init(std.testing.allocator);
    defer {
        for (messages_received.items) |msg| {
            msg.deinit();
        }
        messages_received.deinit();
    }

    const TestContext = struct {
        messages: *std.ArrayList(*Message),
        mutex: std.Thread.Mutex = .{},

        pub fn handleMessage(msg: *Message, self: *@This()) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.messages.append(msg);
        }
    };

    var ctx = TestContext{ .messages = &messages_received };

    const sub = try conn.subscribe("auto.async.test", TestContext.handleMessage, .{&ctx});
    defer sub.deinit();

    // Set autounsubscribe limit to 2 messages
    try sub.autoUnsubscribe(2);
    try conn.flush();

    // Publish 4 messages
    for (0..4) |i| {
        var buf: [32]u8 = undefined;
        const msg_data = try std.fmt.bufPrint(&buf, "async_message_{d}", .{i});
        try conn.publish("auto.async.test", msg_data);
    }
    try conn.flush();

    // Wait for message processing with bounded wait loop
    const deadline_ms = std.time.milliTimestamp() + 1000;
    var count: usize = 0;
    while (std.time.milliTimestamp() < deadline_ms) {
        ctx.mutex.lock();
        count = messages_received.items.len;
        ctx.mutex.unlock();
        if (count >= 2) break;
        std.time.sleep(10 * std.time.ns_per_ms);
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "autounsubscribe error conditions" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("error.test");
    defer sub.deinit();

    // Test invalid max (zero)
    try std.testing.expectError(error.InvalidMax, sub.autoUnsubscribe(0));

    // Publish and receive a message first
    try conn.publish("error.test", "first message");
    try conn.flush();

    const msg = try sub.nextMsg(1000);
    defer msg.deinit();

    // Now try to set autounsubscribe to 1 (should fail since we already received 1)
    try std.testing.expectError(error.MaxAlreadyReached, sub.autoUnsubscribe(1));
}

test "autounsubscribe delivered message counter" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("counter.test");
    defer sub.deinit();

    try sub.autoUnsubscribe(2);
    try conn.flush();

    // Verify initial state
    try std.testing.expectEqual(@as(u64, 0), sub.delivered_msgs.load(.acquire));

    // Publish messages and verify counter increments correctly
    try conn.publish("counter.test", "message 1");
    try conn.flush();

    const msg1 = try sub.nextMsg(1000);
    defer msg1.deinit();
    try std.testing.expectEqual(@as(u64, 1), sub.delivered_msgs.load(.acquire));

    try conn.publish("counter.test", "message 2");
    try conn.flush();

    const msg2 = try sub.nextMsg(1000);
    defer msg2.deinit();
    try std.testing.expectEqual(@as(u64, 2), sub.delivered_msgs.load(.acquire));

    // Third message should timeout due to autounsubscribe
    try conn.publish("counter.test", "message 3");
    try conn.flush();
    try std.testing.expectError(error.Timeout, sub.nextMsg(100));
}
