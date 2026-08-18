const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

test "pending_msgs counter sync subscription" {
    const io = std.testing.io;

    var conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    // Create sync subscription
    const sub = try conn.subscribeSync("test.pending.sync");
    defer sub.deinit();

    // Initially should be 0
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);

    // Publish a message
    const msg1_data = "test message 1";
    try conn.publish("test.pending.sync", msg1_data);
    try conn.flush();

    // Wait (up to 1s) for message to arrive
    var attempts: u32 = 0;
    while (attempts < 100) : (attempts += 1) {
        if (sub.pending_msgs.load(.acquire) == 1 and
            sub.pending_bytes.load(.acquire) == msg1_data.len) break;
        try io.sleep(.fromMilliseconds(10), .awake);
    }

    // Should have 1 pending message and correct bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg1_data.len);

    // Publish another
    const msg2_data = "test message 2";
    try conn.publish("test.pending.sync", msg2_data);
    try conn.flush();

    attempts = 0;
    while (attempts < 100) : (attempts += 1) {
        if (sub.pending_msgs.load(.acquire) == 2 and
            sub.pending_bytes.load(.acquire) == msg1_data.len + msg2_data.len) break;
        try io.sleep(.fromMilliseconds(10), .awake);
    }

    // Should have 2 pending messages and correct total bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 2);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg1_data.len + msg2_data.len);

    // Consume one message
    var msg1 = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer msg1.deinit();

    // Should have 1 pending message and bytes for second message
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg2_data.len);

    // Consume second message
    var msg2 = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer msg2.deinit();

    // Should have 0 pending messages and bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);
}

test "pending_msgs counter async subscription" {
    const io = std.testing.io;

    var conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    var message_count = std.atomic.Value(u32).init(0);
    var processed_count = std.atomic.Value(u32).init(0);
    var total_bytes_processed = std.atomic.Value(u64).init(0);

    const TestContext = struct {
        message_count_ptr: *std.atomic.Value(u32),
        processed_count_ptr: *std.atomic.Value(u32),
        total_bytes_ptr: *std.atomic.Value(u64),
        io: std.Io,
    };

    const testHandler = struct {
        fn handle(msg: *nats.Message, ctx: TestContext) void {
            defer msg.deinit();
            _ = ctx.message_count_ptr.fetchAdd(1, .acq_rel);
            _ = ctx.total_bytes_ptr.fetchAdd(@intCast(msg.data.len), .acq_rel);
            // Add a small delay to simulate processing
            ctx.io.sleep(.fromMilliseconds(5), .awake) catch {};
            _ = ctx.processed_count_ptr.fetchAdd(1, .acq_rel);
        }
    }.handle;

    // Create async subscription
    const sub = try conn.subscribe("test.pending.async", testHandler, .{TestContext{
        .message_count_ptr = &message_count,
        .processed_count_ptr = &processed_count,
        .total_bytes_ptr = &total_bytes_processed,
        .io = io,
    }});
    defer sub.deinit();

    // Initially should be 0
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);

    // Publish several messages rapidly
    const msg1_data = "test message 1";
    const msg2_data = "test message 2";
    const msg3_data = "test message 3";
    try conn.publish("test.pending.async", msg1_data);
    try conn.publish("test.pending.async", msg2_data);
    try conn.publish("test.pending.async", msg3_data);
    try conn.flush();

    // Give a moment for messages to arrive but not fully process
    try io.sleep(.fromMilliseconds(20), .awake);

    // Should have some pending messages (might be processing)
    // Note: We can't assert an exact number here since processing might start immediately

    // Wait for all messages to be processed
    var attempts: u32 = 0;
    while (processed_count.load(.acquire) < 3 and attempts < 200) {
        try io.sleep(.fromMilliseconds(10), .awake);
        attempts += 1;
    }

    // All messages should be processed now
    try std.testing.expect(processed_count.load(.acquire) == 3);

    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);

    // Verify total bytes processed matches expected
    const expected_bytes = msg1_data.len + msg2_data.len + msg3_data.len;
    try std.testing.expect(total_bytes_processed.load(.acquire) == expected_bytes);
}

const SlowConsumerTracker = struct {
    var error_count: std.atomic.Value(u32) = .init(0);

    fn errorCallback(_: *nats.Connection, _: []const u8) void {
        _ = error_count.fetchAdd(1, .monotonic);
    }
};

test "slow consumer drops messages over the pending message limit" {
    const io = std.testing.io;

    SlowConsumerTracker.error_count.store(0, .monotonic);

    var conn = try utils.createConnection(io, .node1, .{
        .callbacks = .{ .error_cb = SlowConsumerTracker.errorCallback },
    });
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test.slowconsumer.msgs");
    defer sub.deinit();
    sub.setPendingLimits(2, 0);

    for (0..5) |_| {
        try conn.publish("test.slowconsumer.msgs", "payload");
    }
    try conn.flush();

    // All five messages were processed by the reader before flush returned:
    // two delivered, three dropped, one slow-consumer episode reported.
    try std.testing.expectEqual(@as(u32, 2), sub.pending_msgs.load(.acquire));
    try std.testing.expectEqual(@as(u64, 3), sub.dropped());
    try std.testing.expectEqual(@as(u32, 1), SlowConsumerTracker.error_count.load(.monotonic));

    // Consuming ends the episode; new messages are delivered again.
    for (0..2) |_| {
        const msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        msg.deinit();
    }

    try conn.publish("test.slowconsumer.msgs", "after");
    try conn.flush();

    const msg = try sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    defer msg.deinit();
    try std.testing.expectEqualStrings("after", msg.data);

    // A second overflow is a new episode with its own callback.
    for (0..5) |_| {
        try conn.publish("test.slowconsumer.msgs", "payload");
    }
    try conn.flush();

    try std.testing.expectEqual(@as(u32, 2), SlowConsumerTracker.error_count.load(.monotonic));
    try std.testing.expect(sub.dropped() > 3);
}

test "slow consumer drops messages over the pending bytes limit" {
    const io = std.testing.io;

    var conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test.slowconsumer.bytes");
    defer sub.deinit();
    sub.setPendingLimits(0, 10);

    // 8 bytes fit; the second and third message would exceed 10 bytes.
    for (0..3) |_| {
        try conn.publish("test.slowconsumer.bytes", "8bytes!!");
    }
    try conn.flush();

    try std.testing.expectEqual(@as(u32, 1), sub.pending_msgs.load(.acquire));
    try std.testing.expectEqual(@as(u64, 8), sub.pending_bytes.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), sub.dropped());
}
