const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

test "pending_msgs counter sync subscription" {
    var conn = try utils.createDefaultConnection();
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
    var waited_ms: u32 = 0;
    while (waited_ms < 1000) : (waited_ms += 10) {
        if (sub.pending_msgs.load(.acquire) == 1 and
            sub.pending_bytes.load(.acquire) == msg1_data.len) break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Should have 1 pending message and correct bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg1_data.len);

    // Publish another
    const msg2_data = "test message 2";
    try conn.publish("test.pending.sync", msg2_data);
    try conn.flush();

    waited_ms = 0;
    while (waited_ms < 1000) : (waited_ms += 10) {
        if (sub.pending_msgs.load(.acquire) == 2 and
            sub.pending_bytes.load(.acquire) == msg1_data.len + msg2_data.len) break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Should have 2 pending messages and correct total bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 2);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg1_data.len + msg2_data.len);

    // Consume one message
    var msg1 = try sub.nextMsg(1000);
    defer msg1.deinit();

    // Should have 1 pending message and bytes for second message
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg2_data.len);

    // Consume second message
    var msg2 = try sub.nextMsg(1000);
    defer msg2.deinit();

    // Should have 0 pending messages and bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);
}

test "pending_msgs counter async subscription" {
    var conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var message_count = std.atomic.Value(u32).init(0);
    var processed_count = std.atomic.Value(u32).init(0);
    var total_bytes_processed = std.atomic.Value(u64).init(0);

    const TestContext = struct {
        message_count_ptr: *std.atomic.Value(u32),
        processed_count_ptr: *std.atomic.Value(u32),
        total_bytes_ptr: *std.atomic.Value(u64),
    };

    const testHandler = struct {
        fn handle(msg: *nats.Message, ctx: TestContext) void {
            defer msg.deinit();
            _ = ctx.message_count_ptr.fetchAdd(1, .acq_rel);
            _ = ctx.total_bytes_ptr.fetchAdd(@intCast(msg.data.len), .acq_rel);
            // Add a small delay to simulate processing
            std.Thread.sleep(5 * std.time.ns_per_ms);
            _ = ctx.processed_count_ptr.fetchAdd(1, .acq_rel);
        }
    }.handle;

    // Create async subscription
    const sub = try conn.subscribe("test.pending.async", testHandler, .{TestContext{
        .message_count_ptr = &message_count,
        .processed_count_ptr = &processed_count,
        .total_bytes_ptr = &total_bytes_processed,
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
    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Should have some pending messages (might be processing)
    // Note: We can't assert an exact number here since processing might start immediately

    // Wait for all messages to be processed
    var attempts: u32 = 0;
    while (processed_count.load(.acquire) < 3 and attempts < 200) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
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
