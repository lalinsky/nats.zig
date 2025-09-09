const std = @import("std");
const nats = @import("nats");
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

    // Give it a moment for message to arrive
    std.time.sleep(10 * std.time.ns_per_ms);

    // Should have 1 pending message and correct bytes
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == msg1_data.len);

    // Publish another
    const msg2_data = "test message 2";
    try conn.publish("test.pending.sync", msg2_data);
    try conn.flush();
    std.time.sleep(10 * std.time.ns_per_ms);

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

    var message_count: u32 = 0;
    var processed_count: u32 = 0;
    var total_bytes_processed: u64 = 0;

    const TestContext = struct {
        message_count_ptr: *u32,
        processed_count_ptr: *u32,
        total_bytes_ptr: *u64,
    };

    const testHandler = struct {
        fn handle(msg: *nats.Message, ctx: TestContext) void {
            defer msg.deinit();
            ctx.message_count_ptr.* += 1;
            ctx.total_bytes_ptr.* += msg.data.len;
            // Add a small delay to simulate processing
            std.time.sleep(5 * std.time.ns_per_ms);
            ctx.processed_count_ptr.* += 1;
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
    std.time.sleep(20 * std.time.ns_per_ms);

    // Should have some pending messages (might be processing)
    // Note: We can't assert an exact number here since processing might start immediately

    // Wait for all messages to be processed
    var attempts: u32 = 0;
    while (processed_count < 3 and attempts < 200) {
        std.time.sleep(10 * std.time.ns_per_ms);
        attempts += 1;
    }

    // All messages should be processed now
    try std.testing.expect(processed_count == 3);

    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);
    try std.testing.expect(sub.pending_bytes.load(.acquire) == 0);

    // Verify total bytes processed matches expected
    const expected_bytes = msg1_data.len + msg2_data.len + msg3_data.len;
    try std.testing.expect(total_bytes_processed == expected_bytes);
}
