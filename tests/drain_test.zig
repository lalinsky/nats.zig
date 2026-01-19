const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

test "subscription drain sync - immediate completion" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create sync subscription
    const sub = try conn.subscribeSync("test.drain.sync.immediate");
    defer sub.deinit();

    // Initially not draining
    try std.testing.expect(!sub.isDraining());
    try std.testing.expect(!sub.isDrainComplete());

    // Drain empty subscription (should complete immediately)
    sub.drain();

    // Should be draining and complete immediately
    try std.testing.expect(sub.isDraining());
    try std.testing.expect(sub.isDrainComplete());

    // Wait should return immediately
    try sub.waitForDrainCompletion(1000);
}

test "subscription drain sync - with pending messages" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create sync subscription
    const sub = try conn.subscribeSync("test.drain.sync.pending");
    defer sub.deinit();

    // Publish messages before draining
    const msg1_data = "test message 1";
    const msg2_data = "test message 2";
    try conn.publish("test.drain.sync.pending", msg1_data);
    try conn.publish("test.drain.sync.pending", msg2_data);
    try conn.flush();

    // Wait (up to 1s) for both messages to be counted as pending
    var waited: u64 = 0;
    while (sub.pending_msgs.load(.acquire) < 2 and waited < 1000) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    // Should have pending messages
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 2);

    // Drain subscription
    sub.drain();

    // Should be draining but not complete yet
    try std.testing.expect(sub.isDraining());
    try std.testing.expect(!sub.isDrainComplete());

    // Consume first message
    var msg1 = try sub.nextMsg(1000);
    defer msg1.deinit();
    try std.testing.expect(std.mem.eql(u8, msg1.data, msg1_data));

    // Should still be draining with 1 message left
    try std.testing.expect(sub.isDraining());
    try std.testing.expect(!sub.isDrainComplete());
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);

    // Consume second message
    var msg2 = try sub.nextMsg(1000);
    defer msg2.deinit();
    try std.testing.expect(std.mem.eql(u8, msg2.data, msg2_data));

    // Should now be complete
    try std.testing.expect(sub.isDraining());
    try std.testing.expect(sub.isDrainComplete());
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);

    // Wait should return immediately
    try sub.waitForDrainCompletion(1000);
}

test "subscription drain async - with callback processing" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var messages_processed: u32 = 0;
    var processing_complete: std.Thread.ResetEvent = .{};

    const TestContext = struct {
        processed_count_ptr: *u32,
        completion_event_ptr: *std.Thread.ResetEvent,
    };

    const testHandler = struct {
        fn handle(msg: *nats.Message, ctx: TestContext) void {
            defer msg.deinit();

            // Simulate some processing time
            std.Thread.sleep(5 * std.time.ns_per_ms);

            ctx.processed_count_ptr.* += 1;
            if (ctx.processed_count_ptr.* == 3) {
                ctx.completion_event_ptr.set();
            }
        }
    }.handle;

    // Create async subscription
    const sub = try conn.subscribe("test.drain.async", testHandler, .{TestContext{
        .processed_count_ptr = &messages_processed,
        .completion_event_ptr = &processing_complete,
    }});
    defer sub.deinit();

    // Publish messages
    try conn.publish("test.drain.async", "message 1");
    try conn.publish("test.drain.async", "message 2");
    try conn.publish("test.drain.async", "message 3");
    try conn.flush();

    // Give messages time to arrive but not necessarily process
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Messages should have arrived (they may be processing or queued)
    // Note: pending count may be 0 if already processed, so we'll skip this check
    _ = sub.pending_msgs.load(.acquire); // Just to verify the field works

    // Drain subscription
    sub.drain();

    // Should be draining
    try std.testing.expect(sub.isDraining());

    // Wait for drain completion
    try sub.waitForDrainCompletion(5000); // 5 second timeout

    // Should be complete
    try std.testing.expect(sub.isDrainComplete());
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 0);

    // Wait for all messages to be processed
    processing_complete.wait();
    try std.testing.expect(messages_processed == 3);
}

test "subscription drain blocks new messages" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create sync subscription
    const sub = try conn.subscribeSync("test.drain.block");
    defer sub.deinit();

    // Publish initial message
    try conn.publish("test.drain.block", "before drain");
    try conn.flush();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Should have 1 pending message
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);

    // Drain subscription
    sub.drain();

    // Publish more messages after drain started
    try conn.publish("test.drain.block", "after drain 1");
    try conn.publish("test.drain.block", "after drain 2");
    try conn.flush();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Should still have only 1 pending message (new ones dropped)
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 1);

    // Consume the original message
    var msg = try sub.nextMsg(1000);
    defer msg.deinit();
    try std.testing.expect(std.mem.eql(u8, msg.data, "before drain"));

    // Should be complete
    try std.testing.expect(sub.isDrainComplete());

    // And no extra messages should be retrievable
    const maybe = sub.nextMsg(50);
    try std.testing.expectError(error.Timeout, maybe);
}

test "subscription drain timeout" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create sync subscription and leave one message unconsumed
    const sub = try conn.subscribeSync("test.drain.timeout");
    defer sub.deinit();

    try conn.publish("test.drain.timeout", "will block drain");
    try conn.flush();
    // Wait briefly for arrival
    var waited: u64 = 0;
    while (sub.pending_msgs.load(.acquire) < 1 and waited < 200) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    sub.drain();

    // waitForDrainCompletion should timeout
    const result = sub.waitForDrainCompletion(100); // 100ms timeout
    try std.testing.expectError(error.Timeout, result);
}

test "subscription drain not draining error" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    const sub = try conn.subscribeSync("test.drain.not_draining");
    defer sub.deinit();

    // Should error if not draining
    const result = sub.waitForDrainCompletion(100);
    try std.testing.expectError(error.NotDraining, result);
}

test "connection drain - no subscriptions" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Initially not draining
    try std.testing.expect(!conn.isDraining());

    // Drain connection with no subscriptions (should complete immediately)
    try conn.drain();

    // Wait should return quickly as drain completes immediately
    try conn.waitForDrainCompletion(1000);
}

test "connection drain - single subscription" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create subscription with pending messages
    const sub = try conn.subscribeSync("test.connection.drain.single");
    defer sub.deinit();

    // Publish messages
    try conn.publish("test.connection.drain.single", "message 1");
    try conn.publish("test.connection.drain.single", "message 2");
    try conn.flush();

    // Wait for messages to arrive
    var waited: u64 = 0;
    while (sub.pending_msgs.load(.acquire) < 2 and waited < 1000) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try std.testing.expect(sub.pending_msgs.load(.acquire) == 2);

    // Drain connection
    try conn.drain();
    try std.testing.expect(conn.isDraining());

    // Subscription should be draining but not complete
    try std.testing.expect(sub.isDraining());
    try std.testing.expect(!sub.isDrainComplete());

    // Consume messages to complete drain
    var msg1 = try sub.nextMsg(1000);
    defer msg1.deinit();
    var msg2 = try sub.nextMsg(1000);
    defer msg2.deinit();

    // Wait for connection drain completion
    try conn.waitForDrainCompletion(1000);
}

test "connection drain - multiple subscriptions" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Create multiple subscriptions
    const sub1 = try conn.subscribeSync("test.connection.drain.multi1");
    defer sub1.deinit();
    const sub2 = try conn.subscribeSync("test.connection.drain.multi2");
    defer sub2.deinit();

    // Publish messages to both subscriptions
    try conn.publish("test.connection.drain.multi1", "message 1");
    try conn.publish("test.connection.drain.multi2", "message 2");
    try conn.flush();

    // Wait for messages to arrive
    var waited: u64 = 0;
    while ((sub1.pending_msgs.load(.acquire) < 1 or sub2.pending_msgs.load(.acquire) < 1) and waited < 1000) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try std.testing.expect(sub1.pending_msgs.load(.acquire) == 1);
    try std.testing.expect(sub2.pending_msgs.load(.acquire) == 1);

    // Drain connection
    try conn.drain();
    try std.testing.expect(conn.isDraining());

    // Both subscriptions should be draining
    try std.testing.expect(sub1.isDraining());
    try std.testing.expect(sub2.isDraining());

    // Consume first subscription's message
    var msg1 = try sub1.nextMsg(1000);
    defer msg1.deinit();

    // First subscription should be complete, but connection still draining
    try std.testing.expect(sub1.isDrainComplete());
    try std.testing.expect(!sub2.isDrainComplete());

    // Consume second subscription's message
    var msg2 = try sub2.nextMsg(1000);
    defer msg2.deinit();

    // Now both should be complete and connection drain should complete
    try std.testing.expect(sub2.isDrainComplete());
    try conn.waitForDrainCompletion(1000);
}

test "connection drain - already draining" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // First drain call
    try conn.drain();

    // Second drain call should be a no-op (not error)
    try conn.drain();

    try conn.waitForDrainCompletion(1000);
}

test "connection drain not draining error" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    // Should error if not draining
    const result = conn.waitForDrainCompletion(100);
    try std.testing.expectError(error.NotDraining, result);
}
