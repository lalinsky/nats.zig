const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.jetstream_push_test);

test "basic push subscription" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PUSH_STREAM",
        .subjects = &.{"orders.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Message counter for testing
    var message_count: u32 = 0;

    // Define message handler
    const MessageHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, counter: *u32) void {
            defer js_msg.deinit();
            counter.* += 1;
            
            log.info("Received message #{d}: {s}", .{ counter.*, js_msg.msg.data });
            
            // Acknowledge the message
            js_msg.ack() catch |err| {
                log.err("Failed to ack message: {}", .{err});
            };
        }
    };

    // Create push consumer configuration  
    // Note: deliver_subject must not overlap with stream subjects to avoid cycles
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "test_push_consumer",
        .deliver_subject = "push.orders.processed", // Key for push consumer - different from stream subjects
        .ack_policy = .explicit,
        .deliver_policy = .all,
    };

    // Subscribe to push consumer
    var push_sub = try js.subscribe("TEST_PUSH_STREAM", consumer_config, MessageHandler.handle, .{&message_count});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish some test messages
    try conn.publish("orders.new", "Order #1");
    try conn.publish("orders.new", "Order #2");
    try conn.publish("orders.update", "Order Update");

    // Wait a bit for messages to be processed
    std.time.sleep(100 * std.time.ns_per_ms);

    // Verify messages were received
    try testing.expect(message_count > 0);
    log.info("Total messages processed: {d}", .{message_count});
}

test "push subscription with flow control" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PUSH_FC_STREAM",
        .subjects = &.{"tasks.*"},
        .max_msgs = 50,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    var processed_count: u32 = 0;

    const TaskHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, counter: *u32) void {
            defer js_msg.deinit();
            counter.* += 1;
            
            // Simulate some processing time
            std.time.sleep(10 * std.time.ns_per_ms);
            
            // Acknowledge successful processing
            js_msg.ack() catch |err| {
                log.err("Failed to ack task: {}", .{err});
            };
        }
    };

    // Create push consumer with flow control enabled
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "task_processor",
        .deliver_subject = "push.tasks.process", // Different from stream subjects
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .flow_control = true, // Enable flow control
        .idle_heartbeat = 30_000_000_000, // 30s - required when flow_control=true
        .max_ack_pending = 10, // Limit pending acknowledgments
    };

    var push_sub = try js.subscribe("TEST_PUSH_FC_STREAM", consumer_config, TaskHandler.handle, .{&processed_count});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish several tasks
    for (0..5) |i| {
        const task_data = try std.fmt.allocPrint(testing.allocator, "Task #{d}", .{i});
        defer testing.allocator.free(task_data);
        try conn.publish("tasks.new", task_data);
    }

    // Allow time for processing
    std.time.sleep(200 * std.time.ns_per_ms);

    try testing.expect(processed_count > 0);
    log.info("Processed {d} tasks with flow control", .{processed_count});
}

test "push subscription error handling" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Try to create push subscription without deliver_subject (should fail)
    const invalid_config = nats.ConsumerConfig{
        .durable_name = "invalid_consumer",
        .ack_policy = .explicit,
        // Missing deliver_subject
    };

    const DummyHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage) void {
            defer js_msg.deinit();
        }
    };

    // This should fail with MissingDeliverSubject error
    const result = js.subscribe("NONEXISTENT_STREAM", invalid_config, DummyHandler.handle, .{});
    try testing.expectError(error.MissingDeliverSubject, result);
}