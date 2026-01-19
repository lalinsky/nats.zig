const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "basic push subscription" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

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

    // Subscribe to push consumer (deliver_subject auto-generated, ack_policy defaults to .explicit)
    var push_sub = try js.subscribe("orders.*", MessageHandler.handle, .{&message_count}, .{
        .stream = "TEST_PUSH_STREAM",
        .durable = "test_push_consumer",
        .config = .{
            .deliver_policy = .all,
        },
    });
    defer push_sub.deinit();

    // Publish some test messages
    try conn.publish("orders.new", "Order #1");
    try conn.publish("orders.new", "Order #2");
    try conn.publish("orders.update", "Order Update");

    // Wait a bit for messages to be processed
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Verify messages were received
    try testing.expect(message_count > 0);
    log.info("Total messages processed: {d}", .{message_count});
}

test "push subscription with flow control" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

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
            std.Thread.sleep(10 * std.time.ns_per_ms);

            // Acknowledge successful processing
            js_msg.ack() catch |err| {
                log.err("Failed to ack task: {}", .{err});
            };
        }
    };

    // Subscribe with flow control enabled (deliver_subject auto-generated)
    var push_sub = try js.subscribe("tasks.*", TaskHandler.handle, .{&processed_count}, .{
        .stream = "TEST_PUSH_FC_STREAM",
        .durable = "task_processor",
        .config = .{
            .deliver_policy = .all,
            .flow_control = true, // Enable flow control
            .idle_heartbeat = 30_000_000_000, // 30s - required when flow_control=true
            .max_ack_pending = 10, // Limit pending acknowledgments
        },
    });
    defer push_sub.deinit();

    // Publish several tasks
    for (0..5) |i| {
        const task_data = try std.fmt.allocPrint(testing.allocator, "Task #{d}", .{i});
        defer testing.allocator.free(task_data);
        try conn.publish("tasks.new", task_data);
    }

    // Allow time for processing
    std.Thread.sleep(200 * std.time.ns_per_ms);

    try testing.expect(processed_count > 0);
    log.info("Processed {d} tasks with flow control", .{processed_count});
}

test "push subscription error handling" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    const DummyHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage) void {
            defer js_msg.deinit();
        }
    };

    // This should fail with StreamNotFound error since stream doesn't exist
    const result = js.subscribe("nonexistent.*", DummyHandler.handle, .{}, .{
        .stream = "NONEXISTENT_STREAM",
        .durable = "test_consumer",
    });
    try testing.expectError(error.StreamNotFound, result);
}
