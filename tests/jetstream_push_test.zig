const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const xsync = @import("xsync");
const utils = @import("utils.zig");

const log = std.log.default;

test "basic push subscription" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
    const HandlerContext = struct {
        count: std.atomic.Value(u32) = .init(0),
        called: xsync.Event = .init,
    };
    var handler_context: HandlerContext = .{};

    // Define message handler
    const MessageHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, context: *HandlerContext) void {
            defer js_msg.deinit();
            const count = context.count.fetchAdd(1, .release) + 1;
            context.called.set(std.testing.io);

            log.info("Received message #{d}: {s}", .{ count, js_msg.msg.data });

            // Acknowledge the message
            js_msg.ack() catch |err| {
                log.err("Failed to ack message: {}", .{err});
            };
        }
    };

    // Subscribe to push consumer (deliver_subject auto-generated, ack_policy defaults to .explicit)
    var push_sub = try js.subscribe("orders.*", MessageHandler.handle, .{&handler_context}, .{
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

    try handler_context.called.wait(io);

    // Verify messages were received
    const message_count = handler_context.count.load(.acquire);
    try testing.expect(message_count > 0);
    log.info("Total messages processed: {d}", .{message_count});
}

test "push subscription with flow control" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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

    const HandlerContext = struct {
        count: std.atomic.Value(u32) = .init(0),
        called: xsync.Event = .init,
        io: std.Io,
    };
    var handler_context: HandlerContext = .{ .io = io };

    const TaskHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, context: *HandlerContext) void {
            defer js_msg.deinit();
            _ = context.count.fetchAdd(1, .release);

            // Simulate some processing time
            context.io.sleep(.fromMilliseconds(10), .awake) catch {};

            // Acknowledge successful processing
            js_msg.ack() catch |err| {
                log.err("Failed to ack task: {}", .{err});
            };
            context.called.set(std.testing.io);
        }
    };

    // Subscribe with flow control enabled (deliver_subject auto-generated)
    var push_sub = try js.subscribe("tasks.*", TaskHandler.handle, .{&handler_context}, .{
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

    try handler_context.called.wait(io);

    const processed_count = handler_context.count.load(.acquire);
    try testing.expect(processed_count > 0);
    log.info("Processed {d} tasks with flow control", .{processed_count});
}

test "push subscription error handling" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
