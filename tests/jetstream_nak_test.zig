const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.jetstream_nak_test);

test "NAK redelivery with delivery count verification" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_NAK_STREAM",
        .subjects = &.{"test.nak.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data structure to track deliveries
    const TestData = struct {
        messages: std.ArrayList([]const u8),
        delivery_counts: std.ArrayList(u64),
        first_delivery_data: ?[]const u8 = null,
        nak_count: u32 = 0,
        mutex: std.Thread.Mutex = .{},
        allocator: std.mem.Allocator,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .messages = std.ArrayList([]const u8).init(allocator),
                .delivery_counts = std.ArrayList(u64).init(allocator),
                .allocator = allocator,
            };
        }

        fn deinit(self: *@This()) void {
            for (self.messages.items) |msg| {
                self.allocator.free(msg);
            }
            self.messages.deinit();
            self.delivery_counts.deinit();
            if (self.first_delivery_data) |data| {
                self.allocator.free(data);
            }
        }
    };

    var test_data = TestData.init(testing.allocator);
    defer test_data.deinit();

    // Message handler that NAKs the first delivery and ACKs the second
    const NakHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *TestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            // Store message data copy for comparison
            const msg_copy = data.allocator.dupe(u8, js_msg.msg.data) catch return;
            data.messages.append(msg_copy) catch return;

            // Get delivery count from JetStream message metadata
            const delivery_count = js_msg.metadata.num_delivered;
            data.delivery_counts.append(delivery_count) catch return;

            log.info("Received message (delivery #{}): {s}", .{ delivery_count, js_msg.msg.data });

            if (delivery_count == 1) {
                // First delivery - store the data and NAK it
                data.first_delivery_data = data.allocator.dupe(u8, js_msg.msg.data) catch return;
                data.nak_count += 1;
                js_msg.nak() catch |err| {
                    log.err("Failed to NAK message: {}", .{err});
                };
                log.info("NAK'd message on first delivery", .{});
            } else {
                // Second or later delivery - ACK it
                js_msg.ack() catch |err| {
                    log.err("Failed to ACK message: {}", .{err});
                };
                log.info("ACK'd message on delivery #{}", .{delivery_count});
            }
        }
    };

    // Create push consumer with limited redeliveries
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "nak_test_consumer",
        .deliver_subject = "push.nak.test",
        .ack_policy = .explicit,
        .max_deliver = 3, // Allow up to 3 delivery attempts
    };

    var push_sub = try js.subscribe("TEST_NAK_STREAM", consumer_config, NakHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish a test message
    const test_message = "NAK test message #42";
    try conn.publish("test.nak.message", test_message);

    // Wait for message processing (should get delivered, NAK'd, then redelivered)
    var attempts: u32 = 0;
    while (attempts < 50) { // Wait up to 5 seconds
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const message_count = test_data.messages.items.len;
        test_data.mutex.unlock();

        if (message_count >= 2) break; // Got at least 2 deliveries
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    // Should have received the message at least twice (original + redelivery after NAK)
    try testing.expect(test_data.messages.items.len >= 2);
    try testing.expect(test_data.delivery_counts.items.len >= 2);
    try testing.expect(test_data.nak_count >= 1);

    // Verify first delivery had count = 1
    try testing.expectEqual(@as(u64, 1), test_data.delivery_counts.items[0]);

    // Verify second delivery had count = 2
    try testing.expectEqual(@as(u64, 2), test_data.delivery_counts.items[1]);

    // Verify both deliveries have the same message content
    try testing.expectEqualStrings(test_message, test_data.messages.items[0]);
    try testing.expectEqualStrings(test_message, test_data.messages.items[1]);

    // Verify the redelivered message is identical to the first
    try testing.expectEqualStrings(test_data.first_delivery_data.?, test_data.messages.items[1]);

    log.info("NAK redelivery test completed successfully:", .{});
    log.info("- Total deliveries: {}", .{test_data.messages.items.len});
    log.info("- NAK count: {}", .{test_data.nak_count});
    log.info("- First delivery count: {}", .{test_data.delivery_counts.items[0]});
    log.info("- Second delivery count: {}", .{test_data.delivery_counts.items[1]});
}

test "NAK with max delivery limit" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_NAK_LIMIT_STREAM",
        .subjects = &.{"test.nak.limit.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Combined test data structure
    const LimitTestData = struct {
        delivery_count: u32 = 0,
        received_deliveries: [5]u64 = undefined,
        mutex: std.Thread.Mutex = .{},
    };

    var test_data = LimitTestData{};

    // Handler that always NAKs to test max delivery limit
    const AlwaysNakHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *LimitTestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            // Get delivery count from JetStream message metadata
            const delivery_num = js_msg.metadata.num_delivered;

            log.info("Received message delivery #{}", .{delivery_num});

            if (data.delivery_count < data.received_deliveries.len) {
                data.received_deliveries[data.delivery_count] = delivery_num;
                data.delivery_count += 1;
            }

            // Always NAK to trigger redelivery (up to max_deliver limit)
            js_msg.nak() catch |err| {
                log.err("Failed to NAK: {}", .{err});
            };
        }
    };

    // Consumer with max_deliver = 2 (original + 1 redelivery)
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "nak_limit_consumer",
        .deliver_subject = "push.nak.limit",
        .ack_policy = .explicit,
        .max_deliver = 2,
    };

    var push_sub = try js.subscribe("TEST_NAK_LIMIT_STREAM", consumer_config, AlwaysNakHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish test message
    try conn.publish("test.nak.limit.msg", "Test max delivery limit");

    // Wait for all deliveries (should stop at max_deliver = 2)
    var wait_attempts: u32 = 0;
    while (wait_attempts < 30) { // Wait up to 3 seconds
        std.time.sleep(100 * std.time.ns_per_ms);
        wait_attempts += 1;

        test_data.mutex.lock();
        const count = test_data.delivery_count;
        test_data.mutex.unlock();

        // Should stop at max_deliver limit
        if (count >= 2) {
            // Give a bit more time to ensure no additional deliveries
            std.time.sleep(200 * std.time.ns_per_ms);
            break;
        }
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    // Should have received exactly max_deliver deliveries
    try testing.expectEqual(@as(u32, 2), test_data.delivery_count);

    // Verify delivery counts increment correctly
    try testing.expectEqual(@as(u64, 1), test_data.received_deliveries[0]); // First delivery
    try testing.expectEqual(@as(u64, 2), test_data.received_deliveries[1]); // Second delivery

    log.info("Max delivery limit test completed:", .{});
    log.info("- Total deliveries received: {}", .{test_data.delivery_count});
    log.info("- First delivery count: {}", .{test_data.received_deliveries[0]});
    log.info("- Second delivery count: {}", .{test_data.received_deliveries[1]});
}

test "JetStream message metadata parsing" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_METADATA_STREAM",
        .subjects = &.{"test.metadata.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    var received_message = false;
    var metadata_verified = false;
    var mutex = std.Thread.Mutex{};

    // Handler that verifies all JetStream metadata fields
    const MetadataHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, received: *bool, verified: *bool, mtx: *std.Thread.Mutex) void {
            defer js_msg.deinit();

            mtx.lock();
            defer mtx.unlock();

            received.* = true;

            log.info("JetStream message metadata:", .{});
            log.info("- Stream: {s}", .{js_msg.metadata.stream});
            log.info("- Consumer: {s}", .{js_msg.metadata.consumer});
            log.info("- Consumer sequence: {?}", .{js_msg.metadata.sequence.consumer});
            log.info("- Stream sequence: {?}", .{js_msg.metadata.sequence.stream});
            log.info("- Delivered count: {}", .{js_msg.metadata.num_delivered});
            log.info("- Pending count: {?}", .{js_msg.metadata.num_pending});

            // Verify metadata is populated correctly
            const stream_name = js_msg.metadata.stream;
            const consumer_name = js_msg.metadata.consumer;

            if (std.mem.eql(u8, stream_name, "TEST_METADATA_STREAM") and
                std.mem.eql(u8, consumer_name, "metadata_consumer") and
                js_msg.metadata.num_delivered == 1 and
                js_msg.metadata.sequence.consumer != null and
                js_msg.metadata.sequence.stream != null)
            {
                verified.* = true;
            }

            // ACK the message
            js_msg.ack() catch |err| {
                log.err("Failed to ACK: {}", .{err});
            };
        }
    };

    // Create consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "metadata_consumer",
        .deliver_subject = "push.metadata.test",
        .ack_policy = .explicit,
    };

    var push_sub = try js.subscribe("TEST_METADATA_STREAM", consumer_config, MetadataHandler.handle, .{ &received_message, &metadata_verified, &mutex });
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish a test message
    try conn.publish("test.metadata.msg", "Test metadata parsing");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        mutex.lock();
        const done = received_message;
        mutex.unlock();

        if (done) break;
    }

    // Verify results
    mutex.lock();
    defer mutex.unlock();

    try testing.expect(received_message);
    try testing.expect(metadata_verified);

    log.info("JetStream metadata parsing test completed successfully", .{});
}
