const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "NAK redelivery with delivery count verification" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

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
                .messages = std.ArrayList([]const u8){},
                .delivery_counts = std.ArrayList(u64){},
                .allocator = allocator,
            };
        }

        fn deinit(self: *@This()) void {
            for (self.messages.items) |msg| {
                self.allocator.free(msg);
            }
            self.messages.deinit(self.allocator);
            self.delivery_counts.deinit(self.allocator);
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
            data.messages.append(data.allocator, msg_copy) catch return;

            // Get delivery count from JetStream message metadata
            const delivery_count = js_msg.metadata.num_delivered;
            data.delivery_counts.append(data.allocator, delivery_count) catch return;

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

    // Create push consumer with limited redeliveries (deliver_subject auto-generated, ack_policy defaults to .explicit)
    var push_sub = try js.subscribe("test.nak.*", NakHandler.handle, .{&test_data}, .{
        .stream = "TEST_NAK_STREAM",
        .durable = "nak_test_consumer",
        .config = .{
            .max_deliver = 3, // Allow up to 3 delivery attempts
        },
    });
    defer push_sub.deinit();

    // Publish a test message
    const test_message = "NAK test message #42";
    try conn.publish("test.nak.message", test_message);

    // Wait for message processing (should get delivered, NAK'd, then redelivered)
    var attempts: u32 = 0;
    while (attempts < 50) { // Wait up to 5 seconds
        std.Thread.sleep(100 * std.time.ns_per_ms);
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

    var push_sub = try js.subscribe("test.nak.limit.*", AlwaysNakHandler.handle, .{&test_data}, .{
        .stream = "TEST_NAK_LIMIT_STREAM",
        .durable = "nak_limit_consumer",
        .config = .{
            .deliver_policy = .all,
            .max_deliver = 2,
        },
    });
    defer push_sub.deinit();

    // Publish test message
    try conn.publish("test.nak.limit.msg", "Test max delivery limit");

    // Wait for all deliveries (should stop at max_deliver = 2)
    var wait_attempts: u32 = 0;
    while (wait_attempts < 30) { // Wait up to 3 seconds
        std.Thread.sleep(100 * std.time.ns_per_ms);
        wait_attempts += 1;

        test_data.mutex.lock();
        const count = test_data.delivery_count;
        test_data.mutex.unlock();

        // Should stop at max_deliver limit
        if (count >= 2) {
            // Give a bit more time to ensure no additional deliveries
            std.Thread.sleep(200 * std.time.ns_per_ms);
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
            log.info("- Consumer sequence: {d}", .{js_msg.metadata.sequence.consumer});
            log.info("- Stream sequence: {d}", .{js_msg.metadata.sequence.stream});
            log.info("- Delivered count: {d}", .{js_msg.metadata.num_delivered});
            log.info("- Pending count: {d}", .{js_msg.metadata.num_pending});

            // Verify metadata is populated correctly
            const stream_name = js_msg.metadata.stream;
            const consumer_name = js_msg.metadata.consumer;

            if (std.mem.eql(u8, stream_name, "TEST_METADATA_STREAM") and
                std.mem.eql(u8, consumer_name, "metadata_consumer") and
                js_msg.metadata.num_delivered == 1 and
                js_msg.metadata.sequence.consumer > 0 and
                js_msg.metadata.sequence.stream > 0)
            {
                verified.* = true;
            }

            // ACK the message
            js_msg.ack() catch |err| {
                log.err("Failed to ACK: {}", .{err});
            };
        }
    };

    var push_sub = try js.subscribe("test.metadata.*", MetadataHandler.handle, .{ &received_message, &metadata_verified, &mutex }, .{
        .stream = "TEST_METADATA_STREAM",
        .durable = "metadata_consumer",
        .config = .{
            .deliver_policy = .all,
        },
    });
    defer push_sub.deinit();

    // Publish a test message
    try conn.publish("test.metadata.msg", "Test metadata parsing");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
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

test "NAK with delay redelivery timing" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_NAK_DELAY_STREAM",
        .subjects = &.{"test.nak.delay.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data structure to track deliveries and timing
    const DelayTestData = struct {
        delivery_times: std.ArrayList(i64),
        delivery_count: u32 = 0,
        first_delivery_time: i64 = 0,
        second_delivery_time: i64 = 0,
        mutex: std.Thread.Mutex = .{},
        allocator: std.mem.Allocator,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .delivery_times = std.ArrayList(i64){},
                .allocator = allocator,
            };
        }

        fn deinit(self: *@This()) void {
            self.delivery_times.deinit(self.allocator);
        }
    };

    var test_data = DelayTestData.init(testing.allocator);
    defer test_data.deinit();

    // Message handler that NAKs with delay on first delivery
    const DelayHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *DelayTestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            const current_time = std.time.milliTimestamp();
            data.delivery_times.append(data.allocator, current_time) catch return;

            const delivery_count = js_msg.metadata.num_delivered;
            data.delivery_count += 1;

            log.info("Received message delivery #{} at time {}", .{ delivery_count, current_time });

            if (delivery_count == 1) {
                // First delivery - NAK with 500ms delay
                data.first_delivery_time = current_time;
                js_msg.nakWithDelay(500) catch |err| {
                    log.err("Failed to NAK with delay: {}", .{err});
                };
                log.info("NAK'd message with 500ms delay", .{});
            } else {
                // Second delivery - ACK it
                data.second_delivery_time = current_time;
                js_msg.ack() catch |err| {
                    log.err("Failed to ACK message: {}", .{err});
                };
                log.info("ACK'd message on redelivery", .{});
            }
        }
    };

    var push_sub = try js.subscribe("test.nak.delay.*", DelayHandler.handle, .{&test_data}, .{
        .stream = "TEST_NAK_DELAY_STREAM",
        .durable = "nak_delay_consumer",
        .config = .{
            .deliver_policy = .all,
            .max_deliver = 3,
        },
    });
    defer push_sub.deinit();

    // Publish a test message
    const test_message = "NAK delay test message";
    try conn.publish("test.nak.delay.message", test_message);

    // Wait for both deliveries (original + redelivery after delay)
    var attempts: u32 = 0;
    while (attempts < 100) { // Wait up to 10 seconds
        std.Thread.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const count = test_data.delivery_count;
        test_data.mutex.unlock();

        if (count >= 2) break; // Got both deliveries
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    // Should have received the message exactly twice
    try testing.expectEqual(@as(u32, 2), test_data.delivery_count);
    try testing.expect(test_data.delivery_times.items.len >= 2);

    // Verify the delay was respected (should be at least 400ms, allowing some tolerance)
    const delivery_delay = test_data.second_delivery_time - test_data.first_delivery_time;
    try testing.expect(delivery_delay >= 400); // At least 400ms (allowing for some timing variance)

    log.info("NAK with delay test completed successfully:", .{});
    log.info("- Total deliveries: {}", .{test_data.delivery_count});
    log.info("- First delivery time: {}", .{test_data.first_delivery_time});
    log.info("- Second delivery time: {}", .{test_data.second_delivery_time});
    log.info("- Actual delay: {}ms", .{delivery_delay});
}

test "NAK with zero delay behaves like regular NAK" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_NAK_ZERO_DELAY_STREAM",
        .subjects = &.{"test.nak.zero.*"},
        .max_msgs = 100,
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    var delivery_count: u32 = 0;
    var mutex = std.Thread.Mutex{};

    // Message handler that NAKs with zero delay on first delivery
    const ZeroDelayHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, count: *u32, mtx: *std.Thread.Mutex) void {
            defer js_msg.deinit();

            mtx.lock();
            defer mtx.unlock();

            const delivery_num = js_msg.metadata.num_delivered;
            count.* += 1;

            log.info("Received message delivery #{}", .{delivery_num});

            if (delivery_num == 1) {
                // First delivery - NAK with zero delay (should behave like regular NAK)
                js_msg.nakWithDelay(0) catch |err| {
                    log.err("Failed to NAK with zero delay: {}", .{err});
                };
                log.info("NAK'd message with zero delay", .{});
            } else {
                // Second delivery - ACK it
                js_msg.ack() catch |err| {
                    log.err("Failed to ACK message: {}", .{err});
                };
                log.info("ACK'd message on redelivery", .{});
            }
        }
    };

    var push_sub = try js.subscribe("test.nak.zero.*", ZeroDelayHandler.handle, .{ &delivery_count, &mutex }, .{
        .stream = "TEST_NAK_ZERO_DELAY_STREAM",
        .durable = "nak_zero_delay_consumer",
        .config = .{
            .deliver_policy = .all,
            .max_deliver = 3,
        },
    });
    defer push_sub.deinit();

    // Publish a test message
    try conn.publish("test.nak.zero.message", "Zero delay NAK test");

    // Wait for both deliveries
    var attempts: u32 = 0;
    while (attempts < 30) { // Wait up to 3 seconds
        std.Thread.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        mutex.lock();
        const count = delivery_count;
        mutex.unlock();

        if (count >= 2) break;
    }

    // Verify results
    mutex.lock();
    defer mutex.unlock();

    // Should have received the message exactly twice
    try testing.expectEqual(@as(u32, 2), delivery_count);

    log.info("NAK with zero delay test completed successfully", .{});
    log.info("- Total deliveries: {}", .{delivery_count});
}
