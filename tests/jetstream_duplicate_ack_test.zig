const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "ack should succeed on first call" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_DUP_ACK_STREAM",
        .subjects = &.{"test.dup.ack.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data to track received messages
    const TestData = struct {
        received: bool = false,
        acknowledged: bool = false,
        mutex: std.Thread.Mutex = .{},
    };

    var test_data = TestData{};

    // Handler that performs first ack
    const AckHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *TestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            data.received = true;

            // First ack should succeed
            js_msg.ack() catch |err| {
                log.err("Failed to ACK message: {}", .{err});
                return;
            };

            // Verify message is acknowledged
            data.acknowledged = js_msg.isAcked();
        }
    };

    // Create consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "dup_ack_consumer",
        .deliver_subject = "push.dup.ack",
        .ack_policy = .explicit,
    };

    var push_sub = try js.subscribe("TEST_DUP_ACK_STREAM", consumer_config, AckHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish test message
    try conn.publish("test.dup.ack.msg", "test ack message");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const done = test_data.received;
        test_data.mutex.unlock();

        if (done) break;
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    try testing.expect(test_data.received);
    try testing.expect(test_data.acknowledged);
}

test "ack should fail on second call" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_DUP_ACK2_STREAM",
        .subjects = &.{"test.dup.ack2.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data to track results
    const TestData = struct {
        received: bool = false,
        first_ack_success: bool = false,
        second_ack_failed: bool = false,
        mutex: std.Thread.Mutex = .{},
    };

    var test_data = TestData{};

    // Handler that performs double ack
    const DoubleAckHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *TestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            data.received = true;

            // First ack should succeed
            if (js_msg.ack()) {
                data.first_ack_success = true;
            } else |_| {
                return;
            }

            // Second ack should fail with AlreadyAcked
            if (js_msg.ack()) {
                // Should not reach here
            } else |err| {
                if (err == nats.AckError.AlreadyAcked) {
                    data.second_ack_failed = true;
                }
            }
        }
    };

    // Create consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "dup_ack2_consumer",
        .deliver_subject = "push.dup.ack2",
        .ack_policy = .explicit,
    };

    var push_sub = try js.subscribe("TEST_DUP_ACK2_STREAM", consumer_config, DoubleAckHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish test message
    try conn.publish("test.dup.ack2.msg", "test double ack message");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const done = test_data.received;
        test_data.mutex.unlock();

        if (done) break;
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    try testing.expect(test_data.received);
    try testing.expect(test_data.first_ack_success);
    try testing.expect(test_data.second_ack_failed);
}

test "nak should fail after ack" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_ACK_NAK_STREAM",
        .subjects = &.{"test.ack.nak.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data to track results
    const TestData = struct {
        received: bool = false,
        ack_success: bool = false,
        nak_failed: bool = false,
        mutex: std.Thread.Mutex = .{},
    };

    var test_data = TestData{};

    // Handler that acks then naks
    const AckNakHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *TestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            data.received = true;

            // First ack should succeed
            if (js_msg.ack()) {
                data.ack_success = true;
            } else |_| {
                return;
            }

            // NAK after ack should fail
            if (js_msg.nak()) {
                // Should not reach here
            } else |err| {
                if (err == nats.AckError.AlreadyAcked) {
                    data.nak_failed = true;
                }
            }
        }
    };

    // Create consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "ack_nak_consumer",
        .deliver_subject = "push.ack.nak",
        .ack_policy = .explicit,
    };

    var push_sub = try js.subscribe("TEST_ACK_NAK_STREAM", consumer_config, AckNakHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish test message
    try conn.publish("test.ack.nak.msg", "test ack then nak message");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const done = test_data.received;
        test_data.mutex.unlock();

        if (done) break;
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    try testing.expect(test_data.received);
    try testing.expect(test_data.ack_success);
    try testing.expect(test_data.nak_failed);
}

test "inProgress can be called multiple times" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PROGRESS_STREAM",
        .subjects = &.{"test.progress.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Test data to track results
    const TestData = struct {
        received: bool = false,
        progress_calls: u32 = 0,
        final_ack: bool = false,
        not_acknowledged_after_progress: bool = false,
        mutex: std.Thread.Mutex = .{},
    };

    var test_data = TestData{};

    // Handler that calls inProgress multiple times then acks
    const ProgressHandler = struct {
        fn handle(js_msg: *nats.JetStreamMessage, data: *TestData) void {
            defer js_msg.deinit();

            data.mutex.lock();
            defer data.mutex.unlock();

            data.received = true;

            // Call inProgress multiple times
            var i: u32 = 0;
            while (i < 3) : (i += 1) {
                if (js_msg.inProgress()) {
                    data.progress_calls += 1;
                } else |_| {
                    return;
                }
            }

            // Check not acknowledged after inProgress calls
            if (!js_msg.isAcked()) {
                data.not_acknowledged_after_progress = true;
            }

            // Final ack should still work
            if (js_msg.ack()) {
                data.final_ack = true;
            } else |_| {
                return;
            }
        }
    };

    // Create consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "progress_consumer",
        .deliver_subject = "push.progress",
        .ack_policy = .explicit,
    };

    var push_sub = try js.subscribe("TEST_PROGRESS_STREAM", consumer_config, ProgressHandler.handle, .{&test_data});
    defer push_sub.deinit();
    defer push_sub.unsubscribe() catch {};

    // Publish test message
    try conn.publish("test.progress.msg", "test progress message");

    // Wait for message processing
    var attempts: u32 = 0;
    while (attempts < 30) {
        std.time.sleep(100 * std.time.ns_per_ms);
        attempts += 1;

        test_data.mutex.lock();
        const done = test_data.received;
        test_data.mutex.unlock();

        if (done) break;
    }

    // Verify results
    test_data.mutex.lock();
    defer test_data.mutex.unlock();

    try testing.expect(test_data.received);
    try testing.expectEqual(@as(u32, 3), test_data.progress_calls);
    try testing.expect(test_data.not_acknowledged_after_progress);
    try testing.expect(test_data.final_ack);
}
