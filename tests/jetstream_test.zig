const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "connect" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();
}

test "get account info" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    var result = try js.getAccountInfo();
    defer result.deinit();

    try testing.expect(result.value.streams >= 0);
    try testing.expect(result.value.consumers >= 0);
}

test "add consumer" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const consumer_name = try utils.generateUniqueConsumerName(testing.allocator);
    defer testing.allocator.free(consumer_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // First create a stream for the consumer
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .deliver_policy = .all,
    };

    var consumer_info = try js.addConsumer(stream_name, consumer_config);
    defer consumer_info.deinit();

    // Verify consumer was created with correct configuration
    try testing.expectEqualStrings(consumer_name, consumer_info.value.name);
    try testing.expectEqualStrings(stream_name, consumer_info.value.stream_name);
}

test "list consumer names" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const consumer_name = try utils.generateUniqueConsumerName(testing.allocator);
    defer testing.allocator.free(consumer_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
    };
    var consumer_info = try js.addConsumer(stream_name, consumer_config);
    defer consumer_info.deinit();

    // List consumer names and verify our consumer is included
    var result = try js.listConsumerNames(stream_name);
    defer result.deinit();

    // Should contain at least our test consumer
    try testing.expect(result.value.len >= 1);

    // Find our consumer in the list
    var found = false;
    for (result.value) |name| {
        if (std.mem.eql(u8, name, consumer_name)) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "list consumers" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const consumer_name = try utils.generateUniqueConsumerName(testing.allocator);
    defer testing.allocator.free(consumer_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .max_ack_pending = 100,
    };
    var consumer_info = try js.addConsumer(stream_name, consumer_config);
    defer consumer_info.deinit();

    // List consumers and verify our consumer is included
    var result = try js.listConsumers(stream_name);
    defer result.deinit();

    // Should contain at least our test consumer
    try testing.expect(result.value.len >= 1);

    // Find our consumer in the list and verify its configuration
    var found = false;
    for (result.value) |info| {
        if (std.mem.eql(u8, info.config.durable_name.?, consumer_name)) {
            found = true;
            try testing.expect(info.config.ack_policy == .explicit);
            try testing.expect(info.config.max_ack_pending == 100);
            break;
        }
    }
    try testing.expect(found);
}

test "get consumer info" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const consumer_name = try utils.generateUniqueConsumerName(testing.allocator);
    defer testing.allocator.free(consumer_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
        .max_deliver = 5,
    };
    var consumer_info = try js.addConsumer(stream_name, consumer_config);
    defer consumer_info.deinit();

    // Get consumer info
    var retrieved_info = try js.getConsumerInfo(stream_name, consumer_name);
    defer retrieved_info.deinit();

    // Verify the retrieved info matches what we created
    // Note: stream_name is not included in consumer info responses
    try testing.expectEqualStrings(consumer_name, retrieved_info.value.config.durable_name.?);
    try testing.expect(retrieved_info.value.config.ack_policy == .explicit);
    try testing.expect(retrieved_info.value.config.max_deliver == 5);
}

test "delete consumer" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const consumer_name = try utils.generateUniqueConsumerName(testing.allocator);
    defer testing.allocator.free(consumer_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer to delete
    const consumer_config = nats.ConsumerConfig{
        .durable_name = consumer_name,
        .ack_policy = .explicit,
    };
    var consumer_info = try js.addConsumer(stream_name, consumer_config);
    defer consumer_info.deinit();

    // Verify consumer exists
    var consumers_before = try js.listConsumerNames(stream_name);
    defer consumers_before.deinit();

    var found_before = false;
    for (consumers_before.value) |name| {
        if (std.mem.eql(u8, name, consumer_name)) {
            found_before = true;
            break;
        }
    }
    try testing.expect(found_before);

    // Delete the consumer
    try js.deleteConsumer(stream_name, consumer_name);

    // Verify consumer no longer exists
    var consumers_after = try js.listConsumerNames(stream_name);
    defer consumers_after.deinit();

    var found_after = false;
    for (consumers_after.value) |name| {
        if (std.mem.eql(u8, name, consumer_name)) {
            found_after = true;
            break;
        }
    }
    try testing.expect(!found_after);
}

// // Test stream management functionality
// test "jetstream stream creation and management" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     // Create JetStream context
//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     // Create a test stream
//     const stream_name = "TEST_STREAM_CREATION";
//     const stream_config = nats.StreamConfig{
//         .name = stream_name,
//         .subjects = &.{"test.stream.*"},
//         .retention = .limits,
//         .storage = .file,
//         .max_msgs = 1000,
//         .max_bytes = 1024 * 1024, // 1MB
//         .max_age = 0, // No age limit
//         .num_replicas = 1,
//     };

//     // Test stream creation
//     log.info("Creating stream: {s}", .{stream_name});
//     const stream = js.createStream(stream_config) catch |err| {
//         log.err("Failed to create stream: {}", .{err});
//         return err;
//     };
//     defer stream.deinit();

//     // Verify stream was created successfully
//     try testing.expect(std.mem.eql(u8, stream.info.config().name, stream_name));

//     // Test getting stream info
//     log.info("Getting stream info for: {s}", .{stream_name});
//     const retrieved_stream = js.getStream(stream_name) catch |err| {
//         log.err("Failed to get stream info: {}", .{err});
//         return err;
//     };
//     defer retrieved_stream.deinit();

//     try testing.expect(std.mem.eql(u8, retrieved_stream.info.config().name, stream_name));

//     // Test listing streams
//     log.info("Listing all streams", .{});
//     const stream_list = js.listStreams(testing.allocator) catch |err| {
//         log.err("Failed to list streams: {}", .{err});
//         return err;
//     };
//     defer {
//         // Deinit each stream info and free the slice
//         for (stream_list) |stream_info| {
//             stream_info.deinit();
//             testing.allocator.destroy(stream_info);
//         }
//         testing.allocator.free(stream_list);
//     }

//     // Verify our stream is in the list
//     var found = false;
//     for (stream_list) |info| {
//         const config = info.config();
//         if (std.mem.eql(u8, config.name, stream_name)) {
//             found = true;
//             break;
//         }
//     }
//     try testing.expect(found);

//     // Test stream deletion
//     log.info("Deleting stream: {s}", .{stream_name});
//     js.deleteStream(stream_name) catch |err| {
//         log.err("Failed to delete stream: {}", .{err});
//         return err;
//     };

//     // Verify stream was deleted
//     const deleted_stream_result = js.getStream(stream_name);
//     try testing.expectError(nats.JetStreamError.StreamNotFound, deleted_stream_result);
// }

// test "jetstream stream configuration options" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     // Test different stream configurations
//     const configs = [_]struct {
//         name: []const u8,
//         config: nats.StreamConfig,
//     }{
//         .{
//             .name = "MEMORY_STREAM",
//             .config = .{
//                 .name = "MEMORY_STREAM",
//                 .subjects = &.{"memory.test.*"},
//                 .storage = .memory,
//                 .max_msgs = 100,
//                 .retention = .limits,
//             },
//         },
//         .{
//             .name = "WORKQUEUE_STREAM",
//             .config = .{
//                 .name = "WORKQUEUE_STREAM",
//                 .subjects = &.{"work.queue.*"},
//                 .retention = .workqueue,
//                 .storage = .file,
//                 .max_msgs = 500,
//             },
//         },
//         .{
//             .name = "INTEREST_STREAM",
//             .config = .{
//                 .name = "INTEREST_STREAM",
//                 .subjects = &.{"interest.test.*"},
//                 .retention = .interest,
//                 .storage = .file,
//                 .max_msgs = 200,
//             },
//         },
//     };

//     // Create each stream and verify configuration
//     for (configs) |test_config| {
//         log.info("Creating stream with config: {s}", .{test_config.name});

//         const stream = js.createStream(test_config.config) catch |err| {
//             log.err("Failed to create stream {s}: {}", .{ test_config.name, err });
//             return err;
//         };
//         defer stream.deinit();

//         // Verify configuration was applied correctly
//         const config = stream.info.config();
//         try testing.expect(std.mem.eql(u8, config.name, test_config.name));
//         try testing.expect(config.storage == test_config.config.storage);
//         try testing.expect(config.retention == test_config.config.retention);
//         try testing.expect(config.max_msgs == test_config.config.max_msgs);

//         // Cleanup
//         js.deleteStream(test_config.name) catch |err| {
//             log.warn("Failed to clean up stream {s}: {}", .{ test_config.name, err });
//         };
//     }
// }

// test "jetstream stream operations" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     const stream_name = "TEST_OPERATIONS";
//     const stream_config = nats.StreamConfig{
//         .name = stream_name,
//         .subjects = &.{"ops.test.*"},
//         .retention = .limits,
//         .storage = .file,
//         .max_msgs = 100,
//     };

//     // Create stream for testing operations
//     const stream = js.createStream(stream_config) catch |err| {
//         log.err("Failed to create stream for operations test: {}", .{err});
//         return err;
//     };
//     defer stream.deinit();
//     defer js.deleteStream(stream_name) catch {};

//     // Test publishing messages to stream
//     log.info("Publishing test messages", .{});
//     const test_messages = [_]struct {
//         subject: []const u8,
//         data: []const u8,
//     }{
//         .{ .subject = "ops.test.msg1", .data = "Hello JetStream 1" },
//         .{ .subject = "ops.test.msg2", .data = "Hello JetStream 2" },
//         .{ .subject = "ops.test.msg3", .data = "Hello JetStream 3" },
//     };

//     for (test_messages) |msg| {
//         const pub_ack = js.publish(msg.subject, msg.data) catch |err| {
//             log.err("Failed to publish message to {s}: {}", .{ msg.subject, err });
//             return err;
//         };
//         defer pub_ack.deinit();

//         try testing.expect(pub_ack.seq() > 0);
//         try testing.expect(std.mem.eql(u8, pub_ack.stream(), stream_name));
//     }

//     // Test getting stream info after publishing
//     const updated_info = stream.getInfo() catch |err| {
//         log.err("Failed to get updated stream info: {}", .{err});
//         return err;
//     };
//     defer updated_info.deinit();

//     // Should have 3 messages now
//     const state = updated_info.state();
//     try testing.expect(state.messages == 3);
//     try testing.expect(state.last_seq >= 3);

//     // Test getting specific messages
//     log.info("Testing message retrieval", .{});
//     const msg1 = stream.getMessage(1) catch |err| {
//         log.err("Failed to get message 1: {}", .{err});
//         return err;
//     };
//     defer msg1.deinit();

//     try testing.expect(std.mem.eql(u8, msg1.data, "Hello JetStream 1"));

//     // Test message deletion
//     log.info("Testing message deletion", .{});
//     const deleted = stream.deleteMessage(2) catch |err| {
//         log.err("Failed to delete message 2: {}", .{err});
//         return err;
//     };
//     try testing.expect(deleted);

//     // Test stream purging
//     log.info("Testing stream purge", .{});
//     const purge_response = stream.purge(null) catch |err| {
//         log.err("Failed to purge stream: {}", .{err});
//         return err;
//     };
//     try testing.expect(purge_response.success);
// }

// test "jetstream stream update" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     const stream_name = "TEST_UPDATE";
//     const initial_config = nats.StreamConfig{
//         .name = stream_name,
//         .subjects = &.{"update.test.*"},
//         .retention = .limits,
//         .storage = .file,
//         .max_msgs = 100,
//     };

//     // Create initial stream
//     const stream = js.createStream(initial_config) catch |err| {
//         log.err("Failed to create stream for update test: {}", .{err});
//         return err;
//     };
//     defer stream.deinit();
//     defer js.deleteStream(stream_name) catch {};

//     // Update stream configuration
//     log.info("Updating stream configuration", .{});
//     var updated_config = initial_config;
//     updated_config.max_msgs = 200; // Double the limit

//     const updated_stream = js.updateStream(stream_name, updated_config) catch |err| {
//         log.err("Failed to update stream: {}", .{err});
//         return err;
//     };
//     defer updated_stream.deinit();

//     // Verify the update was applied
//     try testing.expect(updated_stream.info.config().max_msgs == 200);
// }

// test "jetstream account info" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     // Test getting account information
//     log.info("Getting account info", .{});
//     const account_info = js.accountInfo() catch |err| {
//         log.err("Failed to get account info: {}", .{err});
//         return err;
//     };
//     defer account_info.deinit();

//     // Basic validation - account info should have some reasonable values
//     try testing.expect(account_info.api().total >= 0);
//     try testing.expect(account_info.api().errors >= 0);
//     try testing.expect(account_info.streams() >= 0);
//     try testing.expect(account_info.consumers() >= 0);
// }

// test "jetstream error handling" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     // Test error handling for non-existent stream
//     log.info("Testing error handling", .{});
//     const non_existent_result = js.getStream("NON_EXISTENT_STREAM");
//     try testing.expectError(nats.JetStreamError.StreamNotFound, non_existent_result);

//     // Test deleting non-existent stream
//     const delete_result = js.deleteStream("NON_EXISTENT_STREAM");
//     try testing.expectError(nats.JetStreamError.StreamNotFound, delete_result);

//     // Test invalid stream configuration
//     const invalid_config = nats.StreamConfig{
//         .name = "", // Empty name should be invalid
//         .subjects = &.{},
//     };

//     const invalid_create_result = js.createStream(invalid_config);
//     try testing.expectError(nats.JetStreamError.StreamInvalidConfig, invalid_create_result);
// }

// test "jetstream concurrent stream operations" {
//     const conn = try utils.createDefaultConnection();
//     defer utils.closeConnection(conn);

//     var js = conn.jetstreamDefault();
//     defer js.deinit();

//     const stream_name = "TEST_CONCURRENT";
//     const stream_config = nats.StreamConfig{
//         .name = stream_name,
//         .subjects = &.{"concurrent.test.*"},
//         .retention = .limits,
//         .storage = .file,
//         .max_msgs = 1000,
//     };

//     // Create stream
//     const stream = js.createStream(stream_config) catch |err| {
//         log.err("Failed to create stream for concurrent test: {}", .{err});
//         return err;
//     };
//     defer stream.deinit();
//     defer js.deleteStream(stream_name) catch {};

//     // Publish multiple messages concurrently (simulated)
//     log.info("Testing concurrent publishing", .{});
//     const num_messages = 10;
//     var published_count: u32 = 0;

//     for (0..num_messages) |i| {
//         const subject = try std.fmt.allocPrint(testing.allocator, "concurrent.test.{d}", .{i});
//         defer testing.allocator.free(subject);

//         const data = try std.fmt.allocPrint(testing.allocator, "Message {d}", .{i});
//         defer testing.allocator.free(data);

//         const pub_ack = js.publish(subject, data) catch |err| {
//             log.err("Failed to publish concurrent message {d}: {}", .{ i, err });
//             return err;
//         };
//         defer pub_ack.deinit();

//         published_count += 1;
//         try testing.expect(pub_ack.seq() > 0);
//     }

//     try testing.expect(published_count == num_messages);

//     // Verify all messages were stored
//     const final_info = stream.getInfo() catch |err| {
//         log.err("Failed to get final stream info: {}", .{err});
//         return err;
//     };
//     defer final_info.deinit();

//     try testing.expect(final_info.state().messages == num_messages);
// }

test "JetStream publish basic message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish a message using JetStream publish
    const test_data = "Hello JetStream!";
    var pub_ack = try js.publish(subject, test_data, .{});
    defer pub_ack.deinit();

    // Verify publish acknowledgment
    try testing.expectEqualStrings(stream_name, pub_ack.value.stream);
    try testing.expect(pub_ack.value.sequence > 0);
    try testing.expect(!pub_ack.value.duplicate);
}

test "JetStream publish with message deduplication" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream with duplicate window
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
        .duplicate_window = 60_000_000_000, // 60 seconds
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    const test_data = "Deduplicated message";
    const msg_id = "unique-msg-id-12345";

    // Publish the same message twice with the same message ID
    var pub_ack1 = try js.publish(subject, test_data, .{ .msg_id = msg_id });
    defer pub_ack1.deinit();

    var pub_ack2 = try js.publish(subject, test_data, .{ .msg_id = msg_id });
    defer pub_ack2.deinit();

    // Verify first publish was successful
    try testing.expectEqualStrings(stream_name, pub_ack1.value.stream);
    try testing.expect(pub_ack1.value.sequence > 0);
    try testing.expect(!pub_ack1.value.duplicate);

    // Verify second publish was deduplicated
    try testing.expectEqualStrings(stream_name, pub_ack2.value.stream);
    try testing.expect(pub_ack2.value.sequence == pub_ack1.value.sequence);
    try testing.expect(pub_ack2.value.duplicate);
}

test "JetStream publish with expected sequence" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // First publish (should succeed)
    var pub_ack1 = try js.publish(subject, "first message", .{});
    defer pub_ack1.deinit();

    // Second publish with correct expected sequence
    var pub_ack2 = try js.publish(subject, "second message", .{ .expected_last_seq = pub_ack1.value.sequence });
    defer pub_ack2.deinit();

    try testing.expect(pub_ack2.value.sequence == pub_ack1.value.sequence + 1);

    // Third publish with incorrect expected sequence (should fail)
    const result = js.publish(subject, "third message", .{ .expected_last_seq = 999 });
    try testing.expectError(error.JetStreamError, result);
}

test "JetStream publishMsg with pre-constructed message" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique names
    const stream_name = try utils.generateUniqueStreamName(testing.allocator);
    defer testing.allocator.free(stream_name);
    const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
    defer testing.allocator.free(subject);

    // Create stream
    const stream_config = nats.StreamConfig{
        .name = stream_name,
        .subjects = &.{subject},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create message with custom headers
    const msg = try conn.newMsg();
    defer msg.deinit();

    try msg.setSubject(subject);
    try msg.setPayload("Custom message with headers");
    try msg.headerSet("Custom-Header", "custom-value");

    // Publish the pre-constructed message
    var pub_ack = try js.publishMsg(msg, .{ .msg_id = "msg-with-headers" });
    defer pub_ack.deinit();

    // Verify publish was successful
    try testing.expectEqualStrings(stream_name, pub_ack.value.stream);
    try testing.expect(pub_ack.value.sequence > 0);
}
