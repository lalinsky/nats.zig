const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.jetstream_test);

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

    try testing.expect(result.value.streams == 0);
}

test "list stream names" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream first
    const stream_config = nats.StreamConfig{
        .name = "TEST_LIST_STREAM",
        .subjects = &.{"test.list.*"},
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Now list stream names and verify our stream is included
    var result = try js.listStreamNames();
    defer result.deinit();

    // Should contain at least our test stream
    try testing.expect(result.value.len >= 1);

    // Find our stream in the list
    var found = false;
    for (result.value) |name| {
        if (std.mem.eql(u8, name, "TEST_LIST_STREAM")) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "add stream" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    const stream_config = nats.StreamConfig{
        .name = "TEST_STREAM",
        .subjects = &.{"test.stream.*"},
        .retention = .limits,
        .storage = .file,
        .max_msgs = 1000,
        .max_bytes = 1024 * 1024, // 1MB
        .max_age = 0, // No age limit
        .num_replicas = 1,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Verify stream was created with correct configuration
    try testing.expectEqualStrings("TEST_STREAM", stream_info.value.config.name);
    try testing.expect(stream_info.value.config.subjects.len == 1);
    try testing.expectEqualStrings("test.stream.*", stream_info.value.config.subjects[0]);
    try testing.expect(stream_info.value.config.retention == .limits);
    try testing.expect(stream_info.value.config.storage == .file);
    try testing.expect(stream_info.value.config.max_msgs == 1000);
}

test "list streams" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Create a test stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_LIST_STREAMS",
        .subjects = &.{"test.list.*"},
        .max_msgs = 500,
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Now list all streams and verify our stream is included
    var result = try js.listStreams();
    defer result.deinit();

    // Should contain at least our test stream
    try testing.expect(result.value.len >= 1);

    // Find our stream in the list and verify its configuration
    var found = false;

    for (result.value) |info| {
        if (std.mem.eql(u8, info.config.name, "TEST_LIST_STREAMS")) {
            found = true;
            try testing.expect(info.config.subjects.len == 1);
            try testing.expectEqualStrings("test.list.*", info.config.subjects[0]);
            try testing.expect(info.config.max_msgs == 500);
            break;
        }
    }

    try testing.expect(found);
}

test "update stream" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const initial_config = nats.StreamConfig{
        .name = "TEST_UPDATE_STREAM",
        .subjects = &.{"test.update.*"},
        .max_msgs = 100,
    };

    var initial_stream = try js.addStream(initial_config);
    defer initial_stream.deinit();

    // Verify initial configuration
    try testing.expectEqualStrings("TEST_UPDATE_STREAM", initial_stream.value.config.name);
    try testing.expect(initial_stream.value.config.max_msgs == 100);

    // Update the stream configuration
    const updated_config = nats.StreamConfig{
        .name = "TEST_UPDATE_STREAM",
        .subjects = &.{"test.update.*"},
        .max_msgs = 200, // Double the limit
    };

    var updated_stream = try js.updateStream(updated_config);
    defer updated_stream.deinit();

    // Verify the update was applied
    try testing.expectEqualStrings("TEST_UPDATE_STREAM", updated_stream.value.config.name);
    try testing.expect(updated_stream.value.config.max_msgs == 200);
}

test "delete stream" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream to delete
    const stream_config = nats.StreamConfig{
        .name = "TEST_DELETE_STREAM",
        .subjects = &.{"test.delete.*"},
    };

    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Verify stream exists by listing streams
    var streams_before = try js.listStreamNames();
    defer streams_before.deinit();

    var found_before = false;
    for (streams_before.value) |name| {
        if (std.mem.eql(u8, name, "TEST_DELETE_STREAM")) {
            found_before = true;
            break;
        }
    }
    try testing.expect(found_before);

    // Delete the stream
    try js.deleteStream("TEST_DELETE_STREAM");

    // Verify stream no longer exists
    var streams_after = try js.listStreamNames();
    defer streams_after.deinit();

    var found_after = false;
    for (streams_after.value) |name| {
        if (std.mem.eql(u8, name, "TEST_DELETE_STREAM")) {
            found_after = true;
            break;
        }
    }
    try testing.expect(!found_after);
}

test "add consumer" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream for the consumer
    const stream_config = nats.StreamConfig{
        .name = "TEST_CONSUMER_STREAM",
        .subjects = &.{"test.consumer.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "TEST_CONSUMER",
        .ack_policy = .explicit,
        .deliver_policy = .all,
    };

    var consumer_info = try js.addConsumer("TEST_CONSUMER_STREAM", consumer_config);
    defer consumer_info.deinit();

    // Verify consumer was created with correct configuration
    try testing.expectEqualStrings("TEST_CONSUMER", consumer_info.value.name);
    try testing.expectEqualStrings("TEST_CONSUMER_STREAM", consumer_info.value.stream_name);
}

test "list consumer names" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_LIST_CONSUMER_STREAM",
        .subjects = &.{"test.listcons.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "TEST_LIST_CONSUMER",
        .ack_policy = .explicit,
    };
    var consumer_info = try js.addConsumer("TEST_LIST_CONSUMER_STREAM", consumer_config);
    defer consumer_info.deinit();

    // List consumer names and verify our consumer is included
    var result = try js.listConsumerNames("TEST_LIST_CONSUMER_STREAM");
    defer result.deinit();

    // Should contain at least our test consumer
    try testing.expect(result.value.len >= 1);

    // Find our consumer in the list
    var found = false;
    for (result.value) |name| {
        if (std.mem.eql(u8, name, "TEST_LIST_CONSUMER")) {
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

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_LIST_CONSUMERS_STREAM",
        .subjects = &.{"test.listconsumers.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "TEST_LIST_CONSUMERS",
        .ack_policy = .explicit,
        .max_ack_pending = 100,
    };
    var consumer_info = try js.addConsumer("TEST_LIST_CONSUMERS_STREAM", consumer_config);
    defer consumer_info.deinit();

    // List consumers and verify our consumer is included
    var result = try js.listConsumers("TEST_LIST_CONSUMERS_STREAM");
    defer result.deinit();

    // Should contain at least our test consumer
    try testing.expect(result.value.len >= 1);

    // Find our consumer in the list and verify its configuration
    var found = false;
    for (result.value) |info| {
        if (std.mem.eql(u8, info.config.durable_name.?, "TEST_LIST_CONSUMERS")) {
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

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_GET_CONSUMER_STREAM",
        .subjects = &.{"test.getcons.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "TEST_GET_CONSUMER",
        .ack_policy = .explicit,
        .max_deliver = 5,
    };
    var consumer_info = try js.addConsumer("TEST_GET_CONSUMER_STREAM", consumer_config);
    defer consumer_info.deinit();

    // Get consumer info
    var retrieved_info = try js.getConsumerInfo("TEST_GET_CONSUMER_STREAM", "TEST_GET_CONSUMER");
    defer retrieved_info.deinit();

    // Verify the retrieved info matches what we created
    // Note: stream_name is not included in consumer info responses
    try testing.expectEqualStrings("TEST_GET_CONSUMER", retrieved_info.value.config.durable_name.?);
    try testing.expect(retrieved_info.value.config.ack_policy == .explicit);
    try testing.expect(retrieved_info.value.config.max_deliver == 5);
}

test "delete consumer" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_DELETE_CONSUMER_STREAM",
        .subjects = &.{"test.delcons.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Create a consumer to delete
    const consumer_config = nats.ConsumerConfig{
        .durable_name = "TEST_DELETE_CONSUMER",
        .ack_policy = .explicit,
    };
    var consumer_info = try js.addConsumer("TEST_DELETE_CONSUMER_STREAM", consumer_config);
    defer consumer_info.deinit();

    // Verify consumer exists
    var consumers_before = try js.listConsumerNames("TEST_DELETE_CONSUMER_STREAM");
    defer consumers_before.deinit();

    var found_before = false;
    for (consumers_before.value) |name| {
        if (std.mem.eql(u8, name, "TEST_DELETE_CONSUMER")) {
            found_before = true;
            break;
        }
    }
    try testing.expect(found_before);

    // Delete the consumer
    try js.deleteConsumer("TEST_DELETE_CONSUMER_STREAM", "TEST_DELETE_CONSUMER");

    // Verify consumer no longer exists
    var consumers_after = try js.listConsumerNames("TEST_DELETE_CONSUMER_STREAM");
    defer consumers_after.deinit();

    var found_after = false;
    for (consumers_after.value) |name| {
        if (std.mem.eql(u8, name, "TEST_DELETE_CONSUMER")) {
            found_after = true;
            break;
        }
    }
    try testing.expect(!found_after);
}

test "purge stream" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_STREAM",
        .subjects = &.{"test.purge.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish some messages to the stream
    try conn.publish("test.purge.msg1", "First message");
    try conn.publish("test.purge.msg2", "Second message");
    try conn.publish("test.purge.msg3", "Third message");

    // Allow messages to be processed
    std.time.sleep(100 * std.time.ns_per_ms); // 100ms

    // Test basic purge (purge all messages)
    const purge_request = nats.StreamPurgeRequest{};
    var purge_result = try js.purgeStream("TEST_PURGE_STREAM", purge_request);
    defer purge_result.deinit();

    // Verify purge was successful and messages were removed
    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged > 0);
}

test "purge stream with filter" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_FILTER_STREAM",
        .subjects = &.{"test.filter.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages with different subjects
    try conn.publish("test.filter.keep", "Keep this message");
    try conn.publish("test.filter.purge", "Purge this message");
    try conn.publish("test.filter.purge", "Purge this message too");

    // Allow messages to be processed
    std.time.sleep(100 * std.time.ns_per_ms);

    // Test purge with filter (only purge messages with "purge" subject)
    const purge_request = nats.StreamPurgeRequest{
        .filter = "test.filter.purge",
    };
    var purge_result = try js.purgeStream("TEST_PURGE_FILTER_STREAM", purge_request);
    defer purge_result.deinit();

    // Should have purged 2 messages with the matching filter
    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 2);
}

test "purge stream with sequence limit" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_SEQ_STREAM",
        .subjects = &.{"test.seq.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages
    try conn.publish("test.seq.msg", "Message 1");
    try conn.publish("test.seq.msg", "Message 2");
    try conn.publish("test.seq.msg", "Message 3");
    try conn.publish("test.seq.msg", "Message 4");

    // Allow messages to be processed
    std.time.sleep(100 * std.time.ns_per_ms);

    // Test purge up to sequence 3 (should purge messages 1 and 2)
    const purge_request = nats.StreamPurgeRequest{
        .seq = 3,
    };
    var purge_result = try js.purgeStream("TEST_PURGE_SEQ_STREAM", purge_request);
    defer purge_result.deinit();

    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 2);
}

test "purge stream with keep parameter" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_PURGE_KEEP_STREAM",
        .subjects = &.{"test.keep.*"},
    };
    var stream_info = try js.addStream(stream_config);
    defer stream_info.deinit();

    // Publish messages
    try conn.publish("test.keep.msg", "Message 1");
    try conn.publish("test.keep.msg", "Message 2");
    try conn.publish("test.keep.msg", "Message 3");
    try conn.publish("test.keep.msg", "Message 4");
    try conn.publish("test.keep.msg", "Message 5");

    // Allow messages to be processed
    std.time.sleep(100 * std.time.ns_per_ms);

    // Test purge with keep=2 (should keep the 2 most recent messages)
    const purge_request = nats.StreamPurgeRequest{
        .keep = 2,
    };
    var purge_result = try js.purgeStream("TEST_PURGE_KEEP_STREAM", purge_request);
    defer purge_result.deinit();

    try testing.expect(purge_result.value.success);
    try testing.expect(purge_result.value.purged == 3);
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
