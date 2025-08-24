const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.jetstream_stream_test);

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

test "get stream info" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // First create a stream
    const stream_config = nats.StreamConfig{
        .name = "TEST_GET_STREAM_INFO",
        .subjects = &.{"test.streaminfo.*"},
        .max_msgs = 1500,
        .retention = .limits,
        .storage = .file,
    };
    var created_stream = try js.addStream(stream_config);
    defer created_stream.deinit();

    // Get stream info using the new endpoint
    var retrieved_info = try js.getStreamInfo("TEST_GET_STREAM_INFO");
    defer retrieved_info.deinit();

    // Verify the retrieved info matches what we created
    try testing.expectEqualStrings("TEST_GET_STREAM_INFO", retrieved_info.value.config.name);
    try testing.expect(retrieved_info.value.config.subjects.len == 1);
    try testing.expectEqualStrings("test.streaminfo.*", retrieved_info.value.config.subjects[0]);
    try testing.expect(retrieved_info.value.config.max_msgs == 1500);
    try testing.expect(retrieved_info.value.config.retention == .limits);
    try testing.expect(retrieved_info.value.config.storage == .file);
}