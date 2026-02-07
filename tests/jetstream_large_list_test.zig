const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "create many streams and list them" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    const num_streams = 10;
    var created_streams: [num_streams][]u8 = undefined;

    // Create multiple streams
    for (0..num_streams) |i| {
        const stream_name = try utils.generateUniqueStreamName(testing.allocator);
        created_streams[i] = stream_name;

        const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
        defer testing.allocator.free(subject);

        const stream_config = nats.StreamConfig{
            .name = stream_name,
            .subjects = &.{subject},
            .max_msgs = 1000,
            .max_bytes = 1024 * 1024, // 1MB
        };

        var stream_info = try js.addStream(stream_config);
        defer stream_info.deinit();

        log.info("Created stream {d}: {s}", .{ i, stream_name });
    }

    // Clean up stream names
    defer {
        for (created_streams) |stream_name| {
            testing.allocator.free(stream_name);
        }
    }

    log.info("Listing all streams...", .{});

    // Now list all streams - this should handle large responses properly
    var result = try js.listStreams();
    defer result.deinit();

    log.info("Total streams found: {d}", .{result.value.len});

    // Verify all our streams are present
    var found_count: usize = 0;
    for (created_streams) |stream_name| {
        for (result.value) |info| {
            if (std.mem.eql(u8, info.config.name, stream_name)) {
                found_count += 1;
                break;
            }
        }
    }

    try testing.expect(found_count == num_streams);
    try testing.expect(result.value.len >= num_streams);

    log.info("Successfully found all {d} created streams in list of {d} total streams", .{ found_count, result.value.len });
}

test "stress test: create 20 streams and list them" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    const num_streams = 20;
    var created_streams: [num_streams][]u8 = undefined;

    // Create many streams quickly
    for (0..num_streams) |i| {
        const stream_name = try utils.generateUniqueStreamName(testing.allocator);
        created_streams[i] = stream_name;

        const subject = try utils.generateSubjectFromStreamName(testing.allocator, stream_name);
        defer testing.allocator.free(subject);

        const stream_config = nats.StreamConfig{
            .name = stream_name,
            .subjects = &.{subject},
            .max_msgs = 500,
        };

        var stream_info = try js.addStream(stream_config);
        defer stream_info.deinit();
    }

    // Clean up stream names
    defer {
        for (created_streams) |stream_name| {
            testing.allocator.free(stream_name);
        }
    }

    // This is where the bug should manifest - large JSON response split across packets
    var result = try js.listStreams();
    defer result.deinit();

    log.info("Stress test: found {d} total streams", .{result.value.len});

    // Just verify the operation completed without errors
    try testing.expect(result.value.len >= num_streams);
}
