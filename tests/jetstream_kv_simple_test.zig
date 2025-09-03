const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "KV simple put and get" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "simple");
    defer testing.allocator.free(bucket_name);

    // Create KV manager and bucket
    var kv_manager = js.kvManager();
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .history = 1,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    // Test put operation
    const key = "testkey";
    const value = "testvalue";
    std.log.warn("About to put key: {s}, value: {s}", .{ key, value });
    const revision = try kv.put(key, value, .{});
    std.log.warn("Put succeeded with revision: {d}", .{revision});
    
    // Test get operation
    std.log.warn("About to get key: {s}", .{key});
    const entry = try kv.get(key);
    defer entry.deinit();
    std.log.warn("Get succeeded: key={s}, value={s}, revision={d}", .{ entry.key, entry.value, entry.revision });

    try testing.expectEqualStrings(key, entry.key);
    try testing.expectEqualStrings(value, entry.value);
    try testing.expect(entry.revision == revision);
}