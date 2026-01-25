const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "KV basic create bucket" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
    defer testing.allocator.free(bucket_name);

    // Create KV manager
    var kv_manager = js.kvManager();

    // Create bucket
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .description = "Test bucket",
        .history = 5,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    // Clean up
    try kv_manager.deleteBucket(bucket_name);
}

test "KV put and get operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
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
    const key = "test-key";
    const value = "test-value";
    const revision = try kv.put(key, value, .{});
    try testing.expect(revision > 0);

    // Test get operation
    var entry = try kv.get(key);
    defer entry.deinit();

    try testing.expectEqualStrings(bucket_name, entry.bucket);
    try testing.expectEqualStrings(key, entry.key);
    try testing.expectEqualStrings(value, entry.value);
    try testing.expect(entry.revision == revision);
    try testing.expect(entry.delta == 0); // Latest value
    try testing.expect(entry.operation == .PUT);
}

test "KV create and update operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
    defer testing.allocator.free(bucket_name);

    // Create KV manager and bucket
    var kv_manager = js.kvManager();
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .history = 3,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    const key = "test-key";

    // Test create operation - should succeed
    const revision1 = try kv.create(key, "value1", .{});
    try testing.expect(revision1 > 0);

    // Test create operation on existing key - should fail
    try testing.expectError(nats.KVError.KeyExists, kv.create(key, "value2", .{}));

    // Test update operation with correct revision
    const revision2 = try kv.update(key, "value2", revision1);
    try testing.expect(revision2 > revision1);

    // Test update operation with wrong revision - should fail
    try testing.expectError(nats.KVError.WrongLastRevision, kv.update(key, "value3", revision1));

    // Verify final value
    var entry = try kv.get(key);
    defer entry.deinit();
    try testing.expectEqualStrings("value2", entry.value);
    try testing.expect(entry.revision == revision2);
}

test "KV delete operation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
    defer testing.allocator.free(bucket_name);

    // Create KV manager and bucket
    var kv_manager = js.kvManager();
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .history = 5,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    const key = "test-key";

    // Put a value
    _ = try kv.put(key, "test-value", .{});

    // Verify value exists
    var entry = try kv.get(key);
    try testing.expect(entry.operation == .PUT);
    entry.deinit();

    // Delete the key
    try kv.delete(key);

    // Try to get deleted key - should return KeyNotFound per ADR-8
    try testing.expectError(nats.KVError.KeyNotFound, kv.get(key));

    // Create should work on deleted key
    _ = try kv.create(key, "new-value", .{});
}

test "KV purge operation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
    defer testing.allocator.free(bucket_name);

    // Create KV manager and bucket
    var kv_manager = js.kvManager();
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .history = 5,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    const key = "test-key";

    // Put a value
    _ = try kv.put(key, "test-value", .{});

    // Verify value exists
    var entry = try kv.get(key);
    try testing.expect(entry.operation == .PUT);
    entry.deinit();

    // Purge the key
    try kv.purge(key, .{});

    // Try to get purged key - should return KeyNotFound per ADR-8
    try testing.expectError(nats.KVError.KeyNotFound, kv.get(key));
}

test "KV status operation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection(rt);
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});

    // Generate unique bucket name
    const bucket_name = try utils.generateUniqueName(testing.allocator, "testbucket");
    defer testing.allocator.free(bucket_name);

    // Create KV manager and bucket
    var kv_manager = js.kvManager();
    const config = nats.KVConfig{
        .bucket = bucket_name,
        .description = "Test bucket",
        .history = 3,
        .compression = true,
    };

    var kv = try kv_manager.createBucket(config);
    defer kv.deinit();

    // Add some data
    _ = try kv.put("key1", "value1", .{});
    _ = try kv.put("key2", "value2", .{});

    // Get status
    const status = try kv.status();
    defer status.deinit();

    try testing.expectEqualStrings(bucket_name, status.value.bucket);
    try testing.expect(status.value.values >= 2); // At least 2 messages
    try testing.expect(status.value.history == 3);
    try testing.expect(status.value.is_compressed == true);
    try testing.expectEqualStrings("JetStream", status.value.backing_store);
}
