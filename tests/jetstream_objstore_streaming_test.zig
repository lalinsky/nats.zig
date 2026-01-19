const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

test "ObjectStore streaming put and get" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create JetStream context
    const js = conn.jetstream(.{});

    // Create ObjectStore manager
    var osm = js.objectStoreManager();

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create test object store
    const store_config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test store for streaming",
    };

    var store = try osm.createStore(store_config);
    defer store.deinit();
    defer osm.deleteStore(store_name) catch {};

    // Test data
    const test_data = "Hello, streaming world! This is a test of the new streaming put/get functionality.";

    // Test streaming put with ObjectMeta
    const meta = nats.ObjectMeta{
        .name = "streaming-test-object",
        .description = "Test object for streaming",
        .opts = null,
    };

    var stream = std.io.fixedBufferStream(test_data);
    const put_result = try store.put(meta, stream.reader());

    try testing.expect(put_result.value.size == test_data.len);
    try testing.expect(put_result.value.chunks > 0);
    try testing.expectEqualStrings("streaming-test-object", put_result.value.name);
    defer put_result.deinit();

    // Test streaming get
    var get_result = try store.get("streaming-test-object");
    defer get_result.deinit();

    try testing.expectEqualStrings("streaming-test-object", get_result.info.name);
    try testing.expect(get_result.info.size == test_data.len);

    // Read data from stream
    var buffer: [1024]u8 = undefined;
    var total_read: usize = 0;
    while (total_read < get_result.info.size) {
        const n = try get_result.read(buffer[total_read..]);
        if (n == 0) break;
        total_read += n;
    }

    try testing.expectEqualStrings(test_data, buffer[0..total_read]);

    // Verify digest
    try get_result.verify();

    // Test that getBytes still works (compatibility)
    const bytes_result = try store.getBytes("streaming-test-object");
    defer bytes_result.deinit();

    try testing.expectEqualStrings(test_data, bytes_result.value);
}

test "ObjectStore streaming empty object" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create JetStream context
    const js = conn.jetstream(.{});

    // Create ObjectStore manager
    var osm = js.objectStoreManager();

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "testempty");
    defer testing.allocator.free(store_name);

    // Create test object store
    const store_config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test store for empty objects",
    };

    var store = try osm.createStore(store_config);
    defer store.deinit();
    defer osm.deleteStore(store_name) catch {};

    // Test empty data
    const empty_data = "";

    // Test streaming put with empty data
    const meta = nats.ObjectMeta{
        .name = "empty-object",
        .description = "Empty test object",
        .opts = null,
    };

    var stream = std.io.fixedBufferStream(empty_data);
    const put_result = try store.put(meta, stream.reader());

    try testing.expect(put_result.value.size == 0);
    try testing.expect(put_result.value.chunks == 0);
    defer put_result.deinit();

    // Test streaming get for empty object
    var get_result = try store.get("empty-object");
    defer get_result.deinit();

    try testing.expect(get_result.info.size == 0);

    // Read should immediately return 0 (EOF)
    var buffer: [10]u8 = undefined;
    const n = try get_result.read(&buffer);
    try testing.expect(n == 0);

    // Verify should succeed for empty object
    try get_result.verify();
}
