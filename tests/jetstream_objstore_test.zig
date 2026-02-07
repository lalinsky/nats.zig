const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "ObjectStore basic create store" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager
    var objstore_manager = js.objectStoreManager();

    // Create store
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store",
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();

    // Clean up
    try objstore_manager.deleteStore(store_name);
}

test "ObjectStore put and get operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager and store
    var objstore_manager = js.objectStoreManager();
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store",
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    // Test data
    const test_data = "Hello, ObjectStore World!";
    const object_name = "test-object.txt";

    // Put object
    const put_result = try objstore.putBytes(object_name, test_data);
    defer put_result.deinit();
    try testing.expectEqualStrings(object_name, put_result.value.name);
    try testing.expectEqualStrings(store_name, put_result.value.bucket);
    try testing.expect(put_result.value.size == test_data.len);
    try testing.expect(put_result.value.chunks > 0);
    try testing.expect(!put_result.value.deleted);

    // Get object
    const get_result = try objstore.getBytes(object_name);
    defer get_result.deinit();
    try testing.expectEqualStrings(test_data, get_result.value);

    // Get object info
    const info_result = try objstore.info(object_name);
    defer info_result.deinit();
    try testing.expectEqualStrings(object_name, info_result.value.name);
    try testing.expectEqualStrings(store_name, info_result.value.bucket);
    try testing.expect(info_result.value.size == test_data.len);
    try testing.expect(!info_result.value.deleted);
}

test "ObjectStore chunked operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager and store
    var objstore_manager = js.objectStoreManager();
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store for chunked data",
        .chunk_size = 1024,
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    // Create test data larger than default chunk size
    const chunk_size = 1024; // 1KB chunks
    const data_size = chunk_size * 3 + 512; // 3.5 chunks worth
    const large_data = try testing.allocator.alloc(u8, data_size);
    defer testing.allocator.free(large_data);

    // Fill with pattern
    for (large_data, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    const object_name = "large-object.bin";

    // Put large object with custom chunk size
    const put_result = try objstore.putBytes(object_name, large_data);
    defer put_result.deinit();
    try testing.expectEqualStrings(object_name, put_result.value.name);
    try testing.expect(put_result.value.size == large_data.len);
    try testing.expect(put_result.value.chunks == 4); // Should be 4 chunks

    // Get large object
    const get_result = try objstore.getBytes(object_name);
    defer get_result.deinit();
    try testing.expectEqualSlices(u8, large_data, get_result.value);
}

test "ObjectStore delete operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager and store
    var objstore_manager = js.objectStoreManager();
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store for delete operations",
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    const test_data = "Data to be deleted";
    const object_name = "doomed-object.txt";

    // Put object
    const put_result = try objstore.putBytes(object_name, test_data);
    defer put_result.deinit();

    // Verify object exists
    const get_result = try objstore.getBytes(object_name);
    defer get_result.deinit();
    try testing.expectEqualStrings(test_data, get_result.value);

    // Delete object
    try objstore.delete(object_name);

    // Verify object is deleted
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.getBytes(object_name));

    // Info should show deleted status
    const info_result = try objstore.info(object_name);
    defer info_result.deinit();
    try testing.expect(info_result.value.deleted);
}

test "ObjectStore list operations" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager and store
    var objstore_manager = js.objectStoreManager();
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store for list operations",
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    // Put multiple objects
    const objects = [_]struct { name: []const u8, data: []const u8 }{
        .{ .name = "file1.txt", .data = "Content of file 1" },
        .{ .name = "file2.txt", .data = "Content of file 2" },
        .{ .name = "subdir/file3.txt", .data = "Content of file 3" },
    };

    for (objects) |obj| {
        const put_result = try objstore.putBytes(obj.name, obj.data);
        defer put_result.deinit();
    }

    // List all objects
    const list_result = try objstore.list();
    defer list_result.deinit();

    try testing.expect(list_result.value.len >= objects.len);

    // Verify all objects are in the list
    for (objects) |expected| {
        var found = false;
        for (list_result.value) |info| {
            if (std.mem.eql(u8, info.name, expected.name)) {
                found = true;
                try testing.expect(!info.deleted);
                try testing.expect(info.size == expected.data.len);
                break;
            }
        }
        try testing.expect(found);
    }

    // Delete one object and verify it's no longer in the list
    try objstore.delete("file1.txt");
    const list_result2 = try objstore.list();
    defer list_result2.deinit();

    var found_deleted = false;
    for (list_result2.value) |info| {
        if (std.mem.eql(u8, info.name, "file1.txt")) {
            found_deleted = true;
            break;
        }
    }
    try testing.expect(!found_deleted); // Should not be in the list since it's deleted
}

test "ObjectStore validation" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Test store name validation
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName(""));
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName("invalid space"));
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName("invalid.dot"));

    // Test object name validation
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName(""));
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName("/starts-with-slash"));
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName("ends-with-slash/"));
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName(".starts-with-dot"));
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName("ends-with-dot."));

    // Valid names should pass
    try nats.validateOSBucketName("valid-store_name123");
    try nats.validateOSObjectName("valid-object/name_123.txt");
}

test "ObjectStore error handling" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    // Generate unique store name
    const store_name = try utils.generateUniqueName(testing.allocator, "teststore");
    defer testing.allocator.free(store_name);

    // Create ObjectStore manager and store
    var objstore_manager = js.objectStoreManager();
    const config = nats.ObjectStoreConfig{
        .store_name = store_name,
        .description = "Test object store for error handling",
    };

    var objstore = try objstore_manager.createStore(config);
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    // Try to get non-existent object
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.getBytes("nonexistent.txt"));
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.info("nonexistent.txt"));

    // Try to delete non-existent object
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.delete("nonexistent.txt"));
}
