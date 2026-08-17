const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "ObjectStore basic create store" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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

    // Verify object is deleted; per ADR-20, deleted objects are treated
    // as not found by both get and info.
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.getBytes(object_name));
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.info(object_name));

    // Deleting again succeeds: delete is retryable so that a failed chunk
    // purge can be finished on a retry.
    try objstore.delete(object_name);

    // Deleting an object that never existed is an error.
    try testing.expectError(nats.ObjectStoreError.ObjectNotFound, objstore.delete("never-existed"));
}

test "ObjectStore list operations" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    // Test store name validation
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName(""));
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName("invalid space"));
    try testing.expectError(error.InvalidOSBucketName, nats.validateOSBucketName("invalid.dot"));

    // Object names are unrestricted per ADR-20; only empty is invalid.
    try testing.expectError(error.InvalidOSObjectName, nats.validateOSObjectName(""));

    // Valid names should pass
    try nats.validateOSBucketName("valid-store_name123");
    try nats.validateOSObjectName("valid-object/name_123.txt");
    try nats.validateOSObjectName("any name, even with spaces & symbols!");
}

test "ObjectStore error handling" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
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

test "ObjectStore wire format matches ADR-20" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    const store_name = try utils.generateUniqueName(testing.allocator, "wirestore");
    defer testing.allocator.free(store_name);

    var objstore_manager = js.objectStoreManager();
    var objstore = try objstore_manager.createStore(.{
        .store_name = store_name,
        .chunk_size = 8,
    });
    defer objstore.deinit();

    // A name that is not subject-safe, so it must be base64url-encoded.
    const object_name = "dir/some file.txt";
    const content = "hello object store wire format"; // 30 bytes -> 4 chunks of 8

    var put_result = try objstore.putBytes(object_name, content);
    defer put_result.deinit();

    // The meta message must live on $O.<bucket>.M.<base64url(name)>,
    // fetched here through the raw stream API, not the ObjectStore API.
    var name_buf: [64]u8 = undefined;
    const encoded_name = std.base64.url_safe.Encoder.encode(&name_buf, object_name);
    const meta_subject = try std.fmt.allocPrint(testing.allocator, "$O.{s}.M.{s}", .{ store_name, encoded_name });
    defer testing.allocator.free(meta_subject);
    const stream_name = try std.fmt.allocPrint(testing.allocator, "OBJ_{s}", .{store_name});
    defer testing.allocator.free(stream_name);

    {
        const meta_msg = try js.getMsg(stream_name, .{ .last_by_subj = meta_subject, .direct = true });
        defer meta_msg.deinit();

        // The raw JSON must follow the ADR-20 schema.
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, meta_msg.data, .{});
        defer parsed.deinit();
        const obj = parsed.value.object;

        try testing.expect(obj.get("opts") == null); // pre-ADR schema must be gone
        try testing.expectEqual(@as(i64, 4), obj.get("chunks").?.integer);
        try testing.expectEqual(@as(i64, content.len), obj.get("size").?.integer);

        const options = obj.get("options").?.object;
        try testing.expectEqual(@as(i64, 8), options.get("max_chunk_size").?.integer);

        // Digest: "SHA-256=" + padded base64url of the SHA-256 hash.
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(content, &hash, .{});
        var digest_value_buf: [64]u8 = undefined;
        const digest_value = std.base64.url_safe.Encoder.encode(&digest_value_buf, &hash);
        const expected_digest = try std.fmt.allocPrint(testing.allocator, "SHA-256={s}", .{digest_value});
        defer testing.allocator.free(expected_digest);
        try testing.expectEqualStrings(expected_digest, obj.get("digest").?.string);
    }

    // 4 chunks + 1 meta message in the stream.
    {
        const si = try js.getStreamInfo(stream_name);
        defer si.deinit();
        try testing.expectEqual(@as(u64, 5), si.value.state.messages);
    }

    // Replace the object: the meta message rolls up and the previous
    // object's chunks are purged, leaving 1 new chunk + 1 meta.
    var put2_result = try objstore.putBytes(object_name, "xy");
    defer put2_result.deinit();

    {
        const si = try js.getStreamInfo(stream_name);
        defer si.deinit();
        try testing.expectEqual(@as(u64, 2), si.value.state.messages);
    }

    // Delete: rolled-up tombstone with zeroed instance fields, chunks purged.
    try objstore.delete(object_name);

    {
        const tomb_msg = try js.getMsg(stream_name, .{ .last_by_subj = meta_subject, .direct = true });
        defer tomb_msg.deinit();

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, tomb_msg.data, .{});
        defer parsed.deinit();
        const obj = parsed.value.object;

        try testing.expectEqual(true, obj.get("deleted").?.bool);
        try testing.expectEqual(@as(i64, 0), obj.get("size").?.integer);
        try testing.expectEqual(@as(i64, 0), obj.get("chunks").?.integer);
    }

    {
        const si = try js.getStreamInfo(stream_name);
        defer si.deinit();
        try testing.expectEqual(@as(u64, 1), si.value.state.messages);
    }

    try objstore_manager.deleteStore(store_name);
}

test "ObjectStore put with zero chunk size uses the default" {
    const io = std.testing.io;

    const conn = try utils.createDefaultConnection(io);
    defer utils.closeConnection(conn);

    const js = conn.jetstream(.{});

    const store_name = try utils.generateUniqueName(testing.allocator, "zerochunk");
    defer testing.allocator.free(store_name);

    var objstore_manager = js.objectStoreManager();
    var objstore = try objstore_manager.createStore(.{ .store_name = store_name });
    defer objstore.deinit();
    defer objstore_manager.deleteStore(store_name) catch {};

    // An explicit zero chunk size must fall back to the default instead of
    // silently consuming no input and storing an empty object.
    const content = "not an empty object";
    var stream = nats.SliceReader{ .data = content };
    const put_result = try objstore.put(.{
        .name = "zero-chunk-object",
        .opts = .{ .max_chunk_size = 0 },
    }, &stream);
    defer put_result.deinit();

    try testing.expectEqual(@as(u64, content.len), put_result.value.size);
    try testing.expectEqual(@as(u32, 1), put_result.value.chunks);

    const get_result = try objstore.getBytes("zero-chunk-object");
    defer get_result.deinit();
    try testing.expectEqualStrings(content, get_result.value);
}
