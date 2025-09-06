// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const utils = @import("utils.zig");

test "KV history retrieval" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    var kv_manager = js.kvManager();

    const bucket_name = try utils.generateUniqueName(testing.allocator, "test-history");
    defer testing.allocator.free(bucket_name);

    // Create bucket with history=3
    var kv = try kv_manager.createBucket(.{
        .bucket = bucket_name,
        .history = 3,
    });
    defer kv.deinit();
    defer kv_manager.deleteBucket(bucket_name) catch {};

    const key = "test-key";

    // Put multiple values to create history
    _ = try kv.put(key, "value1", .{});
    _ = try kv.put(key, "value2", .{});
    _ = try kv.put(key, "value3", .{});

    // Get history
    const history = try kv.history(key, testing.allocator);
    defer {
        for (history) |*entry| {
            entry.deinit();
        }
        testing.allocator.free(history);
    }

    try testing.expect(history.len == 3);
    // History should be in chronological order (oldest to newest)
    try testing.expectEqualSlices(u8, "value1", history[0].value);
    try testing.expectEqualSlices(u8, "value2", history[1].value);
    try testing.expectEqualSlices(u8, "value3", history[2].value);
}

test "KV keys listing" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    var kv_manager = js.kvManager();

    const bucket_name = try utils.generateUniqueName(testing.allocator, "test-keys");
    defer testing.allocator.free(bucket_name);

    var kv = try kv_manager.createBucket(.{
        .bucket = bucket_name,
    });
    defer kv.deinit();
    defer kv_manager.deleteBucket(bucket_name) catch {};

    // Put some test data
    _ = try kv.put("key1", "value1", .{});
    _ = try kv.put("key2", "value2", .{});
    _ = try kv.put("folder.key3", "value3", .{});

    // Get all keys
    const keys_result = try kv.keys();
    defer keys_result.deinit();

    const keys = keys_result.value;

    try testing.expect(keys.len == 3);

    // Check that all keys are present (order may vary)
    var found_key1 = false;
    var found_key2 = false;
    var found_key3 = false;

    for (keys) |key| {
        if (std.mem.eql(u8, key, "key1")) found_key1 = true;
        if (std.mem.eql(u8, key, "key2")) found_key2 = true;
        if (std.mem.eql(u8, key, "folder.key3")) found_key3 = true;
    }

    try testing.expect(found_key1);
    try testing.expect(found_key2);
    try testing.expect(found_key3);
}

test "KV keys with filters" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    var kv_manager = js.kvManager();

    const bucket_name = try utils.generateUniqueName(testing.allocator, "test-keys-filter");
    defer testing.allocator.free(bucket_name);

    var kv = try kv_manager.createBucket(.{
        .bucket = bucket_name,
    });
    defer kv.deinit();
    defer kv_manager.deleteBucket(bucket_name) catch {};

    // Put some test data
    _ = try kv.put("user.alice", "data1", .{});
    _ = try kv.put("user.bob", "data2", .{});
    _ = try kv.put("config.setting", "data3", .{});

    // Get filtered keys
    const keys_result = try kv.keysWithFilters(&.{"user.*"});
    defer keys_result.deinit();

    const keys = keys_result.value;

    try testing.expect(keys.len == 2);

    // Check that only user keys are present
    var found_alice = false;
    var found_bob = false;

    for (keys) |key| {
        if (std.mem.eql(u8, key, "user.alice")) found_alice = true;
        if (std.mem.eql(u8, key, "user.bob")) found_bob = true;
        // Should not find config.setting
        try testing.expect(!std.mem.eql(u8, key, "config.setting"));
    }

    try testing.expect(found_alice);
    try testing.expect(found_bob);
}

test "KV watch basic functionality" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var js = conn.jetstream(.{});
    defer js.deinit();

    var kv_manager = js.kvManager();

    const bucket_name = try utils.generateUniqueName(testing.allocator, "test-watch");
    defer testing.allocator.free(bucket_name);

    var kv = try kv_manager.createBucket(.{
        .bucket = bucket_name,
    });
    defer kv.deinit();
    defer kv_manager.deleteBucket(bucket_name) catch {};

    const key = "watch-key";

    // Put initial value
    _ = try kv.put(key, "initial", .{});

    // Start watching
    var watcher = try kv.watch(key, .{});
    defer watcher.deinit();

    // Should get initial value (with timeout)
    const maybe_entry = try watcher.next(1000);
    try testing.expect(maybe_entry != null); // Should not be the completion marker
    var entry = maybe_entry.?;
    defer entry.deinit();

    try testing.expectEqualSlices(u8, key, entry.key);
    try testing.expectEqualSlices(u8, "initial", entry.value);
    try testing.expect(entry.operation == .PUT);

    // Should get completion marker (null) indicating initial data is done
    const completion_marker = try watcher.next(1000);
    try testing.expect(completion_marker == null);

    // After completion marker, should timeout on further attempts
    const result = watcher.next(1000);
    try testing.expect(result == error.Timeout or result == error.QueueEmpty);
}
