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

/// A result wrapper that manages memory for parsed data, following std.json.Parsed pattern
pub fn Result(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Arena allocator for managing parsed data memory (heap-allocated)
        arena: *std.heap.ArenaAllocator,
        /// The parsed value
        value: T,

        /// Initialize a Result with a prepared arena allocator
        pub fn init(allocator: std.mem.Allocator, value: T) std.mem.Allocator.Error!Self {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            errdefer allocator.destroy(arena);
            arena.* = std.heap.ArenaAllocator.init(allocator);

            return Self{
                .arena = arena,
                .value = value,
            };
        }

        /// Clean up allocated memory (follows std.json.Parsed pattern)
        pub fn deinit(self: Self) void {
            const allocator = self.arena.child_allocator;
            self.arena.deinit();
            allocator.destroy(self.arena);
        }
    };
}

test "Result init and deinit" {
    const TestStruct = struct {
        name: []const u8,
        value: u32,
    };

    var result = try Result(TestStruct).init(std.testing.allocator, TestStruct{
        .name = "test",
        .value = 42,
    });
    defer result.deinit();

    try std.testing.expectEqualStrings("test", result.value.name);
    try std.testing.expectEqual(@as(u32, 42), result.value.value);
}

test "Result arena allocator usage" {
    const TestStruct = struct {
        name: []const u8,
    };

    var result = try Result(TestStruct).init(std.testing.allocator, TestStruct{
        .name = "initial",
    });
    defer result.deinit();

    // Allocate memory using the arena
    const arena_allocated_string = try result.arena.allocator().dupe(u8, "arena allocated");

    // Update value to use arena-allocated memory
    result.value.name = arena_allocated_string;

    try std.testing.expectEqualStrings("arena allocated", result.value.name);
}
