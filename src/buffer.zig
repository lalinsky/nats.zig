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
const Allocator = std.mem.Allocator;

/// Errors that can occur during buffer operations
pub const BufferError = error{
    OutOfMemory,
    InvalidArgument,
    BufferOverflow,
};

/// A growable buffer similar to nats.c/src/buf.c
/// Supports both owned and borrowed data, with automatic expansion
pub const Buffer = struct {
    /// Pointer to the data
    buffer: []u8,
    /// Current write position within data
    pos: usize,
    /// Current length of valid data
    len: usize,
    /// Total capacity of the buffer
    capacity: usize,
    /// Whether we own the data and should free it
    own_data: bool,
    /// Allocator used for memory management
    allocator: ?Allocator,

    const Self = @This();

    /// Initialize a buffer with the given capacity
    /// The buffer will own its data and manage memory allocation
    pub fn init(allocator: Allocator, capacity: usize) !Self {
        if (capacity == 0) {
            return Self{
                .buffer = &.{},
                .pos = 0,
                .len = 0,
                .capacity = 0,
                .own_data = false,
                .allocator = allocator,
            };
        }

        const buf_data = try allocator.alloc(u8, capacity);
        return Self{
            .buffer = buf_data,
            .pos = 0,
            .len = 0,
            .capacity = capacity,
            .own_data = true,
            .allocator = allocator,
        };
    }

    /// Initialize a buffer with existing data as backend
    /// The buffer will not own the data initially, but may take ownership if expansion is needed
    pub fn initWithBackend(allocator: Allocator, backend: []u8, initial_len: usize) !Self {
        if (initial_len > backend.len) {
            return BufferError.InvalidArgument;
        }

        return Self{
            .buffer = backend,
            .pos = initial_len,
            .len = initial_len,
            .capacity = backend.len,
            .own_data = false,
            .allocator = allocator,
        };
    }

    /// Create an empty buffer (no capacity, no allocation)
    pub fn empty() Self {
        return Self{
            .buffer = &.{},
            .pos = 0,
            .len = 0,
            .capacity = 0,
            .own_data = false,
            .allocator = null,
        };
    }

    /// Free any owned resources
    pub fn deinit(self: *Self) void {
        if (self.own_data and self.buffer.len > 0 and self.allocator != null) {
            self.allocator.?.free(self.buffer);
        }
        self.* = empty();
    }

    /// Reset the buffer to empty state (keeps capacity)
    pub fn reset(self: *Self) void {
        self.len = 0;
        self.pos = 0;
    }

    /// Move the position to a specific location
    pub fn moveTo(self: *Self, new_position: usize) !void {
        if (new_position > self.capacity) {
            return BufferError.InvalidArgument;
        }
        self.len = new_position;
        self.pos = new_position;
    }

    /// Expand the buffer to a new size
    pub fn expand(self: *Self, new_size: usize) !void {
        if (new_size <= self.capacity) {
            return BufferError.InvalidArgument;
        }

        const allocator = self.allocator orelse return BufferError.OutOfMemory;
        
        if (self.own_data and self.buffer.len > 0) {
            // We own the data, we can resize it
            const new_data = try allocator.realloc(self.buffer, new_size);
            self.buffer = new_data;
        } else {
            // We don't own the data, need to allocate new and copy
            const new_data = try allocator.alloc(u8, new_size);
            if (self.len > 0) {
                @memcpy(new_data[0..self.len], self.buffer[0..self.len]);
            }
            self.buffer = new_data;
            self.own_data = true;
        }

        self.capacity = new_size;
    }

    /// Append data to the buffer, expanding if necessary
    pub fn append(self: *Self, bytes: []const u8) !void {
        if (bytes.len == 0) return; // Nothing to do

        const new_len = self.len + bytes.len;
        
        // Check for overflow
        if (new_len < self.len) {
            return BufferError.BufferOverflow;
        }

        // Expand if necessary
        if (new_len > self.capacity) {
            // Calculate new size with 10% growth + minimum 64 bytes
            const extra = @max(new_len / 10, 64);
            const new_size = new_len + extra;
            
            // Check for overflow again
            if (new_size < new_len) {
                return BufferError.BufferOverflow;
            }
            
            try self.expand(new_size);
        }

        // Copy the data
        @memcpy(self.buffer[self.pos..self.pos + bytes.len], bytes);
        self.pos += bytes.len;
        self.len += bytes.len;
    }

    /// Append a single byte to the buffer
    pub fn appendByte(self: *Self, byte: u8) !void {
        if (self.len == self.capacity) {
            // Calculate new size with 10% growth + minimum 64 bytes  
            const extra = @max(self.capacity / 10, 64);
            const new_size = self.capacity + extra;
            
            // Check for overflow
            if (new_size < self.capacity) {
                return BufferError.BufferOverflow;
            }
            
            try self.expand(new_size);
        }

        self.buffer[self.pos] = byte;
        self.pos += 1;
        self.len += 1;
    }

    /// Consume n bytes from the beginning of the buffer
    /// Moves remaining data to the front
    pub fn consume(self: *Self, n: usize) void {
        if (n >= self.len) {
            // Consuming everything
            self.reset();
            return;
        }

        const remaining = self.len - n;
        // Move remaining data to the front
        std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[n..self.len]);
        
        self.len = remaining;
        self.pos = remaining;
    }

    /// Get the raw data slice (read-only)
    pub fn data(self: *const Self) []const u8 {
        return self.buffer[0..self.len];
    }

    /// Get the raw data slice (mutable)
    pub fn dataMut(self: *Self) []u8 {
        return self.buffer[0..self.len];
    }

    /// Get available space
    pub fn available(self: *const Self) usize {
        return self.capacity - self.len;
    }

    /// Get a slice of available space for writing
    pub fn availableSlice(self: *Self) []u8 {
        return self.buffer[self.len..self.capacity];
    }

    /// Advance the position and length by n bytes (after writing directly to availableSlice)
    pub fn advance(self: *Self, n: usize) !void {
        if (self.len + n > self.capacity) {
            return BufferError.BufferOverflow;
        }
        self.len += n;
        self.pos += n;
    }

    /// Format into the buffer (similar to std.fmt.bufPrint)
    pub fn print(self: *Self, comptime format: []const u8, args: anytype) !void {
        // Estimate needed space
        var counting_writer = std.io.countingWriter(std.io.null_writer);
        try std.fmt.format(counting_writer.writer(), format, args);
        
        const needed = counting_writer.bytes_written;
        
        // Ensure we have enough space
        const new_len = self.len + needed;
        if (new_len > self.capacity) {
            const extra = @max(needed / 10, 64);
            try self.expand(new_len + extra);
        }

        // Write the formatted string
        const written = try std.fmt.bufPrint(self.buffer[self.pos..self.capacity], format, args);
        self.pos += written.len;
        self.len += written.len;
    }
};

// Tests
test "Buffer basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buf = try Buffer.init(allocator, 10);
    defer buf.deinit();

    // Test initial state
    try testing.expectEqual(@as(usize, 10), buf.capacity);
    try testing.expectEqual(@as(usize, 0), buf.len);
    try testing.expectEqual(@as(usize, 10), buf.available());

    // Test append
    try buf.append("hello");
    try testing.expectEqual(@as(usize, 5), buf.len);
    try testing.expectEqualStrings("hello", buf.data());

    // Test append byte
    try buf.appendByte('!');
    try testing.expectEqual(@as(usize, 6), buf.len);
    try testing.expectEqualStrings("hello!", buf.data());
}

test "Buffer expansion" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buf = try Buffer.init(allocator, 5);
    defer buf.deinit();

    // Fill to capacity
    try buf.append("hello");
    try testing.expectEqual(@as(usize, 5), buf.len);

    // This should trigger expansion
    try buf.append(" world");
    try testing.expectEqual(@as(usize, 11), buf.len);
    try testing.expectEqualStrings("hello world", buf.data());
    try testing.expect(buf.capacity > 11);
}

test "Buffer with backend" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var backend = [_]u8{0} ** 10;
    @memcpy(backend[0..5], "hello");

    var buf = try Buffer.initWithBackend(allocator, &backend, 5);
    defer buf.deinit();

    try testing.expectEqual(@as(usize, 5), buf.len);
    try testing.expectEqualStrings("hello", buf.data());
    try testing.expectEqual(false, buf.own_data);

    // Append within capacity
    try buf.append("!");
    try testing.expectEqualStrings("hello!", buf.data());
    try testing.expectEqual(false, buf.own_data);

    // This should trigger ownership change
    try buf.append(" world");
    try testing.expectEqualStrings("hello! world", buf.data());
    try testing.expectEqual(true, buf.own_data);
}

test "Buffer consume" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buf = try Buffer.init(allocator, 20);
    defer buf.deinit();

    try buf.append("hello world");
    try testing.expectEqualStrings("hello world", buf.data());

    // Consume first 6 characters
    buf.consume(6);
    try testing.expectEqualStrings("world", buf.data());
    try testing.expectEqual(@as(usize, 5), buf.len);
}

test "Buffer print" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buf = try Buffer.init(allocator, 50);
    defer buf.deinit();

    try buf.print("Hello {s}, you are {d} years old!", .{ "Alice", 30 });
    try testing.expectEqualStrings("Hello Alice, you are 30 years old!", buf.data());
}