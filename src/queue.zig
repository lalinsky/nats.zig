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
const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;
const Allocator = std.mem.Allocator;

const PopError = error{
    QueueEmpty,
    QueueClosed,
    BufferFrozen,
};

const PushError = error{
    QueueClosed,
    ChunkLimitExceeded,
    OutOfMemory,
    BufferFrozen,
};

/// A single chunk in the linked list with inline data
fn ChunkType(comptime T: type, comptime capacity: usize) type {
    return struct {
        /// The data buffer for this chunk (inline)
        data: [capacity]T,
        /// Number of items written to this chunk (protected by mutex)
        write_pos: usize,
        /// Number of items read from this chunk (protected by mutex)
        read_pos: usize,
        /// Next chunk in the list (protected by mutex when modifying)
        next: ?*Self,
        /// Whether this chunk is full and sealed (protected by mutex)
        is_sealed: bool,

        const Self = @This();

        fn init() Self {
            return .{
                .data = undefined,
                .write_pos = 0,
                .read_pos = 0,
                .next = null,
                .is_sealed = false,
            };
        }

        fn reset(self: *Self) void {
            self.write_pos = 0;
            self.read_pos = 0;
            self.next = null;
            self.is_sealed = false;
        }

        fn availableToWrite(self: *const Self) usize {
            return capacity - self.write_pos;
        }

        fn availableToRead(self: *const Self) usize {
            return self.write_pos - self.read_pos;
        }

        fn isFullyConsumed(self: *const Self) bool {
            return self.is_sealed and self.read_pos >= self.write_pos;
        }

        fn getWriteSlice(self: *Self) []T {
            return self.data[self.write_pos..];
        }

        fn getReadSlice(self: *const Self) []const T {
            return self.data[self.read_pos..self.write_pos];
        }

        /// Push a single item
        fn pushItem(self: *Self, item: T) bool {
            if (self.availableToWrite() == 0) return false;
            self.data[self.write_pos] = item;
            self.write_pos += 1;
            return true;
        }

        /// Pop a single item
        fn popItem(self: *Self) ?T {
            if (self.availableToRead() == 0) return null;
            const item = self.data[self.read_pos];
            self.read_pos += 1;
            return item;
        }
    };
}

/// A view into readable data that can be consumed
pub fn ReadView(comptime T: type, comptime chunk_size: usize) type {
    return struct {
        /// The readable data slice
        data: []const T,
        /// Reference to the chunk
        chunk: *Chunk,
        /// Reference to the parent queue
        queue: *ConcurrentQueue(T, chunk_size),

        const Chunk = ChunkType(T, chunk_size);

        /// Consume items after processing
        pub fn consume(self: *@This(), items_consumed: usize) void {
            if (items_consumed > self.data.len) {
                std.debug.panic("Attempting to consume {} items but only {} available", .{ items_consumed, self.data.len });
            }
            self.queue.consumeItems(self.chunk, items_consumed);
        }
    };
}

/// Pool of reusable chunks to reduce allocations
fn ChunkPool(comptime T: type, comptime chunk_size: usize) type {
    return struct {
        chunks: std.ArrayList(*Chunk),
        max_size: usize,

        const Chunk = ChunkType(T, chunk_size);
        const Self = @This();

        fn init(allocator: Allocator, max_size: usize) Self {
            return .{
                .chunks = std.ArrayList(*Chunk).init(allocator),
                .max_size = max_size,
            };
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            for (self.chunks.items) |chunk| {
                allocator.destroy(chunk);
            }
            self.chunks.deinit();
        }

        fn get(self: *Self) ?*Chunk {
            if (self.chunks.items.len == 0) return null;
            return self.chunks.pop();
        }

        fn put(self: *Self, chunk: *Chunk) bool {
            if (self.chunks.items.len >= self.max_size) {
                return false;
            }
            chunk.reset();
            self.chunks.append(chunk) catch return false;
            return true;
        }
    };
}

/// Concurrent queue using linked list of chunks
pub fn ConcurrentQueue(comptime T: type, comptime chunk_size: usize) type {
    return struct {
        allocator: Allocator,

        /// Single mutex protecting all operations
        mutex: Mutex,
        /// Condition variable for waiting readers
        data_cond: Condition,

        /// Head of the linked list (oldest chunk, protected by mutex)
        head: ?*Chunk,
        /// Tail of the linked list (newest chunk, protected by mutex)
        tail: ?*Chunk,

        /// Counter for available items (protected by mutex)
        items_available: usize,
        /// Total number of chunks allocated
        total_chunks: usize,
        /// Maximum chunks allowed (0 = unlimited)
        max_chunks: usize,
        /// Maximum total bytes allowed (0 = unlimited)
        max_size: usize,

        /// Pool of reusable chunks (protected by mutex)
        chunk_pool: Pool,
        /// Whether the queue is closed for writes (protected by mutex)
        is_closed: bool,
        /// Whether the buffer is frozen (blocks both readers and writers, protected by mutex)
        is_frozen: bool,

        const Self = @This();
        const Chunk = ChunkType(T, chunk_size);
        const Pool = ChunkPool(T, chunk_size);
        pub const View = ReadView(T, chunk_size);

        /// Configuration options
        pub const Config = struct {
            /// Maximum number of chunks to keep in reuse pool
            max_pool_size: usize = 8,
            /// Maximum total chunks allowed (0 = unlimited)
            max_chunks: usize = 0,
            /// Maximum total bytes allowed (0 = unlimited)
            max_size: usize = 0,
        };

        pub fn init(allocator: Allocator, config: Config) Self {
            return .{
                .allocator = allocator,
                .mutex = .{},
                .data_cond = .{},
                .head = null,
                .tail = null,
                .items_available = 0,
                .total_chunks = 0,
                .max_chunks = config.max_chunks,
                .max_size = config.max_size,
                .chunk_pool = Pool.init(allocator, config.max_pool_size),
                .is_closed = false,
                .is_frozen = false,
            };
        }

        pub fn deinit(self: *Self) void {
            // Free all chunks in the linked list
            var current = self.head;
            while (current) |chunk| {
                const next = chunk.next;
                self.allocator.destroy(chunk);
                current = next;
            }

            // Free chunks in the pool
            self.chunk_pool.deinit(self.allocator);
        }

        /// Push a single item (thread-safe)
        pub fn push(self: *Self, item: T) PushError!void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.is_closed) {
                return PushError.QueueClosed;
            }
            if (self.is_frozen) {
                return PushError.BufferFrozen;
            }

            // Check size limit before adding (overflow-safe)
            if (self.max_size > 0) {
                const max_items = self.max_size / @sizeOf(T);
                if (self.items_available >= max_items) {
                    return PushError.OutOfMemory;
                }
            }

            const chunk = try self.ensureWritableChunk();
            const success = chunk.pushItem(item);
            std.debug.assert(success);

            if (chunk.availableToWrite() == 0) {
                chunk.is_sealed = true;
            }

            self.items_available += 1;
            self.data_cond.signal();
        }

        /// Push multiple items (thread-safe)
        pub fn pushSlice(self: *Self, items: []const T) PushError!void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.is_closed) {
                return PushError.QueueClosed;
            }
            if (self.is_frozen) {
                return PushError.BufferFrozen;
            }

            // Check size limit before adding (overflow-safe)
            if (self.max_size > 0) {
                const max_items = self.max_size / @sizeOf(T);
                if (self.items_available >= max_items) {
                    return PushError.OutOfMemory;
                }
                if (items.len > max_items - self.items_available) {
                    return PushError.OutOfMemory;
                }
            }
            var remaining = items;
            var total_written: usize = 0;

            while (remaining.len > 0) {
                const chunk = try self.ensureWritableChunk();

                const available = chunk.availableToWrite();
                const to_write = @min(available, remaining.len);

                @memcpy(chunk.getWriteSlice()[0..to_write], remaining[0..to_write]);

                chunk.write_pos += to_write;
                remaining = remaining[to_write..];
                total_written += to_write;

                if (chunk.availableToWrite() == 0) {
                    chunk.is_sealed = true;
                }
            }

            self.items_available += total_written;
            self.data_cond.signal();
        }

        /// Pop a single item with timeout (0 = non-blocking)
        pub fn pop(self: *Self, timeout_ms: u64) PopError!T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Fast path for non-blocking (timeout_ms == 0)
            if (timeout_ms == 0) {
                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                if (self.is_frozen) {
                    return PopError.BufferFrozen;
                }
                if (self.items_available == 0) {
                    return PopError.QueueEmpty;
                }
            } else {
                var timer = std.time.Timer.start() catch unreachable;
                const timeout_ns = timeout_ms * std.time.ns_per_ms;

                while ((self.items_available == 0 or self.is_frozen) and !self.is_closed) {
                    const elapsed_ns = timer.read();
                    if (elapsed_ns >= timeout_ns) {
                        if (self.is_closed and self.items_available == 0) {
                            return PopError.QueueClosed;
                        }
                        if (self.is_frozen) {
                            return PopError.BufferFrozen;
                        }
                        return PopError.QueueEmpty;
                    }

                    const remaining_ns = timeout_ns - elapsed_ns;
                    self.data_cond.timedWait(&self.mutex, remaining_ns) catch {};
                }

                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                if (self.is_frozen) {
                    return PopError.BufferFrozen;
                }
            }

            // At this point we have data, pop it
            const chunk = self.head orelse return PopError.QueueEmpty;
            const item = chunk.popItem() orelse return PopError.QueueEmpty;

            self.items_available -= 1;

            // Check if chunk is fully consumed
            if (chunk.isFullyConsumed()) {
                self.head = chunk.next;

                if (self.tail == chunk) {
                    self.tail = null;
                }

                self.recycleChunk(chunk);
            }

            return item;
        }

        /// Try to pop a single item (non-blocking, returns null if empty)
        pub fn tryPop(self: *Self) ?T {
            return self.pop(0) catch null;
        }

        /// Get readable slice with timeout (0 = non-blocking, maxInt(u64) = wait forever)
        pub fn getSlice(self: *Self, timeout_ms: u64) PopError!View {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Fast path for non-blocking (timeout_ms == 0)
            if (timeout_ms == 0) {
                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                if (self.is_frozen) {
                    return PopError.BufferFrozen;
                }
                if (self.items_available == 0) {
                    return PopError.QueueEmpty;
                }
            } else if (timeout_ms == std.math.maxInt(u64)) {
                // Wait indefinitely
                while ((self.items_available == 0 or self.is_frozen) and !self.is_closed) {
                    self.data_cond.wait(&self.mutex);
                }

                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                if (self.is_frozen) {
                    return PopError.BufferFrozen;
                }
            } else {
                // Wait with timeout
                var timer = std.time.Timer.start() catch unreachable;
                const timeout_ns = timeout_ms * std.time.ns_per_ms;

                while ((self.items_available == 0 or self.is_frozen) and !self.is_closed) {
                    const elapsed_ns = timer.read();
                    if (elapsed_ns >= timeout_ns) {
                        if (self.is_closed and self.items_available == 0) {
                            return PopError.QueueClosed;
                        }
                        if (self.is_frozen) {
                            return PopError.BufferFrozen;
                        }
                        return PopError.QueueEmpty;
                    }

                    const remaining_ns = timeout_ns - elapsed_ns;
                    self.data_cond.timedWait(&self.mutex, remaining_ns) catch {};
                }

                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                if (self.is_frozen) {
                    return PopError.BufferFrozen;
                }
            }

            const chunk = self.head orelse return PopError.QueueEmpty;
            const available = chunk.availableToRead();

            if (available == 0) {
                return PopError.QueueEmpty;
            }

            return View{
                .data = chunk.getReadSlice(),
                .chunk = chunk,
                .queue = self,
            };
        }

        /// Try to get readable slice without blocking
        pub fn tryGetSlice(self: *Self) ?View {
            return self.getSlice(0) catch null;
        }

        /// Consume items after processing
        pub fn consumeItems(self: *Self, chunk: *Chunk, items_consumed: usize) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            chunk.read_pos += items_consumed;
            self.items_available -= items_consumed;

            // Check if we can advance head and recycle chunks
            while (self.head) |head_chunk| {
                if (!head_chunk.isFullyConsumed()) break;

                self.head = head_chunk.next;

                if (self.tail == head_chunk) {
                    self.tail = null;
                }

                self.recycleChunk(head_chunk);
            }
        }

        /// Get total items available for reading
        pub fn getItemsAvailable(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.items_available;
        }

        /// Check if queue has data without locking
        pub fn hasData(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.items_available > 0;
        }

        /// Close the queue to prevent further writes
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.is_closed = true;
            self.data_cond.broadcast();
        }

        /// Check if the queue is closed
        pub fn isClosed(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.is_closed;
        }

        /// Freeze the buffer (blocks both readers and writers)
        pub fn freeze(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.is_frozen = true;
        }

        /// Unfreeze the buffer and wake up waiting threads
        pub fn unfreeze(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.is_frozen = false;
            self.data_cond.broadcast();
        }

        /// Check if buffer is frozen
        pub fn isFrozen(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.is_frozen;
        }

        /// Reset the queue to empty state
        pub fn reset(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Free all chunks in the linked list
            var current = self.head;
            while (current) |chunk| {
                const next = chunk.next;
                self.recycleChunk(chunk);
                current = next;
            }

            // Reset state
            self.head = null;
            self.tail = null;
            self.items_available = 0;
            self.is_closed = false;
            self.is_frozen = false;

            // Wake up any waiting threads
            self.data_cond.broadcast();
        }

        // Private helper functions

        fn ensureWritableChunk(self: *Self) PushError!*Chunk {
            if (self.tail) |tail| {
                if (!tail.is_sealed and tail.availableToWrite() > 0) {
                    return tail;
                }
            }

            const new_chunk = try self.allocateChunk();

            if (self.tail) |tail| {
                tail.next = new_chunk;
            } else {
                self.head = new_chunk;
            }
            self.tail = new_chunk;

            return new_chunk;
        }

        fn allocateChunk(self: *Self) PushError!*Chunk {
            if (self.chunk_pool.get()) |chunk| {
                return chunk;
            }
            if (self.max_chunks > 0 and self.total_chunks >= self.max_chunks) {
                return PushError.ChunkLimitExceeded;
            }

            const chunk = self.allocator.create(Chunk) catch return PushError.OutOfMemory;
            chunk.* = Chunk.init();
            self.total_chunks += 1;
            return chunk;
        }

        fn recycleChunk(self: *Self, chunk: *Chunk) void {
            if (!self.chunk_pool.put(chunk)) {
                self.allocator.destroy(chunk);
                self.total_chunks -= 1;
            }
        }
    };
}

/// Specialized byte buffer using the generic queue
pub fn ConcurrentWriteBuffer(comptime chunk_size: usize) type {
    return struct {
        queue: Queue,

        const Self = @This();
        const Queue = ConcurrentQueue(u8, chunk_size);
        pub const Config = Queue.Config;

        pub fn init(allocator: Allocator, config: Config) Self {
            return .{
                .queue = Queue.init(allocator, config),
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
        }

        /// Append bytes to the buffer
        pub fn append(self: *Self, data: []const u8) PushError!void {
            return self.queue.pushSlice(data);
        }

        /// Close the buffer to prevent further writes
        pub fn close(self: *Self) void {
            self.queue.close();
        }

        /// Check if the buffer is closed
        pub fn isClosed(self: *Self) bool {
            return self.queue.isClosed();
        }

        /// Get readable byte slice
        pub fn tryGetSlice(self: *Self) ?Queue.View {
            return self.queue.tryGetSlice();
        }

        /// Get readable byte slice with timeout
        pub fn getSlice(self: *Self, timeout_ms: u64) !Queue.View {
            return self.queue.getSlice(timeout_ms);
        }

        /// Get bytes available
        pub fn getBytesAvailable(self: *Self) usize {
            return self.queue.getItemsAvailable();
        }

        /// Check if has data
        pub fn hasData(self: *Self) bool {
            return self.queue.hasData();
        }

        /// Reset the buffer to empty state
        pub fn reset(self: *Self) void {
            self.queue.reset();
        }

        /// Freeze the buffer (blocks both readers and writers)
        pub fn freeze(self: *Self) void {
            self.queue.freeze();
        }

        /// Unfreeze the buffer and wake up waiting threads
        pub fn unfreeze(self: *Self) void {
            self.queue.unfreeze();
        }

        /// Check if buffer is frozen
        pub fn isFrozen(self: *Self) bool {
            return self.queue.isFrozen();
        }

        /// Get multiple readable slices for vectored I/O
        pub fn gatherReadVectors(self: *Self, iovecs: []std.posix.iovec_const) usize {
            self.queue.mutex.lock();
            defer self.queue.mutex.unlock();

            var count: usize = 0;
            var current = self.queue.head;

            while (current) |chunk| {
                if (count >= iovecs.len) break;

                const slice = chunk.getReadSlice();
                if (slice.len > 0) {
                    iovecs[count] = .{
                        .base = @constCast(slice.ptr),
                        .len = slice.len,
                    };
                    count += 1;
                }

                if (!chunk.is_sealed and chunk.next != null) {
                    break;
                }

                current = chunk.next;
            }

            return count;
        }

        /// Consume bytes after vectored write
        pub fn consumeBytesMultiple(self: *Self, total_bytes: usize) void {
            self.queue.mutex.lock();
            defer self.queue.mutex.unlock();

            // Validate that we're not consuming more than available
            if (total_bytes > self.queue.items_available) {
                std.debug.panic("Attempting to consume {} bytes but only {} available", .{ total_bytes, self.queue.items_available });
            }

            var remaining = total_bytes;

            self.queue.items_available -= total_bytes;

            while (self.queue.head) |head_chunk| {
                if (remaining == 0) break;

                const available = head_chunk.availableToRead();
                const to_consume = @min(available, remaining);

                head_chunk.read_pos += to_consume;
                remaining -= to_consume;

                if (head_chunk.isFullyConsumed()) {
                    self.queue.head = head_chunk.next;

                    if (self.queue.tail == head_chunk) {
                        self.queue.tail = null;
                    }

                    self.queue.recycleChunk(head_chunk);
                }

                if (to_consume < available) {
                    break;
                }
            }
        }

        /// Move all data from this buffer to another buffer atomically (no copy).
        pub fn moveToBuffer(self: *Self, dest: *Self) PushError!void {
            if (self == dest) return; // no-op

            // Lock both buffers in a stable order to avoid deadlocks.
            const self_addr = @intFromPtr(self);
            const dest_addr = @intFromPtr(dest);
            var first: *Self = if (self_addr <= dest_addr) self else dest;
            var second: *Self = if (self_addr <= dest_addr) dest else self;

            first.queue.mutex.lock();
            defer first.queue.mutex.unlock();
            second.queue.mutex.lock();
            defer second.queue.mutex.unlock();

            // Use direct fields (don't call methods that relock).
            if (self.queue.items_available == 0) return;
            if (dest.queue.is_closed) return PushError.QueueClosed;

            // Count chunks to enforce dest.max_chunks if needed.
            var moved_chunk_count: usize = 0;
            var cur = self.queue.head;
            while (cur) |ch| : (cur = ch.next) {
                moved_chunk_count += 1;
            }

            // Enforce dest limits before splicing (overflow-safe).
            if (dest.queue.max_chunks > 0) {
                if (dest.queue.total_chunks >= dest.queue.max_chunks) {
                    return PushError.ChunkLimitExceeded;
                }
                if (moved_chunk_count > dest.queue.max_chunks - dest.queue.total_chunks) {
                    return PushError.ChunkLimitExceeded;
                }
            }
            if (dest.queue.max_size > 0) {
                const max_items = dest.queue.max_size / @sizeOf(u8);
                if (dest.queue.items_available >= max_items) {
                    return PushError.OutOfMemory;
                }
                if (self.queue.items_available > max_items - dest.queue.items_available) {
                    return PushError.OutOfMemory;
                }
            }

            // Splice self's list onto dest's list.
            if (dest.queue.tail) |tail| {
                tail.next = self.queue.head;
            } else {
                dest.queue.head = self.queue.head;
            }
            dest.queue.tail = self.queue.tail;
            dest.queue.items_available += self.queue.items_available;
            dest.queue.total_chunks += moved_chunk_count;
            // Wake a waiting reader on dest (consistent with push/pushSlice)
            dest.queue.data_cond.signal();

            // Reset source queue state (we transferred ownership).
            self.queue.head = null;
            self.queue.tail = null;
            self.queue.items_available = 0;
            // Sanity: we only subtract list-chunks; pooled chunks remain accounted.
            std.debug.assert(self.queue.total_chunks >= moved_chunk_count);
            self.queue.total_chunks -= moved_chunk_count;
        }

        /// Wait for data to become available with timeout (0 = non-blocking)
        pub fn waitForData(self: *Self, timeout_ms: u64) !void {
            self.queue.mutex.lock();
            defer self.queue.mutex.unlock();

            // Fast path for non-blocking
            if (timeout_ms == 0) {
                if (self.queue.is_closed and self.queue.items_available == 0) {
                    return error.QueueClosed;
                }
                if (self.queue.is_frozen) {
                    return error.BufferFrozen;
                }
                if (self.queue.items_available == 0) {
                    return error.QueueEmpty;
                }
                return;
            }

            var timer = std.time.Timer.start() catch unreachable;
            const timeout_ns = timeout_ms * std.time.ns_per_ms;

            while ((self.queue.items_available == 0 or self.queue.is_frozen) and !self.queue.is_closed) {
                const elapsed_ns = timer.read();
                if (elapsed_ns >= timeout_ns) {
                    if (self.queue.is_closed and self.queue.items_available == 0) {
                        return error.QueueClosed;
                    }
                    if (self.queue.is_frozen) {
                        return error.BufferFrozen;
                    }
                    return error.QueueEmpty;
                }

                const remaining_ns = timeout_ns - elapsed_ns;
                self.queue.data_cond.timedWait(&self.queue.mutex, remaining_ns) catch {};
            }

            if (self.queue.is_closed and self.queue.items_available == 0) {
                return error.QueueClosed;
            }
            if (self.queue.is_frozen) {
                return error.BufferFrozen;
            }
        }

        /// Wait for more data to become available with timeout
        pub fn waitForMoreData(self: *Self, timeout_ns: u64) !void {
            self.queue.mutex.lock();
            defer self.queue.mutex.unlock();

            if (self.queue.is_closed) {
                return error.QueueClosed;
            }

            const initial_data = self.queue.items_available;
            var timer = std.time.Timer.start() catch unreachable;

            while (self.queue.items_available <= initial_data and !self.queue.is_closed) {
                const elapsed_ns = timer.read();
                if (elapsed_ns >= timeout_ns) break;

                const remaining_ns = timeout_ns - elapsed_ns;
                self.queue.data_cond.timedWait(&self.queue.mutex, remaining_ns) catch {};
            }

            // Check if closed after waiting
            if (self.queue.is_closed) {
                return error.QueueClosed;
            }
        }
    };
}

// Tests
test "generic queue with integers" {
    const allocator = std.testing.allocator;

    const IntQueue = ConcurrentQueue(i32, 4);
    var queue = IntQueue.init(allocator, .{});
    defer queue.deinit();

    // Push individual items
    try queue.push(42);
    try queue.push(43);

    // Pop them
    try std.testing.expectEqual(@as(i32, 42), try queue.pop(1000));
    try std.testing.expectEqual(@as(i32, 43), queue.tryPop().?);

    // Should be empty
    try std.testing.expect(queue.tryPop() == null);
}

test "generic queue with structs" {
    const allocator = std.testing.allocator;

    const Message = struct {
        id: u32,
        data: [8]u8,
    };

    const MsgQueue = ConcurrentQueue(Message, 16);
    var queue = MsgQueue.init(allocator, .{});
    defer queue.deinit();

    // Push messages
    const messages = [_]Message{
        .{ .id = 1, .data = "hello   ".* },
        .{ .id = 2, .data = "world   ".* },
    };

    try queue.pushSlice(&messages);

    // Get slice view
    var view_opt = queue.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqual(@as(usize, 2), view.data.len);
        try std.testing.expectEqual(@as(u32, 1), view.data[0].id);
        view.consume(2);
    }
}

test "byte buffer specialization" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    try buffer.append("Hello, World!");

    var view_opt = buffer.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.consume(view.data.len);
    }
}

test "concurrent push and pop" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(u64, 32);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    const Producer = struct {
        fn run(q: *Queue) !void {
            for (0..100) |i| {
                try q.push(i);
            }
        }
    };

    var sum: u64 = 0;

    const Consumer = struct {
        fn run(q: *Queue, s: *u64) !void {
            for (0..100) |_| {
                s.* += try q.pop(1000);
            }
        }
    };

    const producer = try std.Thread.spawn(.{}, Producer.run, .{&queue});
    const consumer = try std.Thread.spawn(.{}, Consumer.run, .{ &queue, &sum });

    producer.join();
    consumer.join();

    // Sum of 0..99 = 4950
    try std.testing.expectEqual(4950, sum);
}

test "queue close functionality" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Push some items before closing
    try queue.push(1);
    try queue.push(2);

    // Close the queue
    queue.close();

    // Should not be able to push after closing
    try std.testing.expectError(PushError.QueueClosed, queue.push(3));
    try std.testing.expectError(PushError.QueueClosed, queue.pushSlice(&[_]i32{ 4, 5 }));

    // Should still be able to read existing data
    try std.testing.expectEqual(@as(i32, 1), try queue.pop(1000));
    try std.testing.expectEqual(@as(i32, 2), queue.tryPop().?);

    // Verify closed state
    try std.testing.expect(queue.isClosed());
}

test "blocking pop handles queue closure" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Start a thread that will close the queue after a delay
    const Closer = struct {
        fn run(q: *Queue) !void {
            std.time.sleep(10 * std.time.ns_per_ms);
            q.close();
        }
    };

    const closer = try std.Thread.spawn(.{}, Closer.run, .{&queue});

    // This should return QueueClosed when the queue is closed
    const result = queue.pop(1000);
    try std.testing.expectError(PopError.QueueClosed, result);

    closer.join();
}

test "getSlice handles queue closure with indefinite wait" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Start a thread that will close the queue after a delay
    const Closer = struct {
        fn run(q: *Queue) !void {
            std.time.sleep(10 * std.time.ns_per_ms);
            q.close();
        }
    };

    const closer = try std.Thread.spawn(.{}, Closer.run, .{&queue});

    // This should return QueueClosed when the queue is closed
    const result = queue.getSlice(std.math.maxInt(u64));
    try std.testing.expectError(PopError.QueueClosed, result);

    closer.join();
}

test "buffer close functionality" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Append some data before closing
    try buffer.append("Hello");

    // Close the buffer
    buffer.close();

    // Should not be able to append after closing
    try std.testing.expectError(PushError.QueueClosed, buffer.append(" World"));

    // Should still be able to read existing data
    var view_opt = buffer.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello", view.data);
        view.consume(view.data.len);
    }

    // Verify closed state
    try std.testing.expect(buffer.isClosed());
}

test "buffer moveToBuffer functionality" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(32);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data to source buffer
    try source.append("Hello, ");
    try source.append("World!");

    // Verify source has data
    try std.testing.expectEqual(@as(usize, 13), source.getBytesAvailable());

    // Move data from source to destination
    try source.moveToBuffer(&dest);

    // Verify source is now empty
    try std.testing.expectEqual(@as(usize, 0), source.getBytesAvailable());

    // Verify destination has the data
    try std.testing.expectEqual(@as(usize, 13), dest.getBytesAvailable());

    // Read and verify the moved data
    var view_opt = dest.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.consume(view.data.len);
    }
}

test "buffer moveToBuffer with multiple chunks" {
    const allocator = std.testing.allocator;

    // Use small chunk size to force multiple chunks
    const Buffer = ConcurrentWriteBuffer(8);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data that spans multiple chunks
    try source.append("First chunk "); // 12 bytes, spans 2 chunks
    try source.append("Second chunk "); // 13 bytes, spans 2 more chunks
    try source.append("Third"); // 5 bytes

    const total_bytes = 12 + 13 + 5; // 30 bytes
    try std.testing.expectEqual(total_bytes, source.getBytesAvailable());

    // Move all data to destination
    try source.moveToBuffer(&dest);

    // Verify source is empty
    try std.testing.expectEqual(@as(usize, 0), source.getBytesAvailable());

    // Verify destination has all the data
    try std.testing.expectEqual(total_bytes, dest.getBytesAvailable());

    // Read and verify the moved data by consuming all chunks
    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();

    while (dest.getBytesAvailable() > 0) {
        var view_opt = dest.tryGetSlice();
        if (view_opt) |*view| {
            try result.appendSlice(view.data);
            view.consume(view.data.len);
        } else {
            break;
        }
    }

    try std.testing.expectEqualStrings("First chunk Second chunk Third", result.items);
}

test "buffer moveToBuffer empty source" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add some data to destination first
    try dest.append("Already here");

    // Move from empty source (should be no-op)
    try source.moveToBuffer(&dest);

    // Verify destination still has original data
    try std.testing.expectEqual(@as(usize, 12), dest.getBytesAvailable());

    var view_opt = dest.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Already here", view.data);
        view.consume(view.data.len);
    }
}

test "buffer max_size limit" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{ .max_size = 10 });
    defer buffer.deinit();

    // Should be able to add up to max_size
    try buffer.append("Hello");
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable());

    // Should be able to add exactly to the limit
    try buffer.append("World");
    try std.testing.expectEqual(@as(usize, 10), buffer.getBytesAvailable());

    // Should fail when exceeding the limit
    try std.testing.expectError(PushError.OutOfMemory, buffer.append("!"));

    // Verify data is still intact
    var view_opt = buffer.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("HelloWorld", view.data);
        view.consume(view.data.len);
    }
}

test "freeze/unfreeze basic functionality" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Add some data
    try queue.push(42);
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable());

    // Freeze buffer
    queue.freeze();
    try std.testing.expect(queue.isFrozen());

    // Non-blocking read should return BufferFrozen
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(0));
    try std.testing.expectError(PopError.BufferFrozen, queue.getSlice(0));

    // Data should still be available after failed reads
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable());

    // Unfreeze buffer
    queue.unfreeze();
    try std.testing.expect(!queue.isFrozen());

    // Should be able to read now
    try std.testing.expectEqual(@as(i32, 42), try queue.pop(1000));
    try std.testing.expectEqual(@as(usize, 0), queue.getItemsAvailable());
}

test "writers cannot push while buffer is frozen" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Freeze buffer
    queue.freeze();

    // Writers should be blocked
    try std.testing.expectError(PushError.BufferFrozen, queue.push(1));
    try std.testing.expectError(PushError.BufferFrozen, queue.pushSlice(&[_]i32{ 2, 3 }));
    try std.testing.expectEqual(@as(usize, 0), queue.getItemsAvailable());

    // Readers should be blocked
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(0));

    // Unfreeze and add data
    queue.unfreeze();
    try queue.push(1);
    try queue.push(2);
    try std.testing.expectEqual(@as(i32, 1), try queue.pop(1000));
    try std.testing.expectEqual(@as(i32, 2), try queue.pop(1000));
}

test "blocking operations during freeze" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    queue.freeze();

    // Start a thread that will resume after a delay
    const Resumer = struct {
        fn run(q: *Queue) !void {
            std.time.sleep(10 * std.time.ns_per_ms);
            q.unfreeze();
            try q.push(99);
        }
    };

    const resumer = try std.Thread.spawn(.{}, Resumer.run, .{&queue});

    // This should block until resumed and data is available
    const result = try queue.pop(1000);
    try std.testing.expectEqual(@as(i32, 99), result);

    resumer.join();
}

test "timeout while frozen returns BufferFrozen" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Add data then freeze buffer
    try queue.push(42);
    queue.freeze();

    // Should timeout and return BufferFrozen
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(10));
    try std.testing.expectError(PopError.BufferFrozen, queue.getSlice(10));

    // Data should still be there
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable());
}

test "freeze/unfreeze interaction with close" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Freeze then close
    queue.freeze();
    queue.close();

    // Should return QueueClosed instead of BufferFrozen
    try std.testing.expectError(PopError.QueueClosed, queue.pop(0));

    // Can't push when closed
    try std.testing.expectError(PushError.QueueClosed, queue.push(42));
}

test "ConcurrentWriteBuffer freeze/unfreeze functionality" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Add some data
    try buffer.append("Hello, World!");

    // Freeze buffer
    buffer.freeze();
    try std.testing.expect(buffer.isFrozen());

    // Non-blocking read should return BufferFrozen
    try std.testing.expect(buffer.tryGetSlice() == null);
    try std.testing.expectError(PopError.BufferFrozen, buffer.getSlice(0));

    // Writers should be blocked
    try std.testing.expectError(PushError.BufferFrozen, buffer.append(" More data"));

    // Unfreeze buffer
    buffer.unfreeze();
    try std.testing.expect(!buffer.isFrozen());

    // Now writers work
    try buffer.append(" More data");

    // Should be able to read now
    var view_opt = buffer.tryGetSlice();
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World! More data", view.data);
        view.consume(view.data.len);
    }
}

test "ConcurrentWriteBuffer waitForData smoke test" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Test waitForData with immediate data available
    try buffer.append("Hello");

    // Should return immediately since data is available
    try buffer.waitForData(1000);

    // Verify data is still there
    try std.testing.expect(buffer.hasData());
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable());
}

test "ConcurrentWriteBuffer waitForData with closed buffer" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close();

    // waitForData should return QueueClosed error
    try std.testing.expectError(error.QueueClosed, buffer.waitForData(1000));
}

test "ConcurrentWriteBuffer waitForMoreData smoke test" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Add initial data
    try buffer.append("Hello");

    // Wait for more data with short timeout (should timeout)
    try buffer.waitForMoreData(1 * std.time.ns_per_ms);

    // Should still work normally
    try std.testing.expect(buffer.hasData());
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable());
}

test "ConcurrentWriteBuffer waitForMoreData with closed buffer" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close();

    // waitForMoreData should return QueueClosed error
    try std.testing.expectError(error.QueueClosed, buffer.waitForMoreData(1 * std.time.ns_per_ms));
}
