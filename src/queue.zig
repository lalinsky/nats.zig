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
const zio = @import("zio");
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
        pub fn consume(self: *@This(), rt: *zio.Runtime, items_consumed: usize) void {
            if (items_consumed > self.data.len) {
                std.debug.panic("Attempting to consume {} items but only {} available", .{ items_consumed, self.data.len });
            }
            self.queue.consumeItems(rt, self.chunk, items_consumed);
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
            _ = allocator;
            return .{
                .chunks = std.ArrayList(*Chunk){},
                .max_size = max_size,
            };
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            for (self.chunks.items) |chunk| {
                allocator.destroy(chunk);
            }
            self.chunks.deinit(allocator);
        }

        fn get(self: *Self) ?*Chunk {
            if (self.chunks.items.len == 0) return null;
            return self.chunks.pop();
        }

        fn put(self: *Self, allocator: Allocator, chunk: *Chunk) bool {
            if (self.chunks.items.len >= self.max_size) {
                return false;
            }
            chunk.reset();
            self.chunks.append(allocator, chunk) catch return false;
            return true;
        }
    };
}

/// Concurrent queue using linked list of chunks
pub fn ConcurrentQueue(comptime T: type, comptime chunk_size: usize) type {
    return struct {
        allocator: Allocator,

        /// Single mutex protecting all operations
        mutex: zio.Mutex,
        /// Condition variable for waiting readers
        data_cond: zio.Condition,

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
        /// Reset counter to detect buffer state changes
        reset_id: u64,

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
                .reset_id = 0,
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

        /// Push a single item (fiber-safe, cancelable)
        pub fn push(self: *Self, rt: *zio.Runtime, item: T) (zio.Cancelable || PushError)!void {
            try self.mutex.lock(rt);
            defer self.mutex.unlock(rt);

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
            self.data_cond.signal(rt);
        }

        /// Push multiple items (fiber-safe, cancelable)
        pub fn pushSlice(self: *Self, rt: *zio.Runtime, items: []const T) (zio.Cancelable || PushError)!void {
            try self.mutex.lock(rt);
            defer self.mutex.unlock(rt);

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
            self.data_cond.signal(rt);
        }

        /// Internal helper to wait for data availability with timeout handling
        /// Assumes mutex is already held.
        fn waitForDataInternal(self: *Self, rt: *zio.Runtime, timeout_ms: u64) (zio.Cancelable || PopError)!void {
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
                return;
            } else if (timeout_ms == std.math.maxInt(u64)) {
                // Wait indefinitely
                while ((self.items_available == 0 or self.is_frozen) and !self.is_closed) {
                    try self.data_cond.wait(rt, &self.mutex);
                }

                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                return;
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
                    self.data_cond.timedWait(rt, &self.mutex, .fromNanoseconds(remaining_ns)) catch |err| switch (err) {
                        error.Canceled => return error.Canceled,
                        error.Timeout => {}, // Continue loop to check conditions
                    };
                }

                if (self.is_closed and self.items_available == 0) {
                    return PopError.QueueClosed;
                }
                return;
            }
        }

        /// Pop a single item with timeout (0 = non-blocking)
        pub fn pop(self: *Self, rt: *zio.Runtime, timeout_ms: u64) (zio.Cancelable || PopError)!T {
            try self.mutex.lock(rt);
            defer self.mutex.unlock(rt);

            try self.waitForDataInternal(rt, timeout_ms);

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
        pub fn tryPop(self: *Self, rt: *zio.Runtime) ?T {
            return self.pop(rt, 0) catch null;
        }

        /// Get readable slice with timeout (0 = non-blocking, maxInt(u64) = wait forever)
        pub fn getSlice(self: *Self, rt: *zio.Runtime, timeout_ms: u64) (zio.Cancelable || PopError)!View {
            try self.mutex.lock(rt);
            defer self.mutex.unlock(rt);

            try self.waitForDataInternal(rt, timeout_ms);

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
        pub fn tryGetSlice(self: *Self, rt: *zio.Runtime) ?View {
            return self.getSlice(rt, 0) catch null;
        }

        /// Consume items after processing
        pub fn consumeItems(self: *Self, rt: *zio.Runtime, chunk: *Chunk, items_consumed: usize) void {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

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
        pub fn getItemsAvailable(self: *Self, rt: *zio.Runtime) usize {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            return self.items_available;
        }

        /// Check if queue has data
        pub fn hasData(self: *Self, rt: *zio.Runtime) bool {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            return self.items_available > 0;
        }

        /// Close the queue to prevent further writes
        pub fn close(self: *Self, rt: *zio.Runtime) void {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            self.is_closed = true;
            self.is_frozen = false;
            self.data_cond.broadcast(rt);
        }

        /// Check if the queue is closed
        pub fn isClosed(self: *Self, rt: *zio.Runtime) bool {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            return self.is_closed;
        }

        /// Freeze the buffer (blocks both readers and writers)
        pub fn freeze(self: *Self, rt: *zio.Runtime) void {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            self.is_frozen = true;
        }

        /// Unfreeze the buffer and wake up waiting fibers
        pub fn unfreeze(self: *Self, rt: *zio.Runtime) void {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            self.is_frozen = false;
            self.data_cond.broadcast(rt);
        }

        /// Check if buffer is frozen
        pub fn isFrozen(self: *Self, rt: *zio.Runtime) bool {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

            return self.is_frozen;
        }

        /// Reset the queue to empty state
        pub fn reset(self: *Self, rt: *zio.Runtime) void {
            self.mutex.lockUncancelable(rt);
            defer self.mutex.unlock(rt);

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
            self.reset_id +%= 1;

            // Wake up any waiting fibers
            self.data_cond.broadcast(rt);
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
            if (!self.chunk_pool.put(self.allocator, chunk)) {
                self.allocator.destroy(chunk);
                self.total_chunks -= 1;
            }
        }
    };
}

/// A view into vectored readable data that can be consumed safely
pub fn VectorGather(comptime T: type, comptime chunk_size: usize) type {
    const Chunk = ChunkType(T, chunk_size);

    return struct {
        reset_id: u64,
        first_chunk: *Chunk,
        first_chunk_read_pos: usize,
        slices: [][]const u8,
        total_bytes: usize,
        buffer: *ConcurrentWriteBuffer(chunk_size),

        const Self = @This();

        pub fn consume(self: Self, rt: *zio.Runtime, bytes_consumed: usize) (zio.Cancelable || error{ BufferReset, ConcurrentConsumer })!void {
            if (bytes_consumed > self.total_bytes) {
                std.debug.panic("Attempting to consume {} bytes but only {} were gathered", .{ bytes_consumed, self.total_bytes });
            }

            try self.buffer.queue.mutex.lock(rt);
            defer self.buffer.queue.mutex.unlock(rt);

            // Validate reset ID hasn't changed
            if (self.reset_id != self.buffer.queue.reset_id) {
                return error.BufferReset;
            }

            // Validate we're still the only consumer (first chunk unchanged)
            if (self.buffer.queue.head != self.first_chunk) {
                return error.ConcurrentConsumer;
            }

            // Validate read position hasn't been advanced by another consumer
            if (self.first_chunk.read_pos != self.first_chunk_read_pos) {
                return error.ConcurrentConsumer;
            }

            // Proceed with consumption (no freeze check - data was already gathered)
            self.buffer.consumeBytesInternal(bytes_consumed);
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
        pub const Gather = VectorGather(u8, chunk_size);

        pub fn init(allocator: Allocator, config: Config) Self {
            return .{
                .queue = Queue.init(allocator, config),
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
        }

        /// Append bytes to the buffer
        pub fn append(self: *Self, rt: *zio.Runtime, data: []const u8) (zio.Cancelable || PushError)!void {
            return self.queue.pushSlice(rt, data);
        }

        /// Close the buffer to prevent further writes
        pub fn close(self: *Self, rt: *zio.Runtime) void {
            self.queue.close(rt);
        }

        /// Check if the buffer is closed
        pub fn isClosed(self: *Self, rt: *zio.Runtime) bool {
            return self.queue.isClosed(rt);
        }

        /// Get readable byte slice
        pub fn tryGetSlice(self: *Self, rt: *zio.Runtime) ?Queue.View {
            return self.queue.tryGetSlice(rt);
        }

        /// Get readable byte slice with timeout
        pub fn getSlice(self: *Self, rt: *zio.Runtime, timeout_ms: u64) (zio.Cancelable || PopError)!Queue.View {
            return self.queue.getSlice(rt, timeout_ms);
        }

        /// Get bytes available
        pub fn getBytesAvailable(self: *Self, rt: *zio.Runtime) usize {
            return self.queue.getItemsAvailable(rt);
        }

        /// Check if has data
        pub fn hasData(self: *Self, rt: *zio.Runtime) bool {
            return self.queue.hasData(rt);
        }

        /// Reset the buffer to empty state
        pub fn reset(self: *Self, rt: *zio.Runtime) void {
            self.queue.reset(rt);
        }

        /// Freeze the buffer (blocks both readers and writers)
        pub fn freeze(self: *Self, rt: *zio.Runtime) void {
            self.queue.freeze(rt);
        }

        /// Unfreeze the buffer and wake up waiting fibers
        pub fn unfreeze(self: *Self, rt: *zio.Runtime) void {
            self.queue.unfreeze(rt);
        }

        /// Check if buffer is frozen
        pub fn isFrozen(self: *Self, rt: *zio.Runtime) bool {
            return self.queue.isFrozen(rt);
        }

        /// Get multiple readable slices for vectored I/O with timeout (0 = non-blocking)
        pub fn gatherReadSlices(self: *Self, rt: *zio.Runtime, slices: [][]const u8, timeout_ms: u64) (zio.Cancelable || PopError)!Gather {
            try self.queue.mutex.lock(rt);
            defer self.queue.mutex.unlock(rt);

            try self.queue.waitForDataInternal(rt, timeout_ms);

            // At this point we have data and are not frozen - gather slices
            var count: usize = 0;
            var total_bytes: usize = 0;
            var current = self.queue.head;
            const first_chunk = current.?; // Safe: waitForDataInternal ensures data exists
            const first_chunk_read_pos = first_chunk.read_pos;

            while (current) |chunk| {
                if (count >= slices.len) break;

                const slice = chunk.getReadSlice();
                if (slice.len > 0) {
                    slices[count] = slice;
                    total_bytes += slice.len;
                    count += 1;
                }

                if (!chunk.is_sealed and chunk.next != null) {
                    break;
                }
                current = chunk.next;
            }

            return Gather{
                .reset_id = self.queue.reset_id,
                .first_chunk = first_chunk,
                .first_chunk_read_pos = first_chunk_read_pos,
                .slices = slices[0..count],
                .total_bytes = total_bytes,
                .buffer = self,
            };
        }

        /// Internal helper for consuming bytes (no freeze checking)
        /// Assumes mutex is already held.
        fn consumeBytesInternal(self: *Self, total_bytes: usize) void {
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
        pub fn moveToBuffer(self: *Self, rt: *zio.Runtime, dest: *Self) (zio.Cancelable || PushError)!void {
            if (self == dest) return; // no-op

            // Lock both buffers in a stable order to avoid deadlocks.
            const self_addr = @intFromPtr(self);
            const dest_addr = @intFromPtr(dest);
            var first: *Self = if (self_addr <= dest_addr) self else dest;
            var second: *Self = if (self_addr <= dest_addr) dest else self;

            try first.queue.mutex.lock(rt);
            defer first.queue.mutex.unlock(rt);
            try second.queue.mutex.lock(rt);
            defer second.queue.mutex.unlock(rt);

            // Respect freeze semantics:
            if (dest.queue.is_frozen) {
                return PushError.BufferFrozen;
            }
            if (self.queue.is_frozen) {
                return PushError.BufferFrozen;
            }

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
            dest.queue.data_cond.signal(rt);

            // Reset source queue state (we transferred ownership).
            self.queue.head = null;
            self.queue.tail = null;
            self.queue.items_available = 0;
            // Sanity: we only subtract list-chunks; pooled chunks remain accounted.
            std.debug.assert(self.queue.total_chunks >= moved_chunk_count);
            self.queue.total_chunks -= moved_chunk_count;
        }

        /// Wait for data to become available with timeout (0 = non-blocking)
        pub fn waitForData(self: *Self, rt: *zio.Runtime, timeout_ms: u64) (zio.Cancelable || PopError)!void {
            try self.queue.mutex.lock(rt);
            defer self.queue.mutex.unlock(rt);

            try self.queue.waitForDataInternal(rt, timeout_ms);
        }

        /// Wait for more data to become available with timeout
        pub fn waitForMoreData(self: *Self, rt: *zio.Runtime, timeout_ns: u64) (zio.Cancelable || error{QueueClosed})!void {
            try self.queue.mutex.lock(rt);
            defer self.queue.mutex.unlock(rt);

            if (self.queue.is_closed) {
                return error.QueueClosed;
            }

            const initial_data = self.queue.items_available;
            var timer = std.time.Timer.start() catch unreachable;

            while (self.queue.items_available <= initial_data and !self.queue.is_closed) {
                const elapsed_ns = timer.read();
                if (elapsed_ns >= timeout_ns) break;

                const remaining_ns = timeout_ns - elapsed_ns;
                self.queue.data_cond.timedWait(rt, &self.queue.mutex, .fromNanoseconds(remaining_ns)) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Timeout => {}, // Continue loop
                };
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

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const IntQueue = ConcurrentQueue(i32, 4);
    var queue = IntQueue.init(allocator, .{});
    defer queue.deinit();

    // Push individual items
    try queue.push(rt, 42);
    try queue.push(rt, 43);

    // Pop them
    try std.testing.expectEqual(@as(i32, 42), try queue.pop(rt, 1000));
    try std.testing.expectEqual(@as(i32, 43), queue.tryPop(rt).?);

    // Should be empty
    try std.testing.expect(queue.tryPop(rt) == null);
}

test "generic queue with structs" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

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

    try queue.pushSlice(rt, &messages);

    // Get slice view
    var view_opt = queue.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqual(@as(usize, 2), view.data.len);
        try std.testing.expectEqual(@as(u32, 1), view.data[0].id);
        view.consume(rt, 2);
    }
}

test "byte buffer specialization" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    try buffer.append(rt, "Hello, World!");

    var view_opt = buffer.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.consume(rt, view.data.len);
    }
}

test "concurrent push and pop" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(u64, 32);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    var sum: u64 = 0;

    const TestFn = struct {
        fn producer(runtime: *zio.Runtime, q: *Queue) void {
            for (0..100) |i| {
                q.push(runtime, i) catch return;
            }
        }

        fn consumer(runtime: *zio.Runtime, q: *Queue, s: *u64) void {
            for (0..100) |_| {
                s.* += q.pop(runtime, 1000) catch return;
            }
        }
    };

    var group: zio.Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, TestFn.producer, .{ rt, &queue });
    try group.spawn(rt, TestFn.consumer, .{ rt, &queue, &sum });

    try group.wait(rt);

    // Sum of 0..99 = 4950
    try std.testing.expectEqual(4950, sum);
}

test "queue close functionality" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Push some items before closing
    try queue.push(rt, 1);
    try queue.push(rt, 2);

    // Close the queue
    queue.close(rt);

    // Should not be able to push after closing
    try std.testing.expectError(PushError.QueueClosed, queue.push(rt, 3));
    try std.testing.expectError(PushError.QueueClosed, queue.pushSlice(rt, &[_]i32{ 4, 5 }));

    // Should still be able to read existing data
    try std.testing.expectEqual(@as(i32, 1), try queue.pop(rt, 1000));
    try std.testing.expectEqual(@as(i32, 2), queue.tryPop(rt).?);

    // Verify closed state
    try std.testing.expect(queue.isClosed(rt));
}

test "blocking pop handles queue closure" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    var pop_result: ?(zio.Cancelable || PopError) = null;

    const TestFn = struct {
        fn closer(runtime: *zio.Runtime, q: *Queue) void {
            runtime.sleep(.fromMilliseconds(10)) catch return;
            q.close(runtime);
        }

        fn popper(runtime: *zio.Runtime, q: *Queue, result: *?(zio.Cancelable || PopError)) void {
            _ = q.pop(runtime, 1000) catch |err| {
                result.* = err;
                return;
            };
        }
    };

    var group: zio.Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, TestFn.closer, .{ rt, &queue });
    try group.spawn(rt, TestFn.popper, .{ rt, &queue, &pop_result });

    try group.wait(rt);

    try std.testing.expectEqual(error.QueueClosed, pop_result.?);
}

test "getSlice handles queue closure with indefinite wait" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    var get_result: ?(zio.Cancelable || PopError) = null;

    const TestFn = struct {
        fn closer(runtime: *zio.Runtime, q: *Queue) void {
            runtime.sleep(.fromMilliseconds(10)) catch return;
            q.close(runtime);
        }

        fn getter(runtime: *zio.Runtime, q: *Queue, result: *?(zio.Cancelable || PopError)) void {
            _ = q.getSlice(runtime, std.math.maxInt(u64)) catch |err| {
                result.* = err;
                return;
            };
        }
    };

    var group: zio.Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, TestFn.closer, .{ rt, &queue });
    try group.spawn(rt, TestFn.getter, .{ rt, &queue, &get_result });

    try group.wait(rt);

    try std.testing.expectEqual(error.QueueClosed, get_result.?);
}

test "buffer close functionality" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Append some data before closing
    try buffer.append(rt, "Hello");

    // Close the buffer
    buffer.close(rt);

    // Should not be able to append after closing
    try std.testing.expectError(PushError.QueueClosed, buffer.append(rt, " World"));

    // Should still be able to read existing data
    var view_opt = buffer.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello", view.data);
        view.consume(rt, view.data.len);
    }

    // Verify closed state
    try std.testing.expect(buffer.isClosed(rt));
}

test "buffer moveToBuffer functionality" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(32);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data to source buffer
    try source.append(rt, "Hello, ");
    try source.append(rt, "World!");

    // Verify source has data
    try std.testing.expectEqual(@as(usize, 13), source.getBytesAvailable(rt));

    // Move data from source to destination
    try source.moveToBuffer(rt, &dest);

    // Verify source is now empty
    try std.testing.expectEqual(@as(usize, 0), source.getBytesAvailable(rt));

    // Verify destination has the data
    try std.testing.expectEqual(@as(usize, 13), dest.getBytesAvailable(rt));

    // Read and verify the moved data
    var view_opt = dest.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.consume(rt, view.data.len);
    }
}

test "buffer moveToBuffer with multiple chunks" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Use small chunk size to force multiple chunks
    const Buffer = ConcurrentWriteBuffer(8);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data that spans multiple chunks
    try source.append(rt, "First chunk "); // 12 bytes, spans 2 chunks
    try source.append(rt, "Second chunk "); // 13 bytes, spans 2 more chunks
    try source.append(rt, "Third"); // 5 bytes

    const total_bytes = 12 + 13 + 5; // 30 bytes
    try std.testing.expectEqual(total_bytes, source.getBytesAvailable(rt));

    // Move all data to destination
    try source.moveToBuffer(rt, &dest);

    // Verify source is empty
    try std.testing.expectEqual(@as(usize, 0), source.getBytesAvailable(rt));

    // Verify destination has all the data
    try std.testing.expectEqual(total_bytes, dest.getBytesAvailable(rt));

    // Read and verify the moved data by consuming all chunks
    var result = std.ArrayList(u8){};
    defer result.deinit(allocator);

    while (dest.getBytesAvailable(rt) > 0) {
        var view_opt = dest.tryGetSlice(rt);
        if (view_opt) |*view| {
            try result.appendSlice(allocator, view.data);
            view.consume(rt, view.data.len);
        } else {
            break;
        }
    }

    try std.testing.expectEqualStrings("First chunk Second chunk Third", result.items);
}

test "buffer moveToBuffer empty source" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add some data to destination first
    try dest.append(rt, "Already here");

    // Move from empty source (should be no-op)
    try source.moveToBuffer(rt, &dest);

    // Verify destination still has original data
    try std.testing.expectEqual(@as(usize, 12), dest.getBytesAvailable(rt));

    var view_opt = dest.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Already here", view.data);
        view.consume(rt, view.data.len);
    }
}

test "buffer max_size limit" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{ .max_size = 10 });
    defer buffer.deinit();

    // Should be able to add up to max_size
    try buffer.append(rt, "Hello");
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable(rt));

    // Should be able to add exactly to the limit
    try buffer.append(rt, "World");
    try std.testing.expectEqual(@as(usize, 10), buffer.getBytesAvailable(rt));

    // Should fail when exceeding the limit
    try std.testing.expectError(PushError.OutOfMemory, buffer.append(rt, "!"));

    // Verify data is still intact
    var view_opt = buffer.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("HelloWorld", view.data);
        view.consume(rt, view.data.len);
    }
}

test "freeze/unfreeze basic functionality" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Add some data
    try queue.push(rt, 42);
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable(rt));

    // Freeze buffer
    queue.freeze(rt);
    try std.testing.expect(queue.isFrozen(rt));

    // Non-blocking read should return BufferFrozen
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(rt, 0));
    try std.testing.expectError(PopError.BufferFrozen, queue.getSlice(rt, 0));

    // Data should still be available after failed reads
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable(rt));

    // Unfreeze buffer
    queue.unfreeze(rt);
    try std.testing.expect(!queue.isFrozen(rt));

    // Should be able to read now
    try std.testing.expectEqual(@as(i32, 42), try queue.pop(rt, 1000));
    try std.testing.expectEqual(@as(usize, 0), queue.getItemsAvailable(rt));
}

test "writers cannot push while buffer is frozen" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Freeze buffer
    queue.freeze(rt);

    // Writers should be blocked
    try std.testing.expectError(PushError.BufferFrozen, queue.push(rt, 1));
    try std.testing.expectError(PushError.BufferFrozen, queue.pushSlice(rt, &[_]i32{ 2, 3 }));
    try std.testing.expectEqual(@as(usize, 0), queue.getItemsAvailable(rt));

    // Readers should be blocked
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(rt, 0));

    // Unfreeze and add data
    queue.unfreeze(rt);
    try queue.push(rt, 1);
    try queue.push(rt, 2);
    try std.testing.expectEqual(@as(i32, 1), try queue.pop(rt, 1000));
    try std.testing.expectEqual(@as(i32, 2), try queue.pop(rt, 1000));
}

test "blocking operations during freeze" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    queue.freeze(rt);

    var pop_result: ?i32 = null;

    const TestFn = struct {
        fn resumer(runtime: *zio.Runtime, q: *Queue) void {
            runtime.sleep(.fromMilliseconds(10)) catch return;
            q.unfreeze(runtime);
            q.push(runtime, 99) catch return;
        }

        fn popper(runtime: *zio.Runtime, q: *Queue, result: *?i32) void {
            result.* = q.pop(runtime, 1000) catch return;
        }
    };

    var group: zio.Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, TestFn.resumer, .{ rt, &queue });
    try group.spawn(rt, TestFn.popper, .{ rt, &queue, &pop_result });

    try group.wait(rt);

    try std.testing.expectEqual(@as(i32, 99), pop_result.?);
}

test "timeout while frozen returns BufferFrozen" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Add data then freeze buffer
    try queue.push(rt, 42);
    queue.freeze(rt);

    // Should timeout and return BufferFrozen
    try std.testing.expectError(PopError.BufferFrozen, queue.pop(rt, 10));
    try std.testing.expectError(PopError.BufferFrozen, queue.getSlice(rt, 10));

    // Data should still be there
    try std.testing.expectEqual(@as(usize, 1), queue.getItemsAvailable(rt));
}

test "freeze/unfreeze interaction with close" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, .{});
    defer queue.deinit();

    // Freeze then close
    queue.freeze(rt);
    queue.close(rt);

    // Should return QueueClosed instead of BufferFrozen
    try std.testing.expectError(PopError.QueueClosed, queue.pop(rt, 0));

    // Can't push when closed
    try std.testing.expectError(PushError.QueueClosed, queue.push(rt, 42));
}

test "ConcurrentWriteBuffer freeze/unfreeze functionality" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Add some data
    try buffer.append(rt, "Hello, World!");

    // Freeze buffer
    buffer.freeze(rt);
    try std.testing.expect(buffer.isFrozen(rt));

    // Non-blocking read should return BufferFrozen
    try std.testing.expect(buffer.tryGetSlice(rt) == null);
    try std.testing.expectError(PopError.BufferFrozen, buffer.getSlice(rt, 0));

    // Writers should be blocked
    try std.testing.expectError(PushError.BufferFrozen, buffer.append(rt, " More data"));

    // Unfreeze buffer
    buffer.unfreeze(rt);
    try std.testing.expect(!buffer.isFrozen(rt));

    // Now writers work
    try buffer.append(rt, " More data");

    // Should be able to read now
    var view_opt = buffer.tryGetSlice(rt);
    if (view_opt) |*view| {
        try std.testing.expectEqualStrings("Hello, World! More data", view.data);
        view.consume(rt, view.data.len);
    }
}

test "ConcurrentWriteBuffer waitForData smoke test" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Test waitForData with immediate data available
    try buffer.append(rt, "Hello");

    // Should return immediately since data is available
    try buffer.waitForData(rt, 1000);

    // Verify data is still there
    try std.testing.expect(buffer.hasData(rt));
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable(rt));
}

test "ConcurrentWriteBuffer waitForData with closed buffer" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close(rt);

    // waitForData should return QueueClosed error
    try std.testing.expectError(error.QueueClosed, buffer.waitForData(rt, 1000));
}

test "ConcurrentWriteBuffer waitForMoreData smoke test" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Add initial data
    try buffer.append(rt, "Hello");

    // Wait for more data with short timeout (should timeout)
    try buffer.waitForMoreData(rt, 1 * std.time.ns_per_ms);

    // Should still work normally
    try std.testing.expect(buffer.hasData(rt));
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable(rt));
}

test "ConcurrentWriteBuffer waitForMoreData with closed buffer" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close(rt);

    // waitForMoreData should return QueueClosed error
    try std.testing.expectError(error.QueueClosed, buffer.waitForMoreData(rt, 1 * std.time.ns_per_ms));
}

test "gatherReadSlices respects freeze state" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Add data to buffer
    try buffer.append(rt, "Hello, World!");

    // Normal operation should gather vectors
    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(rt, &slices, 0);
    try std.testing.expect(gather.slices.len > 0);
    try std.testing.expect(gather.total_bytes > 0);

    // Freeze buffer
    buffer.freeze(rt);

    // gatherReadSlices should return BufferFrozen when frozen (non-blocking)
    try std.testing.expectError(PopError.BufferFrozen, buffer.gatherReadSlices(rt, &slices, 0));

    // Should timeout and return BufferFrozen (blocking with timeout)
    try std.testing.expectError(PopError.BufferFrozen, buffer.gatherReadSlices(rt, &slices, 10));

    // Unfreeze and gather successfully
    buffer.unfreeze(rt);
    const unfrozen_gather = try buffer.gatherReadSlices(rt, &slices, 1000);
    try std.testing.expect(unfrozen_gather.slices.len > 0);
    try std.testing.expect(unfrozen_gather.total_bytes > 0);

    // Should be able to consume gathered data
    try unfrozen_gather.consume(rt, 5); // Consume "Hello"
}

test "VectorGather thread safety validation" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    try buffer.append(rt, "Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(rt, &slices, 0);
    try std.testing.expect(gather.slices.len > 0);

    // Should be able to consume gathered data normally
    try gather.consume(rt, 5); // Consume "Hello"

    // Reset the buffer which should increment reset_id
    buffer.reset(rt);

    // Now trying to consume with the old gather should fail
    try std.testing.expectError(error.BufferReset, gather.consume(rt, 1));
}

test "VectorGather blocking behavior" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    // Should return QueueEmpty immediately when no data (non-blocking)
    var slices: [4][]const u8 = undefined;
    try std.testing.expectError(PopError.QueueEmpty, buffer.gatherReadSlices(rt, &slices, 0));

    // Add data and freeze
    try buffer.append(rt, "Hello, World!");
    buffer.freeze(rt);

    // Should return BufferFrozen immediately (non-blocking)
    try std.testing.expectError(PopError.BufferFrozen, buffer.gatherReadSlices(rt, &slices, 0));

    // Should timeout and return BufferFrozen (blocking with timeout)
    try std.testing.expectError(PopError.BufferFrozen, buffer.gatherReadSlices(rt, &slices, 10));

    // Unfreeze and gather successfully
    buffer.unfreeze(rt);
    const gather = try buffer.gatherReadSlices(rt, &slices, 1000);
    try std.testing.expect(gather.slices.len > 0);
    try std.testing.expect(gather.total_bytes > 0);

    // Should be able to consume gathered data
    try gather.consume(rt, gather.total_bytes);
}

test "moveToBuffer respects freeze state on source" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data to source
    try source.append(rt, "Hello");

    // Freeze source
    source.freeze(rt);

    // Moving from frozen source should return BufferFrozen
    try std.testing.expectError(PushError.BufferFrozen, source.moveToBuffer(rt, &dest));

    // Data should still be in source
    try std.testing.expectEqual(@as(usize, 5), source.getBytesAvailable(rt));
    try std.testing.expectEqual(@as(usize, 0), dest.getBytesAvailable(rt));
}

test "moveToBuffer respects freeze state on destination" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var source = Buffer.init(allocator, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, .{});
    defer dest.deinit();

    // Add data to source
    try source.append(rt, "Hello");

    // Freeze destination
    dest.freeze(rt);

    // Moving to frozen destination should return BufferFrozen
    try std.testing.expectError(PushError.BufferFrozen, source.moveToBuffer(rt, &dest));

    // Data should still be in source
    try std.testing.expectEqual(@as(usize, 5), source.getBytesAvailable(rt));
    try std.testing.expectEqual(@as(usize, 0), dest.getBytesAvailable(rt));
}

test "VectorGather detects buffer reset between gather and consume" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    try buffer.append(rt, "Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(rt, &slices, 0);
    try std.testing.expect(gather.slices.len > 0);

    // Reset the buffer which should increment reset_id
    buffer.reset(rt);

    // Now trying to consume with the old gather should fail with BufferReset
    try std.testing.expectError(error.BufferReset, gather.consume(rt, 1));
}

test "VectorGather detects concurrent consumer advancing buffer" {
    const allocator = std.testing.allocator;

    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, .{});
    defer buffer.deinit();

    try buffer.append(rt, "Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(rt, &slices, 0);
    try std.testing.expect(gather.slices.len > 0);
    try std.testing.expect(gather.total_bytes > 0);

    // Simulate another consumer advancing the read position
    var view = try buffer.getSlice(rt, 0);
    view.consume(rt, 2); // Advance read_pos by 2 bytes

    // Now trying to consume with the original gather should fail with ConcurrentConsumer
    try std.testing.expectError(error.ConcurrentConsumer, gather.consume(rt, 3));
}
