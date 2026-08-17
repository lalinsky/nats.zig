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
const xsync = @import("xsync");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const io_util = @import("io_util.zig");

pub const PopError = Io.Timeout.Error || error{
    Closed,
};

pub const PushError = error{
    Closed,
    ChunkLimitExceeded,
    OutOfMemory,
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
            _ = allocator;
            return .{
                .chunks = std.ArrayList(*Chunk).empty,
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

        /// The `std.Io` instance used for blocking and waking
        io: Io,

        /// Single mutex protecting all operations
        mutex: xsync.Mutex,
        /// Condition variable for waiting readers
        data_cond: xsync.Condition,

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

        pub fn init(allocator: Allocator, io: Io, config: Config) Self {
            return .{
                .allocator = allocator,
                .io = io,
                .mutex = .init,
                .data_cond = .init,
                .head = null,
                .tail = null,
                .items_available = 0,
                .total_chunks = 0,
                .max_chunks = config.max_chunks,
                .max_size = config.max_size,
                .chunk_pool = Pool.init(allocator, config.max_pool_size),
                .is_closed = false,
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
        pub fn push(self: *Self, item: T) (Io.Cancelable || PushError)!void {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            if (self.is_closed) {
                return PushError.Closed;
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
            self.data_cond.signal(self.io);
        }

        /// Push multiple items (fiber-safe, cancelable)
        pub fn pushSlice(self: *Self, items: []const T) (Io.Cancelable || PushError)!void {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            if (self.is_closed) {
                return PushError.Closed;
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
            self.data_cond.signal(self.io);
        }

        /// Internal helper to wait for data availability with timeout handling.
        /// Assumes mutex is already held.
        fn waitForDataInternal(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!void {
            switch (timeout) {
                .none => {
                    while (self.items_available == 0 and !self.is_closed) {
                        try self.data_cond.wait(self.io, &self.mutex);
                    }

                    if (self.is_closed and self.items_available == 0) {
                        return PopError.Closed;
                    }
                    return;
                },
                .duration, .deadline => {},
            }

            const deadline_timeout = timeout.toDeadline(self.io);

            while (self.items_available == 0 and !self.is_closed) {
                if (io_util.expired(self.io, deadline_timeout)) {
                    if (self.is_closed and self.items_available == 0) {
                        return PopError.Closed;
                    }
                    return PopError.Timeout;
                }

                self.data_cond.waitTimeout(self.io, &self.mutex, deadline_timeout) catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    error.Timeout => {}, // Continue loop to check conditions
                };
            }

            if (self.is_closed and self.items_available == 0) {
                return PopError.Closed;
            }
        }

        /// Pop a single item, waiting according to `timeout`.
        pub fn pop(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!T {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            try self.waitForDataInternal(timeout);

            return self.popLocked() orelse unreachable;
        }

        /// Pop the next available item; assumes the mutex is held.
        fn popLocked(self: *Self) ?T {
            const chunk = self.head orelse return null;
            const item = chunk.popItem() orelse return null;

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

        /// Try to pop a single item (non-blocking, returns null if empty).
        /// Not a cancelation point, so it is safe in cleanup paths.
        pub fn tryPop(self: *Self) ?T {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            return self.popLocked();
        }

        /// Get a readable slice, waiting according to `timeout`.
        pub fn getSlice(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!View {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);

            try self.waitForDataInternal(timeout);

            return self.getSliceLocked() orelse unreachable;
        }

        /// Get a readable slice if data is available; assumes the mutex is held.
        fn getSliceLocked(self: *Self) ?View {
            const chunk = self.head orelse return null;
            const available = chunk.availableToRead();

            if (available == 0) {
                return null;
            }

            return View{
                .data = chunk.getReadSlice(),
                .chunk = chunk,
                .queue = self,
            };
        }

        /// Try to get readable slice without blocking.
        /// Not a cancelation point, so it is safe in cleanup paths.
        pub fn tryGetSlice(self: *Self) ?View {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            return self.getSliceLocked();
        }

        /// Consume items after processing
        pub fn consumeItems(self: *Self, chunk: *Chunk, items_consumed: usize) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

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
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            return self.items_available;
        }

        /// Check if queue has data
        pub fn hasData(self: *Self) bool {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            return self.items_available > 0;
        }

        /// Close the queue to prevent further writes
        pub fn close(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            self.is_closed = true;
            self.data_cond.broadcast(self.io);
        }

        /// Check if the queue is closed
        pub fn isClosed(self: *Self) bool {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            return self.is_closed;
        }

        /// Reset the queue to empty state
        pub fn reset(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

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
            self.reset_id +%= 1;

            // Wake up any waiting fibers
            self.data_cond.broadcast(self.io);
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

        pub fn consume(self: Self, bytes_consumed: usize) (Io.Cancelable || error{ BufferReset, ConcurrentConsumer })!void {
            if (bytes_consumed > self.total_bytes) {
                std.debug.panic("Attempting to consume {} bytes but only {} were gathered", .{ bytes_consumed, self.total_bytes });
            }

            try self.buffer.queue.mutex.lock(self.buffer.queue.io);
            defer self.buffer.queue.mutex.unlock(self.buffer.queue.io);

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

        pub fn init(allocator: Allocator, io: Io, config: Config) Self {
            return .{
                .queue = Queue.init(allocator, io, config),
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
        }

        /// Append bytes to the buffer
        pub fn append(self: *Self, data: []const u8) (Io.Cancelable || PushError)!void {
            return self.queue.pushSlice(data);
        }

        /// Append multiple slices to the buffer in a single operation
        /// More efficient than calling append() multiple times as it only takes the lock once
        pub fn appendMany(self: *Self, slices: []const []const u8) (Io.Cancelable || PushError)!void {
            try self.queue.mutex.lock(self.queue.io);
            defer self.queue.mutex.unlock(self.queue.io);

            if (self.queue.is_closed) {
                return PushError.Closed;
            }

            // Calculate total size needed
            var total_size: usize = 0;
            for (slices) |slice| {
                total_size += slice.len;
            }

            // Check size limit before adding (overflow-safe)
            if (self.queue.max_size > 0) {
                const max_items = self.queue.max_size;
                if (self.queue.items_available >= max_items) {
                    return PushError.OutOfMemory;
                }
                if (total_size > max_items - self.queue.items_available) {
                    return PushError.OutOfMemory;
                }
            }

            // Append each slice
            for (slices) |slice| {
                var remaining = slice;

                while (remaining.len > 0) {
                    const chunk = try self.queue.ensureWritableChunk();

                    const available = chunk.availableToWrite();
                    const to_write = @min(available, remaining.len);

                    @memcpy(chunk.getWriteSlice()[0..to_write], remaining[0..to_write]);

                    chunk.write_pos += to_write;
                    remaining = remaining[to_write..];

                    if (chunk.availableToWrite() == 0) {
                        chunk.is_sealed = true;
                    }
                }
            }

            self.queue.items_available += total_size;
            self.queue.data_cond.signal(self.queue.io);
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
        pub fn getSlice(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!Queue.View {
            return self.queue.getSlice(timeout);
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

        /// Get multiple readable slices for vectored I/O, waiting according to `timeout`.
        pub fn gatherReadSlices(self: *Self, slices: [][]const u8, timeout: Io.Timeout) (Io.Cancelable || PopError)!Gather {
            try self.queue.mutex.lock(self.queue.io);
            defer self.queue.mutex.unlock(self.queue.io);

            try self.queue.waitForDataInternal(timeout);

            // At this point we have data - gather slices
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

        /// Internal helper for consuming bytes.
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
        pub fn moveToBuffer(self: *Self, dest: *Self) (Io.Cancelable || PushError)!void {
            if (self == dest) return; // no-op

            // Lock both buffers in a stable order to avoid deadlocks.
            const self_addr = @intFromPtr(self);
            const dest_addr = @intFromPtr(dest);
            var first: *Self = if (self_addr <= dest_addr) self else dest;
            var second: *Self = if (self_addr <= dest_addr) dest else self;

            try first.queue.mutex.lock(first.queue.io);
            defer first.queue.mutex.unlock(first.queue.io);
            try second.queue.mutex.lock(second.queue.io);
            defer second.queue.mutex.unlock(second.queue.io);

            // Use direct fields (don't call methods that relock).
            if (self.queue.items_available == 0) return;
            if (dest.queue.is_closed) return PushError.Closed;

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

            // Splice self's list onto dest's list. The destination tail must
            // be sealed: an unsealed chunk with a successor can neither be
            // unlinked by consumers (only sealed chunks are considered fully
            // consumed) nor safely read past, so the reader would gather
            // zero slices forever once it drained that chunk.
            if (dest.queue.tail) |tail| {
                tail.is_sealed = true;
                tail.next = self.queue.head;
            } else {
                dest.queue.head = self.queue.head;
            }
            dest.queue.tail = self.queue.tail;
            dest.queue.items_available += self.queue.items_available;
            dest.queue.total_chunks += moved_chunk_count;
            // Wake a waiting reader on dest (consistent with push/pushSlice)
            dest.queue.data_cond.signal(dest.queue.io);

            // Reset source queue state (we transferred ownership).
            self.queue.head = null;
            self.queue.tail = null;
            self.queue.items_available = 0;
            // Sanity: we only subtract list-chunks; pooled chunks remain accounted.
            std.debug.assert(self.queue.total_chunks >= moved_chunk_count);
            self.queue.total_chunks -= moved_chunk_count;
        }

        /// Wait for data to become available according to `timeout`.
        pub fn waitForData(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!void {
            try self.queue.mutex.lock(self.queue.io);
            defer self.queue.mutex.unlock(self.queue.io);

            try self.queue.waitForDataInternal(timeout);
        }

        /// Wait for more data to become available with timeout
        pub fn waitForMoreData(self: *Self, timeout: Io.Timeout) (Io.Cancelable || PopError)!void {
            try self.queue.mutex.lock(self.queue.io);
            defer self.queue.mutex.unlock(self.queue.io);

            if (self.queue.is_closed) {
                return error.Closed;
            }

            const initial_data = self.queue.items_available;
            const deadline = timeout.toTimestamp(self.queue.io);

            while (self.queue.items_available <= initial_data and !self.queue.is_closed) {
                if (deadline) |d| {
                    if (io_util.expired(self.queue.io, .{ .deadline = d })) return error.Timeout;
                }

                if (deadline) |d| {
                    self.queue.data_cond.waitTimeout(self.queue.io, &self.queue.mutex, .{ .deadline = d }) catch |err| switch (err) {
                        error.Canceled => return error.Canceled,
                        error.Timeout => {},
                    };
                } else {
                    try self.queue.data_cond.wait(self.queue.io, &self.queue.mutex);
                }
            }

            // Check if closed after waiting
            if (self.queue.is_closed) {
                return error.Closed;
            }
        }
    };
}

// Tests
test "generic queue with integers" {
    const allocator = std.testing.allocator;

    const IntQueue = ConcurrentQueue(i32, 4);
    var queue = IntQueue.init(allocator, std.testing.io, .{});
    defer queue.deinit();

    // Push individual items
    try queue.push(42);
    try queue.push(43);

    // Pop them
    try std.testing.expectEqual(@as(i32, 42), try queue.pop(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }));
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
    var queue = MsgQueue.init(allocator, std.testing.io, .{});
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
    var buffer = Buffer.init(allocator, std.testing.io, .{});
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
    const io = std.testing.io;

    const Queue = ConcurrentQueue(u64, 32);
    var queue = Queue.init(allocator, io, .{});
    defer queue.deinit();

    var sum: u64 = 0;

    const TestFn = struct {
        fn producer(q: *Queue) void {
            for (0..100) |i| {
                q.push(i) catch return;
            }
        }

        fn consumer(q: *Queue, s: *u64) void {
            for (0..100) |_| {
                s.* += q.pop(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }) catch return;
            }
        }
    };

    var group: Io.Group = .init;
    defer group.cancel(io);

    try group.concurrent(io, TestFn.producer, .{&queue});
    try group.concurrent(io, TestFn.consumer, .{ &queue, &sum });

    try group.await(io);

    // Sum of 0..99 = 4950
    try std.testing.expectEqual(4950, sum);
}

test "queue close functionality" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, std.testing.io, .{});
    defer queue.deinit();

    // Push some items before closing
    try queue.push(1);
    try queue.push(2);

    // Close the queue
    queue.close();

    // Should not be able to push after closing
    try std.testing.expectError(PushError.Closed, queue.push(3));
    try std.testing.expectError(PushError.Closed, queue.pushSlice(&[_]i32{ 4, 5 }));

    // Should still be able to read existing data
    try std.testing.expectEqual(@as(i32, 1), try queue.pop(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }));
    try std.testing.expectEqual(@as(i32, 2), queue.tryPop().?);

    // Verify closed state
    try std.testing.expect(queue.isClosed());
}

test "blocking pop handles queue closure" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, std.testing.io, .{});
    defer queue.deinit();

    var pop_result: ?(Io.Cancelable || PopError) = null;

    const TestFn = struct {
        fn closer(q: *Queue) void {
            std.testing.io.sleep(.fromMilliseconds(10), .awake) catch return;
            q.close();
        }

        fn popper(q: *Queue, result: *?(Io.Cancelable || PopError)) void {
            _ = q.pop(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }) catch |err| {
                result.* = err;
                return;
            };
        }
    };

    var group: Io.Group = .init;
    defer group.cancel(std.testing.io);

    try group.concurrent(std.testing.io, TestFn.closer, .{&queue});
    try group.concurrent(std.testing.io, TestFn.popper, .{ &queue, &pop_result });

    try group.await(std.testing.io);

    try std.testing.expectEqual(error.Closed, pop_result.?);
}

test "getSlice handles queue closure with indefinite wait" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, std.testing.io, .{});
    defer queue.deinit();

    var get_result: ?(Io.Cancelable || PopError) = null;

    const TestFn = struct {
        fn closer(q: *Queue) void {
            std.testing.io.sleep(.fromMilliseconds(10), .awake) catch return;
            q.close();
        }

        fn getter(q: *Queue, result: *?(Io.Cancelable || PopError)) void {
            _ = q.getSlice(.none) catch |err| {
                result.* = err;
                return;
            };
        }
    };

    var group: Io.Group = .init;
    defer group.cancel(std.testing.io);

    try group.concurrent(std.testing.io, TestFn.closer, .{&queue});
    try group.concurrent(std.testing.io, TestFn.getter, .{ &queue, &get_result });

    try group.await(std.testing.io);

    try std.testing.expectEqual(error.Closed, get_result.?);
}

test "buffer close functionality" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // Append some data before closing
    try buffer.append("Hello");

    // Close the buffer
    buffer.close();

    // Should not be able to append after closing
    try std.testing.expectError(PushError.Closed, buffer.append(" World"));

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
    var source = Buffer.init(allocator, std.testing.io, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, std.testing.io, .{});
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

test "buffer moveToBuffer into partially consumed destination" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(32);
    var source = Buffer.init(allocator, std.testing.io, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, std.testing.io, .{});
    defer dest.deinit();

    // Fill the destination and drain it completely, leaving an unsealed,
    // fully consumed tail chunk - the state a live write buffer is in
    // after the flusher catches up.
    try dest.append("CONNECT\r\n");
    {
        var view = dest.tryGetSlice().?;
        try std.testing.expectEqualStrings("CONNECT\r\n", view.data);
        view.consume(view.data.len);
    }
    try std.testing.expectEqual(@as(usize, 0), dest.getBytesAvailable());

    try source.append("PUB a 1\r\nx\r\n");
    try source.moveToBuffer(&dest);

    // The moved data must be readable past the drained old tail chunk.
    var slices: [4][]const u8 = undefined;
    const gather = try dest.gatherReadSlices(&slices, .{ .duration = .{ .raw = .zero, .clock = .awake } });
    try std.testing.expect(gather.total_bytes == 12);
    try gather.consume(gather.total_bytes);

    // And the buffer must remain usable for further appends.
    try dest.append("PING\r\n");
    var view = dest.tryGetSlice().?;
    try std.testing.expectEqualStrings("PING\r\n", view.data);
    view.consume(view.data.len);
}

test "buffer moveToBuffer with multiple chunks" {
    const allocator = std.testing.allocator;

    // Use small chunk size to force multiple chunks
    const Buffer = ConcurrentWriteBuffer(8);
    var source = Buffer.init(allocator, std.testing.io, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, std.testing.io, .{});
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
    var result = std.ArrayList(u8).empty;
    defer result.deinit(allocator);

    while (dest.getBytesAvailable() > 0) {
        var view_opt = dest.tryGetSlice();
        if (view_opt) |*view| {
            try result.appendSlice(allocator, view.data);
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
    var source = Buffer.init(allocator, std.testing.io, .{});
    defer source.deinit();

    var dest = Buffer.init(allocator, std.testing.io, .{});
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
    var buffer = Buffer.init(allocator, std.testing.io, .{ .max_size = 10 });
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

test "queue close wakes readers" {
    const allocator = std.testing.allocator;

    const Queue = ConcurrentQueue(i32, 4);
    var queue = Queue.init(allocator, std.testing.io, .{});
    defer queue.deinit();

    queue.close();

    try std.testing.expectError(PopError.Closed, queue.pop(.{ .duration = .{ .raw = .zero, .clock = .awake } }));
    try std.testing.expectError(PushError.Closed, queue.push(42));
}

test "ConcurrentWriteBuffer waitForData smoke test" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // Test waitForData with immediate data available
    try buffer.append("Hello");

    // Should return immediately since data is available
    try buffer.waitForData(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });

    // Verify data is still there
    try std.testing.expect(buffer.hasData());
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable());
}

test "ConcurrentWriteBuffer waitForData with closed buffer" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close();

    // waitForData should return Closed error
    try std.testing.expectError(error.Closed, buffer.waitForData(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }));
}

test "ConcurrentWriteBuffer waitForMoreData smoke test" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // Add initial data
    try buffer.append("Hello");

    // Wait for more data with short timeout (should timeout)
    try std.testing.expectError(error.Timeout, buffer.waitForMoreData(.{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }));

    // Should still work normally
    try std.testing.expect(buffer.hasData());
    try std.testing.expectEqual(@as(usize, 5), buffer.getBytesAvailable());
}

test "ConcurrentWriteBuffer waitForMoreData with closed buffer" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // Close the buffer
    buffer.close();

    // waitForMoreData should return Closed error
    try std.testing.expectError(error.Closed, buffer.waitForMoreData(.{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }));
}

test "VectorGather thread safety validation" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    try buffer.append("Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(&slices, .{ .duration = .{ .raw = .zero, .clock = .awake } });
    try std.testing.expect(gather.slices.len > 0);

    // Should be able to consume gathered data normally
    try gather.consume(5); // Consume "Hello"

    // Reset the buffer which should increment reset_id
    buffer.reset();

    // Now trying to consume with the old gather should fail
    try std.testing.expectError(error.BufferReset, gather.consume(1));
}

test "VectorGather blocking behavior" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    // A zero-duration blocking read should time out immediately when no data is available.
    var slices: [4][]const u8 = undefined;
    try std.testing.expectError(PopError.Timeout, buffer.gatherReadSlices(&slices, .{ .duration = .{ .raw = .zero, .clock = .awake } }));

    // Add data and gather successfully
    try buffer.append("Hello, World!");
    const gather = try buffer.gatherReadSlices(&slices, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    try std.testing.expect(gather.slices.len > 0);
    try std.testing.expect(gather.total_bytes > 0);

    // Should be able to consume gathered data
    try gather.consume(gather.total_bytes);
}

test "VectorGather detects buffer reset between gather and consume" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    try buffer.append("Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(&slices, .{ .duration = .{ .raw = .zero, .clock = .awake } });
    try std.testing.expect(gather.slices.len > 0);

    // Reset the buffer which should increment reset_id
    buffer.reset();

    // Now trying to consume with the old gather should fail with BufferReset
    try std.testing.expectError(error.BufferReset, gather.consume(1));
}

test "VectorGather detects concurrent consumer advancing buffer" {
    const allocator = std.testing.allocator;

    const Buffer = ConcurrentWriteBuffer(64);
    var buffer = Buffer.init(allocator, std.testing.io, .{});
    defer buffer.deinit();

    try buffer.append("Hello, World!");

    var slices: [4][]const u8 = undefined;
    const gather = try buffer.gatherReadSlices(&slices, .{ .duration = .{ .raw = .zero, .clock = .awake } });
    try std.testing.expect(gather.slices.len > 0);
    try std.testing.expect(gather.total_bytes > 0);

    // Simulate another consumer advancing the read position
    var view = try buffer.getSlice(.{ .duration = .{ .raw = .zero, .clock = .awake } });
    view.consume(2); // Advance read_pos by 2 bytes

    // Now trying to consume with the original gather should fail with ConcurrentConsumer
    try std.testing.expectError(error.ConcurrentConsumer, gather.consume(3));
}
