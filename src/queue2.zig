const std = @import("std");
const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;
const Allocator = std.mem.Allocator;
const atomic = std.atomic;

/// A single chunk in the linked list with inline data
fn ChunkType(comptime T: type, comptime capacity: usize) type {
    return struct {
        /// The data buffer for this chunk (inline)
        data: [capacity]T,
        /// Number of items written to this chunk (protected by write_mutex)
        write_pos: usize,
        /// Number of items read from this chunk (protected by read_mutex)
        read_pos: usize,
        /// Next chunk in the list (protected by write_mutex when modifying)
        next: ?*Self,
        /// Whether this chunk is full and sealed (protected by write_mutex)
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
            std.debug.assert(items_consumed <= self.data.len);
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
            return self.chunks.popOrNull();
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
        
        /// Protects tail and all write operations
        write_mutex: Mutex,
        /// Protects head and read operations
        read_mutex: Mutex,
        /// Condition variable for waiting readers
        data_cond: Condition,
        
        /// Head of the linked list (oldest chunk, protected by read_mutex)
        head: ?*Chunk,
        /// Tail of the linked list (newest chunk, protected by write_mutex)
        tail: ?*Chunk,
        
        /// Shared atomic counter for available items
        items_available: atomic.Value(usize),
        /// Total number of chunks allocated
        total_chunks: usize,
        /// Maximum chunks allowed (0 = unlimited)
        max_chunks: usize,
        
        /// Pool of reusable chunks (protected by write_mutex)
        chunk_pool: Pool,
        
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
        };
        
        pub fn init(allocator: Allocator, config: Config) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .write_mutex = .{},
                .read_mutex = .{},
                .data_cond = .{},
                .head = null,
                .tail = null,
                .items_available = atomic.Value(usize).init(0),
                .total_chunks = 0,
                .max_chunks = config.max_chunks,
                .chunk_pool = Pool.init(allocator, config.max_pool_size),
            };
            return self;
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
            
            // Free self
            self.allocator.destroy(self);
        }
        
        /// Push a single item (thread-safe)
        pub fn push(self: *Self, item: T) !void {
            self.write_mutex.lock();
            defer self.write_mutex.unlock();
            
            const chunk = try self.ensureWritableChunk();
            const success = chunk.pushItem(item);
            std.debug.assert(success);
            
            if (chunk.availableToWrite() == 0) {
                chunk.is_sealed = true;
            }
            
            _ = self.items_available.fetchAdd(1, .monotonic);
            self.data_cond.signal();
        }
        
        /// Push multiple items (thread-safe)
        pub fn pushSlice(self: *Self, items: []const T) !void {
            self.write_mutex.lock();
            defer self.write_mutex.unlock();
            
            var remaining = items;
            var total_written: usize = 0;
            
            while (remaining.len > 0) {
                const chunk = try self.ensureWritableChunk();
                
                const available = chunk.availableToWrite();
                const to_write = @min(available, remaining.len);
                
                @memcpy(
                    chunk.getWriteSlice()[0..to_write],
                    remaining[0..to_write]
                );
                
                chunk.write_pos += to_write;
                remaining = remaining[to_write..];
                total_written += to_write;
                
                if (chunk.availableToWrite() == 0) {
                    chunk.is_sealed = true;
                }
            }
            
            _ = self.items_available.fetchAdd(total_written, .monotonic);
            self.data_cond.signal();
        }
        
        /// Pop a single item (blocking)
        pub fn pop(self: *Self) !T {
            self.read_mutex.lock();
            defer self.read_mutex.unlock();
            
            // Wait for data to be available
            while (self.items_available.load(.monotonic) == 0) {
                self.data_cond.wait(&self.read_mutex);
            }
            
            const chunk = self.head orelse unreachable;
            const item = chunk.popItem() orelse unreachable;
            
            _ = self.items_available.fetchSub(1, .monotonic);
            
            // Check if chunk is fully consumed
            if (chunk.isFullyConsumed()) {
                self.head = chunk.next;
                
                if (self.tail == chunk) {
                    self.write_mutex.lock();
                    self.tail = null;
                    self.write_mutex.unlock();
                }
                
                self.recycleChunk(chunk);
            }
            
            return item;
        }
        
        /// Try to pop a single item (non-blocking)
        pub fn tryPop(self: *Self) ?T {
            self.read_mutex.lock();
            defer self.read_mutex.unlock();
            
            if (self.items_available.load(.monotonic) == 0) {
                return null;
            }
            
            const chunk = self.head orelse return null;
            const item = chunk.popItem() orelse return null;
            
            _ = self.items_available.fetchSub(1, .monotonic);
            
            // Check if chunk is fully consumed
            if (chunk.isFullyConsumed()) {
                self.head = chunk.next;
                
                if (self.tail == chunk) {
                    self.write_mutex.lock();
                    self.tail = null;
                    self.write_mutex.unlock();
                }
                
                self.recycleChunk(chunk);
            }
            
            return item;
        }
        
        /// Wait for data and get next readable slice
        pub fn waitAndGetSlice(self: *Self) !View {
            self.read_mutex.lock();
            defer self.read_mutex.unlock();
            
            // Wait for data to be available
            while (self.items_available.load(.monotonic) == 0) {
                self.data_cond.wait(&self.read_mutex);
            }
            
            const chunk = self.head orelse unreachable;
            
            return View{
                .data = chunk.getReadSlice(),
                .chunk = chunk,
                .queue = self,
            };
        }
        
        /// Try to get readable slice without blocking
        pub fn tryGetSlice(self: *Self) ?View {
            self.read_mutex.lock();
            defer self.read_mutex.unlock();
            
            if (self.items_available.load(.monotonic) == 0) {
                return null;
            }
            
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
        
        /// Consume items after processing
        pub fn consumeItems(self: *Self, chunk: *Chunk, items_consumed: usize) void {
            self.read_mutex.lock();
            defer self.read_mutex.unlock();
            
            chunk.read_pos += items_consumed;
            _ = self.items_available.fetchSub(items_consumed, .monotonic);
            
            // Check if we can advance head and recycle chunks
            while (self.head) |head_chunk| {
                if (!head_chunk.isFullyConsumed()) break;
                
                self.head = head_chunk.next;
                
                if (self.tail == head_chunk) {
                    self.write_mutex.lock();
                    self.tail = null;
                    self.write_mutex.unlock();
                }
                
                self.recycleChunk(head_chunk);
            }
        }
        
        /// Get total items available for reading
        pub fn getItemsAvailable(self: *const Self) usize {
            return self.items_available.load(.monotonic);
        }
        
        /// Check if queue has data without locking
        pub fn hasData(self: *const Self) bool {
            return self.items_available.load(.monotonic) > 0;
        }
        
        // Private helper functions
        
        fn ensureWritableChunk(self: *Self) !*Chunk {
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
        
        fn allocateChunk(self: *Self) !*Chunk {
            if (self.max_chunks > 0 and self.total_chunks >= self.max_chunks) {
                return error.ChunkLimitExceeded;
            }
            
            if (self.chunk_pool.get()) |chunk| {
                return chunk;
            }
            
            const chunk = try self.allocator.create(Chunk);
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
        queue: *Queue,
        
        const Self = @This();
        const Queue = ConcurrentQueue(u8, chunk_size);
        pub const Config = Queue.Config;
        
        pub fn init(allocator: Allocator, config: Config) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .queue = try Queue.init(allocator, config),
            };
            return self;
        }
        
        pub fn deinit(self: *Self) void {
            const allocator = self.queue.allocator;
            self.queue.deinit();
            allocator.destroy(self);
        }
        
        /// Append bytes to the buffer
        pub fn append(self: *Self, data: []const u8) !void {
            return self.queue.pushSlice(data);
        }
        
        /// Get readable byte slice
        pub fn tryGetSlice(self: *Self) ?Queue.View {
            return self.queue.tryGetSlice();
        }
        
        /// Wait and get readable byte slice
        pub fn waitAndGetSlice(self: *Self) !Queue.View {
            return self.queue.waitAndGetSlice();
        }
        
        /// Get bytes available
        pub fn getBytesAvailable(self: *const Self) usize {
            return self.queue.getItemsAvailable();
        }
        
        /// Check if has data
        pub fn hasData(self: *const Self) bool {
            return self.queue.hasData();
        }
        
        /// Get multiple readable slices for vectored I/O
        pub fn gatherReadVectors(self: *Self, iovecs: []std.posix.iovec) usize {
            self.queue.read_mutex.lock();
            defer self.queue.read_mutex.unlock();
            
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
            self.queue.read_mutex.lock();
            defer self.queue.read_mutex.unlock();
            
            var remaining = total_bytes;
            
            _ = self.queue.items_available.fetchSub(total_bytes, .monotonic);
            
            while (self.queue.head) |head_chunk| {
                if (remaining == 0) break;
                
                const available = head_chunk.availableToRead();
                const to_consume = @min(available, remaining);
                
                head_chunk.read_pos += to_consume;
                remaining -= to_consume;
                
                if (head_chunk.isFullyConsumed()) {
                    self.queue.head = head_chunk.next;
                    
                    if (self.queue.tail == head_chunk) {
                        self.queue.write_mutex.lock();
                        self.queue.tail = null;
                        self.queue.write_mutex.unlock();
                    }
                    
                    self.queue.recycleChunk(head_chunk);
                }
                
                if (to_consume < available) {
                    break;
                }
            }
        }
    };
}

// Tests
test "generic queue with integers" {
    const allocator = std.testing.allocator;
    
    const IntQueue = ConcurrentQueue(i32, 4);
    const queue = try IntQueue.init(allocator, .{});
    defer queue.deinit();
    
    // Push individual items
    try queue.push(42);
    try queue.push(43);
    
    // Pop them
    try std.testing.expectEqual(@as(i32, 42), try queue.pop());
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
    const queue = try MsgQueue.init(allocator, .{});
    defer queue.deinit();
    
    // Push messages
    const messages = [_]Message{
        .{ .id = 1, .data = "hello   ".* },
        .{ .id = 2, .data = "world   ".* },
    };
    
    try queue.pushSlice(&messages);
    
    // Get slice view
    if (queue.tryGetSlice()) |view| {
        try std.testing.expectEqual(@as(usize, 2), view.data.len);
        try std.testing.expectEqual(@as(u32, 1), view.data[0].id);
        view.consume(2);
    }
}

test "byte buffer specialization" {
    const allocator = std.testing.allocator;
    
    const Buffer = ConcurrentWriteBuffer(64);
    const buffer = try Buffer.init(allocator, .{});
    defer buffer.deinit();
    
    try buffer.append("Hello, World!");
    
    if (buffer.tryGetSlice()) |view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.consume(view.data.len);
    }
}

test "concurrent push and pop" {
    const allocator = std.testing.allocator;
    
    const Queue = ConcurrentQueue(u64, 32);
    const queue = try Queue.init(allocator, .{});
    defer queue.deinit();
    
    const Producer = struct {
        fn run(q: *Queue) !void {
            for (0..100) |i| {
                try q.push(i);
            }
        }
    };
    
    const Consumer = struct {
        fn run(q: *Queue) !void {
            var sum: u64 = 0;
            for (0..100) |_| {
                sum += try q.pop();
            }
            // Sum of 0..99 = 4950
            try std.testing.expectEqual(@as(u64, 4950), sum);
        }
    };
    
    const producer = try std.Thread.spawn(.{}, Producer.run, .{queue});
    const consumer = try std.Thread.spawn(.{}, Consumer.run, .{queue});
    
    producer.join();
    consumer.join();
}
