const std = @import("std");
const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;
const Allocator = std.mem.Allocator;
const atomic = std.atomic;

/// A single chunk in the linked list
const Chunk = struct {
    /// The data buffer for this chunk
    data: []u8,
    /// Number of bytes written to this chunk (protected by write_mutex)
    write_pos: usize,
    /// Number of bytes read from this chunk (protected by read_mutex)
    read_pos: usize,
    /// Next chunk in the list (protected by write_mutex when modifying)
    next: ?*Chunk,
    /// Whether this chunk is full and sealed (protected by write_mutex)
    is_sealed: bool,
    
    fn init(allocator: Allocator, size: usize) !*Chunk {
        const chunk = try allocator.create(Chunk);
        errdefer allocator.destroy(chunk);
        
        chunk.* = .{
            .data = try allocator.alloc(u8, size),
            .write_pos = 0,
            .read_pos = 0,
            .next = null,
            .is_sealed = false,
        };
        return chunk;
    }
    
    fn deinit(self: *Chunk, allocator: Allocator) void {
        allocator.free(self.data);
        allocator.destroy(self);
    }
    
    fn reset(self: *Chunk) void {
        self.write_pos = 0;
        self.read_pos = 0;
        self.next = null;
        self.is_sealed = false;
    }
    
    fn availableToWrite(self: *const Chunk) usize {
        return self.data.len - self.write_pos;
    }
    
    fn availableToRead(self: *const Chunk) usize {
        return self.write_pos - self.read_pos;
    }
    
    fn isFullyConsumed(self: *const Chunk) bool {
        return self.is_sealed and self.read_pos >= self.write_pos;
    }
};

/// A view into readable data that can be consumed after network write
pub const ReadView = struct {
    /// The readable data slice
    data: []const u8,
    /// Reference to the chunk
    chunk: *Chunk,
    /// Reference to the parent buffer
    buffer: *ConcurrentWriteBuffer,
    
    /// Call after successfully writing bytes to socket
    pub fn consume(self: *ReadView, bytes_consumed: usize) void {
        std.debug.assert(bytes_consumed <= self.data.len);
        self.buffer.consumeBytes(self.chunk, bytes_consumed);
    }
};

/// Pool of reusable chunks to reduce allocations
const ChunkPool = struct {
    chunks: std.ArrayList(*Chunk),
    max_size: usize,
    
    fn init(allocator: Allocator, max_size: usize) ChunkPool {
        return .{
            .chunks = std.ArrayList(*Chunk).init(allocator),
            .max_size = max_size,
        };
    }
    
    fn deinit(self: *ChunkPool, allocator: Allocator) void {
        for (self.chunks.items) |chunk| {
            chunk.deinit(allocator);
        }
        self.chunks.deinit();
    }
    
    fn get(self: *ChunkPool) ?*Chunk {
        return self.chunks.popOrNull();
    }
    
    fn put(self: *ChunkPool, chunk: *Chunk) bool {
        if (self.chunks.items.len >= self.max_size) {
            return false;
        }
        chunk.reset();
        self.chunks.append(chunk) catch return false;
        return true;
    }
};

/// Configuration for the buffer
pub const Config = struct {
    /// Size of each chunk in bytes
    chunk_size: usize = 4096,
    /// Maximum number of chunks to keep in reuse pool
    max_pool_size: usize = 8,
    /// Maximum total chunks allowed (null = unlimited)
    max_chunks: ?usize = null,
};

/// Concurrent write buffer using linked list of chunks
pub const ConcurrentWriteBuffer = struct {
    allocator: Allocator,
    config: Config,
    
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
    
    /// Shared atomic counter for available bytes
    bytes_available: atomic.Value(usize),
    /// Total number of chunks allocated
    total_chunks: usize,
    
    /// Pool of reusable chunks (protected by write_mutex)
    chunk_pool: ChunkPool,
    
    const Self = @This();
    
    pub fn init(allocator: Allocator, config: Config) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .config = config,
            .write_mutex = .{},
            .read_mutex = .{},
            .data_cond = .{},
            .head = null,
            .tail = null,
            .bytes_available = atomic.Value(usize).init(0),
            .total_chunks = 0,
            .chunk_pool = ChunkPool.init(allocator, config.max_pool_size),
        };
        return self;
    }
    
    pub fn deinit(self: *Self) void {
        // Free all chunks in the linked list
        var current = self.head;
        while (current) |chunk| {
            const next = chunk.next;
            chunk.deinit(self.allocator);
            current = next;
        }
        
        // Free chunks in the pool
        self.chunk_pool.deinit(self.allocator);
        
        // Free self
        self.allocator.destroy(self);
    }
    
    /// Append data to the buffer (thread-safe)
    pub fn append(self: *Self, data: []const u8) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        
        var remaining = data;
        var total_written: usize = 0;
        
        while (remaining.len > 0) {
            // Get or create a writable chunk
            const chunk = try self.ensureWritableChunk();
            
            // Write what fits in this chunk
            const available = chunk.availableToWrite();
            const to_write = @min(available, remaining.len);
            
            @memcpy(
                chunk.data[chunk.write_pos..][0..to_write],
                remaining[0..to_write]
            );
            
            chunk.write_pos += to_write;
            remaining = remaining[to_write..];
            total_written += to_write;
            
            // Seal chunk if full
            if (chunk.availableToWrite() == 0) {
                chunk.is_sealed = true;
            }
        }
        
        // Update shared counter and signal readers
        _ = self.bytes_available.fetchAdd(total_written, .monotonic);
        self.data_cond.signal();
    }
    
    /// Wait for data and get next readable slice
    pub fn waitAndGetSlice(self: *Self) !ReadView {
        self.read_mutex.lock();
        defer self.read_mutex.unlock();
        
        // Wait for data to be available
        while (self.bytes_available.load(.monotonic) == 0) {
            self.data_cond.wait(&self.read_mutex);
        }
        
        // Get the head chunk
        const chunk = self.head orelse unreachable;
        const available = chunk.availableToRead();
        
        return ReadView{
            .data = chunk.data[chunk.read_pos..chunk.write_pos],
            .chunk = chunk,
            .buffer = self,
        };
    }
    
    /// Try to get readable slice without blocking
    pub fn tryGetSlice(self: *Self) ?ReadView {
        self.read_mutex.lock();
        defer self.read_mutex.unlock();
        
        if (self.bytes_available.load(.monotonic) == 0) {
            return null;
        }
        
        const chunk = self.head orelse return null;
        const available = chunk.availableToRead();
        
        if (available == 0) {
            return null;
        }
        
        return ReadView{
            .data = chunk.data[chunk.read_pos..chunk.write_pos],
            .chunk = chunk,
            .buffer = self,
        };
    }
    
    /// Get multiple readable slices for vectored I/O
    pub fn gatherReadVectors(self: *Self, iovecs: []std.posix.iovec) usize {
        self.read_mutex.lock();
        defer self.read_mutex.unlock();
        
        var count: usize = 0;
        var current = self.head;
        
        while (current) |chunk| {
            if (count >= iovecs.len) break;
            
            const available = chunk.availableToRead();
            if (available > 0) {
                iovecs[count] = .{
                    .base = chunk.data[chunk.read_pos..].ptr,
                    .len = available,
                };
                count += 1;
            }
            
            // Only include consecutive chunks that are sealed or have data
            if (!chunk.is_sealed and chunk.next != null) {
                break;
            }
            
            current = chunk.next;
        }
        
        return count;
    }
    
    /// Consume bytes after successful network write
    pub fn consumeBytes(self: *Self, chunk: *Chunk, bytes_consumed: usize) void {
        self.read_mutex.lock();
        defer self.read_mutex.unlock();
        
        chunk.read_pos += bytes_consumed;
        _ = self.bytes_available.fetchSub(bytes_consumed, .monotonic);
        
        // Check if we can advance head and recycle chunks
        while (self.head) |head_chunk| {
            if (!head_chunk.isFullyConsumed()) break;
            
            // Move head forward
            self.head = head_chunk.next;
            
            // If this was also the tail, clear it
            if (self.tail == head_chunk) {
                self.write_mutex.lock();
                self.tail = null;
                self.write_mutex.unlock();
            }
            
            // Try to recycle the chunk
            self.recycleChunk(head_chunk);
        }
    }
    
    /// Get total bytes available for reading
    pub fn getBytesAvailable(self: *const Self) usize {
        return self.bytes_available.load(.monotonic);
    }
    
    /// Check if buffer has data without locking
    pub fn hasData(self: *const Self) bool {
        return self.bytes_available.load(.monotonic) > 0;
    }
    
    // Private helper functions
    
    fn ensureWritableChunk(self: *Self) !*Chunk {
        // Check if current tail is writable
        if (self.tail) |tail| {
            if (!tail.is_sealed and tail.availableToWrite() > 0) {
                return tail;
            }
        }
        
        // Need a new chunk
        const new_chunk = try self.allocateChunk();
        
        // Link it to the list
        if (self.tail) |tail| {
            tail.next = new_chunk;
        } else {
            // First chunk
            self.head = new_chunk;
        }
        self.tail = new_chunk;
        
        return new_chunk;
    }
    
    fn allocateChunk(self: *Self) !*Chunk {
        // Check if we hit the chunk limit
        if (self.config.max_chunks) |max| {
            if (self.total_chunks >= max) {
                return error.ChunkLimitExceeded;
            }
        }
        
        // Try to get from pool first
        if (self.chunk_pool.get()) |chunk| {
            return chunk;
        }
        
        // Allocate new chunk
        const chunk = try Chunk.init(self.allocator, self.config.chunk_size);
        self.total_chunks += 1;
        return chunk;
    }
    
    fn recycleChunk(self: *Self, chunk: *Chunk) void {
        // Try to put in pool
        if (!self.chunk_pool.put(chunk)) {
            // Pool is full, free the chunk
            chunk.deinit(self.allocator);
            self.total_chunks -= 1;
        }
    }
};

// Tests
test "basic append and read" {
    const allocator = std.testing.allocator;
    
    const buffer = try ConcurrentWriteBuffer.init(allocator, .{
        .chunk_size = 16,
    });
    defer buffer.deinit();
    
    // Write some data
    try buffer.append("Hello, ");
    try buffer.append("World!");
    
    // Read it back
    if (buffer.tryGetSlice()) |view| {
        try std.testing.expectEqualStrings("Hello, World!", view.data);
        view.buffer.consumeBytes(view.chunk, view.data.len);
    }
    
    // Should be empty now
    try std.testing.expect(buffer.tryGetSlice() == null);
}

test "multiple chunks" {
    const allocator = std.testing.allocator;
    
    const buffer = try ConcurrentWriteBuffer.init(allocator, .{
        .chunk_size = 8,
    });
    defer buffer.deinit();
    
    // This will span multiple chunks
    try buffer.append("Hello, World! This is a longer message.");
    
    // Read chunks one by one
    var total_read: usize = 0;
    while (buffer.tryGetSlice()) |view| {
        total_read += view.data.len;
        view.buffer.consumeBytes(view.chunk, view.data.len);
    }
    
    try std.testing.expect(total_read == 39);
}

test "concurrent append and read" {
    const allocator = std.testing.allocator;
    
    const buffer = try ConcurrentWriteBuffer.init(allocator, .{
        .chunk_size = 64,
    });
    defer buffer.deinit();
    
    const Writer = struct {
        fn run(buf: *ConcurrentWriteBuffer) !void {
            for (0..10) |i| {
                var data: [8]u8 = undefined;
                _ = try std.fmt.bufPrint(&data, "msg{d:04}", .{i});
                try buf.append(&data);
                std.time.sleep(1000000); // 1ms
            }
        }
    };
    
    const Reader = struct {
        fn run(buf: *ConcurrentWriteBuffer) !void {
            var count: usize = 0;
            while (count < 10) {
                if (buf.tryGetSlice()) |view| {
                    count += 1;
                    view.buffer.consumeBytes(view.chunk, view.data.len);
                } else {
                    std.time.sleep(500000); // 0.5ms
                }
            }
        }
    };
    
    // Start writer and reader threads
    const writer = try std.Thread.spawn(.{}, Writer.run, .{buffer});
    const reader = try std.Thread.spawn(.{}, Reader.run, .{buffer});
    
    writer.join();
    reader.join();
    
    try std.testing.expect(buffer.getBytesAvailable() == 0);
}
