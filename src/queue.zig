const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        items: std.ArrayListUnmanaged(T) = .{},
        head: usize = 0,
        mutex: std.Thread.Mutex = .{},
        condition: std.Thread.Condition = .{},
        closed: bool = false,

        pub fn init(allocator: Allocator) Self {
            return Self{
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.close();
            self.items.deinit(self.allocator);
        }

        /// Compact the queue by moving remaining items to the front
        /// and shrinking the array. Must be called with mutex held.
        fn compact(self: *Self) void {
            if (self.head > 0) {
                const remaining = self.items.items.len - self.head;
                if (remaining > 0) {
                    std.mem.copyForwards(T, self.items.items[0..remaining], self.items.items[self.head..]);
                }
                self.items.shrinkRetainingCapacity(remaining);
                self.head = 0;
            }
        }

        pub fn push(self: *Self, item: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return error.QueueClosed;
            }

            // Ensure we have space; prefer compaction before growth
            if (self.items.items.len == self.items.capacity) {
                self.compact();
                if (self.items.items.len == self.items.capacity) {
                    const new_capacity = if (self.items.capacity == 0) 8 else self.items.capacity * 2;
                    try self.items.ensureTotalCapacity(self.allocator, new_capacity);
                }
            }

            // Add item at the end
            try self.items.append(self.allocator, item);
            self.condition.signal();
        }

        pub fn tryPop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.head >= self.items.items.len) {
                return null;
            }

            const item = self.items.items[self.head];
            self.head += 1;

            // Clean up when we've consumed most items
            if (self.head * 2 >= self.items.items.len and self.items.items.len > 16) {
                self.compact();
            }

            return item;
        }

        pub fn pop(self: *Self, timeout_ms: u64) ?T {
            if (timeout_ms == 0) {
                return self.tryPop();
            }

            const timeout_ns = timeout_ms * std.time.ns_per_ms;
            var timer = std.time.Timer.start() catch unreachable;

            self.mutex.lock();
            defer self.mutex.unlock();

            while (!self.closed) {
                if (self.head < self.items.items.len) {
                    const item = self.items.items[self.head];
                    self.head += 1;

                    // Clean up when we've consumed most items
                    if (self.head * 2 >= self.items.items.len and self.items.items.len > 16) {
                        self.compact();
                    }

                    return item;
                }

                const elapsed_ns = timer.read();
                if (elapsed_ns >= timeout_ns) {
                    return null;
                }

                const remaining_ns = timeout_ns - elapsed_ns;
                self.condition.timedWait(&self.mutex, remaining_ns) catch {};
            }

            return null;
        }

        pub fn len(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return if (self.items.items.len >= self.head) self.items.items.len - self.head else 0;
        }

        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head >= self.items.items.len;
        }

        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (!self.closed) {
                self.closed = true;
                self.condition.broadcast();
            }
        }

        pub fn isClosed(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.closed;
        }
    };
}

pub const QueueError = error{
    QueueClosed,
} || Allocator.Error;

test "Queue basic operations" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    // Test empty queue
    try testing.expect(queue.isEmpty());
    try testing.expectEqual(@as(usize, 0), queue.len());
    try testing.expectEqual(@as(?i32, null), queue.tryPop());

    // Test push and pop
    try queue.push(42);
    try testing.expect(!queue.isEmpty());
    try testing.expectEqual(@as(usize, 1), queue.len());

    const item = queue.tryPop();
    try testing.expectEqual(@as(?i32, 42), item);
    try testing.expect(queue.isEmpty());
    try testing.expectEqual(@as(usize, 0), queue.len());
}

test "Queue FIFO ordering" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    // Push multiple items
    try queue.push(1);
    try queue.push(2);
    try queue.push(3);

    try testing.expectEqual(@as(usize, 3), queue.len());

    // Pop in FIFO order
    try testing.expectEqual(@as(?i32, 1), queue.tryPop());
    try testing.expectEqual(@as(?i32, 2), queue.tryPop());
    try testing.expectEqual(@as(?i32, 3), queue.tryPop());
    try testing.expectEqual(@as(?i32, null), queue.tryPop());
}

test "Queue timeout behavior" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    // Test immediate timeout (0ms)
    try testing.expectEqual(@as(?i32, null), queue.pop(0));

    // Test short timeout on empty queue
    var timer = std.time.Timer.start() catch unreachable;
    const result = queue.pop(50);
    const elapsed_ms = timer.read() / std.time.ns_per_ms;

    try testing.expectEqual(@as(?i32, null), result);
    try testing.expect(elapsed_ms >= 45); // Allow some tolerance
}

test "Queue close behavior" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    // Test push after close
    queue.close();
    try testing.expect(queue.isClosed());
    try testing.expectError(error.QueueClosed, queue.push(42));
}

test "Queue compaction" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    // Push many items to trigger capacity growth
    for (0..100) |i| {
        try queue.push(@intCast(i));
    }

    // Pop most items to trigger compaction
    for (0..90) |_| {
        _ = queue.tryPop();
    }

    // Verify remaining items are still correct
    try testing.expectEqual(@as(usize, 10), queue.len());
    for (90..100) |i| {
        try testing.expectEqual(@as(?i32, @intCast(i)), queue.tryPop());
    }
}

test "Queue push compacts when head > 0 at capacity (no duplication)" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var q = Queue(i32).init(allocator);
    defer q.deinit();

    // Fill to initial capacity (8)
    for (0..8) |i| try q.push(@intCast(i));

    // Pop a few to advance head
    try testing.expectEqual(@as(?i32, 0), q.tryPop());
    try testing.expectEqual(@as(?i32, 1), q.tryPop());
    try testing.expectEqual(@as(?i32, 2), q.tryPop());
    try testing.expectEqual(@as(usize, 5), q.len());

    // Push enough to require space; should compact, not rotate duplicates
    for (8..12) |i| try q.push(@intCast(i));

    // We should see the remaining original items [3..7], then [8..11]
    for (3..12) |i| try testing.expectEqual(@as(?i32, @intCast(i)), q.tryPop());
    try testing.expect(q.isEmpty());
}

test "Queue multithreaded producer/consumer" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = Queue(i32).init(allocator);
    defer queue.deinit();

    const Context = struct {
        queue: *Queue(i32),
        values: []i32,
        count: std.atomic.Value(usize),
        done: std.atomic.Value(bool),
    };

    var consumed = [_]i32{0} ** 100;
    var ctx = Context{
        .queue = &queue,
        .values = &consumed,
        .count = std.atomic.Value(usize).init(0),
        .done = std.atomic.Value(bool).init(false),
    };

    // Producer thread
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            for (0..100) |i| {
                context.queue.push(@intCast(i)) catch unreachable;
            }
            context.done.store(true, .release);
        }
    }.run, .{&ctx});

    // Consumer thread
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            while (context.count.load(.acquire) < 100) {
                if (context.queue.pop(10)) |value| {
                    const idx = context.count.fetchAdd(1, .acq_rel);
                    if (idx < 100) {
                        context.values[idx] = value;
                    }
                } else if (context.done.load(.acquire)) {
                    // Producer finished, drain remaining
                    while (context.count.load(.acquire) < 100) {
                        if (context.queue.tryPop()) |value| {
                            const idx = context.count.fetchAdd(1, .acq_rel);
                            if (idx < 100) {
                                context.values[idx] = value;
                            }
                        } else {
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    // Verify all values were consumed (order may not be guaranteed in this test)
    var sum: i32 = 0;
    for (consumed) |value| {
        sum += value;
    }
    const expected_sum: i32 = (99 * 100) / 2; // Sum of 0..99
    try testing.expectEqual(expected_sum, sum);
}