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
const Message = @import("message.zig").Message;
const Subscription = @import("subscription.zig").Subscription;
const ConcurrentQueue = @import("queue.zig").ConcurrentQueue;

const log = std.log.scoped(.dispatcher);

/// Message to be dispatched to a subscription handler
pub const DispatchMessage = struct {
    subscription: *Subscription,
    message: *Message,

    pub fn init(subscription: *Subscription, message: *Message) DispatchMessage {
        subscription.retain();
        return .{
            .subscription = subscription,
            .message = message,
        };
    }

    pub fn deinit(self: *const DispatchMessage) void {
        self.subscription.release();
    }
};

/// Dispatcher thread that processes messages for multiple subscriptions
pub const Dispatcher = struct {
    allocator: Allocator,
    thread: ?std.Thread = null,
    queue: DispatchQueue,

    const DispatchQueue = ConcurrentQueue(DispatchMessage, 1024);

    pub fn init(allocator: Allocator) Dispatcher {
        return .{
            .allocator = allocator,
            .queue = DispatchQueue.init(allocator, .{}),
        };
    }

    pub fn deinit(self: *Dispatcher) void {
        self.stop();
        // Clean up any remaining messages in the queue
        while (self.queue.tryPop()) |dispatch_msg| {
            log.warn("Dropping unprocessed message for subscription {}", .{dispatch_msg.subscription.sid});
            dispatch_msg.message.deinit();
            dispatch_msg.deinit(); // Release subscription reference
        }
        self.queue.deinit();
    }

    /// Start the dispatcher thread
    pub fn start(self: *Dispatcher) !void {
        if (self.thread != null) return; // Already running

        self.thread = try std.Thread.spawn(.{}, dispatcherLoop, .{self});
    }

    /// Stop the dispatcher thread
    pub fn stop(self: *Dispatcher) void {
        if (self.thread == null) return; // Already stopped

        self.queue.close(); // Wake up thread and signal it to stop

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Enqueue a message for dispatch
    pub fn enqueue(self: *Dispatcher, subscription: *Subscription, message: *Message) !void {
        const dispatch_msg = DispatchMessage.init(subscription, message);
        errdefer dispatch_msg.deinit();
        try self.queue.push(dispatch_msg);
    }

    /// Main dispatcher thread loop
    fn dispatcherLoop(self: *Dispatcher) void {
        log.debug("Dispatcher thread started", .{});

        while (true) {
            // Wait for messages with timeout
            if (self.queue.pop(100)) |dispatch_msg| { // 100ms timeout
                self.processMessage(dispatch_msg);
            } else |err| {
                if (err == error.QueueClosed) {
                    log.debug("Dispatcher queue closed, stopping", .{});
                    break;
                }
                // Timeout - continue loop until queue is closed
            }
        }

        // Process any remaining messages before shutdown
        while (self.queue.tryPop()) |dispatch_msg| {
            self.processMessage(dispatch_msg);
        }

        log.debug("Dispatcher thread stopped", .{});
    }

    /// Process a single dispatch message
    fn processMessage(self: *Dispatcher, dispatch_msg: DispatchMessage) void {
        _ = self; // unused
        defer dispatch_msg.deinit(); // Release subscription reference

        const subscription = dispatch_msg.subscription;
        const message = dispatch_msg.message;

        // Call the subscription's handler in this dispatcher thread context
        // Message ownership is transferred to the handler - handler is responsible for cleanup
        if (subscription.handler) |handler| {
            handler.call(message) catch |err| {
                log.err("Message handler failed for subscription {}: {}", .{ subscription.sid, err });
                // Handler owns the message even on error - no cleanup here
            };
        } else {
            // No handler - this shouldn't happen for async subscriptions
            log.warn("Received message for subscription {} without handler", .{subscription.sid});
            message.deinit(); // Clean up orphaned message (no handler to transfer ownership to)
        }
    }
};

/// Pool of dispatcher threads with round-robin assignment
pub const DispatcherPool = struct {
    allocator: Allocator,
    dispatchers: []Dispatcher,
    thread_count: usize,
    use_next: usize = 0,
    mutex: std.Thread.Mutex = .{},

    pub fn init(allocator: Allocator, thread_count: usize) !*DispatcherPool {
        const pool = try allocator.create(DispatcherPool);
        errdefer allocator.destroy(pool);

        const dispatchers = try allocator.alloc(Dispatcher, thread_count);
        errdefer allocator.free(dispatchers);

        // Initialize all dispatchers
        for (dispatchers) |*dispatcher| {
            dispatcher.* = Dispatcher.init(allocator);
        }

        pool.* = .{
            .allocator = allocator,
            .dispatchers = dispatchers,
            .thread_count = thread_count,
        };

        return pool;
    }

    pub fn deinit(self: *DispatcherPool) void {
        self.stop();

        // Deinitialize all dispatchers
        for (self.dispatchers) |*dispatcher| {
            dispatcher.deinit();
        }

        self.allocator.free(self.dispatchers);
        self.allocator.destroy(self);
    }

    /// Start all dispatcher threads
    pub fn start(self: *DispatcherPool) !void {
        log.info("Starting {} dispatcher threads", .{self.thread_count});

        for (self.dispatchers) |*dispatcher| {
            try dispatcher.start();
        }
    }

    /// Stop all dispatcher threads
    pub fn stop(self: *DispatcherPool) void {
        log.info("Stopping {} dispatcher threads", .{self.thread_count});

        for (self.dispatchers) |*dispatcher| {
            dispatcher.stop();
        }
    }

    /// Assign dispatcher to subscription using round-robin (like C library)
    pub fn assignDispatcher(self: *DispatcherPool) *Dispatcher {
        self.mutex.lock();
        defer self.mutex.unlock();

        const dispatcher = &self.dispatchers[self.use_next];
        self.use_next = (self.use_next + 1) % self.thread_count;

        return dispatcher;
    }
};

// Global dispatcher pool with reference counting
var global_pool: ?*DispatcherPool = null;
var global_pool_ref_count: u32 = 0;
var global_pool_mutex: std.Thread.Mutex = .{};

/// Get thread pool size from environment variable NATS_THREAD_POOL_MAX, defaulting to 1
fn getThreadPoolSize(allocator: Allocator) usize {
    if (std.process.getEnvVarOwned(allocator, "NATS_THREAD_POOL_MAX")) |env_value| {
        defer allocator.free(env_value);
        if (std.fmt.parseInt(usize, env_value, 10)) |size| {
            if (size > 0) {
                log.debug("Using NATS_THREAD_POOL_MAX={}", .{size});
                return size;
            } else {
                log.warn("NATS_THREAD_POOL_MAX must be > 0, using default", .{});
            }
        } else |_| {
            log.warn("Invalid NATS_THREAD_POOL_MAX value '{s}', using default", .{env_value});
        }
    } else |_| {
        // Environment variable not set, use default
    }
    return 1; // Default size
}

/// Acquire the global dispatcher pool, creating it if necessary
/// Call releaseGlobalPool() when done to ensure proper cleanup
pub fn acquireGlobalPool(allocator: Allocator) !*DispatcherPool {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();

    if (global_pool == null) {
        const thread_count = getThreadPoolSize(allocator);
        log.debug("Creating global dispatcher pool with {} threads", .{thread_count});
        global_pool = try DispatcherPool.init(allocator, thread_count);
        try global_pool.?.start();
    }

    global_pool_ref_count += 1;
    log.debug("Global dispatcher pool acquired, ref count: {}", .{global_pool_ref_count});
    return global_pool.?;
}

/// Release the global dispatcher pool
/// When the last reference is released, the pool is shutdown and cleaned up
pub fn releaseGlobalPool() void {
    global_pool_mutex.lock();
    defer global_pool_mutex.unlock();

    global_pool_ref_count -= 1;

    if (global_pool_ref_count == 0) {
        log.debug("Last reference released, shutting down global dispatcher pool", .{});
        if (global_pool) |pool| {
            pool.deinit();
            global_pool = null;
        }
    } else {
        log.debug("Global dispatcher pool released, ref count: {}", .{global_pool_ref_count});
    }
}
