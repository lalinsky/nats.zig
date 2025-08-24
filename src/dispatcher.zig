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
        if (subscription.handler) |handler| {
            handler.call(message);
        } else {
            // No handler - this shouldn't happen for async subscriptions
            log.warn("Received message for subscription {} without handler", .{subscription.sid});
            message.deinit(); // Clean up orphaned message
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