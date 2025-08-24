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
};

/// Dispatcher thread that processes messages for multiple subscriptions
pub const Dispatcher = struct {
    allocator: Allocator,
    thread: ?std.Thread = null,
    queue: DispatchQueue,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    
    const DispatchQueue = ConcurrentQueue(DispatchMessage, 1024);
    
    pub fn init(allocator: Allocator) Dispatcher {
        return .{
            .allocator = allocator,
            .queue = DispatchQueue.init(allocator, .{}),
        };
    }
    
    pub fn deinit(self: *Dispatcher) void {
        self.stop();
        self.queue.deinit();
    }
    
    /// Start the dispatcher thread
    pub fn start(self: *Dispatcher) !void {
        if (self.thread != null) return; // Already running
        
        self.running.store(true, .monotonic);
        self.thread = try std.Thread.spawn(.{}, dispatcherLoop, .{self});
    }
    
    /// Stop the dispatcher thread
    pub fn stop(self: *Dispatcher) void {
        if (self.thread == null) return; // Already stopped
        
        self.running.store(false, .monotonic);
        self.queue.close(); // Wake up thread
        
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }
    
    /// Enqueue a message for dispatch
    pub fn enqueue(self: *Dispatcher, subscription: *Subscription, message: *Message) !void {
        const dispatch_msg = DispatchMessage{
            .subscription = subscription,
            .message = message,
        };
        try self.queue.push(dispatch_msg);
    }
    
    /// Main dispatcher thread loop
    fn dispatcherLoop(self: *Dispatcher) void {
        log.debug("Dispatcher thread started", .{});
        
        while (self.running.load(.monotonic)) {
            // Wait for messages with timeout
            if (self.queue.pop(100)) |dispatch_msg| { // 100ms timeout
                self.processMessage(dispatch_msg);
            } else |err| {
                if (err == error.QueueClosed) {
                    log.debug("Dispatcher queue closed, stopping", .{});
                    break;
                }
                // Timeout - continue loop to check running flag
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

/// Pool of dispatcher threads with SID-based hashing
pub const DispatcherPool = struct {
    allocator: Allocator,
    dispatchers: []Dispatcher,
    thread_count: usize,
    
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
    
    /// Get dispatcher for a subscription based on SID hash
    pub fn getDispatcher(self: *DispatcherPool, sid: u64) *Dispatcher {
        // Simple modulo for consistent distribution - SIDs are already well-distributed
        const index = sid % self.thread_count;
        return &self.dispatchers[index];
    }
    
    /// Enqueue message for dispatch using SID-based routing
    pub fn dispatch(self: *DispatcherPool, subscription: *Subscription, message: *Message) !void {
        const dispatcher = self.getDispatcher(subscription.sid);
        try dispatcher.enqueue(subscription, message);
    }
};