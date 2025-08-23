const std = @import("std");
const nats = @import("nats");

// Test server connection helper - connects to pre-started Docker Compose server
pub fn connectToTestServer(allocator: std.mem.Allocator) !nats.Connection {
    var conn = nats.Connection.init(allocator, .{
        .timeout_ms = 2000,
        .reconnect = .{ .allow_reconnect = false },
    });
    errdefer conn.deinit();
    
    conn.connect("nats://127.0.0.1:14222") catch |err| {
        std.debug.print("⚠️  Could not connect to NATS test server on port 14222: {}\n", .{err});
        std.debug.print("   Make sure to run: docker compose -f docker-compose.test.yml up -d nats-test\n", .{});
        return err;
    };
    
    return conn;
}

/// Wait for a condition to be true with timeout
pub fn waitFor(
    comptime condition_fn: fn () bool,
    timeout_ms: u64,
    check_interval_ms: u64,
) !void {
    const start = std.time.milliTimestamp();
    
    while (std.time.milliTimestamp() - start < timeout_ms) {
        if (condition_fn()) return;
        std.time.sleep(check_interval_ms * std.time.ns_per_ms);
    }
    
    return error.WaitTimeout;
}

/// Wait for a value to match expected with timeout
pub fn waitForValue(
    comptime T: type,
    expected: T,
    actual_fn: *const fn () T,
    timeout_ms: u64,
) !void {
    const start = std.time.milliTimestamp();
    
    while (std.time.milliTimestamp() - start < timeout_ms) {
        if (actual_fn() == expected) return;
        std.time.sleep(10 * std.time.ns_per_ms);
    }
    
    return error.WaitTimeout;
}


/// Helper to check that a message is received with expected data
pub fn expectMessage(sub: *nats.Subscription, expected_data: []const u8, timeout_ms: u64) !void {
    const msg = try sub.nextMsgTimeout(timeout_ms);
    defer msg.deinit();
    
    try std.testing.expectEqualStrings(expected_data, msg.data);
}

/// Helper to check that a message is received with expected subject
pub fn expectMessageSubject(sub: *nats.Subscription, expected_subject: []const u8, timeout_ms: u64) !void {
    const msg = try sub.nextMsgTimeout(timeout_ms);
    defer msg.deinit();
    
    try std.testing.expectEqualStrings(expected_subject, msg.subject);
}

/// Helper to drain all pending messages from a subscription
pub fn drainSubscription(sub: *nats.Subscription, timeout_ms: u64) !u32 {
    var count: u32 = 0;
    
    while (sub.nextMsgTimeout(timeout_ms)) |msg| {
        msg.deinit();
        count += 1;
    } else |_| {
        // Timeout expected when no more messages
    }
    
    return count;
}

/// Thread counter for leak detection
var thread_count_mutex = std.Thread.Mutex{};
var base_thread_count: ?usize = null;

/// Get current thread count (Linux only for now)
pub fn getThreadCount() usize {
    if (@import("builtin").os.tag != .linux) {
        return 0; // Not supported on other platforms yet
    }
    
    const file = std.fs.openFileAbsolute("/proc/self/status", .{}) catch return 0;
    defer file.close();
    
    var buf: [4096]u8 = undefined;
    const content_len = file.read(&buf) catch return 0;
    const content = buf[0..content_len];
    
    // Look for "Threads:" line
    var iter = std.mem.tokenizeAny(u8, content, "\n");
    while (iter.next()) |line| {
        if (std.mem.startsWith(u8, line, "Threads:")) {
            var parts = std.mem.tokenizeAny(u8, line, "\t ");
            _ = parts.next(); // Skip "Threads:"
            if (parts.next()) |count_str| {
                return std.fmt.parseInt(usize, count_str, 10) catch 0;
            }
        }
    }
    
    return 0;
}

/// Set baseline thread count
pub fn setBaselineThreadCount() void {
    thread_count_mutex.lock();
    defer thread_count_mutex.unlock();
    
    base_thread_count = getThreadCount();
}

/// Check for thread leaks
pub fn checkNoThreadLeak(action: []const u8) !void {
    thread_count_mutex.lock();
    defer thread_count_mutex.unlock();
    
    const base = base_thread_count orelse {
        std.log.warn("No baseline thread count set", .{});
        return;
    };
    
    const current = getThreadCount();
    if (current > base) {
        std.log.err("{d} threads still exist after {s} (base: {d}, current: {d})", .{
            current - base,
            action,
            base,
            current,
        });
        return error.ThreadLeak;
    }
}

/// Test-specific message handler that collects messages
pub const TestMessageCollector = struct {
    messages: std.ArrayList(nats.Message),
    mutex: std.Thread.Mutex = .{},
    
    pub fn init(allocator: std.mem.Allocator) TestMessageCollector {
        return .{
            .messages = std.ArrayList(nats.Message).init(allocator),
        };
    }
    
    pub fn deinit(self: *TestMessageCollector) void {
        for (self.messages.items) |*msg| {
            msg.deinit();
        }
        self.messages.deinit();
    }
    
    pub fn handleMessage(self: *TestMessageCollector, msg: *nats.Message) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        // Clone the message for storage
        const cloned = msg.clone() catch return;
        self.messages.append(cloned) catch return;
    }
    
    pub fn count(self: *TestMessageCollector) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.messages.items.len;
    }
    
    pub fn getMessage(self: *TestMessageCollector, index: usize) ?nats.Message {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        if (index >= self.messages.items.len) return null;
        return self.messages.items[index];
    }
    
    pub fn waitForCount(self: *TestMessageCollector, expected_count: usize, timeout_ms: u64) !void {
        const start = std.time.milliTimestamp();
        
        while (std.time.milliTimestamp() - start < timeout_ms) {
            if (self.count() >= expected_count) return;
            std.time.sleep(10 * std.time.ns_per_ms);
        }
        
        return error.MessageCountTimeout;
    }
};

/// Generate a unique subject for testing to avoid conflicts
pub fn uniqueSubject(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    const timestamp = @as(u64, @intCast(std.time.timestamp()));
    const random = std.crypto.random.int(u32);
    return std.fmt.allocPrint(allocator, "{s}.test.{d}.{d}", .{ prefix, timestamp, random });
}

/// Test logging helper
pub fn testLog(comptime format: []const u8, args: anytype) void {
    if (std.posix.getenv("TEST_VERBOSE")) |_| {
        std.debug.print("[TEST] " ++ format ++ "\n", args);
    }
}

/// Ensure test cleanup even on failure
pub const TestCleanup = struct {
    cleanups: std.ArrayList(*const fn () void),
    allocator: std.mem.Allocator,
    
    pub fn init(allocator: std.mem.Allocator) TestCleanup {
        return .{
            .cleanups = std.ArrayList(*const fn () void).init(allocator),
            .allocator = allocator,
        };
    }
    
    pub fn add(self: *TestCleanup, cleanup_fn: *const fn () void) !void {
        try self.cleanups.append(cleanup_fn);
    }
    
    pub fn deinit(self: *TestCleanup) void {
        // Run cleanups in reverse order
        while (self.cleanups.popOrNull()) |cleanup_fn| {
            cleanup_fn();
        }
        self.cleanups.deinit();
    }
};