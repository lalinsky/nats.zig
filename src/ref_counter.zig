const std = @import("std");

/// Thread-safe atomic reference counter
/// 
/// This implementation follows best practices for atomic reference counting:
/// - Uses .monotonic ordering for increments (weakest safe ordering)
/// - Uses .release ordering for decrements (ensures memory operations are visible)
/// - Uses .acquire load when count reaches zero (synchronizes with all releases)
///
/// This pattern is equivalent to std::shared_ptr in C++ and follows the same
/// memory ordering guarantees as established in academic literature.
pub fn RefCounter(comptime T: type) type {
    return struct {
        refs: std.atomic.Value(T),

        pub const Self = @This();

        pub fn init() Self {
            return .{
                .refs = std.atomic.Value(T).init(1),
            };
        }

        /// Increases the reference count.
        /// Uses monotonic ordering since new references can only be created
        /// from existing ones, which already provide necessary synchronization.
        pub fn incr(self: *Self) void {
            const prev_ref_count = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(prev_ref_count > 0);
        }

        /// Decreases the reference count and returns true if it reached zero.
        /// Uses release ordering to ensure all previous memory operations
        /// are visible before the count reaches zero.
        pub fn decr(self: *Self) bool {
            const prev_ref_count = self.refs.fetchSub(1, .release);
            if (prev_ref_count == 1) {
                // Use acquire load as substitute for fence (Zig 0.14 doesn't have @fence)
                // This synchronizes with all release operations from other threads
                _ = self.refs.load(.acquire);
                return true;
            }
            return false;
        }

        /// Get current reference count (for debugging only)
        pub fn count(self: *const Self) T {
            return self.refs.load(.monotonic);
        }
    };
}

// Test the RefCounter implementation
test "RefCounter basic operations" {
    var counter = RefCounter(u32).init();
    
    // Initial count should be 1
    try std.testing.expect(counter.count() == 1);
    
    // Increment
    counter.incr();
    try std.testing.expect(counter.count() == 2);
    
    // Decrement (not yet zero)
    try std.testing.expect(!counter.decr());
    try std.testing.expect(counter.count() == 1);
    
    // Final decrement (reaches zero)
    try std.testing.expect(counter.decr());
    try std.testing.expect(counter.count() == 0);
}

test "RefCounter thread safety simulation" {
    var counter = RefCounter(u32).init();
    
    // Simulate multiple increments
    counter.incr();
    counter.incr();
    counter.incr();
    try std.testing.expect(counter.count() == 4);
    
    // Simulate multiple decrements
    try std.testing.expect(!counter.decr()); // 3
    try std.testing.expect(!counter.decr()); // 2
    try std.testing.expect(!counter.decr()); // 1
    try std.testing.expect(counter.decr());  // 0 - should return true
}