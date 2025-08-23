const std = @import("std");

// Import all test modules
pub const minimal_tests = @import("minimal_test.zig");
pub const headers_tests = @import("headers_test.zig");
pub const hpub_format_tests = @import("hpub_format_test.zig");
pub const debug_hpub_tests = @import("debug_hpub.zig");
//pub const basic_tests = @import("basic_test.zig");
// pub const pubsub_tests = @import("pubsub_test.zig");

// Re-export test declarations so they run
test {
    std.testing.refAllDecls(@This());

    // Print test header
    std.debug.print("\n=== NATS.zig Integration Tests ===\n", .{});
    std.debug.print("Make sure Docker is running and ports 14222-14225 are available\n", .{});
    std.debug.print("Run 'docker compose -f docker-compose.test.yml up -d' to start test servers\n\n", .{});
}
