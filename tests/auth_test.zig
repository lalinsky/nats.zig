const std = @import("std");
const nats = @import("nats");
const zio = @import("zio");
const utils = @import("utils.zig");

const log = std.log.default;

test "token authentication success" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Test against actual NATS server with token auth (port 14225)
    const opts = nats.ConnectionOptions{
        .token = "test_token_123",
    };

    const conn = try utils.createConnection(rt, .token_auth, opts);
    defer utils.closeConnection(conn);

    // If we reach here, authentication succeeded
    // Test basic publish/subscribe to verify connection works
    try conn.publish("test.auth.success", "authenticated message");
    try conn.flush();
}

test "token handler authentication" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Test token handler callback against real server
    const TestTokenHandler = struct {
        fn getToken() []const u8 {
            return "test_token_123"; // Return valid token for auth server
        }
    };

    const opts = nats.ConnectionOptions{
        .token_handler = TestTokenHandler.getToken,
    };

    const conn = try utils.createConnection(rt, .token_auth, opts);
    defer utils.closeConnection(conn);

    // If we reach here, the token handler was called and authentication succeeded
    try conn.publish("test.auth.handler", "handler authenticated");
    try conn.flush();
}

test "token handler takes precedence over static token" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Test that dynamic token handler takes precedence over static token
    const TestTokenHandler = struct {
        fn getToken() []const u8 {
            return "test_token_123"; // Valid token (handler wins)
        }
    };

    const opts = nats.ConnectionOptions{
        .token = "invalid_static_token", // Invalid static token
        .token_handler = TestTokenHandler.getToken,
    };

    // Should succeed because handler returns valid token
    const conn = try utils.createConnection(rt, .token_auth, opts);
    defer utils.closeConnection(conn);

    // Authentication succeeded, proving handler took precedence
    try conn.publish("test.auth.precedence", "handler wins");
    try conn.flush();
}

test "token authentication failure" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Test authentication failure with invalid token and short timeout
    const opts = nats.ConnectionOptions{
        .token = "invalid_token",
        .timeout_ms = 2000, // 2 second timeout
    };

    // This should fail with AuthFailed error
    const result = utils.createConnection(rt, .token_auth, opts);

    if (result) |conn| {
        defer utils.closeConnection(conn);
        // Should not reach here
        std.log.err("Connection unexpectedly succeeded with invalid token", .{});
        try std.testing.expect(false);
    } else |err| {
        std.log.info("Got error: {}", .{err});
        // Now we get specific protocol errors
        try std.testing.expect(err == nats.ProtocolError.AuthorizationViolation);
    }
}

test "no authentication options against auth server" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Test connection without token to auth server (should fail)
    const opts = nats.ConnectionOptions{};

    const result = utils.createConnection(rt, .token_auth, opts);

    if (result) |conn| {
        defer utils.closeConnection(conn);
        // Should not reach here
        try std.testing.expect(false);
    } else |err| {
        try std.testing.expect(err == nats.ProtocolError.AuthorizationViolation);
    }
}
