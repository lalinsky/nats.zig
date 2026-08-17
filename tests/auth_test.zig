const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

test "token authentication success" {
    const io = std.testing.io;

    // Test against actual NATS server with token auth (port 14225)
    const opts = nats.ConnectionOptions{
        .token = "test_token_123",
    };

    const conn = try utils.createConnection(io, .token_auth, opts);
    defer utils.closeConnection(conn);

    // If we reach here, authentication succeeded
    // Test basic publish/subscribe to verify connection works
    try conn.publish("test.auth.success", "authenticated message");
    try conn.flush();
}

test "token handler authentication" {
    const io = std.testing.io;

    // Test token handler callback against real server
    const TestTokenHandler = struct {
        fn getToken() []const u8 {
            return "test_token_123"; // Return valid token for auth server
        }
    };

    const opts = nats.ConnectionOptions{
        .token_handler = TestTokenHandler.getToken,
    };

    const conn = try utils.createConnection(io, .token_auth, opts);
    defer utils.closeConnection(conn);

    // If we reach here, the token handler was called and authentication succeeded
    try conn.publish("test.auth.handler", "handler authenticated");
    try conn.flush();
}

test "token handler takes precedence over static token" {
    const io = std.testing.io;

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
    const conn = try utils.createConnection(io, .token_auth, opts);
    defer utils.closeConnection(conn);

    // Authentication succeeded, proving handler took precedence
    try conn.publish("test.auth.precedence", "handler wins");
    try conn.flush();
}

test "token authentication failure" {
    const io = std.testing.io;

    // Test authentication failure with invalid token and short timeout
    const opts = nats.ConnectionOptions{
        .token = "invalid_token",
        .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } },
    };

    // This should fail with AuthFailed error
    const result = utils.createConnection(io, .token_auth, opts);

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

test "user password authentication success" {
    const io = std.testing.io;

    const opts = nats.ConnectionOptions{
        .user = "test_user",
        .password = "test_password",
    };

    const conn = try utils.createConnection(io, .user_pass, opts);
    defer utils.closeConnection(conn);

    try conn.publish("test.auth.userpass", "authenticated message");
    try conn.flush();
}

test "user password authentication failure" {
    const io = std.testing.io;

    const opts = nats.ConnectionOptions{
        .user = "test_user",
        .password = "wrong_password",
        .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } },
    };

    const result = utils.createConnection(io, .user_pass, opts);

    if (result) |conn| {
        defer utils.closeConnection(conn);
        try std.testing.expect(false);
    } else |err| {
        try std.testing.expect(err == nats.ProtocolError.AuthorizationViolation);
    }
}

test "url credentials authentication" {
    const io = std.testing.io;

    const conn = try utils.createConnectionWithUrl(
        io,
        "nats://test_user:test_password@127.0.0.1:14227",
        .{},
    );
    defer utils.closeConnection(conn);

    try conn.publish("test.auth.urlcreds", "url authenticated");
    try conn.flush();
}

test "url credentials take precedence over options" {
    const io = std.testing.io;

    // The URL credentials are valid, the options ones are not; the URL
    // must win, matching the C client's precedence.
    const opts = nats.ConnectionOptions{
        .user = "test_user",
        .password = "wrong_password",
    };

    const conn = try utils.createConnectionWithUrl(
        io,
        "nats://test_user:test_password@127.0.0.1:14227",
        opts,
    );
    defer utils.closeConnection(conn);

    try conn.publish("test.auth.urlprecedence", "url wins");
    try conn.flush();
}

test "url token authentication" {
    const io = std.testing.io;

    // A username without a password in the URL is treated as a token.
    const conn = try utils.createConnectionWithUrl(
        io,
        "nats://test_token_123@127.0.0.1:14225",
        .{},
    );
    defer utils.closeConnection(conn);

    try conn.publish("test.auth.urltoken", "token authenticated");
    try conn.flush();
}

// Fixture seeds; the nkey server config (tests/configs/nkey.conf)
// authorizes only the public key derived from the first one.
const nkey_seed = "SUABCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEKPBU";
const wrong_nkey_seed = "SUACEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIU6OY";

test "nkey authentication success" {
    const io = std.testing.io;

    const opts = nats.ConnectionOptions{
        .nkey_seed = nkey_seed,
    };

    const conn = try utils.createConnection(io, .nkey_auth, opts);
    defer utils.closeConnection(conn);

    try conn.publish("test.auth.nkey", "nkey authenticated");
    try conn.flush();
}

test "nkey authentication failure with unauthorized key" {
    const io = std.testing.io;

    const opts = nats.ConnectionOptions{
        .nkey_seed = wrong_nkey_seed,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } },
    };

    const result = utils.createConnection(io, .nkey_auth, opts);

    if (result) |conn| {
        defer utils.closeConnection(conn);
        try std.testing.expect(false);
    } else |err| {
        try std.testing.expect(err == nats.ProtocolError.AuthorizationViolation);
    }
}

test "invalid nkey seed fails before connecting" {
    const io = std.testing.io;

    const opts = nats.ConnectionOptions{
        .nkey_seed = "SUANOTAVALIDSEED",
    };

    // Even the unused port works here: the seed is validated before dialing.
    const result = utils.createConnection(io, .unknown, opts);
    try std.testing.expectError(nats.nkeys.Error.InvalidSeed, result);
}

test "no authentication options against auth server" {
    const io = std.testing.io;

    // Test connection without token to auth server (should fail)
    const opts = nats.ConnectionOptions{};

    const result = utils.createConnection(io, .token_auth, opts);

    if (result) |conn| {
        defer utils.closeConnection(conn);
        // Should not reach here
        try std.testing.expect(false);
    } else |err| {
        try std.testing.expect(err == nats.ProtocolError.AuthorizationViolation);
    }
}
