const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.scoped(.request_reply_test);

// Echo handler function for basic request/reply testing
fn echoHandler(msg: *nats.Message, connection: *nats.Connection) void {
    defer msg.deinit();
    if (msg.reply) |reply_subject| {
        const response = std.fmt.allocPrint(std.testing.allocator, "echo: {s}", .{msg.data}) catch return;
        defer std.testing.allocator.free(response);
        
        connection.publish(reply_subject, response) catch return;
    }
}

// Simple echo handler that just returns the original data
fn simpleEchoHandler(msg: *nats.Message, connection: *nats.Connection) void {
    defer msg.deinit();
    if (msg.reply) |reply_subject| {
        connection.publish(reply_subject, msg.data) catch return;
    }
}

// Slow echo handler that delays before responding (for timeout testing)
fn slowEchoHandler(msg: *nats.Message, connection: *nats.Connection) void {
    defer msg.deinit();
    
    // Sleep for 200ms to simulate slow processing
    std.time.sleep(200_000_000); // 200ms
    
    if (msg.reply) |reply_subject| {
        const response = std.fmt.allocPrint(std.testing.allocator, "slow: {s}", .{msg.data}) catch return;
        defer std.testing.allocator.free(response);
        
        connection.publish(reply_subject, response) catch return;
    }
}

test "basic request reply" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    const replier_sub = try conn.subscribe("test.echo", echoHandler, .{conn});
    defer replier_sub.deinit();

    // Give the subscription time to register
    std.time.sleep(10_000_000); // 10ms

    // Send a request
    const request_data = "hello world";
    const response = try conn.request("test.echo", request_data, 1000);

    if (response) |msg| {
        defer msg.deinit();
        
        // Verify the echo response
        const expected = "echo: hello world";
        try std.testing.expectEqualStrings(expected, msg.data);
    } else {
        return error.NoResponse;
    }
}

test "simple request reply functionality" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    // Create a replier that echoes back the request
    const replier_sub = try conn.subscribe("test.simple.echo", simpleEchoHandler, .{conn});
    defer replier_sub.deinit();
    
    // Give the subscription time to register
    std.time.sleep(10_000_000); // 10ms
    
    // Send a request
    const request_data = "hello world";
    const response = try conn.request("test.simple.echo", request_data, 1000);
    
    if (response) |msg| {
        defer msg.deinit();
        
        // Verify the echo response
        try std.testing.expectEqualStrings(request_data, msg.data);
    } else {
        return error.NoResponse;
    }
}

test "concurrent request reply" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    // Create echo handler
    const replier_sub = try conn.subscribe("test.concurrent", echoHandler, .{conn});
    defer replier_sub.deinit();

    std.time.sleep(10_000_000); // 10ms

    // Send multiple requests concurrently
    var requests: [5]?*nats.Message = undefined;
    for (&requests, 0..) |*request, i| {
        const data = try std.fmt.allocPrint(std.testing.allocator, "request-{d}", .{i});
        defer std.testing.allocator.free(data);
        
        request.* = try conn.request("test.concurrent", data, 1000);
    }

    // Verify all responses
    for (requests, 0..) |maybe_response, i| {
        if (maybe_response) |response| {
            defer response.deinit();
            
            const expected = try std.fmt.allocPrint(std.testing.allocator, "echo: request-{d}", .{i});
            defer std.testing.allocator.free(expected);
            
            try std.testing.expectEqualStrings(expected, response.data);
        } else {
            return error.NoResponse;
        }
    }
}

test "request with no responders" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    // Send a request to a subject with no responder - should return NoResponders error
    const response = conn.request("test.no.responder", "no responder test", 1000);
    
    // Should get NoResponders error
    try std.testing.expectError(error.NoResponders, response);
}

test "request timeout with slow responder" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    // Set up a slow responder that takes 200ms to respond
    const slow_sub = try conn.subscribe("test.slow", slowEchoHandler, .{conn});
    defer slow_sub.deinit();
    
    std.time.sleep(10_000_000); // 10ms
    
    // Send request with 100ms timeout (less than the 200ms handler delay)
    const response = try conn.request("test.slow", "timeout test", 100);
    
    // Should return null due to timeout
    try std.testing.expectEqual(@as(?*nats.Message, null), response);
}

test "request with different subjects" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);
    
    // Set up multiple echo handlers on different subjects
    const replier1 = try conn.subscribe("test.subject1", echoHandler, .{conn});
    defer replier1.deinit();
    
    const replier2 = try conn.subscribe("test.subject2", echoHandler, .{conn});  
    defer replier2.deinit();
    
    std.time.sleep(10_000_000); // 10ms
    
    // Test requests to different subjects
    const response1 = try conn.request("test.subject1", "message1", 1000);
    const response2 = try conn.request("test.subject2", "message2", 1000);
    
    if (response1) |msg1| {
        defer msg1.deinit();
        try std.testing.expectEqualStrings("echo: message1", msg1.data);
    } else {
        return error.NoResponse;
    }
    
    if (response2) |msg2| {
        defer msg2.deinit();
        try std.testing.expectEqualStrings("echo: message2", msg2.data);
    } else {
        return error.NoResponse;
    }
}