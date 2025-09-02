const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const log = std.log.default;

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
    var msg = try conn.request("test.echo", "hello world", 1000);
    defer msg.deinit();

    // Verify the echo response
    try std.testing.expectEqualStrings("echo: hello world", msg.data);
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
    var msg = try conn.request("test.simple.echo", "hello world", 1000);
    defer msg.deinit();

    try std.testing.expectEqualStrings("hello world", msg.data);
}

test "concurrent request reply" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create echo handler
    const replier_sub = try conn.subscribe("test.concurrent", echoHandler, .{conn});
    defer replier_sub.deinit();

    std.time.sleep(10_000_000); // 10ms

    // Send multiple requests concurrently
    var requests: [5]*nats.Message = undefined;
    for (&requests, 0..) |*request, i| {
        const data = try std.fmt.allocPrint(std.testing.allocator, "request-{d}", .{i});
        defer std.testing.allocator.free(data);

        request.* = try conn.request("test.concurrent", data, 1000);
    }

    // Verify all responses
    for (requests, 0..) |response, i| {
        defer response.deinit();

        const expected = try std.fmt.allocPrint(std.testing.allocator, "echo: request-{d}", .{i});
        defer std.testing.allocator.free(expected);

        try std.testing.expectEqualStrings(expected, response.data);
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
    const response = conn.request("test.slow", "timeout test", 100);

    // Should return timeout error
    try std.testing.expectError(error.Timeout, response);
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
    defer response1.deinit();
    try std.testing.expectEqualStrings("echo: message1", response1.data);

    const response2 = try conn.request("test.subject2", "message2", 1000);
    defer response2.deinit();
    try std.testing.expectEqualStrings("echo: message2", response2.data);
}

test "requestMsg basic functionality" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create echo handler
    const replier_sub = try conn.subscribe("test.requestmsg", simpleEchoHandler, .{conn});
    defer replier_sub.deinit();

    std.time.sleep(10_000_000); // 10ms

    var request_msg = try conn.newMsg();
    defer request_msg.deinit();

    try request_msg.setSubject("test.requestmsg");
    try request_msg.setPayload("requestMsg test data");

    // Send request using requestMsg
    const response = try conn.requestMsg(request_msg, 1000);
    defer response.deinit();

    // Verify response
    try std.testing.expectEqualStrings("requestMsg test data", response.data);
}

test "requestMsg with headers" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Create handler that echoes back with headers
    const replier_sub = try conn.subscribe("test.requestmsg.headers", echoHandler, .{conn});
    defer replier_sub.deinit();

    std.time.sleep(10_000_000); // 10ms

    var request_msg = try conn.newMsg();
    defer request_msg.deinit();

    try request_msg.setSubject("test.requestmsg.headers");
    try request_msg.setPayload("header test");

    // Add some headers
    try request_msg.headerSet("X-Test-Header", "test-value");
    try request_msg.headerSet("X-Request-ID", "12345");

    // Send request using requestMsg
    const response = try conn.requestMsg(request_msg, 1000);
    defer response.deinit();

    // Verify response
    try std.testing.expectEqualStrings("echo: header test", response.data);
}

test "requestMsg validation errors" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    var request_msg = try conn.newMsg();
    defer request_msg.deinit();

    // Test invalid subject (empty)
    try request_msg.setSubject("");
    try request_msg.setPayload("test data");

    const result = conn.requestMsg(request_msg, 1000);
    try std.testing.expectError(error.InvalidSubject, result);
}

// Handler that responds multiple times to a single request
fn multiResponder(msg: *nats.Message, connection: *nats.Connection) void {
    defer msg.deinit();
    if (msg.reply) |reply_subject| {
        // Send 3 responses
        for (0..3) |i| {
            const response = std.fmt.allocPrint(std.testing.allocator, "response-{d}", .{i}) catch return;
            defer std.testing.allocator.free(response);
            connection.publish(reply_subject, response) catch return;
        }
    }
}

test "requestMany with max_messages" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Set up handler that sends multiple responses
    const replier_sub = try conn.subscribe("test.many", multiResponder, .{conn});
    defer replier_sub.deinit();

    // Request with max 2 messages
    var messages = try conn.requestMany("test.many", "get many", 1000, .{ .max_messages = 2 });
    defer {
        // Clean up messages
        while (messages.pop()) |msg| {
            msg.deinit();
        }
    }

    // Should get exactly 2 messages
    try std.testing.expectEqual(2, messages.len);
}

test "requestMany with timeout collecting all" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Set up handler that sends multiple responses
    const replier_sub = try conn.subscribe("test.many.all", multiResponder, .{conn});
    defer replier_sub.deinit();

    // Request with no max, should collect all 3 and timeout
    var messages = try conn.requestMany("test.many.all", "get all", 100, .{});

    // Should get all 3 messages
    try std.testing.expectEqual(3, messages.len);

    // Verify message contents
    var i: usize = 0;
    while (messages.pop()) |msg| {
        defer msg.deinit();
        const expected = try std.fmt.allocPrint(std.testing.allocator, "response-{d}", .{i});
        defer std.testing.allocator.free(expected);
        try std.testing.expectEqualStrings(expected, msg.data);
        i += 1;
    }
}

// Handler that sends responses with a sentinel message
fn sentinelResponder(msg: *nats.Message, connection: *nats.Connection) void {
    defer msg.deinit();
    if (msg.reply) |reply_subject| {
        // Send several responses ending with "END"
        const responses = [_][]const u8{ "data-1", "data-2", "data-3", "END" };
        for (responses) |response| {
            connection.publish(reply_subject, response) catch return;
        }
    }
}

test "requestMany with sentinel function" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Set up handler that sends responses with sentinel
    const replier_sub = try conn.subscribe("test.sentinel", sentinelResponder, .{conn});
    defer replier_sub.deinit();

    std.time.sleep(10_000_000); // 10ms

    // Sentinel function that stops when it sees "END" message (ADR-47: false = stop)
    const sentinel = struct {
        fn check(msg: *nats.Message) bool {
            return !std.mem.eql(u8, msg.data, "END"); // true = continue, false = stop
        }
    }.check;

    // Request with sentinel function
    var messages = try conn.requestMany("test.sentinel", "get until end", 1000, .{ .sentinelFn = sentinel });

    // Should get all 4 messages including the sentinel
    try std.testing.expectEqual(@as(usize, 4), messages.len);

    // Verify last message is "END"
    try std.testing.expect(messages.tail != null);
    try std.testing.expectEqualStrings("END", messages.tail.?.data);

    // Clean up messages
    while (messages.pop()) |msg| {
        msg.deinit();
    }
}

test "requestMany with no responders" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    const result = conn.requestMany("test.no.responder.many", "no one", 100, .{});
    try std.testing.expectError(error.NoResponders, result);
}

test "requestMany with stall timeout" {
    const conn = try utils.createDefaultConnection();
    defer utils.closeConnection(conn);

    // Set up sync subscription to handle the request manually
    const replier_sub = try conn.subscribeSync("test.stall");
    defer replier_sub.deinit();

    // Thread that will send delayed responses
    const ResponderThread = struct {
        fn run(connection: *nats.Connection, sub: *nats.Subscription) void {
            // Wait for the request message
            const request_msg = sub.nextMsg(1000) catch return;
            defer request_msg.deinit();
            
            const reply_subject = request_msg.reply orelse return;
            
            // Send first response immediately
            connection.publish(reply_subject, "response-1") catch return;
            
            // Send second response after 10ms (within stall timeout)  
            std.time.sleep(10_000_000); // 10ms
            connection.publish(reply_subject, "response-2") catch return;
            
            // Wait 150ms (longer than 100ms stall timeout) then try to send third response
            std.time.sleep(150_000_000); // 150ms  
            connection.publish(reply_subject, "response-3") catch return;
        }
    };

    // Start responder thread
    const responder_thread = try std.Thread.spawn(.{}, ResponderThread.run, .{ conn, replier_sub });
    defer responder_thread.join();

    std.time.sleep(10_000_000); // 10ms to ensure subscription is ready

    // Request with 100ms stall timeout - should get only first 2 responses
    var messages = try conn.requestMany("test.stall", "get with stall", 1000, .{ .stall_ms = 100 });
    defer {
        while (messages.pop()) |response| {
            response.deinit();
        }
    }

    // Should get exactly 2 messages before stall timeout
    try std.testing.expectEqual(2, messages.len);
}
