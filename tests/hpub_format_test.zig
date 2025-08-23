const std = @import("std");
const nats = @import("nats");

test "HPUB message format" {
    // Create a message with headers
    const msg = try nats.Message.init(
        std.testing.allocator,
        "test.subject",
        null,
        "Hello World"
    );
    defer msg.deinit();

    // Set headers
    try msg.headerSet("X-Test", "test-value");
    try msg.headerSet("Content-Type", "text/plain");

    // Encode headers to see what they look like
    var headers_buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer headers_buffer.deinit();
    try msg.encodeHeaders(headers_buffer.writer());

    std.debug.print("Headers buffer: {s}\n", .{headers_buffer.items});
    std.debug.print("Headers buffer bytes: {any}\n", .{headers_buffer.items});

    // Simulate what publishMsg does
    const headers_len = headers_buffer.items.len;
    const total_len = headers_len + msg.data.len;

    var pub_buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer pub_buffer.deinit();

    try pub_buffer.writer().print("HPUB {s} {d} {d}\r\n", .{ msg.subject, headers_len, total_len });
    try pub_buffer.appendSlice(headers_buffer.items);
    try pub_buffer.appendSlice(msg.data);
    try pub_buffer.appendSlice("\r\n");

    std.debug.print("Full HPUB message: {s}\n", .{pub_buffer.items});
    std.debug.print("Full HPUB message bytes: {any}\n", .{pub_buffer.items});

    // Verify format matches protocol specification
    // Expected format: HPUB test.subject <headers_len> <total_len>\r\nNATS/1.0\r\n<headers>\r\n\r\n<payload>\r\n
    
    const expected_start = "HPUB test.subject ";
    try std.testing.expect(std.mem.startsWith(u8, pub_buffer.items, expected_start));
    
    // Should contain NATS/1.0
    try std.testing.expect(std.mem.indexOf(u8, pub_buffer.items, "NATS/1.0\r\n") != null);
    
    // Should end with Hello World\r\n
    try std.testing.expect(std.mem.endsWith(u8, pub_buffer.items, "Hello World\r\n"));
}