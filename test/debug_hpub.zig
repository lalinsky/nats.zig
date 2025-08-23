const std = @import("std");
const nats = @import("nats");

test "debug HPUB byte counting" {
    const raw_headers = "NATS/1.0\r\nX-Test-Key: test-value\r\nX-Another-Key: another-value\r\n\r\n";
    const payload = "Hello with headers!";
    
    std.debug.print("Raw headers: '{s}'\n", .{raw_headers});
    std.debug.print("Raw headers length: {d}\n", .{raw_headers.len});
    std.debug.print("Payload: '{s}'\n", .{payload});
    std.debug.print("Payload length: {d}\n", .{payload.len});
    std.debug.print("Total length: {d}\n", .{raw_headers.len + payload.len});
    
    // Expected HPUB line: HPUB test.headers <header_len> <total_len>
    const expected_header_len = raw_headers.len;
    const expected_total_len = raw_headers.len + payload.len;
    
    std.debug.print("Expected HPUB line: HPUB test.headers {d} {d}\n", .{expected_header_len, expected_total_len});
    
    // Let's check if this matches what our code generated: "HPUB test.headers 66 85"
    try std.testing.expectEqual(@as(usize, 66), expected_header_len);
    try std.testing.expectEqual(@as(usize, 85), expected_total_len);
}