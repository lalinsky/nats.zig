const std = @import("std");
const testing = std.testing;
const nats = @import("nats");
const JetStreamMessage = @import("../src/jetstream_message.zig").JetStreamMessage;
const JetStreamError = @import("../src/jetstream_message.zig").JetStreamError;
const Message = @import("../src/message.zig").Message;

// Mock JetStream context for testing
const MockJetStream = struct {
    nc: *MockConnection,
    
    const MockConnection = struct {
        published_messages: std.ArrayList(struct { subject: []const u8, data: []const u8 }),
        allocator: std.mem.Allocator,
        
        fn init(allocator: std.mem.Allocator) MockConnection {
            return MockConnection{
                .published_messages = std.ArrayList(struct { subject: []const u8, data: []const u8 }).init(allocator),
                .allocator = allocator,
            };
        }
        
        fn deinit(self: *MockConnection) void {
            for (self.published_messages.items) |msg| {
                self.allocator.free(msg.subject);
                self.allocator.free(msg.data);
            }
            self.published_messages.deinit();
        }
        
        fn publish(self: *MockConnection, subject: []const u8, data: []const u8) !void {
            const subject_copy = try self.allocator.dupe(u8, subject);
            const data_copy = try self.allocator.dupe(u8, data);
            try self.published_messages.append(.{ .subject = subject_copy, .data = data_copy });
        }
    };
    
    fn init(allocator: std.mem.Allocator) MockJetStream {
        return MockJetStream{
            .nc = try allocator.create(MockConnection),
        };
    }
    
    fn deinit(self: *MockJetStream, allocator: std.mem.Allocator) void {
        self.nc.deinit();
        allocator.destroy(self.nc);
    }
};

fn createTestMessage(allocator: std.mem.Allocator, reply_subject: ?[]const u8) !*Message {
    var arena = std.heap.ArenaAllocator.init(allocator);
    const msg = try arena.allocator().create(Message);
    msg.* = Message{
        .subject = "test.subject",
        .reply = reply_subject,
        .data = "test data",
        .headers = std.StringHashMap(std.ArrayList([]const u8)).init(arena.allocator()),
        .arena = arena,
    };
    return msg;
}

test "ack should succeed on first call" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First ack should succeed
    try js_msg.ack();
    
    // Verify message was published
    try testing.expect(mock_nc.published_messages.items.len == 1);
    try testing.expectEqualStrings("+ACK", mock_nc.published_messages.items[0].data);
    
    // Verify acknowledgment status
    try testing.expect(js_msg.isAcknowledged());
}

test "ack should fail on second call" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First ack should succeed
    try js_msg.ack();
    
    // Second ack should fail
    try testing.expectError(JetStreamError.MessageAlreadyAcknowledged, js_msg.ack());
    
    // Verify only one message was published
    try testing.expect(mock_nc.published_messages.items.len == 1);
}

test "nak should succeed on first call" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First nak should succeed
    try js_msg.nak();
    
    // Verify message was published
    try testing.expect(mock_nc.published_messages.items.len == 1);
    try testing.expectEqualStrings("-NAK", mock_nc.published_messages.items[0].data);
    
    // Verify acknowledgment status
    try testing.expect(js_msg.isAcknowledged());
}

test "nak should fail on second call" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First nak should succeed
    try js_msg.nak();
    
    // Second nak should fail
    try testing.expectError(JetStreamError.MessageAlreadyAcknowledged, js_msg.nak());
    
    // Verify only one message was published
    try testing.expect(mock_nc.published_messages.items.len == 1);
}

test "ack after nak should fail" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First nak should succeed
    try js_msg.nak();
    
    // ack after nak should fail
    try testing.expectError(JetStreamError.MessageAlreadyAcknowledged, js_msg.ack());
    
    // Verify only one message was published (the nak)
    try testing.expect(mock_nc.published_messages.items.len == 1);
    try testing.expectEqualStrings("-NAK", mock_nc.published_messages.items[0].data);
}

test "nak after ack should fail" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First ack should succeed
    try js_msg.ack();
    
    // nak after ack should fail
    try testing.expectError(JetStreamError.MessageAlreadyAcknowledged, js_msg.nak());
    
    // Verify only one message was published (the ack)
    try testing.expect(mock_nc.published_messages.items.len == 1);
    try testing.expectEqualStrings("+ACK", mock_nc.published_messages.items[0].data);
}

test "term should fail after ack" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // First ack should succeed
    try js_msg.ack();
    
    // term after ack should fail
    try testing.expectError(JetStreamError.MessageAlreadyAcknowledged, js_msg.term());
    
    // Verify only one message was published (the ack)
    try testing.expect(mock_nc.published_messages.items.len == 1);
    try testing.expectEqualStrings("+ACK", mock_nc.published_messages.items[0].data);
}

test "inProgress can be called multiple times" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // Multiple inProgress calls should succeed
    try js_msg.inProgress();
    try js_msg.inProgress();
    try js_msg.inProgress();
    
    // Verify multiple messages were published
    try testing.expect(mock_nc.published_messages.items.len == 3);
    for (mock_nc.published_messages.items) |published_msg| {
        try testing.expectEqualStrings("+WPI", published_msg.data);
    }
    
    // Message should not be marked as acknowledged after inProgress
    try testing.expect(!js_msg.isAcknowledged());
    
    // ack should still work after inProgress calls
    try js_msg.ack();
    try testing.expect(js_msg.isAcknowledged());
}

test "inProgress works after ack" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    // Create mock JetStream
    var mock_nc = MockJetStream.MockConnection.init(allocator);
    defer mock_nc.deinit();
    var mock_js = MockJetStream{ .nc = &mock_nc };
    
    // Create test message with reply subject
    const msg = try createTestMessage(allocator, "$JS.ACK.test_stream.test_consumer.1.1.1.123456789.0");
    defer msg.deinit();
    
    const js_msg = try @import("../src/jetstream_message.zig").createJetStreamMessage(&mock_js, msg);
    
    // ack first
    try js_msg.ack();
    try testing.expect(js_msg.isAcknowledged());
    
    // inProgress should still work
    try js_msg.inProgress();
    
    // Verify both messages were published
    try testing.expect(mock_nc.published_messages.items.len == 2);
    try testing.expectEqualStrings("+ACK", mock_nc.published_messages.items[0].data);
    try testing.expectEqualStrings("+WPI", mock_nc.published_messages.items[1].data);
}