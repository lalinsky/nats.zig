const std = @import("std");
const Message = @import("message.zig").Message;

// Token positions for parsing ACK reply subjects (matching Go NATS library)
const ACK_DOMAIN_TOKEN_POS = 2;
const ACK_ACC_HASH_TOKEN_POS = 3;
const ACK_STREAM_TOKEN_POS = 4;
const ACK_CONSUMER_TOKEN_POS = 5;
const ACK_NUM_DELIVERED_TOKEN_POS = 6;
const ACK_STREAM_SEQ_TOKEN_POS = 7;
const ACK_CONSUMER_SEQ_TOKEN_POS = 8;
const ACK_TIMESTAMP_SEQ_TOKEN_POS = 9;
const ACK_NUM_PENDING_TOKEN_POS = 10;

/// ACK response types for JetStream messages
const AckType = enum {
    ack,      // +ACK - successful processing
    nak,      // -NAK - negative ack, request redelivery
    term,     // +TERM - terminate delivery, don't redeliver
    progress, // +WPI - work in progress, extend ack wait
};

/// Consumer and stream sequence pair (matches Go NATS library SequencePair)
pub const SequencePair = struct {
    consumer: ?u64 = null,  // Consumer sequence number
    stream: ?u64 = null,    // Stream sequence number
};

/// JetStream message metadata (matches Go NATS library MsgMetadata)
pub const MsgMetadata = struct {
    sequence: SequencePair = .{},         // Consumer and stream sequence numbers
    num_delivered: u64 = 1,               // Number of times this message has been delivered
    num_pending: ?u64 = null,             // Number of pending messages for this consumer
    timestamp: ?u64 = null,               // Message timestamp (nanoseconds since epoch) from reply subject
    stream: ?[]const u8 = null,           // Stream name
    consumer: ?[]const u8 = null,         // Consumer name
    domain: ?[]const u8 = null,           // JetStream domain (for clustered JetStream)
};

pub const JetStreamMessage = struct {
    /// Underlying NATS message
    msg: *Message,
    /// JetStream context for acknowledgments
    js: *anyopaque, // Forward declaration - will be cast to *JetStream
    
    /// JetStream metadata (parsed from headers and reply subject)
    metadata: MsgMetadata = .{},
    /// Original subject (from Nats-Subject header) 
    subject: ?[]const u8 = null,
    /// Reply subject for ACK/NAK
    reply: ?[]const u8 = null,
    
    pub fn deinit(self: *JetStreamMessage) void {
        self.msg.deinit();
    }
    
    /// Acknowledge successful processing
    pub fn ack(self: *JetStreamMessage) !void {
        try self.sendAck(.ack);
    }
    
    /// Negative acknowledge - request redelivery
    pub fn nak(self: *JetStreamMessage) !void {
        try self.sendAck(.nak);
    }
    
    /// Terminate delivery - don't redeliver this message
    pub fn term(self: *JetStreamMessage) !void {
        try self.sendAck(.term);
    }
    
    /// Indicate work in progress - extend ack wait timer
    pub fn inProgress(self: *JetStreamMessage) !void {
        try self.sendAck(.progress);
    }
    
    /// Send acknowledgment to JetStream
    fn sendAck(self: *JetStreamMessage, ack_type: AckType) !void {
        if (self.reply) |reply_subject| {
            const ack_payload = switch (ack_type) {
                .ack => "+ACK",
                .nak => "-NAK", 
                .term => "+TERM",
                .progress => "+WPI",
            };
            
            // Import JetStream to access connection
            const JetStream = @import("jetstream.zig").JetStream;
            const js: *JetStream = @ptrCast(@alignCast(self.js));
            try js.nc.publish(reply_subject, ack_payload);
        }
    }
};

/// Parse v1 ACK subject format: $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
fn parseAckV1(subject: []const u8, metadata: *MsgMetadata) void {
    var iter = std.mem.splitScalar(u8, subject, '.');
    var index: u8 = 0;
    
    while (iter.next()) |token| {
        switch (index) {
            0 => if (!std.mem.eql(u8, token, "$JS")) return,
            1 => if (!std.mem.eql(u8, token, "ACK")) return,
            2 => metadata.stream = token,
            3 => metadata.consumer = token,
            4 => metadata.num_delivered = std.fmt.parseInt(u64, token, 10) catch 1,
            5 => metadata.sequence.stream = std.fmt.parseInt(u64, token, 10) catch null,
            6 => metadata.sequence.consumer = std.fmt.parseInt(u64, token, 10) catch null,
            7 => metadata.timestamp = std.fmt.parseInt(u64, token, 10) catch null,
            8 => metadata.num_pending = std.fmt.parseInt(u64, token, 10) catch null,
            else => break,
        }
        index += 1;
    }
}

/// Parse v2 ACK subject format: $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<token>
fn parseAckV2(subject: []const u8, metadata: *MsgMetadata) void {
    var iter = std.mem.splitScalar(u8, subject, '.');
    var index: u8 = 0;
    
    while (iter.next()) |token| {
        switch (index) {
            0 => if (!std.mem.eql(u8, token, "$JS")) return,
            1 => if (!std.mem.eql(u8, token, "ACK")) return,
            2 => {
                if (token.len > 0 and !std.mem.eql(u8, token, "_")) {
                    metadata.domain = token;
                }
            },
            3 => {}, // Skip account hash
            4 => metadata.stream = token,
            5 => metadata.consumer = token,
            6 => metadata.num_delivered = std.fmt.parseInt(u64, token, 10) catch 1,
            7 => metadata.sequence.stream = std.fmt.parseInt(u64, token, 10) catch null,
            8 => metadata.sequence.consumer = std.fmt.parseInt(u64, token, 10) catch null,
            9 => metadata.timestamp = std.fmt.parseInt(u64, token, 10) catch null,
            10 => metadata.num_pending = std.fmt.parseInt(u64, token, 10) catch null,
            else => break,
        }
        index += 1;
    }
}

/// Parse ACK subject and populate metadata
pub fn parseAckSubject(subject: []const u8, metadata: *MsgMetadata) void {
    const token_count = std.mem.count(u8, subject, ".") + 1;
    
    // v1 has 9 tokens, v2 has 11+ tokens
    if (token_count == 9) {
        parseAckV1(subject, metadata);
    } else if (token_count >= 11) {
        parseAckV2(subject, metadata);
    }
    // Invalid token counts are silently ignored
}

/// Parse JetStream headers from a message and create JetStreamMessage wrapper
pub fn createJetStreamMessage(js: *anyopaque, allocator: std.mem.Allocator, msg: *Message) !*JetStreamMessage {
    const js_msg = try allocator.create(JetStreamMessage);
    js_msg.* = JetStreamMessage{
        .msg = msg,
        .js = js,
        .reply = msg.reply,
    };
    
    // Parse JetStream headers if present
    if (msg.headers.get("Nats-Stream")) |stream_values| {
        if (stream_values.items.len > 0) js_msg.metadata.stream = stream_values.items[0];
    }
    if (msg.headers.get("Nats-Subject")) |subject_values| {
        if (subject_values.items.len > 0) js_msg.subject = subject_values.items[0];
    }
    // Note: We don't parse timestamp from headers - Go library only uses reply subject timestamp
    
    // Parse sequence number from headers
    if (msg.headers.get("Nats-Sequence")) |seq_values| {
        if (seq_values.items.len > 0) {
            // This is typically the stream sequence, but could be consumer sequence in some contexts
            js_msg.metadata.sequence.stream = std.fmt.parseInt(u64, seq_values.items[0], 10) catch null;
        }
    }
    
    // Parse JetStream metadata from reply subject
    if (msg.reply) |reply_subject| {
        parseAckSubject(reply_subject, &js_msg.metadata);
    }
    
    return js_msg;
}