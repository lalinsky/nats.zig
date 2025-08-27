const std = @import("std");
const Message = @import("message.zig").Message;
const Connection = @import("connection.zig").Connection;

pub const AckError = error{
    AlreadyAcked,
};

/// ACK response types for JetStream messages
const AckType = enum(u8) {
    ack, // +ACK - successful processing
    nak, // -NAK - negative ack, request redelivery
    term, // +TERM - terminate delivery, don't redeliver
    progress, // +WPI - work in progress, extend ack wait

    pub fn isFinal(self: AckType) bool {
        return self != .progress;
    }

    pub fn toString(self: AckType) []const u8 {
        return switch (self) {
            .ack => "+ACK",
            .nak => "-NAK",
            .term => "+TERM",
            .progress => "+WPI",
        };
    }
};

/// Consumer and stream sequence pair (matches Go NATS library SequencePair)
pub const SequencePair = struct {
    consumer: ?u64 = null, // Consumer sequence number
    stream: ?u64 = null, // Stream sequence number
};

/// JetStream message metadata (matches Go NATS library MsgMetadata)
pub const MsgMetadata = struct {
    sequence: SequencePair = .{}, // Consumer and stream sequence numbers
    num_delivered: u64 = 1, // Number of times this message has been delivered
    num_pending: ?u64 = null, // Number of pending messages for this consumer
    timestamp: ?u64 = null, // Message timestamp (nanoseconds since epoch) from reply subject
    stream: ?[]const u8 = null, // Stream name
    consumer: ?[]const u8 = null, // Consumer name
    domain: ?[]const u8 = null, // JetStream domain (for clustered JetStream)
};

pub const JetStreamMessage = struct {
    /// Underlying NATS message
    msg: *Message,
    /// Connection for sending acknowledgments
    nc: *Connection,
    /// JetStream metadata (parsed from the reply subject)
    metadata: MsgMetadata = .{},
    /// Atomic flag to track acknowledgment status (prevents duplicate ack/nak)
    acked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

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
    /// Note: inProgress can be called multiple times per NATS specification
    pub fn inProgress(self: *JetStreamMessage) !void {
        try self.sendAck(.progress);
    }

    /// Check if message has been acknowledged
    pub fn isAcked(self: *JetStreamMessage) bool {
        return self.acked.load(.acquire);
    }

    /// Send acknowledgment to JetStream
    fn sendAck(self: *JetStreamMessage, ack_type: AckType) !void {
        if (self.msg.reply) |reply_subject| {
            if (ack_type.isFinal()) {
                // Check if already acknowledged using atomic compare-and-swap
                const was_acked = self.acked.cmpxchgStrong(false, true, .acq_rel, .acquire);
                if (was_acked != null) {
                    return AckError.AlreadyAcked;
                }
            }

            // If publish fails, revert the acknowledged flag to allow retry
            errdefer if (ack_type.isFinal()) self.acked.store(false, .release);

            // Send the acknowledgment message
            try self.nc.publish(reply_subject, ack_type.toString());
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
pub fn parseAckSubject(subject: []const u8, metadata: *MsgMetadata) !void {
    const token_count = std.mem.count(u8, subject, ".") + 1;

    if (token_count == 9) {
        parseAckV1(subject, metadata);
    } else if (token_count == 11) {
        parseAckV2(subject, metadata);
    } else {
        return error.InvalidAckSubject;
    }
}

/// Parse JetStream headers from a message and create JetStreamMessage wrapper
pub fn createJetStreamMessage(nc: *Connection, msg: *Message) !*JetStreamMessage {
    // Allocate the JetStreamMessage on the message's arena so it gets cleaned up automatically
    const js_msg = try msg.arena.allocator().create(JetStreamMessage);
    js_msg.* = JetStreamMessage{
        .msg = msg,
        .nc = nc,
    };

    // Parse JetStream metadata from reply subject
    if (msg.reply) |reply_subject| {
        try parseAckSubject(reply_subject, &js_msg.metadata);
    }

    return js_msg;
}
