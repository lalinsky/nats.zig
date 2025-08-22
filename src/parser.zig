const std = @import("std");

/// Maximum size for control line operations (MSG arguments, INFO, ERR, etc.)
pub const MAX_CONTROL_LINE_SIZE = 4096;

pub const ParserState = enum {
    OP_START,
    OP_PLUS,
    OP_PLUS_O,
    OP_PLUS_OK,
    OP_MINUS,
    OP_MINUS_E,
    OP_MINUS_ER,
    OP_MINUS_ERR,
    OP_MINUS_ERR_SPC,
    MINUS_ERR_ARG,
    OP_M,
    OP_MS,
    OP_MSG,
    OP_MSG_SPC,
    MSG_ARG,
    MSG_PAYLOAD,
    MSG_END,
    OP_H,
    OP_P,
    OP_PI,
    OP_PIN,
    OP_PING,
    OP_PO,
    OP_PON,
    OP_PONG,
    OP_I,
    OP_IN,
    OP_INF,
    OP_INFO,
    OP_INFO_SPC,
    INFO_ARG,
};

pub const MsgArg = struct {
    subject: []const u8 = "",
    sid: u64 = 0,
    reply: ?[]const u8 = null,
    hdr: i32 = -1,
    size: i32 = 0,
};

// Forward declaration for connection
pub const Connection = @import("connection.zig").Connection;

pub const Parser = struct {
    state: ParserState = .OP_START,
    after_space: usize = 0,
    drop: usize = 0,
    hdr: i32 = -1,
    ma: MsgArg = .{},
    scratch: [MAX_CONTROL_LINE_SIZE]u8 = undefined,
    arg_buf: std.ArrayList(u8),
    msg_buf: std.ArrayList(u8),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .arg_buf = std.ArrayList(u8).init(allocator),
            .msg_buf = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arg_buf.deinit();
        self.msg_buf.deinit();
    }
    
    pub fn reset(self: *Self) void {
        self.state = .OP_START;
        self.after_space = 0;
        self.drop = 0;
        self.hdr = -1;
        self.ma = .{};
        self.arg_buf.clearRetainingCapacity();
        self.msg_buf.clearRetainingCapacity();
    }

    pub fn parse(self: *Self, conn: anytype, buf: []const u8) !void {
        var i: usize = 0;

        while (i < buf.len) {
            const b = buf[i];

            switch (self.state) {
                .OP_START => {
                    switch (b) {
                        'M', 'm' => {
                            self.state = .OP_M;
                            self.hdr = -1;
                            self.ma.hdr = -1;
                        },
                        'H', 'h' => {
                            self.state = .OP_H;
                            self.hdr = 0;
                            self.ma.hdr = 0;
                        },
                        'P', 'p' => {
                            self.state = .OP_P;
                        },
                        '+' => {
                            self.state = .OP_PLUS;
                        },
                        '-' => {
                            self.state = .OP_MINUS;
                        },
                        'I', 'i' => {
                            self.state = .OP_I;
                        },
                        else => {
                            return error.InvalidProtocol;
                        },
                    }
                },

                .OP_H => switch (b) {
                    'M', 'm' => self.state = .OP_M,
                    else => return error.InvalidProtocol,
                },

                .OP_M => switch (b) {
                    'S', 's' => self.state = .OP_MS,
                    else => return error.InvalidProtocol,
                },

                .OP_MS => switch (b) {
                    'G', 'g' => self.state = .OP_MSG,
                    else => return error.InvalidProtocol,
                },

                .OP_MSG => switch (b) {
                    ' ', '\t' => self.state = .OP_MSG_SPC,
                    else => return error.InvalidProtocol,
                },

                .OP_MSG_SPC => {
                    switch (b) {
                        ' ', '\t' => {}, // Skip multiple spaces
                        else => {
                            self.state = .MSG_ARG;
                            self.after_space = i;
                        },
                    }
                },

                .MSG_ARG => {
                    switch (b) {
                        '\r' => self.drop = 1,
                        '\n' => {
                            // Process message arguments
                            const arg_start = if (self.arg_buf.items.len > 0)
                                self.arg_buf.items
                            else
                                buf[self.after_space..i - self.drop];

                            try self.processMsgArgs(arg_start);

                            self.drop = 0;
                            self.after_space = i + 1;
                            self.state = .MSG_PAYLOAD;

                            // Jump ahead to message end if possible - check bounds
                            // ma.size already contains total size (headers + payload)
                            const msg_end = self.after_space + @as(usize, @intCast(self.ma.size));
                            if (msg_end <= buf.len) {
                                i = msg_end - 1;
                            }
                        },
                        else => {
                            // Accumulate argument bytes
                            try self.arg_buf.append(b);
                        },
                    }
                },

                .MSG_PAYLOAD => {
                    var done = false;

                    // ma.size now contains total size (matching C parser)
                    const total_msg_size = @as(usize, @intCast(self.ma.size));

                    if (self.msg_buf.items.len >= total_msg_size) {
                        // Process accumulated message
                        const payload_start = if (self.hdr >= 0) @as(usize, @intCast(self.hdr)) else 0;
                        const payload_len = if (self.hdr >= 0) total_msg_size - @as(usize, @intCast(self.hdr)) else total_msg_size;
                        const payload = self.msg_buf.items[payload_start..payload_start + payload_len];
                        try conn.processMsg(payload);
                        done = true;
                    } else if (self.msg_buf.items.len == 0 and i - self.after_space >= total_msg_size) {
                        // Process message directly from buffer
                        const msg_start = self.after_space;
                        const payload_start = if (self.hdr >= 0) msg_start + @as(usize, @intCast(self.hdr)) else msg_start;
                        const payload_len = if (self.hdr >= 0) total_msg_size - @as(usize, @intCast(self.hdr)) else total_msg_size;
                        const payload = buf[payload_start..payload_start + payload_len];
                        try conn.processMsg(payload);
                        done = true;
                    } else {
                        // Accumulate message bytes
                        const needed = total_msg_size - self.msg_buf.items.len;
                        const available = buf.len - i;
                        const to_copy = @min(needed, available);

                        if (to_copy > 0) {
                            try self.msg_buf.appendSlice(buf[i..i + to_copy]);
                            i += to_copy - 1;
                        }
                    }

                    if (done) {
                        self.arg_buf.clearRetainingCapacity();
                        self.msg_buf.clearRetainingCapacity();
                        self.state = .MSG_END;
                    }
                },

                .MSG_END => {
                    switch (b) {
                        '\n' => {
                            self.drop = 0;
                            self.after_space = i + 1;
                            self.state = .OP_START;
                        },
                        else => {
                            // Skip characters until newline, like C parser
                        },
                    }
                },

                // PING/PONG parsing
                .OP_P => {
                    switch (b) {
                        'I', 'i' => self.state = .OP_PI,
                        'O', 'o' => self.state = .OP_PO,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PI => {
                    switch (b) {
                        'N', 'n' => self.state = .OP_PIN,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PIN => {
                    switch (b) {
                        'G', 'g' => self.state = .OP_PING,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PING => {
                    switch (b) {
                        '\n' => {
                            try conn.processPing();
                            self.state = .OP_START;
                        },
                        else => {
                            // Skip characters until newline, like C parser
                        },
                    }
                },

                .OP_PO => {
                    switch (b) {
                        'N', 'n' => self.state = .OP_PON,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PON => {
                    switch (b) {
                        'G', 'g' => self.state = .OP_PONG,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PONG => {
                    switch (b) {
                        '\n' => {
                            try conn.processPong();
                            self.state = .OP_START;
                        },
                        else => {
                            // Skip characters until newline, like C parser
                        },
                    }
                },

                // +OK parsing
                .OP_PLUS => {
                    switch (b) {
                        'O', 'o' => self.state = .OP_PLUS_O,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PLUS_O => {
                    switch (b) {
                        'K', 'k' => self.state = .OP_PLUS_OK,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_PLUS_OK => {
                    switch (b) {
                        '\n' => {
                            try conn.processOK();
                            self.drop = 0;
                            self.state = .OP_START;
                        },
                        else => {
                            // Skip characters until newline, like C parser
                        },
                    }
                },

                // -ERR parsing
                .OP_MINUS => {
                    switch (b) {
                        'E', 'e' => self.state = .OP_MINUS_E,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_MINUS_E => {
                    switch (b) {
                        'R', 'r' => self.state = .OP_MINUS_ER,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_MINUS_ER => {
                    switch (b) {
                        'R', 'r' => self.state = .OP_MINUS_ERR,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_MINUS_ERR => {
                    switch (b) {
                        ' ', '\t' => {
                            self.state = .OP_MINUS_ERR_SPC;
                            self.arg_buf.clearRetainingCapacity();
                        },
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_MINUS_ERR_SPC => {
                    switch (b) {
                        ' ', '\t' => {},
                        else => {
                            self.state = .MINUS_ERR_ARG;
                            try self.arg_buf.append(b);
                        },
                    }
                },

                .MINUS_ERR_ARG => {
                    switch (b) {
                        '\n' => {
                            const err_msg = if (self.arg_buf.items.len > 0) self.arg_buf.items else "";
                            try conn.processErr(err_msg);
                            self.state = .OP_START;
                        },
                        '\r' => {}, // Skip CR
                        else => {
                            try self.arg_buf.append(b);
                        },
                    }
                },

                // INFO parsing
                .OP_I => {
                    switch (b) {
                        'N', 'n' => self.state = .OP_IN,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_IN => {
                    switch (b) {
                        'F', 'f' => self.state = .OP_INF,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_INF => {
                    switch (b) {
                        'O', 'o' => self.state = .OP_INFO,
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_INFO => {
                    switch (b) {
                        ' ', '\t' => {
                            self.state = .OP_INFO_SPC;
                            self.arg_buf.clearRetainingCapacity();
                        },
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_INFO_SPC => {
                    switch (b) {
                        ' ', '\t' => {},
                        else => {
                            self.state = .INFO_ARG;
                            try self.arg_buf.append(b);
                        },
                    }
                },

                .INFO_ARG => {
                    switch (b) {
                        '\n' => {
                            const info_json = if (self.arg_buf.items.len > 0) self.arg_buf.items else "";
                            try conn.processInfo(info_json);
                            self.state = .OP_START;
                        },
                        '\r' => {}, // Skip CR
                        else => {
                            try self.arg_buf.append(b);
                        },
                    }
                },
            }

            i += 1;
        }
    }

    fn processMsgArgs(self: *Self, args: []const u8) !void {
        var parts = std.mem.tokenizeScalar(u8, args, ' ');

        const subject = parts.next() orelse return error.InvalidProtocol;
        const sid_str = parts.next() orelse return error.InvalidProtocol;

        var reply: ?[]const u8 = null;
        var size_str: []const u8 = undefined;
        var hdr_str: ?[]const u8 = null;

        if (self.hdr >= 0) {
            // HMSG: subject sid [reply] hdr_len total_len
            const third_part = parts.next() orelse return error.InvalidProtocol;
            const fourth_part = parts.next() orelse return error.InvalidProtocol;
            const fifth_part = parts.next();

            if (fifth_part) |fifth| {
                // 5 parts: subject sid reply hdr_len total_len
                reply = third_part;
                hdr_str = fourth_part;
                size_str = fifth;
            } else {
                // 4 parts: subject sid hdr_len total_len
                hdr_str = third_part;
                size_str = fourth_part;
            }
        } else {
            // MSG: subject sid [reply] size
            if (parts.next()) |next_part| {
                if (parts.next()) |size_part| {
                    // 4 parts: subject sid reply size
                    reply = next_part;
                    size_str = size_part;
                } else {
                    // 3 parts: subject sid size
                    size_str = next_part;
                }
            } else {
                return error.InvalidProtocol;
            }
        }

        const sid = try std.fmt.parseInt(u64, sid_str, 10);
        const size = try std.fmt.parseInt(i32, size_str, 10);
        var hdr_len: i32 = -1;

        if (hdr_str) |hdr| {
            hdr_len = try std.fmt.parseInt(i32, hdr, 10);
        }

        self.ma = MsgArg{
            .subject = subject,
            .sid = sid,
            .reply = reply,
            .hdr = hdr_len,
            .size = size,  // Store total size like C parser
        };

        // Update parser hdr field to match ma.hdr for consistency
        self.hdr = hdr_len;
    }
};

const MockConnection = struct {
        ping_count: u32 = 0,
        pong_count: u32 = 0,
        ok_count: u32 = 0,
        err_count: u32 = 0,
        info_count: u32 = 0,
        msg_count: u32 = 0,
        last_msg: []const u8 = "",
        last_err: []const u8 = "",
        last_info: []const u8 = "",

        const Self = @This();

        pub fn processPing(self: *Self) !void {
            self.ping_count += 1;
        }

        pub fn processPong(self: *Self) !void {
            self.pong_count += 1;
        }

        pub fn processMsg(self: *Self, payload: []const u8) !void {
            self.msg_count += 1;
            self.last_msg = try std.testing.allocator.dupe(u8, payload);
        }

        pub fn processOK(self: *Self) !void {
            self.ok_count += 1;
        }

        pub fn processErr(self: *Self, err_msg: []const u8) !void {
            self.err_count += 1;
            self.last_err = try std.testing.allocator.dupe(u8, err_msg);
        }

        pub fn processInfo(self: *Self, info_json: []const u8) !void {
            self.info_count += 1;
            self.last_info = try std.testing.allocator.dupe(u8, info_json);
        }

        pub fn deinit(self: *Self) void {
            if (self.last_msg.len > 0) std.testing.allocator.free(self.last_msg);
            if (self.last_err.len > 0) std.testing.allocator.free(self.last_err);
            if (self.last_info.len > 0) std.testing.allocator.free(self.last_info);
        }
};

test "parser ping pong" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "PING\r\n");
    try testing.expectEqual(1, mock_conn.ping_count);
    try testing.expectEqual(0, mock_conn.pong_count);

    try parser.parse(&mock_conn, "PONG\r\n");
    try testing.expectEqual(1, mock_conn.ping_count);
    try testing.expectEqual(1, mock_conn.pong_count);
}

test "parser ok" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "+OK\r\n");
    try testing.expectEqual(1, mock_conn.ok_count);
}

test "parser err" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "-ERR 'Unknown Protocol Operation'\r\n");
    try testing.expectEqual(1, mock_conn.err_count);
    try testing.expectEqualStrings("'Unknown Protocol Operation'", mock_conn.last_err);
}

test "parser info" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    const info_json = "{\"server_id\":\"test\",\"version\":\"2.0.0\"}";
    const full_info = "INFO " ++ info_json ++ "\r\n";
    try parser.parse(&mock_conn, full_info);
    try testing.expectEqual(1, mock_conn.info_count);
    try testing.expectEqualStrings(info_json, mock_conn.last_info);
}

test "parser msg" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "MSG foo 1 5\r\nhello\r\n");
    try testing.expectEqual(1, mock_conn.msg_count);
    try testing.expectEqualStrings("hello", mock_conn.last_msg);
}

test "parser msg with reply" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "MSG foo 1 reply.bar 5\r\nhello\r\n");
    try testing.expectEqual(1, mock_conn.msg_count);
    try testing.expectEqualStrings("hello", mock_conn.last_msg);
}

test "parser hmsg" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{};
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "HMSG foo 1 22 27\r\nNATS/1.0\r\nFoo: Bar\r\n\r\nhello\r\n");
    try testing.expectEqual(1, mock_conn.msg_count);
    try testing.expectEqualStrings("hello", mock_conn.last_msg);

    // Verify internal state matches C parser expectations
    try testing.expectEqual(@as(i32, 22), parser.ma.hdr);  // Header size
    try testing.expectEqual(@as(i32, 27), parser.ma.size);  // Total size (like C parser)
}
