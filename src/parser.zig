// Copyright 2025 Lukas Lalinsky
// Copyright 2015-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const Message = @import("message.zig").Message;
const MessagePool = @import("message.zig").MessagePool;
const log = @import("log.zig").log;

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
    HMSG_HEADER_START,
    HMSG_HEADER_NAME,
    HMSG_HEADER_COLON,
    HMSG_HEADER_VALUE,
    HMSG_HEADER_CR,
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
    msg: ?*Message = null,
    payload_buffer: []u8 = undefined,
    payload_writer: std.io.FixedBufferStream([]u8) = undefined,
    hdr_len: usize = 0,
    hdr_parsed: usize = 0,
    // Header parsing state
    header_name_start: usize = 0,
    header_value_start: usize = 0,
    current_header_name: ?[]const u8 = null,
};

// Forward declaration for connection
pub const Connection = @import("connection.zig").Connection;

pub const Parser = struct {
    allocator: std.mem.Allocator,
    state: ParserState = .OP_START,
    after_space: usize = 0,
    drop: usize = 0,
    ma: MsgArg = .{},
    headers: bool = false,
    arg_buf_rec: std.ArrayList(u8), // The actual arg buffer storage
    arg_buf: ?*std.ArrayList(u8) = null, // Nullable pointer, null = fast path
    msg_pool: MessagePool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .arg_buf_rec = std.ArrayList(u8).init(allocator),
            .msg_pool = MessagePool.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arg_buf_rec.deinit();
        if (self.ma.msg) |msg| {
            msg.deinit();
        }
        self.msg_pool.deinit();
    }

    pub fn reset(self: *Self) void {
        self.state = .OP_START;
        self.after_space = 0;
        self.drop = 0;
        self.headers = false;
        if (self.ma.msg) |msg| {
            msg.deinit();
        }
        self.ma = .{};
        self.arg_buf = null; // Reset to fast path
        self.arg_buf_rec.clearRetainingCapacity();
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
                            self.headers = false;
                        },
                        'H', 'h' => {
                            self.state = .OP_H;
                            self.headers = true;
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
                            // Don't use arg_buf for fast path - let MSG_ARG handle it
                        },
                    }
                },

                .MSG_ARG => {
                    switch (b) {
                        '\r' => self.drop = 1,
                        '\n' => {
                            // Process message arguments using C-style two-mode approach
                            const arg_start = if (self.arg_buf) |arg_buf|
                                arg_buf.items // Slow path: accumulated from split buffer
                            else
                                buf[self.after_space .. i - self.drop]; // Fast path: direct slice

                            try self.processMsgArgs(arg_start);

                            self.drop = 0;
                            self.after_space = i + 1;

                            // Choose next state based on whether we have headers
                            if (self.ma.hdr_len > 0) {
                                self.state = .HMSG_HEADER_START;
                                self.ma.header_name_start = i + 1;
                            } else {
                                self.state = .MSG_PAYLOAD;
                            }

                            // Clear split buffer mode
                            if (self.arg_buf) |_| {
                                self.arg_buf = null;
                                self.arg_buf_rec.clearRetainingCapacity();
                            }
                        },
                        else => {
                            // Only accumulate if we're in split buffer mode
                            if (self.arg_buf) |arg_buf| {
                                try arg_buf.append(b);
                            }
                        },
                    }
                },

                .HMSG_HEADER_START => {
                    // Copy header byte into payload buffer for later access
                    self.ma.payload_buffer[self.ma.hdr_parsed] = b;
                    self.ma.hdr_parsed += 1;

                    switch (b) {
                        '\r' => {
                            // Empty line - end of headers
                            self.state = .HMSG_HEADER_CR;
                        },
                        'N' => {
                            // Potential start of NATS/1.0 status line or regular header
                            self.state = .HMSG_HEADER_NAME;
                            self.ma.header_name_start = self.ma.hdr_parsed - 1;
                        },
                        else => {
                            // Start of regular header name
                            self.state = .HMSG_HEADER_NAME;
                            self.ma.header_name_start = self.ma.hdr_parsed - 1;
                        },
                    }
                },

                .HMSG_HEADER_NAME => {
                    // Copy header byte into payload buffer
                    self.ma.payload_buffer[self.ma.hdr_parsed] = b;
                    self.ma.hdr_parsed += 1;

                    switch (b) {
                        ':' => {
                            // Found colon - extract header name
                            const header_name_slice = self.ma.payload_buffer[self.ma.header_name_start .. self.ma.hdr_parsed - 1];
                            const trimmed_name = std.mem.trim(u8, header_name_slice, " \t");

                            if (trimmed_name.len > 0) {
                                self.ma.current_header_name = trimmed_name;
                            }

                            self.state = .HMSG_HEADER_COLON;
                        },
                        '\r' => {
                            // End of line without colon - could be NATS/1.0 status line
                            const header_line = self.ma.payload_buffer[self.ma.header_name_start .. self.ma.hdr_parsed - 1];

                            // Check if this is a NATS status line
                            if (std.mem.startsWith(u8, header_line, "NATS/1.0")) {
                                try self.parseStatusLine(header_line);
                            }

                            self.state = .HMSG_HEADER_CR;
                        },
                        else => {
                            // Continue reading header name or status line
                        },
                    }
                },

                .HMSG_HEADER_COLON => {
                    // Copy header byte into payload buffer
                    self.ma.payload_buffer[self.ma.hdr_parsed] = b;
                    self.ma.hdr_parsed += 1;

                    switch (b) {
                        ' ', '\t' => {
                            // Skip whitespace after colon
                        },
                        else => {
                            // Start of header value
                            self.state = .HMSG_HEADER_VALUE;
                            self.ma.header_value_start = self.ma.hdr_parsed - 1;
                        },
                    }
                },

                .HMSG_HEADER_VALUE => {
                    // Copy header byte into payload buffer
                    self.ma.payload_buffer[self.ma.hdr_parsed] = b;
                    self.ma.hdr_parsed += 1;

                    switch (b) {
                        '\r' => {
                            // End of header value - extract and add to HashMap
                            if (self.ma.current_header_name) |header_name| {
                                const header_value_slice = self.ma.payload_buffer[self.ma.header_value_start .. self.ma.hdr_parsed - 1];
                                const trimmed_value = std.mem.trim(u8, header_value_slice, " \t");

                                const msg = self.ma.msg orelse unreachable;
                                const arena_allocator = msg.arena.allocator();

                                if (trimmed_value.len > 0) {
                                    // Normalize header name to lowercase for case-insensitive matching
                                    const normalized_key = try arena_allocator.alloc(u8, header_name.len);
                                    for (header_name, 0..) |c, j| {
                                        normalized_key[j] = std.ascii.toLower(c);
                                    }

                                    const result = try msg.headers.getOrPut(arena_allocator, normalized_key);
                                    if (!result.found_existing) {
                                        result.value_ptr.* = .{};
                                    }
                                    try result.value_ptr.append(arena_allocator, trimmed_value);
                                }

                                self.ma.current_header_name = null;
                            }

                            self.state = .HMSG_HEADER_CR;
                        },
                        else => {
                            // Continue reading header value
                        },
                    }
                },

                .HMSG_HEADER_CR => {
                    // Copy header byte into payload buffer
                    self.ma.payload_buffer[self.ma.hdr_parsed] = b;
                    self.ma.hdr_parsed += 1;

                    switch (b) {
                        '\n' => {
                            // Check if we've read all header bytes
                            if (self.ma.hdr_parsed == self.ma.hdr_len) {
                                // Headers complete - advance payload writer and transition to payload
                                self.ma.payload_writer.pos = self.ma.hdr_len;
                                self.state = .MSG_PAYLOAD;
                            } else {
                                // More headers to parse
                                self.state = .HMSG_HEADER_START;
                            }
                        },
                        else => {
                            return error.InvalidProtocol;
                        },
                    }
                },

                .MSG_PAYLOAD => {
                    const msg = self.ma.msg orelse unreachable;

                    var needed = self.ma.payload_buffer.len - self.ma.payload_writer.pos;
                    const available = buf.len - i;
                    const to_copy = @min(needed, available);

                    if (to_copy > 0) {
                        const written = try self.ma.payload_writer.write(buf[i .. i + to_copy]);
                        std.debug.assert(written == to_copy);
                        i += to_copy - 1;
                        needed -= written;
                    }

                    if (needed == 0) {
                        self.ma.msg = null; // transfer ownership
                        try conn.processMsg(msg);
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
                        },
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_MINUS_ERR_SPC => {
                    switch (b) {
                        ' ', '\t' => {},
                        else => {
                            self.state = .MINUS_ERR_ARG;
                            self.after_space = i;
                            // Don't set up arg_buf yet - use fast path first
                        },
                    }
                },

                .MINUS_ERR_ARG => {
                    switch (b) {
                        '\r' => self.drop = 1,
                        '\n' => {
                            // Process error message using C-style two-mode approach
                            const err_msg = if (self.arg_buf) |arg_buf|
                                arg_buf.items // Slow path: accumulated from split buffer
                            else
                                buf[self.after_space .. i - self.drop]; // Fast path: direct slice

                            try conn.processErr(err_msg);

                            self.drop = 0;
                            self.after_space = i + 1;
                            self.state = .OP_START;

                            // Clear split buffer mode
                            if (self.arg_buf) |_| {
                                self.arg_buf = null;
                                self.arg_buf_rec.clearRetainingCapacity();
                            }
                        },
                        else => {
                            // Only accumulate if we're in split buffer mode
                            if (self.arg_buf) |arg_buf| {
                                try arg_buf.append(b);
                            }
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
                        },
                        else => return error.InvalidProtocol,
                    }
                },

                .OP_INFO_SPC => {
                    switch (b) {
                        ' ', '\t' => {},
                        else => {
                            self.state = .INFO_ARG;
                            self.after_space = i;
                            // Don't set up arg_buf yet - use fast path first
                        },
                    }
                },

                .INFO_ARG => {
                    switch (b) {
                        '\r' => self.drop = 1,
                        '\n' => {
                            // Process INFO JSON using C-style two-mode approach
                            const info_json = if (self.arg_buf) |arg_buf|
                                arg_buf.items // Slow path: accumulated from split buffer
                            else
                                buf[self.after_space .. i - self.drop]; // Fast path: direct slice

                            try conn.processInfo(info_json);

                            self.drop = 0;
                            self.after_space = i + 1;
                            self.state = .OP_START;

                            // Clear split buffer mode
                            if (self.arg_buf) |_| {
                                self.arg_buf = null;
                                self.arg_buf_rec.clearRetainingCapacity();
                            }
                        },
                        else => {
                            // Only accumulate if we're in split buffer mode
                            if (self.arg_buf) |arg_buf| {
                                try arg_buf.append(b);
                            }
                        },
                    }
                },
            }

            i += 1;
        }

        // Check for split buffer scenarios (like C parser)
        if ((self.state == .MSG_ARG or
            self.state == .MINUS_ERR_ARG or
            self.state == .INFO_ARG) and
            self.arg_buf == null)
        {
            // We're in argument parsing state but haven't finished parsing
            // Set up arg_buf for next parse() call
            try self.setupArgBuf();
            const remaining_args = buf[self.after_space .. i - self.drop];
            try self.arg_buf.?.appendSlice(remaining_args);
        }
    }

    fn setupArgBuf(self: *Self) !void {
        self.arg_buf_rec.clearRetainingCapacity();
        self.arg_buf = &self.arg_buf_rec;
    }

    fn parseStatusLine(self: *Self, status_line: []const u8) !void {
        const msg = self.ma.msg orelse unreachable;
        const arena_allocator = msg.arena.allocator();

        // Check if we have an inlined status (like "NATS/1.0 503" or "NATS/1.0 503 No Responders")
        if (std.mem.startsWith(u8, status_line, "NATS/1.0") and status_line.len > "NATS/1.0".len) {
            const status_part = std.mem.trim(u8, status_line["NATS/1.0".len..], " \t");
            if (status_part.len > 0) {
                // Extract status code (first 3 characters if available)
                const status_len = 3; // Like Go's statusLen
                var status: []const u8 = undefined;
                var description: ?[]const u8 = null;

                if (status_part.len == status_len) {
                    status = status_part;
                } else if (status_part.len > status_len) {
                    status = status_part[0..status_len];
                    const desc_part = std.mem.trim(u8, status_part[status_len..], " \t");
                    if (desc_part.len > 0) {
                        description = desc_part;
                    }
                } else {
                    status = status_part; // Less than 3 chars, use as-is
                }

                // Add Status header
                var status_list = std.ArrayListUnmanaged([]const u8){};
                try status_list.append(arena_allocator, status);
                try msg.headers.put(arena_allocator, "status", status_list);

                // Add Description header if present
                if (description) |desc| {
                    var desc_list = std.ArrayListUnmanaged([]const u8){};
                    try desc_list.append(arena_allocator, desc);
                    try msg.headers.put(arena_allocator, "description", desc_list);
                }
            }
        }
    }

    fn processMsgArgs(self: *Self, args: []const u8) !void {
        var parts = std.mem.tokenizeScalar(u8, args, ' ');

        const subject = parts.next() orelse return error.InvalidProtocol;
        const sid_str = parts.next() orelse return error.InvalidProtocol;

        var reply: ?[]const u8 = null;
        var total_len_str: []const u8 = undefined;
        var hdr_len_str: ?[]const u8 = null;

        if (self.headers) {
            // HMSG: subject sid [reply] hdr_len total_len
            const third_part = parts.next() orelse return error.InvalidProtocol;
            const fourth_part = parts.next() orelse return error.InvalidProtocol;
            const fifth_part = parts.next();

            if (fifth_part) |fifth| {
                // 5 parts: subject sid reply hdr_len total_len
                reply = third_part;
                hdr_len_str = fourth_part;
                total_len_str = fifth;
            } else {
                // 4 parts: subject sid hdr_len total_len
                hdr_len_str = third_part;
                total_len_str = fourth_part;
            }
        } else {
            // MSG: subject sid [reply] size
            if (parts.next()) |next_part| {
                if (parts.next()) |size_part| {
                    // 4 parts: subject sid reply size
                    reply = next_part;
                    total_len_str = size_part;
                } else {
                    // 3 parts: subject sid size
                    total_len_str = next_part;
                }
            } else {
                return error.InvalidProtocol;
            }
        }

        const sid = try std.fmt.parseInt(u64, sid_str, 10);

        const total_len = try std.fmt.parseInt(usize, total_len_str, 10);

        var hdr_len: usize = 0;
        if (hdr_len_str) |str| {
            hdr_len = try std.fmt.parseInt(usize, str, 10);
        }

        if (hdr_len > total_len) {
            return error.InvalidProtocol;
        }

        var msg = try self.msg_pool.acquire();
        errdefer msg.deinit();

        const allocator = msg.arena.allocator();
        msg.subject = try allocator.dupe(u8, subject);
        if (reply) |r| {
            msg.reply = try allocator.dupe(u8, r);
        }
        msg.sid = sid;

        // pre-allocate full payload buffer
        var payload_buffer = try allocator.alloc(u8, total_len);

        msg.data = payload_buffer[hdr_len..];

        self.ma = MsgArg{
            .msg = msg,
            .payload_buffer = payload_buffer,
            .payload_writer = std.io.fixedBufferStream(payload_buffer),
            .hdr_len = hdr_len,
            .hdr_parsed = 0,
        };
    }
};

const MockConnection = struct {
    ping_count: u32 = 0,
    pong_count: u32 = 0,
    ok_count: u32 = 0,
    err_count: u32 = 0,
    info_count: u32 = 0,
    msg_count: u32 = 0,
    last_msg: ?*Message = null,
    last_err: []const u8 = "",
    last_info: []const u8 = "",
    parser_ref: ?*Parser = null,

    const Self = @This();

    pub fn processPing(self: *Self) !void {
        self.ping_count += 1;
    }

    pub fn processPong(self: *Self) !void {
        self.pong_count += 1;
    }

    pub fn processMsg(self: *Self, message: *Message) !void {
        self.msg_count += 1;
        // Store the message directly
        self.last_msg = message;
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
        if (self.last_msg) |msg| msg.deinit();
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
    try testing.expectEqualStrings("hello", mock_conn.last_msg.?.data);
    // Test that subject is parsed correctly (would fail if first char dropped)
    try testing.expectEqualStrings("foo", mock_conn.last_msg.?.subject);
}

test "parser msg with reply" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{ .parser_ref = &parser };
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "MSG foo 1 reply.bar 5\r\nhello\r\n");
    try testing.expectEqual(1, mock_conn.msg_count);
    try testing.expectEqualStrings("hello", mock_conn.last_msg.?.data);
}

test "parser hmsg" {
    const testing = std.testing;

    var parser = Parser.init(testing.allocator);
    defer parser.deinit();

    var mock_conn = MockConnection{ .parser_ref = &parser };
    defer mock_conn.deinit();

    try parser.parse(&mock_conn, "HMSG foo 1 22 27\r\nNATS/1.0\r\nFoo: Bar\r\n\r\nhello\r\n");
    try testing.expectEqual(1, mock_conn.msg_count);
    try testing.expectEqualStrings("hello", mock_conn.last_msg.?.data);
}

fn parseInChunks(parser: *Parser, conn: *MockConnection, data: []const u8, chunk_size: usize) !void {
    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

    while (true) {
        var buf: [1024]u8 = undefined;
        std.debug.assert(chunk_size <= buf.len);
        const n = try reader.read(buf[0..chunk_size]);
        if (n == 0) break;
        try parser.parse(conn, buf[0..n]);
    }
}

test "parser split msg" {
    const data = "MSG foo 1 11\r\nhello world\r\n";
    for (1..data.len) |chunk_size| {
        log.debug("chunk_size: {}", .{chunk_size});

        var parser = Parser.init(std.testing.allocator);
        defer parser.deinit();

        var capture = MockConnection{};
        defer capture.deinit();

        try parseInChunks(&parser, &capture, data, chunk_size);

        try std.testing.expectEqual(1, capture.msg_count);
        try std.testing.expectEqualStrings("foo", capture.last_msg.?.subject);
        try std.testing.expectEqualStrings("hello world", capture.last_msg.?.data);
    }
}

test "parser split hmsg" {
    const data = "HMSG foo 1 22 27\r\nNATS/1.0\r\nFoo: Bar\r\n\r\nhello\r\n";
    for (1..data.len) |chunk_size| {
        log.info("chunk_size: {}", .{chunk_size});

        var parser = Parser.init(std.testing.allocator);
        defer parser.deinit();

        var capture = MockConnection{};
        defer capture.deinit();

        try parseInChunks(&parser, &capture, data, chunk_size);

        try std.testing.expectEqual(1, capture.msg_count);
        if (capture.last_msg) |msg| {
            try std.testing.expectEqualStrings("foo", msg.subject);
            try std.testing.expectEqualStrings("hello", msg.data);
            try std.testing.expectEqualStrings("Bar", msg.headerGet("foo") orelse "");
        } else {
            try std.testing.expect(false);
        }
    }
}

test "parser split err" {
    const data = "-ERR Authentication Required\r\n";
    for (1..data.len) |chunk_size| {
        log.info("chunk_size: {}", .{chunk_size});

        var parser = Parser.init(std.testing.allocator);
        defer parser.deinit();

        var capture = MockConnection{};
        defer capture.deinit();

        try parseInChunks(&parser, &capture, data, chunk_size);

        try std.testing.expectEqual(1, capture.err_count);
        try std.testing.expectEqualStrings("Authentication Required", capture.last_err);
    }
}

test "parser split info" {
    const data = "INFO {\"server_id\":\"test\",\"version\":\"2.0.0\"}\r\n";
    for (1..data.len) |chunk_size| {
        log.info("chunk_size: {}", .{chunk_size});

        var parser = Parser.init(std.testing.allocator);
        defer parser.deinit();

        var capture = MockConnection{};
        defer capture.deinit();

        try parseInChunks(&parser, &capture, data, chunk_size);

        try std.testing.expectEqual(1, capture.info_count);
        try std.testing.expectEqualStrings("{\"server_id\":\"test\",\"version\":\"2.0.0\"}", capture.last_info);
    }
}

test "parser multiple lines" {
    const data =
        \\INFO {"server_id":"test","version":"2.0.0"}
        \\PING
        \\MSG foo 1 11
        \\hello world
        \\
    ;

    for (1..data.len) |chunk_size| {
        log.info("chunk_size: {}", .{chunk_size});

        var parser = Parser.init(std.testing.allocator);
        defer parser.deinit();

        var capture = MockConnection{};
        defer capture.deinit();

        try parseInChunks(&parser, &capture, data, chunk_size);

        try std.testing.expectEqual(1, capture.info_count);
        try std.testing.expectEqualStrings("{\"server_id\":\"test\",\"version\":\"2.0.0\"}", capture.last_info);

        try std.testing.expectEqual(1, capture.ping_count);

        try std.testing.expectEqual(1, capture.msg_count);
        if (capture.last_msg) |msg| {
            try std.testing.expectEqual(1, msg.sid);
            try std.testing.expectEqualStrings("foo", msg.subject);
            try std.testing.expectEqualStrings("hello world", msg.data);
        } else {
            try std.testing.expect(false);
        }
    }
}
