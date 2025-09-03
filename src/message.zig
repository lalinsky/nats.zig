// Copyright 2025 Lukas Lalinsky
// Copyright 2015-2025 The NATS Authors
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
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

// Status line prefix
const NATS_STATUS_PREFIX = "NATS/1.0";

const HDR_STATUS = "Status";
const HDR_DESCRIPTION = "Description";

const HDR_STATUS_CONTROL = "100";
const HDR_STATUS_BAD_REQUEST = "400";
const HDR_STATUS_NOT_FOUND = "404";
const HDR_STATUS_TIMEOUT = "408";
const HDR_STATUS_MAX_BYTES = "409";
const HDR_STATUS_NO_RESPONSE = "503";

pub const MessageList = struct {
    head: ?*Message = null,
    tail: ?*Message = null,
    len: usize = 0,

    pub fn push(self: *MessageList, msg: *Message) void {
        msg.next = null;
        if (self.head == null) {
            self.head = msg;
            self.tail = msg;
        } else {
            self.tail.?.next = msg;
            self.tail = msg;
        }
        self.len += 1;
    }

    pub fn pop(self: *MessageList) ?*Message {
        if (self.head == null) return null;
        const msg = self.head.?;
        self.head = msg.next;
        if (self.head == null) self.tail = null;
        self.len -= 1;
        msg.next = null;
        return msg;
    }
};

// Simple, idiomatic Zig message implementation using ArenaAllocator
pub const Message = struct {
    // Core data - stored as slices
    subject: []const u8,
    reply: ?[]const u8 = null,
    data: []const u8,

    // Metadata
    sid: u64 = 0,
    seq: u64 = 0, // TODO this doesn't really belong here

    // Headers
    headers: std.hash_map.StringHashMapUnmanaged(ArrayListUnmanaged([]const u8)) = .{},

    // Memory management - much simpler with arena
    arena: std.heap.ArenaAllocator,

    // Linked list for message queue
    next: ?*Message = null,
    pool: ?*MessagePool = null,

    const Self = @This();

    // Create empty message with just arena allocation
    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .subject = &[_]u8{},
            .data = &[_]u8{},
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    // Create message with headers pre-parsed
    pub fn initWithHeaders(allocator: std.mem.Allocator, subject: []const u8, reply: ?[]const u8, data: []const u8, raw_headers: []const u8) !Self {
        var msg = Self.init(allocator);
        errdefer msg.deinit();

        try msg.setSubject(subject);
        if (reply) |r| {
            try msg.setReply(r);
        }
        try msg.setPayload(data);
        try msg.parseHeaders(raw_headers);

        return msg;
    }

    pub fn deinit(self: *Self) void {
        if (self.pool) |pool| {
            pool.release(self);
        } else {
            self.arena.deinit();
        }
    }

    pub fn setSubject(self: *Self, subject: []const u8) !void {
        self.subject = try self.arena.allocator().dupe(u8, subject);
    }

    pub fn setReply(self: *Self, reply: []const u8) !void {
        self.reply = try self.arena.allocator().dupe(u8, reply);
    }

    pub fn setPayload(self: *Self, payload: []const u8) !void {
        self.data = try self.arena.allocator().dupe(u8, payload);
    }

    pub fn parseHeaders(self: *Self, raw_headers: []const u8) !void {
        const arena_allocator = self.arena.allocator();

        // Parse headers like Go NATS library
        var lines = std.mem.splitSequence(u8, raw_headers, "\r\n");
        const first_line = lines.next() orelse return;

        // Check if we have an inlined status (like "NATS/1.0 503" or "NATS/1.0 503 No Responders")
        if (std.mem.startsWith(u8, first_line, NATS_STATUS_PREFIX) and first_line.len > NATS_STATUS_PREFIX.len) {
            const status_part = std.mem.trim(u8, first_line[NATS_STATUS_PREFIX.len..], " \t");
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
                const owned_status = try arena_allocator.dupe(u8, status);
                var status_list = ArrayListUnmanaged([]const u8){};
                try status_list.append(arena_allocator, owned_status);
                try self.headers.put(arena_allocator, HDR_STATUS, status_list);

                // Add Description header if present
                if (description) |desc| {
                    const owned_desc = try arena_allocator.dupe(u8, desc);
                    var desc_list = ArrayListUnmanaged([]const u8){};
                    try desc_list.append(arena_allocator, owned_desc);
                    try self.headers.put(arena_allocator, HDR_DESCRIPTION, desc_list);
                }
            }
        }

        while (lines.next()) |line| {
            if (line.len == 0) break; // End of headers

            const colon_pos = std.mem.indexOf(u8, line, ":") orelse continue;
            const key = std.mem.trim(u8, line[0..colon_pos], " \t");
            const value = std.mem.trim(u8, line[colon_pos + 1 ..], " \t");

            if (key.len == 0) continue;

            // Copy key and value using arena
            const owned_key = try arena_allocator.dupe(u8, key);
            const owned_value = try arena_allocator.dupe(u8, value);

            const result = try self.headers.getOrPut(arena_allocator, owned_key);
            if (!result.found_existing) {
                result.value_ptr.* = .{};
            }

            try result.value_ptr.append(arena_allocator, owned_value);
        }
    }

    // Header API
    pub fn headerSet(self: *Self, key: []const u8, value: []const u8) !void {
        const arena_allocator = self.arena.allocator();

        // Validate header field name (prevent injection attacks)
        if (key.len == 0) return error.InvalidHeaderName;
        for (key) |c| {
            if (c == '\r' or c == '\n' or c == ':') {
                return error.InvalidHeaderName;
            }
            if (c <= 32 or c >= 127) {
                return error.InvalidHeaderName;
            }
        }

        // Validate header value (prevent injection attacks)
        for (value) |c| {
            if (c == '\r' or c == '\n') {
                return error.InvalidHeaderValue;
            }
        }

        // Remove existing values (arena will clean up memory automatically)
        _ = self.headers.fetchRemove(key);

        // Copy key and value using arena
        const owned_key = try arena_allocator.dupe(u8, key);
        const owned_value = try arena_allocator.dupe(u8, value);

        var values: ArrayListUnmanaged([]const u8) = .{};
        try values.append(arena_allocator, owned_value);

        try self.headers.put(arena_allocator, owned_key, values);
    }

    pub fn headerGet(self: *Self, key: []const u8) ?[]const u8 {
        if (self.headers.get(key)) |values| {
            if (values.items.len > 0) {
                return values.items[0];
            }
        }

        return null;
    }

    pub fn headerGetAll(self: *Self, key: []const u8) ?[]const []const u8 {
        if (self.headers.get(key)) |values| {
            return values.items; // No copy needed - arena owns the data
        }

        return null;
    }

    pub fn headerDelete(self: *Self, key: []const u8) void {
        // Arena will clean up memory automatically
        _ = self.headers.fetchRemove(key);
    }

    // Check if message indicates "no responders" - matches Go NATS library logic
    pub fn isNoResponders(self: *Self) bool {
        if (self.data.len != 0) return false;

        const status = self.headerGet(HDR_STATUS);
        return status != null and std.mem.eql(u8, status.?, HDR_STATUS_NO_RESPONSE);
    }

    // Encode headers for transmission
    pub fn encodeHeaders(self: *Self, writer: anytype) !void {
        if (self.headers.count() == 0) return;

        try writer.writeAll(NATS_STATUS_PREFIX ++ "\r\n");

        var iter = self.headers.iterator();
        while (iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const values = entry.value_ptr.*;

            for (values.items) |value| {
                try writer.print("{s}: {s}\r\n", .{ key, value });
            }
        }

        try writer.writeAll("\r\n");
    }

    // Reset message for reuse in pool
    pub fn reset(self: *Self) void {
        // Clear all fields except arena and pool
        self.subject = &[_]u8{};
        self.reply = null;
        self.data = &[_]u8{};
        self.sid = 0;
        self.seq = 0;
        self.headers.clearRetainingCapacity();
        self.next = null;
        // Note: pool and arena are intentionally preserved
    }
};

pub const MessagePool = struct {
    allocator: std.mem.Allocator,
    messages: MessageList = .{},
    mutex: std.Thread.Mutex = .{},
    max_size: usize = 100,
    max_arena_size: usize = 1024 * 1024,

    pub const Self = @This();

    pub fn init(allocator: std.mem.Allocator) MessagePool {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.messages.pop()) |msg| {
            msg.pool = null;
            msg.deinit();
            self.allocator.destroy(msg);
        }
    }

    pub fn acquire(self: *Self) !*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.messages.pop()) |msg| {
            return msg;
        }

        const msg = try self.allocator.create(Message);
        errdefer self.allocator.destroy(msg);

        msg.* = Message.init(self.allocator);
        msg.pool = self;
        return msg;
    }

    pub fn release(self: *Self, msg: *Message) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.messages.len < self.max_size) {
            msg.reset();
            _ = msg.arena.reset(.{ .retain_with_limit = self.max_arena_size });
            self.messages.push(msg);
        } else {
            msg.pool = null;
            msg.deinit();
            self.allocator.destroy(msg);
        }
    }
};
