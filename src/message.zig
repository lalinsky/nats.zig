const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

// Header constants (like C NATS library)
const STATUS_HDR = "Status";
const DESCRIPTION_HDR = "Description";
const HDR_STATUS_NO_RESP_503 = "503";

// Simple, idiomatic Zig message implementation using ArenaAllocator
pub const Message = struct {
    // Core data - stored as slices
    subject: []const u8,
    reply: ?[]const u8,
    data: []const u8,
    
    // Headers
    headers: std.hash_map.StringHashMapUnmanaged(ArrayListUnmanaged([]const u8)),
    
    // Raw header data for lazy parsing
    raw_headers: ?[]const u8,
    needs_header_parsing: bool,
    
    // Metadata
    sid: u64,
    seq: u64,
    time: i64,
    
    // Memory management - much simpler with arena
    arena: std.heap.ArenaAllocator,
    
    const Self = @This();
    
    
    // Create message copying all data into an arena
    pub fn init(
        allocator: Allocator,
        subject: []const u8,
        reply: ?[]const u8,
        data: []const u8,
    ) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        
        const arena_allocator = arena.allocator();
        
        const msg = try arena_allocator.create(Self);
        
        // Copy all data using arena - much simpler!
        msg.* = .{
            .subject = try arena_allocator.dupe(u8, subject),
            .reply = if (reply) |r| try arena_allocator.dupe(u8, r) else null,
            .data = try arena_allocator.dupe(u8, data),
            .headers = .{},
            .raw_headers = null,
            .needs_header_parsing = false,
            .sid = 0,
            .seq = 0,
            .time = 0,
            .arena = arena,
        };
        
        return msg;
    }
    
    // Create message with header support
    pub fn initWithHeaders(
        allocator: Allocator,
        subject: []const u8,
        reply: ?[]const u8,
        data: []const u8,
        raw_headers: ?[]const u8,
    ) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        
        const arena_allocator = arena.allocator();
        
        const msg = try arena_allocator.create(Self);
        
        // Copy all data using arena
        msg.* = .{
            .subject = try arena_allocator.dupe(u8, subject),
            .reply = if (reply) |r| try arena_allocator.dupe(u8, r) else null,
            .data = try arena_allocator.dupe(u8, data),
            .headers = .{},
            .raw_headers = if (raw_headers) |h| try arena_allocator.dupe(u8, h) else null,
            .needs_header_parsing = raw_headers != null,
            .sid = 0,
            .seq = 0,
            .time = 0,
            .arena = arena,
        };
        
        return msg;
    }
    
    pub fn deinit(self: *Self) void {
        // Arena takes care of ALL allocations including the message struct itself!
        self.arena.deinit();
    }
    
    // Lazy header parsing
    pub fn ensureHeadersParsed(self: *Self) !void {
        if (!self.needs_header_parsing) return;
        
        const raw = self.raw_headers orelse return;
        
        // Parse headers like Go NATS library
        var lines = std.mem.splitSequence(u8, raw, "\r\n");
        const first_line = lines.next() orelse return;
        
        const arena_allocator = self.arena.allocator();
        
        // Check if we have an inlined status (like "NATS/1.0 503" or "NATS/1.0 503 No Responders")
        const nats_prefix = "NATS/1.0";
        if (std.mem.startsWith(u8, first_line, nats_prefix) and first_line.len > nats_prefix.len) {
            const status_part = std.mem.trim(u8, first_line[nats_prefix.len..], " \t");
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
                
                // Add Status header directly to avoid circular dependency
                const status_copy = try arena_allocator.dupe(u8, status);
                var status_list = ArrayListUnmanaged([]const u8){};
                try status_list.append(arena_allocator, status_copy);
                try self.headers.put(arena_allocator, STATUS_HDR, status_list);
                
                // Add Description header if present
                if (description) |desc| {
                    const desc_copy = try arena_allocator.dupe(u8, desc);
                    var desc_list = ArrayListUnmanaged([]const u8){};
                    try desc_list.append(arena_allocator, desc_copy);
                    try self.headers.put(arena_allocator, DESCRIPTION_HDR, desc_list);
                }
            }
        }
        
        
        while (lines.next()) |line| {
            if (line.len == 0) break; // End of headers
            
            const colon_pos = std.mem.indexOf(u8, line, ":") orelse continue;
            const key = std.mem.trim(u8, line[0..colon_pos], " \t");
            const value = std.mem.trim(u8, line[colon_pos + 1..], " \t");
            
            if (key.len == 0) continue;
            
            // Copy key and value using arena - much simpler!
            const owned_key = try arena_allocator.dupe(u8, key);
            const owned_value = try arena_allocator.dupe(u8, value);
            
            const result = try self.headers.getOrPut(arena_allocator, owned_key);
            if (!result.found_existing) {
                result.value_ptr.* = .{};
            }
            
            try result.value_ptr.append(arena_allocator, owned_value);
        }
        
        self.needs_header_parsing = false;
    }
    
    // Header API
    pub fn headerSet(self: *Self, key: []const u8, value: []const u8) !void {
        try self.ensureHeadersParsed();
        
        const arena_allocator = self.arena.allocator();
        
        
        // Remove existing values (arena will clean up memory automatically)
        _ = self.headers.fetchRemove(key);
        
        // Add new value
        const owned_key = try arena_allocator.dupe(u8, key);
        const owned_value = try arena_allocator.dupe(u8, value);
        
        var values: ArrayListUnmanaged([]const u8) = .{};
        try values.append(arena_allocator, owned_value);
        
        try self.headers.put(arena_allocator, owned_key, values);
    }
    
    pub fn headerGet(self: *Self, key: []const u8) !?[]const u8 {
        try self.ensureHeadersParsed();
        
        if (self.headers.get(key)) |values| {
            if (values.items.len > 0) {
                return values.items[0];
            }
        }
        
        return null;
    }
    
    pub fn headerGetAll(self: *Self, key: []const u8) !?[]const []const u8 {
        try self.ensureHeadersParsed();
        
        if (self.headers.get(key)) |values| {
            return values.items; // No copy needed - arena owns the data
        }
        
        return null;
    }
    
    pub fn headerDelete(self: *Self, key: []const u8) !void {
        try self.ensureHeadersParsed();
        
        // Arena will clean up memory automatically
        _ = self.headers.fetchRemove(key);
    }
    
    // Check if message indicates "no responders" - matches Go NATS library logic
    pub fn isNoResponders(self: *Self) bool {
        if (self.data.len != 0) return false;
        
        const status = self.headerGet(STATUS_HDR) catch return false;
        return status != null and std.mem.eql(u8, status.?, HDR_STATUS_NO_RESP_503);
    }
    
    // Encode headers for transmission
    pub fn encodeHeaders(self: *Self, writer: anytype) !void {
        try self.ensureHeadersParsed();
        
        if (self.headers.count() == 0) return;
        
        try writer.writeAll("NATS/1.0\r\n");
        
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
    
};

