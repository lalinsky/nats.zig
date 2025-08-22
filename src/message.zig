const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;

// Simple, idiomatic Zig message implementation using ArenaAllocator
pub const Message = struct {
    // Core data - stored as slices
    subject: []const u8,
    reply: ?[]const u8,
    data: []const u8,
    
    // Headers (optional)
    headers: ?HashMap([]const u8, ArrayList([]const u8), StringContext, std.hash_map.default_max_load_percentage),
    
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
    
    const StringContext = struct {
        pub fn hash(self: @This(), key: []const u8) u64 {
            _ = self;
            return std.hash_map.hashString(key);
        }
        
        pub fn eql(self: @This(), a: []const u8, b: []const u8) bool {
            _ = self;
            return std.mem.eql(u8, a, b);
        }
    };
    
    // Create message copying all data into an arena
    pub fn init(
        allocator: Allocator,
        subject: []const u8,
        reply: ?[]const u8,
        data: []const u8,
    ) !*Self {
        const msg = try allocator.create(Self);
        errdefer allocator.destroy(msg);
        
        msg.arena = std.heap.ArenaAllocator.init(allocator);
        errdefer msg.arena.deinit();
        
        const arena_allocator = msg.arena.allocator();
        
        // Copy all data using arena - much simpler!
        msg.* = .{
            .subject = try arena_allocator.dupe(u8, subject),
            .reply = if (reply) |r| try arena_allocator.dupe(u8, r) else null,
            .data = try arena_allocator.dupe(u8, data),
            .headers = null,
            .raw_headers = null,
            .needs_header_parsing = false,
            .sid = 0,
            .seq = 0,
            .time = 0,
            .arena = msg.arena,
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
        const msg = try allocator.create(Self);
        errdefer allocator.destroy(msg);
        
        msg.arena = std.heap.ArenaAllocator.init(allocator);
        errdefer msg.arena.deinit();
        
        const arena_allocator = msg.arena.allocator();
        
        // Copy all data using arena
        msg.* = .{
            .subject = try arena_allocator.dupe(u8, subject),
            .reply = if (reply) |r| try arena_allocator.dupe(u8, r) else null,
            .data = try arena_allocator.dupe(u8, data),
            .headers = null,
            .raw_headers = if (raw_headers) |h| try arena_allocator.dupe(u8, h) else null,
            .needs_header_parsing = raw_headers != null,
            .sid = 0,
            .seq = 0,
            .time = 0,
            .arena = msg.arena,
        };
        
        return msg;
    }
    
    pub fn deinit(self: *Self) void {
        const backing_allocator = self.arena.child_allocator;
        
        // Arena takes care of all allocations - super simple!
        self.arena.deinit();
        
        // Only need to free the message struct itself
        backing_allocator.destroy(self);
    }
    
    // Lazy header parsing
    fn ensureHeadersParsed(self: *Self) !void {
        if (!self.needs_header_parsing) return;
        
        const raw = self.raw_headers orelse return;
        
        // Simple header parsing - split by lines
        var lines = std.mem.splitSequence(u8, raw, "\r\n");
        _ = lines.next(); // Skip "NATS/1.0" line
        
        const arena_allocator = self.arena.allocator();
        
        if (self.headers == null) {
            self.headers = HashMap([]const u8, ArrayList([]const u8), StringContext, std.hash_map.default_max_load_percentage).init(arena_allocator);
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
            
            const result = try self.headers.?.getOrPut(owned_key);
            if (!result.found_existing) {
                result.value_ptr.* = ArrayList([]const u8).init(arena_allocator);
            }
            
            try result.value_ptr.append(owned_value);
        }
        
        self.needs_header_parsing = false;
    }
    
    // Header API
    pub fn headerSet(self: *Self, key: []const u8, value: []const u8) !void {
        try self.ensureHeadersParsed();
        
        const arena_allocator = self.arena.allocator();
        
        if (self.headers == null) {
            self.headers = HashMap([]const u8, ArrayList([]const u8), StringContext, std.hash_map.default_max_load_percentage).init(arena_allocator);
        }
        
        // Remove existing values (arena will clean up memory automatically)
        _ = self.headers.?.fetchRemove(key);
        
        // Add new value
        const owned_key = try arena_allocator.dupe(u8, key);
        const owned_value = try arena_allocator.dupe(u8, value);
        
        var values = ArrayList([]const u8).init(arena_allocator);
        try values.append(owned_value);
        
        try self.headers.?.put(owned_key, values);
    }
    
    pub fn headerGet(self: *Self, key: []const u8) !?[]const u8 {
        try self.ensureHeadersParsed();
        
        if (self.headers) |*headers| {
            if (headers.get(key)) |values| {
                if (values.items.len > 0) {
                    return values.items[0];
                }
            }
        }
        
        return null;
    }
    
    pub fn headerGetAll(self: *Self, key: []const u8) !?[]const []const u8 {
        try self.ensureHeadersParsed();
        
        if (self.headers) |*headers| {
            if (headers.get(key)) |values| {
                return values.items; // No copy needed - arena owns the data
            }
        }
        
        return null;
    }
    
    pub fn headerDelete(self: *Self, key: []const u8) !void {
        try self.ensureHeadersParsed();
        
        if (self.headers) |*headers| {
            // Arena will clean up memory automatically
            _ = headers.fetchRemove(key);
        }
    }
    
    // Check if message indicates "no responders"
    pub fn isNoResponders(self: *Self) !bool {
        if (self.data.len != 0) return false;
        
        const status = try self.headerGet("Status");
        return status != null and std.mem.startsWith(u8, status.?, "503");
    }
    
    // Encode headers for transmission
    pub fn encodeHeaders(self: *Self, writer: anytype) !void {
        try self.ensureHeadersParsed();
        
        if (self.headers == null) return;
        
        try writer.writeAll("NATS/1.0\r\n");
        
        var iter = self.headers.?.iterator();
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

