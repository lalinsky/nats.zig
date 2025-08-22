const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub const ConnectionError = error{
    InvalidUrl,
    OutOfMemory,
};

// Server structure matching C library's natsSrv
pub const Server = struct {
    url: []const u8,           // Full URL string
    host: []const u8,          // Hostname/IP
    port: u16,                 // Port number
    username: ?[]const u8 = null,  // Username for auth
    password: ?[]const u8 = null,  // Password for auth
    did_connect: bool = false,     // Has successfully connected
    is_implicit: bool = false,     // Discovered via INFO vs explicit
    reconnects: u32 = 0,           // Number of reconnection attempts
    tls_name: ?[]const u8 = null,  // TLS server name
    last_auth_error: ?anyerror = null, // Last auth error
    last_error: ?anyerror = null,  // Last connection error
    
    pub fn init(allocator: Allocator, url: []const u8, is_implicit: bool) !Server {
        const uri = std.Uri.parse(url) catch return ConnectionError.InvalidUrl;
        
        if (!std.mem.eql(u8, uri.scheme, "nats")) {
            return ConnectionError.InvalidUrl;
        }

        const host_component = uri.host orelse return ConnectionError.InvalidUrl;
        const host = switch (host_component) {
            .raw => |h| h,
            .percent_encoded => |h| h,
        };

        // Extract username/password from URL if present
        var username: ?[]const u8 = null;
        var password: ?[]const u8 = null;
        
        if (uri.user) |user_component| {
            const user_info = switch (user_component) {
                .raw => |u| u,
                .percent_encoded => |u| u,
            };
            const colon_pos = std.mem.indexOf(u8, user_info, ":");
            if (colon_pos) |pos| {
                username = try allocator.dupe(u8, user_info[0..pos]);
                password = try allocator.dupe(u8, user_info[pos + 1..]);
            } else {
                username = try allocator.dupe(u8, user_info);
            }
        }

        return Server{
            .url = try allocator.dupe(u8, url),
            .host = try allocator.dupe(u8, host),
            .port = uri.port orelse 4222,
            .username = username,
            .password = password,
            .is_implicit = is_implicit,
        };
    }
    
    pub fn deinit(self: *Server, allocator: Allocator) void {
        allocator.free(self.url);
        allocator.free(self.host);
        if (self.username) |u| allocator.free(u);
        if (self.password) |p| allocator.free(p);
        if (self.tls_name) |t| allocator.free(t);
    }
};

// Server pool matching C library's natsSrvPool  
pub const ServerPool = struct {
    servers: ArrayList(*Server),          // Dynamic array of server pointers (like C)
    urls: std.StringHashMap(void),        // Hash map for O(1) duplicate detection
    randomize: bool = false,              // Whether to randomize order
    default_user: ?[]const u8 = null,     // Default username from first explicit URL
    default_pwd: ?[]const u8 = null,      // Default password from first explicit URL
    allocator: Allocator,
    
    pub fn init(allocator: Allocator) ServerPool {
        return ServerPool{
            .servers = ArrayList(*Server).init(allocator),
            .urls = std.StringHashMap(void).init(allocator),
            .allocator = allocator,
        };
    }
    
    pub fn deinit(self: *ServerPool) void {
        // Free all servers
        for (self.servers.items) |server| {
            server.deinit(self.allocator);
            self.allocator.destroy(server);
        }
        self.servers.deinit();
        
        // Free all URL strings stored in hash map
        var iterator = self.urls.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.urls.deinit();
        
        if (self.default_user) |u| self.allocator.free(u);
        if (self.default_pwd) |p| self.allocator.free(p);
    }
    
    pub fn addServer(self: *ServerPool, url: []const u8, is_implicit: bool) !void {
        // Create server first to extract host:port (like C library)
        const server = try self.allocator.create(Server);
        server.* = try Server.init(self.allocator, url, is_implicit);
        
        // Create bareURL as host:port using fixed buffer (like C library line 146/164)
        var bare_url_buf: [256]u8 = undefined;
        const bare_url = std.fmt.bufPrint(&bare_url_buf, "{s}:{d}", .{ server.host, server.port }) catch unreachable;
        
        // Check for duplicates using hash map with host:port key (like C library)
        if (self.urls.contains(bare_url)) {
            // Clean up and return if already exists
            server.deinit(self.allocator);
            self.allocator.destroy(server);
            return;
        }
        
        // Set default user/pwd from first explicit server (like C library)
        if (!is_implicit and self.default_user == null) {
            if (server.username) |u| {
                self.default_user = try self.allocator.dupe(u8, u);
            }
            if (server.password) |p| {
                self.default_pwd = try self.allocator.dupe(u8, p);
            }
        }
        
        // Add to hash map with host:port as key (like C library line 165)
        const bare_url_owned = try self.allocator.dupe(u8, bare_url);
        try self.urls.put(bare_url_owned, {});
        
        try self.servers.append(server);
    }
    
    pub fn getCurrentServer(self: *ServerPool, current_server: ?*const Server) ?usize {
        if (current_server == null) return null;
        
        // Find current server's position in pool (like C library)
        for (self.servers.items, 0..) |server, i| {
            if (server == current_server) {
                return i;
            }
        }
        
        return null;
    }
    
    
    // Core server selection algorithm matching C library's natsSrvPool_GetNextServer
    pub fn getNextServer(self: *ServerPool, max_reconnect: i32, current_server: ?*const Server) !?*Server {
        // Get current server index (like C library line 88)
        const current_index = self.getCurrentServer(current_server);
        if (current_index == null) {
            // Initial connection case - C library would return pool->srvrs[0] directly
            if (self.servers.items.len == 0) return null;
            return self.servers.items[0];
        }
        
        const i = current_index.?;
        const current_srv = self.servers.items[i];
        
        // Shift left servers past current to the current's position (like C library lines 92-94)
        var j = i;
        while (j < self.servers.items.len - 1) : (j += 1) {
            self.servers.items[j] = self.servers.items[j + 1];
        }
        
        // Decide server fate based on reconnect attempts (like C library lines 96-107)
        if (max_reconnect < 0 or current_srv.reconnects < max_reconnect) {
            // Move the current server to the back of the list (like C library line 100)
            self.servers.items[self.servers.items.len - 1] = current_srv;
        } else {
            // Remove the server from the list (like C library lines 104-106)
            // Remove from hash map using host:port key with fixed buffer
            var bare_url_buf: [256]u8 = undefined;
            const bare_url = std.fmt.bufPrint(&bare_url_buf, "{s}:{d}", .{ current_srv.host, current_srv.port }) catch unreachable;
            
            if (self.urls.fetchRemove(bare_url)) |kv| {
                self.allocator.free(kv.key); // Free the duplicated host:port string
            }
            current_srv.deinit(self.allocator);
            self.allocator.destroy(current_srv);
            _ = self.servers.pop();
        }
        
        // Return first server in list if pool not empty (like C library lines 109-112)
        if (self.servers.items.len == 0) return null;
        
        return self.servers.items[0];
    }
    
    pub fn getSize(self: *ServerPool) usize {
        return self.servers.items.len;
    }
    
    // Shuffle servers in pool (like C library's _shufflePool)
    pub fn shuffle(self: *ServerPool, offset: usize) void {
        if (self.servers.items.len <= offset + 1) return;
        
        var prng = std.rand.DefaultPrng.init(@bitCast(std.time.nanoTimestamp()));
        const random = prng.random();
        
        var i = offset;
        while (i < self.servers.items.len) : (i += 1) {
            const j = offset + random.uintLessThan(usize, i + 1 - offset);
            const tmp = self.servers.items[i];
            self.servers.items[i] = self.servers.items[j];
            self.servers.items[j] = tmp;
        }
    }
};

test "server pool basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var pool = ServerPool.init(allocator);
    defer pool.deinit();
    
    // Test adding servers
    try pool.addServer("nats://localhost:4222", false);
    try pool.addServer("nats://localhost:4223", false);
    try pool.addServer("nats://localhost:4224", true);
    
    try std.testing.expect(pool.getSize() == 3);
    
    // Test initial server selection
    const server1 = try pool.getNextServer(-1, null);
    try std.testing.expect(server1 != null);
    
    const server2 = try pool.getNextServer(-1, server1);
    try std.testing.expect(server2 != null);
    try std.testing.expect(server2 != server1);
    
    // Pool size should remain the same (server moved, not removed)
    try std.testing.expect(pool.getSize() == 3);
}

test "server pool duplicate prevention" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var pool = ServerPool.init(allocator);
    defer pool.deinit();
    
    // Add same server twice
    try pool.addServer("nats://localhost:4222", false);
    try pool.addServer("nats://localhost:4222", false);
    
    // Should only have one server
    try std.testing.expect(pool.getSize() == 1);
    
    // Test C library pattern: different URLs with same host:port should be duplicates
    try pool.addServer("nats://user:pass@localhost:4222", false);
    try pool.addServer("nats://other:auth@localhost:4222", true);
    
    // Should still only have one server (same host:port)
    try std.testing.expect(pool.getSize() == 1);
    
    // Different host:port should be added
    try pool.addServer("nats://localhost:4223", false);
    try std.testing.expect(pool.getSize() == 2);
}

test "server removal on max reconnects" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var pool = ServerPool.init(allocator);
    defer pool.deinit();
    
    try pool.addServer("nats://localhost:4222", false);
    try pool.addServer("nats://localhost:4223", false);
    
    const server1 = try pool.getNextServer(-1, null);
    try std.testing.expect(server1 != null);
    
    // Set server to max reconnects
    server1.?.reconnects = 5;
    
    // This should remove the server
    _ = try pool.getNextServer(5, server1);
    
    // Pool should now have one less server
    try std.testing.expect(pool.getSize() == 1);
}