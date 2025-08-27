// Copyright 2025 Lukas Lalinsky
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
const ArrayList = std.ArrayList;
const StringArrayHashMapUnmanaged = std.StringArrayHashMapUnmanaged;
const Url = @import("url.zig").Url;

pub const ConnectionError = error{
    InvalidUrl,
    OutOfMemory,
};

// Server structure matching C library's natsSrv
pub const Server = struct {
    parsed_url: Url, // Parsed URL containing all components
    key: []const u8, // Host:port key for hash map lookups
    did_connect: bool = false, // Has successfully connected
    is_implicit: bool = false, // Discovered via INFO vs explicit
    reconnects: u32 = 0, // Number of reconnection attempts
    tls_name: ?[]const u8 = null, // TLS server name
    last_auth_error: ?anyerror = null, // Last auth error
    last_error: ?anyerror = null, // Last connection error

    pub fn init(allocator: Allocator, url_str: []const u8, is_implicit: bool) !Server {
        var parsed_url = Url.parse(allocator, url_str) catch return ConnectionError.InvalidUrl;
        errdefer parsed_url.deinit();

        if (!std.mem.eql(u8, parsed_url.scheme, "nats")) {
            return ConnectionError.InvalidUrl;
        }

        const key = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ parsed_url.host, parsed_url.port });
        errdefer allocator.free(key);

        return Server{
            .parsed_url = parsed_url,
            .key = key,
            .is_implicit = is_implicit,
        };
    }

    pub fn deinit(self: *Server, allocator: Allocator) void {
        self.parsed_url.deinit();
        allocator.free(self.key);
        if (self.tls_name) |t| allocator.free(t);
    }
};

// Server pool matching C library's natsSrvPool
pub const ServerPool = struct {
    servers: StringArrayHashMapUnmanaged(*Server), // Combined hash map with insertion order (host:port -> *Server)
    randomize: bool = false, // Whether to randomize order
    default_user: ?[]const u8 = null, // Default username from first explicit URL
    default_pwd: ?[]const u8 = null, // Default password from first explicit URL
    allocator: Allocator,

    pub fn init(allocator: Allocator) ServerPool {
        return ServerPool{
            .servers = StringArrayHashMapUnmanaged(*Server){},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ServerPool) void {
        // Free all servers
        var iterator = self.servers.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.servers.deinit(self.allocator);

        if (self.default_user) |u| self.allocator.free(u);
        if (self.default_pwd) |p| self.allocator.free(p);
    }

    pub fn addServer(self: *ServerPool, url_str: []const u8, is_implicit: bool) !bool {
        // Create server first to extract host:port (like C library)
        const server = try self.allocator.create(Server);
        errdefer self.allocator.destroy(server);

        server.* = try Server.init(self.allocator, url_str, is_implicit);
        errdefer server.deinit(self.allocator);

        // Use the key stored in the server
        const result = try self.servers.getOrPut(self.allocator, server.key);
        if (result.found_existing) {
            // We already have a server with the same host:port - clean up duplicate
            server.deinit(self.allocator);
            self.allocator.destroy(server);
            return false;
        }

        // Store the server
        result.value_ptr.* = server;

        // Set default user/pwd from first explicit server (like C library)
        if (!is_implicit and self.default_user == null) {
            if (server.parsed_url.username) |u| {
                self.default_user = try self.allocator.dupe(u8, u);
            }
            if (server.parsed_url.password) |p| {
                self.default_pwd = try self.allocator.dupe(u8, p);
            }
        }

        return true;
    }

    // Core server selection algorithm matching C library's natsSrvPool_GetNextServer
    pub fn getNextServer(self: *ServerPool, max_reconnect: i32, current_server: ?*const Server) !?*Server {
        if (current_server == null) {
            // Initial connection case - C library would return pool->srvrs[0] directly
            return self.getFirstServer();
        }

        const current_srv = current_server.?;

        // Decide server fate based on reconnect attempts (like C library lines 96-107)
        if (max_reconnect < 0 or current_srv.reconnects < max_reconnect) {
            // Remove current server and move it to back (like C library)
            // Remove using the stored key and get the mutable server back
            const removed_server = self.servers.fetchSwapRemove(current_srv.key).?;

            // Add current server back at the end - capacity guaranteed since we just removed
            self.servers.putAssumeCapacity(removed_server.key, removed_server.value);
        } else {
            // Remove the server permanently (like C library lines 104-106)
            const removed_server = self.servers.fetchSwapRemove(current_srv.key).?;
            removed_server.value.deinit(self.allocator);
            self.allocator.destroy(removed_server.value);
        }

        // Return first server in list if pool not empty (like C library lines 109-112)
        return self.getFirstServer();
    }

    pub fn getSize(self: *ServerPool) usize {
        return self.servers.count();
    }

    pub fn getFirstServer(self: *ServerPool) ?*Server {
        const values = self.servers.values();
        return if (values.len > 0) values[0] else null;
    }

    // Shuffle servers in pool (like C library's _shufflePool)
    pub fn shuffle(self: *ServerPool, offset: usize) void {
        if (self.servers.count() <= offset + 1) return;

        var prng = std.rand.DefaultPrng.init(@bitCast(std.time.nanoTimestamp()));
        const random = prng.random();

        // Shuffle the underlying entries directly
        var i = offset;
        while (i < self.servers.entries.len) : (i += 1) {
            const j = offset + random.uintLessThan(usize, i + 1 - offset);
            self.servers.entries.swap(i, j);
        }

        // Rebuild the hash index
        self.servers.reIndex(self.allocator);
    }
};

test "server pool basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var pool = ServerPool.init(allocator);
    defer pool.deinit();

    // Test adding servers
    _ = try pool.addServer("nats://localhost:4222", false);
    _ = try pool.addServer("nats://localhost:4223", false);
    _ = try pool.addServer("nats://localhost:4224", true);

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
    _ = try pool.addServer("nats://localhost:4222", false);
    _ = try pool.addServer("nats://localhost:4222", false);

    // Should only have one server
    try std.testing.expect(pool.getSize() == 1);

    // Test C library pattern: different URLs with same host:port should be duplicates
    _ = try pool.addServer("nats://user:pass@localhost:4222", false);
    _ = try pool.addServer("nats://other:auth@localhost:4222", true);

    // Should still only have one server (same host:port)
    try std.testing.expect(pool.getSize() == 1);

    // Different host:port should be added
    _ = try pool.addServer("nats://localhost:4223", false);
    try std.testing.expect(pool.getSize() == 2);
}

test "server removal on max reconnects" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var pool = ServerPool.init(allocator);
    defer pool.deinit();

    _ = try pool.addServer("nats://localhost:4222", false);
    _ = try pool.addServer("nats://localhost:4223", false);

    const server1 = try pool.getNextServer(-1, null);
    try std.testing.expect(server1 != null);

    // Set server to max reconnects
    server1.?.reconnects = 5;

    // This should remove the server
    _ = try pool.getNextServer(5, server1);

    // Pool should now have one less server
    try std.testing.expect(pool.getSize() == 1);
}
