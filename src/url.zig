// Port of NATS C library URL parsing from refs/nats.c/src/url.c
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const UrlError = error{
    InvalidUrl,
    OutOfMemory,
};

pub const Url = struct {
    full_url: []const u8,  // Owned string
    scheme: []const u8,    // Slice into full_url or static string
    host: []const u8,      // Slice into full_url
    port: u16,
    username: ?[]const u8, // Slice into full_url if present
    password: ?[]const u8, // Slice into full_url if present  
    path: ?[]const u8,     // Slice into full_url if present
    allocator: Allocator,

    pub fn parse(allocator: Allocator, url_str: []const u8) !Url {
        if (url_str.len == 0) {
            return UrlError.InvalidUrl;
        }

        // Trim whitespace
        const trimmed = std.mem.trim(u8, url_str, " \t\n\r");

        var scheme: []const u8 = undefined;
        var user: ?[]const u8 = null;
        var pwd: ?[]const u8 = null;
        var host: []const u8 = undefined;
        var port: u16 = 4222;
        var path: ?[]const u8 = null;

        var ptr: []const u8 = trimmed;

        // Parse scheme (matching C library logic)
        if (std.mem.indexOf(u8, trimmed, "://")) |scheme_end| {
            scheme = trimmed[0..scheme_end];
            ptr = trimmed[scheme_end + 3 ..];
        } else {
            // Default to "nats" scheme if none provided (matching C library)
            scheme = "nats";
            ptr = trimmed;
        }

        // Parse user info (user:password@)
        if (std.mem.lastIndexOf(u8, ptr, "@")) |at_pos| {
            const user_info = ptr[0..at_pos];
            host = ptr[at_pos + 1 ..];

            if (user_info.len > 0) {
                if (std.mem.indexOf(u8, user_info, ":")) |colon_pos| {
                    if (colon_pos > 0) {
                        user = user_info[0..colon_pos];
                    }
                    if (colon_pos + 1 < user_info.len) {
                        pwd = user_info[colon_pos + 1 ..];
                    }
                } else {
                    user = user_info;
                }
            }
        } else {
            host = ptr;
        }

        // Parse host and port (handle IPv6 addresses)
        var host_end = host.len;
        
        // Search for end of IPv6 address (if applicable)
        var search_start: usize = 0;
        if (std.mem.lastIndexOf(u8, host, "]")) |ipv6_end| {
            search_start = ipv6_end;
        }

        // From that point, search for the last ':' character
        if (std.mem.lastIndexOfScalar(u8, host[search_start..], ':')) |relative_pos| {
            const port_start = search_start + relative_pos;
            const port_str = host[port_start + 1 ..];
            
            // Check if there's a path after the port
            if (std.mem.indexOf(u8, port_str, "/")) |path_start| {
                const port_only = port_str[0..path_start];
                path = port_str[path_start + 1 ..];
                port = std.fmt.parseInt(u16, port_only, 10) catch return UrlError.InvalidUrl;
            } else {
                port = std.fmt.parseInt(u16, port_str, 10) catch return UrlError.InvalidUrl;
            }
            host_end = port_start;
        } else {
            // Check for path without port
            if (std.mem.indexOf(u8, host, "/")) |path_start| {
                path = host[path_start + 1 ..];
                host_end = path_start;
            }
        }

        host = host[0..host_end];
        if (host.len == 0) {
            host = "localhost";
        }

        // Build full URL first
        const userval = if (user) |u| u else "";
        const usep = if (pwd != null) ":" else "";
        const pwdval = if (pwd) |p| p else "";
        const hsep = if (user != null) "@" else "";
        const pathsep = if (path != null) "/" else "";
        const pathval = if (path) |p| p else "";

        const full_url = try std.fmt.allocPrint(allocator, "{s}://{s}{s}{s}{s}{s}:{d}{s}{s}", .{
            scheme, userval, usep, pwdval, hsep, host, port, pathsep, pathval,
        });

        // Now create slices into the full_url for the components
        // Find scheme position (always starts at 0)
        const scheme_final = full_url[0..scheme.len];
        
        // Find host position after "://" and optional user info
        var host_start: usize = scheme.len + 3; // Skip "://"
        if (user != null) {
            // Skip user:pass@ part
            while (host_start < full_url.len and full_url[host_start] != '@') {
                host_start += 1;
            }
            if (host_start < full_url.len) host_start += 1; // Skip @
        }
        const host_final = full_url[host_start..host_start + host.len];
        
        // Find username/password slices if they exist
        var username_final: ?[]const u8 = null;
        var password_final: ?[]const u8 = null;
        var path_final: ?[]const u8 = null;
        
        if (user != null) {
            const user_start = scheme.len + 3; // After "://"
            username_final = full_url[user_start..user_start + user.?.len];
            
            if (pwd != null) {
                const pwd_start = user_start + user.?.len + 1; // After user and ":"
                password_final = full_url[pwd_start..pwd_start + pwd.?.len];
            }
        }
        
        if (path != null) {
            // Find path after port
            const port_str = try std.fmt.allocPrint(allocator, ":{d}/", .{port});
            defer allocator.free(port_str);
            if (std.mem.indexOf(u8, full_url, port_str)) |path_start| {
                const path_begin = path_start + port_str.len;
                path_final = full_url[path_begin..path_begin + path.?.len];
            }
        }

        return Url{
            .full_url = full_url,
            .scheme = scheme_final,
            .host = host_final,
            .port = port,
            .username = username_final,
            .password = password_final,
            .path = path_final,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Url) void {
        // Only need to free the full_url since all other fields are slices into it
        self.allocator.free(self.full_url);
    }
};


test "url parsing without scheme" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var parsed_url = try Url.parse(allocator, "localhost:4222");
    defer parsed_url.deinit();

    try std.testing.expectEqualStrings("nats", parsed_url.scheme);
    try std.testing.expectEqualStrings("localhost", parsed_url.host);
    try std.testing.expectEqual(@as(u16, 4222), parsed_url.port);
    try std.testing.expectEqualStrings("nats://localhost:4222", parsed_url.full_url);
}

test "url parsing with scheme" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var parsed_url = try Url.parse(allocator, "nats://localhost:4223");
    defer parsed_url.deinit();

    try std.testing.expectEqualStrings("nats", parsed_url.scheme);
    try std.testing.expectEqualStrings("localhost", parsed_url.host);
    try std.testing.expectEqual(@as(u16, 4223), parsed_url.port);
}

test "url parsing with user info" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var parsed_url = try Url.parse(allocator, "nats://user:pass@localhost:4222");
    defer parsed_url.deinit();

    try std.testing.expectEqualStrings("nats", parsed_url.scheme);
    try std.testing.expectEqualStrings("localhost", parsed_url.host);
    try std.testing.expectEqual(@as(u16, 4222), parsed_url.port);
    try std.testing.expectEqualStrings("user", parsed_url.username.?);
    try std.testing.expectEqualStrings("pass", parsed_url.password.?);
}

test "url parsing discovered servers" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_urls = [_][]const u8{
        "172.26.0.3:4222",
        "172.26.0.2:4222",
    };

    for (test_urls) |test_url| {
        var parsed_url = try Url.parse(allocator, test_url);
        defer parsed_url.deinit();

        try std.testing.expectEqualStrings("nats", parsed_url.scheme);
        try std.testing.expectEqual(@as(u16, 4222), parsed_url.port);
    }
}