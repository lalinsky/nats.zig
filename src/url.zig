// Copyright 2025 Lukas Lalinsky
// Copyright 2015-2018 The NATS Authors
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

// Port of NATS C library URL parsing from refs/nats.c/src/url.c
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const UrlError = error{
    InvalidUrl,
    InvalidUrlScheme,
    OutOfMemory,
};

const ParsedComponents = struct {
    scheme: []const u8 = "",
    host: []const u8 = "",
    port: u16 = 4222,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    path: ?[]const u8 = null,
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

    fn parseComponents(url_str: []const u8) ParsedComponents {
        if (url_str.len == 0) {
            return ParsedComponents{};
        }

        // Trim whitespace
        const trimmed = std.mem.trim(u8, url_str, " \t\n\r");
        var components = ParsedComponents{};
        var ptr: []const u8 = trimmed;

        // Parse scheme (matching C library logic)
        if (std.mem.indexOf(u8, trimmed, "://")) |scheme_end| {
            components.scheme = trimmed[0..scheme_end];
            ptr = trimmed[scheme_end + 3 ..];
        } else {
            // Default to "nats" scheme if none provided (matching C library)
            components.scheme = "nats";
            ptr = trimmed;
        }

        // Parse user info (user:password@)
        if (std.mem.lastIndexOf(u8, ptr, "@")) |at_pos| {
            const user_info = ptr[0..at_pos];
            components.host = ptr[at_pos + 1 ..];

            if (user_info.len > 0) {
                if (std.mem.indexOf(u8, user_info, ":")) |colon_pos| {
                    if (colon_pos > 0) {
                        components.username = user_info[0..colon_pos];
                    }
                    if (colon_pos + 1 < user_info.len) {
                        components.password = user_info[colon_pos + 1 ..];
                    }
                } else {
                    components.username = user_info;
                }
            }
        } else {
            components.host = ptr;
        }

        // Parse host and port (handle IPv6 addresses)
        var host_end = components.host.len;
        
        // Search for end of IPv6 address (if applicable)
        var search_start: usize = 0;
        if (std.mem.lastIndexOf(u8, components.host, "]")) |ipv6_end| {
            search_start = ipv6_end;
        }

        // From that point, search for the last ':' character
        if (std.mem.lastIndexOfScalar(u8, components.host[search_start..], ':')) |relative_pos| {
            const port_start = search_start + relative_pos;
            const port_str = components.host[port_start + 1 ..];
            
            // Check if there's a path after the port
            if (std.mem.indexOf(u8, port_str, "/")) |path_start| {
                const port_only = port_str[0..path_start];
                components.path = port_str[path_start + 1 ..];
                components.port = std.fmt.parseInt(u16, port_only, 10) catch 4222;
            } else {
                components.port = std.fmt.parseInt(u16, port_str, 10) catch 4222;
            }
            host_end = port_start;
        } else {
            // Check for path without port
            if (std.mem.indexOf(u8, components.host, "/")) |path_start| {
                components.path = components.host[path_start + 1 ..];
                host_end = path_start;
            }
        }

        components.host = components.host[0..host_end];
        if (components.host.len == 0) {
            components.host = "localhost";
        }

        return components;
    }

    pub fn parse(allocator: Allocator, url_str: []const u8) !Url {
        const components = parseComponents(url_str);
        if (components.scheme.len == 0) {
            return UrlError.InvalidUrl;
        }
        
        // Only accept "nats" scheme
        if (!std.mem.eql(u8, components.scheme, "nats")) {
            return UrlError.InvalidUrlScheme;
        }

        // Build full URL
        const userval = if (components.username) |u| u else "";
        const usep = if (components.password != null) ":" else "";
        const pwdval = if (components.password) |p| p else "";
        const hsep = if (components.username != null) "@" else "";
        const pathsep = if (components.path != null) "/" else "";
        const pathval = if (components.path) |p| p else "";

        const full_url = try std.fmt.allocPrint(allocator, "{s}://{s}{s}{s}{s}{s}:{d}{s}{s}", .{
            components.scheme, userval, usep, pwdval, hsep, components.host, components.port, pathsep, pathval,
        });

        // Parse the normalized URL to get slices into full_url
        const normalized_components = parseComponents(full_url);

        return Url{
            .full_url = full_url,
            .scheme = normalized_components.scheme,
            .host = normalized_components.host,
            .port = normalized_components.port,
            .username = normalized_components.username,
            .password = normalized_components.password,
            .path = normalized_components.path,
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