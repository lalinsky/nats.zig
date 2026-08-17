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

//! Parsing of NATS credentials (.creds) files: the user JWT and the NKey
//! seed, each wrapped in armored blocks that may be surrounded by
//! arbitrary text. Only the line following a BEGIN marker is used,
//! matching the official clients.

const std = @import("std");

pub const Error = error{
    MissingUserJwt,
    MissingNKeySeed,
};

const jwt_begin_marker = "-----BEGIN NATS USER JWT-----";
const seed_begin_marker = "-----BEGIN USER NKEY SEED-----";

/// Slices into the parsed content; they are only valid as long as the
/// source buffer, and the seed is as sensitive as the buffer it points
/// into (scrub the buffer, not the slices).
pub const Credentials = struct {
    jwt: []const u8,
    seed: []const u8,
};

pub fn parse(content: []const u8) Error!Credentials {
    return .{
        .jwt = findBlockValue(content, jwt_begin_marker) orelse return Error.MissingUserJwt,
        .seed = findBlockValue(content, seed_begin_marker) orelse return Error.MissingNKeySeed,
    };
}

/// Return the first non-empty line following the line containing `marker`.
fn findBlockValue(content: []const u8, marker: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, content, '\n');
    var in_block = false;
    while (lines.next()) |line| {
        if (!in_block) {
            if (std.mem.indexOf(u8, line, marker) != null) in_block = true;
            continue;
        }
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (std.mem.startsWith(u8, trimmed, "---")) {
            return null; // END marker before any value
        }
        return trimmed;
    }
    return null;
}

test "parse a well-formed creds file" {
    const content =
        "-----BEGIN NATS USER JWT-----\n" ++
        "eyJ0eXAiOiJKV1QifQ.payload.sig\n" ++
        "------END NATS USER JWT------\n" ++
        "\n" ++
        "************************* IMPORTANT *************************\n" ++
        "NKEY Seed printed below can be used to sign and prove identity.\n" ++
        "\n" ++
        "-----BEGIN USER NKEY SEED-----\n" ++
        "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY\n" ++
        "------END USER NKEY SEED------\n";

    const creds = try parse(content);
    try std.testing.expectEqualStrings("eyJ0eXAiOiJKV1QifQ.payload.sig", creds.jwt);
    try std.testing.expectEqualStrings("SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY", creds.seed);
}

test "parse tolerates CRLF line endings" {
    const content =
        "-----BEGIN NATS USER JWT-----\r\n" ++
        "the.jwt.value\r\n" ++
        "------END NATS USER JWT------\r\n" ++
        "-----BEGIN USER NKEY SEED-----\r\n" ++
        "SUATHESEED\r\n" ++
        "------END USER NKEY SEED------\r\n";

    const creds = try parse(content);
    try std.testing.expectEqualStrings("the.jwt.value", creds.jwt);
    try std.testing.expectEqualStrings("SUATHESEED", creds.seed);
}

test "missing blocks are reported" {
    try std.testing.expectError(Error.MissingUserJwt, parse("not a creds file"));

    const jwt_only =
        "-----BEGIN NATS USER JWT-----\n" ++
        "the.jwt.value\n" ++
        "------END NATS USER JWT------\n";
    try std.testing.expectError(Error.MissingNKeySeed, parse(jwt_only));

    const empty_block =
        "-----BEGIN NATS USER JWT-----\n" ++
        "------END NATS USER JWT------\n";
    try std.testing.expectError(Error.MissingUserJwt, parse(empty_block));
}
