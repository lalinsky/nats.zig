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

//! NKeys: NATS Ed25519 keys in their base32 text encoding.
//!
//! An encoded key is base32 (RFC 4648 alphabet, no padding) over
//! `prefix || key material || crc16`, where the CRC16/XMODEM checksum is
//! stored little-endian. Seeds carry two packed prefix bytes (the seed
//! prefix and the public key type), public keys carry one.
//!
//! Only what the client needs is implemented: decoding a seed into a
//! signing key pair, encoding the corresponding public key, and signing
//! the server-provided nonce.

const std = @import("std");
const Ed25519 = std.crypto.sign.Ed25519;

pub const Error = error{
    InvalidEncoding,
    InvalidChecksum,
    InvalidPrefix,
    InvalidSeed,
};

/// Public key types, stored as the prefix byte value.
pub const KeyType = enum(u8) {
    account = 0, // encodes as 'A'
    cluster = 2 << 3, // encodes as 'C'
    server = 13 << 3, // encodes as 'N'
    operator = 14 << 3, // encodes as 'O'
    user = 20 << 3, // encodes as 'U'

    fn fromByte(b: u8) ?KeyType {
        return switch (b) {
            @intFromEnum(KeyType.account) => .account,
            @intFromEnum(KeyType.cluster) => .cluster,
            @intFromEnum(KeyType.server) => .server,
            @intFromEnum(KeyType.operator) => .operator,
            @intFromEnum(KeyType.user) => .user,
            else => null,
        };
    }
};

/// Prefix byte marking an encoded seed ('S').
const seed_prefix: u8 = 18 << 3;

const raw_seed_len = 2 + 32 + 2; // packed prefixes + seed + crc
const raw_public_key_len = 1 + 32 + 2; // prefix + public key + crc

/// Length of an encoded seed ("SU...").
pub const seed_text_len = base32EncodedLen(raw_seed_len); // 58
/// Length of an encoded public key ("U...").
pub const public_key_text_len = base32EncodedLen(raw_public_key_len); // 56

/// An Ed25519 key pair decoded from an encoded seed.
pub const SeedKeyPair = struct {
    key_pair: Ed25519.KeyPair,
    key_type: KeyType,

    /// Decode an encoded seed ("SU..." for user seeds) and derive the key pair.
    pub fn fromSeed(encoded: []const u8) Error!SeedKeyPair {
        var raw: [raw_seed_len]u8 = undefined;
        defer std.crypto.secureZero(u8, &raw);

        if (encoded.len != seed_text_len) return Error.InvalidSeed;
        const decoded = try base32Decode(encoded, &raw);
        if (decoded.len != raw_seed_len) return Error.InvalidSeed;

        try verifyChecksum(&raw);

        if (raw[0] & 0xf8 != seed_prefix) return Error.InvalidPrefix;
        const key_type_byte = ((raw[0] & 0x07) << 5) | (raw[1] >> 3);
        const key_type = KeyType.fromByte(key_type_byte) orelse return Error.InvalidPrefix;

        const key_pair = Ed25519.KeyPair.generateDeterministic(raw[2..34].*) catch return Error.InvalidSeed;

        return .{ .key_pair = key_pair, .key_type = key_type };
    }

    /// Encode the public key ("U..." for user keys).
    pub fn publicKeyText(self: *const SeedKeyPair, out: *[public_key_text_len]u8) []const u8 {
        var raw: [raw_public_key_len]u8 = undefined;
        raw[0] = @intFromEnum(self.key_type);
        raw[1..33].* = self.key_pair.public_key.toBytes();
        appendChecksum(&raw);
        return base32Encode(&raw, out);
    }

    /// Sign a message (e.g. the server-provided nonce).
    pub fn sign(self: *const SeedKeyPair, msg: []const u8) Error![64]u8 {
        const signature = self.key_pair.sign(msg, null) catch return Error.InvalidSeed;
        return signature.toBytes();
    }

    /// Best-effort scrub of the secret key material.
    pub fn wipe(self: *SeedKeyPair) void {
        std.crypto.secureZero(u8, std.mem.asBytes(&self.key_pair));
    }
};

/// Encode a raw 32-byte seed as an NKeys seed string. Only needed for
/// generating keys (e.g. test fixtures); decoding is the common path.
pub fn encodeSeed(key_type: KeyType, seed: *const [32]u8, out: *[seed_text_len]u8) []const u8 {
    var raw: [raw_seed_len]u8 = undefined;
    defer std.crypto.secureZero(u8, &raw);

    const kt = @intFromEnum(key_type);
    raw[0] = seed_prefix | (kt >> 5);
    raw[1] = (kt & 0x1f) << 3;
    raw[2..34].* = seed.*;
    appendChecksum(&raw);
    return base32Encode(&raw, out);
}

// CRC16/XMODEM: polynomial 0x1021, initial value 0, stored little-endian
// in the trailing two bytes.

fn crc16(data: []const u8) u16 {
    var crc: u16 = 0;
    for (data) |b| {
        crc ^= @as(u16, b) << 8;
        for (0..8) |_| {
            if (crc & 0x8000 != 0) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

fn appendChecksum(raw: []u8) void {
    const crc = crc16(raw[0 .. raw.len - 2]);
    std.mem.writeInt(u16, raw[raw.len - 2 ..][0..2], crc, .little);
}

fn verifyChecksum(raw: []const u8) Error!void {
    const expected = std.mem.readInt(u16, raw[raw.len - 2 ..][0..2], .little);
    if (crc16(raw[0 .. raw.len - 2]) != expected) return Error.InvalidChecksum;
}

// Base32, RFC 4648 alphabet, no padding.

const base32_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

const base32_decode_table = blk: {
    var table: [256]u8 = @splat(0xff);
    for (base32_alphabet, 0..) |c, i| {
        table[c] = i;
    }
    break :blk table;
};

fn base32EncodedLen(raw_len: usize) usize {
    return (raw_len * 8 + 4) / 5;
}

fn base32Encode(data: []const u8, out: []u8) []const u8 {
    var bits: u16 = 0;
    var bit_count: u4 = 0;
    var pos: usize = 0;
    for (data) |b| {
        bits = (bits << 8) | b;
        bit_count += 8;
        while (bit_count >= 5) {
            bit_count -= 5;
            out[pos] = base32_alphabet[(bits >> bit_count) & 0x1f];
            pos += 1;
        }
    }
    if (bit_count > 0) {
        out[pos] = base32_alphabet[(bits << (5 - bit_count)) & 0x1f];
        pos += 1;
    }
    return out[0..pos];
}

fn base32Decode(encoded: []const u8, out: []u8) Error![]const u8 {
    var bits: u16 = 0;
    var bit_count: u4 = 0;
    var pos: usize = 0;
    for (encoded) |c| {
        const value = base32_decode_table[c];
        if (value == 0xff) return Error.InvalidEncoding;
        bits = (bits << 5) | value;
        bit_count += 5;
        if (bit_count >= 8) {
            bit_count -= 8;
            if (pos >= out.len) return Error.InvalidEncoding;
            out[pos] = @truncate(bits >> bit_count);
            pos += 1;
        }
    }
    return out[0..pos];
}

test "decode seed and derive public key" {
    // Example user credentials from the NATS documentation.
    const seed = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY";
    const expected_public = "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4";

    var kp = try SeedKeyPair.fromSeed(seed);
    defer kp.wipe();

    try std.testing.expectEqual(KeyType.user, kp.key_type);

    var buf: [public_key_text_len]u8 = undefined;
    try std.testing.expectEqualStrings(expected_public, kp.publicKeyText(&buf));
}

test "seed round trip" {
    const raw_seed: [32]u8 = @splat(42);

    var seed_buf: [seed_text_len]u8 = undefined;
    const encoded = encodeSeed(.user, &raw_seed, &seed_buf);
    try std.testing.expectEqual(seed_text_len, encoded.len);
    try std.testing.expectEqual(@as(u8, 'S'), encoded[0]);
    try std.testing.expectEqual(@as(u8, 'U'), encoded[1]);

    var kp = try SeedKeyPair.fromSeed(encoded);
    defer kp.wipe();
    try std.testing.expectEqual(KeyType.user, kp.key_type);

    var buf: [public_key_text_len]u8 = undefined;
    const public = kp.publicKeyText(&buf);
    try std.testing.expectEqual(@as(u8, 'U'), public[0]);
}

test "signature verifies against derived public key" {
    const raw_seed: [32]u8 = @splat(7);

    var seed_buf: [seed_text_len]u8 = undefined;
    var kp = try SeedKeyPair.fromSeed(encodeSeed(.user, &raw_seed, &seed_buf));
    defer kp.wipe();

    const nonce = "abcdefghijklmnop";
    const sig_bytes = try kp.sign(nonce);

    const signature = Ed25519.Signature.fromBytes(sig_bytes);
    try signature.verify(nonce, kp.key_pair.public_key);
}

test "corrupted seed is rejected" {
    const seed = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY";

    // Flip a character in the seed body: the checksum must catch it.
    var corrupted: [seed_text_len]u8 = seed.*;
    corrupted[10] = if (corrupted[10] == 'A') 'B' else 'A';
    try std.testing.expectError(Error.InvalidChecksum, SeedKeyPair.fromSeed(&corrupted));

    // Invalid characters and wrong lengths are rejected outright.
    var invalid: [seed_text_len]u8 = seed.*;
    invalid[10] = '0'; // '0' is not in the RFC 4648 base32 alphabet
    try std.testing.expectError(Error.InvalidEncoding, SeedKeyPair.fromSeed(&invalid));
    try std.testing.expectError(Error.InvalidSeed, SeedKeyPair.fromSeed(seed[0 .. seed.len - 1]));
}

test "public key seed is rejected as a prefix error" {
    // Encode with an account key type but corrupt the seed prefix to 'U'.
    const raw_seed: [32]u8 = @splat(1);
    var seed_buf: [seed_text_len]u8 = undefined;
    var raw: [raw_seed_len]u8 = undefined;
    _ = encodeSeed(.user, &raw_seed, &seed_buf);
    _ = try base32Decode(seed_buf[0..seed_text_len], &raw);
    raw[0] = @intFromEnum(KeyType.user); // not the seed prefix
    appendChecksum(&raw);
    var reencoded: [seed_text_len]u8 = undefined;
    _ = base32Encode(&raw, &reencoded);
    try std.testing.expectError(Error.InvalidPrefix, SeedKeyPair.fromSeed(&reencoded));
}
