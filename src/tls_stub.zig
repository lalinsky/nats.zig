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

//! Stub used when the `use_tls` build option is disabled.
//!
//! It mirrors just enough of the tls.zig nonblock API surface for
//! `connection.zig` to type check. The bodies are never reached: TLS
//! connections fail with `error.TlsNotConfigured` before any of this
//! is called.

const std = @import("std");

pub const input_buffer_len: usize = 0;
pub const output_buffer_len: usize = 0;

pub const Cipher = struct {};

pub const config = struct {
    pub const Client = struct {
        host: []const u8 = "",
        root_ca: std.crypto.Certificate.Bundle = .empty,
        insecure_skip_verify: bool = false,
        auth: ?*CertKeyPair = null,
        rng: std.Random = undefined,
        now: std.Io.Timestamp = undefined,
    };

    pub const CertKeyPair = struct {
        pub fn fromFilePath(
            allocator: std.mem.Allocator,
            io: std.Io,
            dir: std.Io.Dir,
            cert_path: []const u8,
            key_path: []const u8,
        ) !CertKeyPair {
            _ = allocator;
            _ = io;
            _ = dir;
            _ = cert_path;
            _ = key_path;
            return error.TlsNotConfigured;
        }

        pub fn deinit(self: *CertKeyPair, allocator: std.mem.Allocator) void {
            _ = self;
            _ = allocator;
        }
    };
};

pub const nonblock = struct {
    pub const Client = struct {
        pub fn init(opt: config.Client) Client {
            _ = opt;
            return .{};
        }

        pub fn done(self: Client) bool {
            _ = self;
            return false;
        }

        pub fn cipher(self: Client) ?Cipher {
            _ = self;
            return null;
        }

        pub fn run(self: *Client, recv_buf: []const u8, send_buf: []u8) !struct {
            recv_pos: usize,
            send_pos: usize,
            unused_recv: []const u8,
            send: []const u8,
        } {
            _ = self;
            _ = recv_buf;
            _ = send_buf;
            return error.TlsNotConfigured;
        }
    };

    pub const Connection = struct {
        inner: Inner = .{},

        pub const Inner = struct {
            key_update_requested: bool = false,
        };

        pub fn init(c: Cipher) Connection {
            _ = c;
            return .{};
        }

        pub fn encrypt(self: *Connection, cleartext: []const u8, ciphertext: []u8) !struct {
            cleartext_pos: usize = 0,
            unused_cleartext: []const u8,
            ciphertext: []u8,
        } {
            _ = self;
            _ = cleartext;
            _ = ciphertext;
            return error.TlsNotConfigured;
        }

        pub fn decrypt(self: *Connection, ciphertext: []const u8, cleartext: []u8) !struct {
            ciphertext_pos: usize,
            unused_ciphertext: []const u8,
            cleartext: []u8,
            closed: bool = false,
        } {
            _ = self;
            _ = ciphertext;
            _ = cleartext;
            return error.TlsNotConfigured;
        }

        pub fn close(self: *Connection, ciphertext: []u8) ![]const u8 {
            _ = self;
            _ = ciphertext;
            return error.TlsNotConfigured;
        }
    };
};
