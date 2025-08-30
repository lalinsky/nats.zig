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
const testing = std.testing;
const nats = @import("nats");
const Socket = nats.Socket;

test "Socket connect and close" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    socket.close();
}

test "Socket setNonBlocking" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    try socket.setNonBlocking();
}

test "Socket setReadTimeout" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    try socket.setReadTimeout(5000);
}

test "Socket setWriteTimeout" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    try socket.setWriteTimeout(5000);
}

test "Socket setKeepAlive" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    try socket.setKeepAlive(true);
    try socket.setKeepAlive(false);
}

test "Socket write" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    const data = "hello world";
    const bytes_written = try socket.write(data);
    try testing.expect(bytes_written <= data.len);
    try testing.expect(bytes_written > 0);
}

test "Socket read" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    var buf: [1024]u8 = undefined;
    const bytes_read = try socket.read(&buf);
    try testing.expect(bytes_read > 0);
}

test "Socket writev" {
    const socket = try Socket.connect(testing.allocator, "127.0.0.1", 14222);
    defer socket.close();

    const data1 = "hello ";
    const data2 = "world";
    var iovecs = [_]std.posix.iovec_const{
        .{ .base = data1.ptr, .len = data1.len },
        .{ .base = data2.ptr, .len = data2.len },
    };

    const bytes_written = try socket.writev(iovecs[0..]);
    try testing.expect(bytes_written <= data1.len + data2.len);
    try testing.expect(bytes_written > 0);
}
