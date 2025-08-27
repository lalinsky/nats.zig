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
const Connection = @import("connection.zig").Connection;
const Message = @import("message.zig").Message;

test "subscription basic setup" {
    const allocator = testing.allocator;

    // Test connection creation (basic state test)
    var conn = Connection.init(allocator, .{});
    defer conn.deinit();

    // Test initial state
    try testing.expect(conn.getStatus() == .disconnected);

    // Test subscription counter initialization
    try testing.expect(conn.next_sid.load(.monotonic) == 1);
}
