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
const Io = std.Io;

/// Convert a nanosecond duration into an `std.Io.Timeout`.
pub fn nsTimeout(ns: u64) Io.Timeout {
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(ns)), .clock = .awake } };
}

/// Convert a millisecond duration into an `std.Io.Timeout`.
pub fn msTimeout(ms: u64) Io.Timeout {
    return .{ .duration = .{ .raw = .fromMilliseconds(@intCast(ms)), .clock = .awake } };
}
