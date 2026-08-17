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

/// Convert a duration into a duration-form `std.Io.Timeout` on the awake clock.
pub fn timeout(duration: Io.Duration) Io.Timeout {
    return .{ .duration = .{ .raw = duration, .clock = .awake } };
}

/// Absolute deadline `duration` from now on the awake clock.
pub fn deadline(io: Io, duration: Io.Duration) Io.Timeout {
    return timeout(duration).toDeadline(io);
}

/// Whether a deadline-form timeout has expired. `.none` never expires.
/// Callers must pass timeouts produced by `deadline` (or
/// `Io.Timeout.toDeadline`), never raw durations.
pub fn expired(io: Io, t: Io.Timeout) bool {
    return switch (t) {
        .none => false,
        .deadline => |d| d.untilNow(io).raw.nanoseconds >= 0,
        .duration => unreachable,
    };
}

/// Time until a deadline-form timeout, or null if it has expired.
/// `.none` never expires and returns `Io.Duration.max`.
pub fn remaining(io: Io, t: Io.Timeout) ?Io.Duration {
    switch (t) {
        .none => return .max,
        .deadline => |d| {
            const until = d.durationFromNow(io).raw;
            if (until.nanoseconds <= 0) return null;
            return until;
        },
        .duration => unreachable,
    }
}

/// The earlier of two deadline-form timeouts (`.none` counts as never).
pub fn earlierDeadline(a: Io.Timeout, b: Io.Timeout) Io.Timeout {
    if (a == .none) return b;
    if (b == .none) return a;
    return if (a.deadline.raw.nanoseconds <= b.deadline.raw.nanoseconds) a else b;
}

/// Monotonic timestamp for elapsed-time measurements.
pub fn now(io: Io) Io.Timestamp {
    return .now(io, .awake);
}
