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

/// Parse ISO 8601 timestamp to nanoseconds since Unix epoch
/// Format: "2025-09-05T05:36:18.039840444Z"
pub fn parseTimestamp(timestamp_str: []const u8) !u64 {
    // Simple ISO 8601 parser for the format used by NATS
    // "2025-09-05T05:36:18.039840444Z"
    if (timestamp_str.len < 20) return error.InvalidTimestamp;
    if (timestamp_str[timestamp_str.len - 1] != 'Z') return error.InvalidTimestamp;
    
    // Parse date part: YYYY-MM-DD
    const year = try std.fmt.parseInt(u16, timestamp_str[0..4], 10);
    if (timestamp_str[4] != '-') return error.InvalidTimestamp;
    const month = try std.fmt.parseInt(u8, timestamp_str[5..7], 10);
    if (timestamp_str[7] != '-') return error.InvalidTimestamp;
    const day = try std.fmt.parseInt(u8, timestamp_str[8..10], 10);
    if (timestamp_str[10] != 'T') return error.InvalidTimestamp;
    
    // Parse time part: HH:MM:SS
    const hour = try std.fmt.parseInt(u8, timestamp_str[11..13], 10);
    if (timestamp_str[13] != ':') return error.InvalidTimestamp;
    const minute = try std.fmt.parseInt(u8, timestamp_str[14..16], 10);
    if (timestamp_str[16] != ':') return error.InvalidTimestamp;
    const second = try std.fmt.parseInt(u8, timestamp_str[17..19], 10);
    
    // Parse nanoseconds if present
    var nanoseconds: u32 = 0;
    if (timestamp_str.len > 20 and timestamp_str[19] == '.') {
        const nanos_start = 20;
        const nanos_end = timestamp_str.len - 1; // exclude 'Z'
        if (nanos_end > nanos_start) {
            const nanos_str = timestamp_str[nanos_start..nanos_end];
            // Pad or truncate to 9 digits (nanoseconds)
            var nanos_buf: [9]u8 = .{'0'} ** 9;
            const copy_len = @min(nanos_str.len, 9);
            @memcpy(nanos_buf[0..copy_len], nanos_str[0..copy_len]);
            nanoseconds = try std.fmt.parseInt(u32, &nanos_buf, 10);
        }
    }
    
    // Convert to Unix timestamp (nanoseconds since epoch)
    const days_since_epoch = daysSinceEpoch(year, month, day);
    const seconds_today = @as(u64, hour) * 3600 + @as(u64, minute) * 60 + @as(u64, second);
    const total_seconds = days_since_epoch * 24 * 3600 + seconds_today;
    const total_nanos = total_seconds * std.time.ns_per_s + @as(u64, nanoseconds);
    
    return total_nanos;
}

/// Helper function to calculate days since Unix epoch (1970-01-01)
fn daysSinceEpoch(year: u16, month: u8, day: u8) u64 {
    // Days in each month (non-leap year)
    const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    
    var days: u64 = 0;
    
    // Add days for complete years since 1970
    var y: u16 = 1970;
    while (y < year) : (y += 1) {
        if (isLeapYear(y)) {
            days += 366;
        } else {
            days += 365;
        }
    }
    
    // Add days for complete months in the current year
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        days += days_in_month[m - 1];
        // Add extra day for February in leap years
        if (m == 2 and isLeapYear(year)) {
            days += 1;
        }
    }
    
    // Add remaining days
    days += day - 1; // -1 because day 1 of month = 0 days elapsed
    
    return days;
}

/// Helper function to determine if a year is a leap year
fn isLeapYear(year: u16) bool {
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
}

// Unit tests
test "parseTimestamp basic format" {
    // 2025-09-05T05:36:18Z -> September 5, 2025, 05:36:18 UTC
    const timestamp = try parseTimestamp("2025-09-05T05:36:18Z");
    
    // Expected: days since epoch * seconds/day * nanoseconds/second + time_of_day_nanos
    const expected_days = daysSinceEpoch(2025, 9, 5);
    const expected_seconds = expected_days * 24 * 3600 + 5 * 3600 + 36 * 60 + 18;
    const expected_nanos = expected_seconds * std.time.ns_per_s;
    
    try std.testing.expectEqual(expected_nanos, timestamp);
}

test "parseTimestamp with nanoseconds" {
    // 2025-09-05T05:36:18.039840444Z
    const timestamp = try parseTimestamp("2025-09-05T05:36:18.039840444Z");
    
    const expected_days = daysSinceEpoch(2025, 9, 5);
    const expected_seconds = expected_days * 24 * 3600 + 5 * 3600 + 36 * 60 + 18;
    const expected_nanos = expected_seconds * std.time.ns_per_s + 39840444; // .039840444 seconds in nanos
    
    try std.testing.expectEqual(expected_nanos, timestamp);
}

test "parseTimestamp with partial nanoseconds" {
    // Test different nanosecond precision levels
    try std.testing.expectEqual(
        try parseTimestamp("2025-01-01T00:00:00.1Z"),
        try parseTimestamp("2025-01-01T00:00:00.100000000Z")
    );
    
    try std.testing.expectEqual(
        try parseTimestamp("2025-01-01T00:00:00.12Z"),
        try parseTimestamp("2025-01-01T00:00:00.120000000Z")
    );
    
    try std.testing.expectEqual(
        try parseTimestamp("2025-01-01T00:00:00.123Z"),
        try parseTimestamp("2025-01-01T00:00:00.123000000Z")
    );
}

test "parseTimestamp Unix epoch" {
    // 1970-01-01T00:00:00Z should be exactly 0
    const timestamp = try parseTimestamp("1970-01-01T00:00:00Z");
    try std.testing.expectEqual(@as(u64, 0), timestamp);
}

test "parseTimestamp Unix epoch plus one day" {
    // 1970-01-02T00:00:00Z should be exactly 1 day in nanoseconds
    const timestamp = try parseTimestamp("1970-01-02T00:00:00Z");
    const expected = 24 * 3600 * std.time.ns_per_s;
    try std.testing.expectEqual(expected, timestamp);
}

test "parseTimestamp leap year handling" {
    // Test February 29 in leap year 2024
    const timestamp = try parseTimestamp("2024-02-29T12:00:00Z");
    
    // Verify it doesn't crash and produces a reasonable result
    try std.testing.expect(timestamp > 0);
    
    // Test that Feb 28 and Mar 1 are exactly 1 day apart in a leap year
    const feb28 = try parseTimestamp("2024-02-28T00:00:00Z");
    const mar01 = try parseTimestamp("2024-03-01T00:00:00Z");
    const expected_diff = 2 * 24 * 3600 * std.time.ns_per_s; // 2 days (28->29, 29->1)
    try std.testing.expectEqual(expected_diff, mar01 - feb28);
}

test "parseTimestamp error cases" {
    // Too short
    try std.testing.expectError(error.InvalidTimestamp, parseTimestamp("2025-01-01"));
    
    // Missing Z
    try std.testing.expectError(error.InvalidTimestamp, parseTimestamp("2025-01-01T00:00:00"));
    
    // Invalid format
    try std.testing.expectError(error.InvalidTimestamp, parseTimestamp("2025/01/01T00:00:00Z"));
    try std.testing.expectError(error.InvalidTimestamp, parseTimestamp("2025-01-01 00:00:00Z"));
    try std.testing.expectError(error.InvalidTimestamp, parseTimestamp("2025-01-01T00-00-00Z"));
    
    // Invalid numbers
    try std.testing.expectError(error.InvalidCharacter, parseTimestamp("abcd-01-01T00:00:00Z"));
}

test "isLeapYear" {
    // Standard cases
    try std.testing.expect(isLeapYear(2024)); // Divisible by 4
    try std.testing.expect(!isLeapYear(2023)); // Not divisible by 4
    
    // Century years (divisible by 100 but not 400)
    try std.testing.expect(!isLeapYear(1900)); // Divisible by 100, not by 400
    try std.testing.expect(!isLeapYear(2100)); // Divisible by 100, not by 400
    
    // Quad-century years (divisible by 400)
    try std.testing.expect(isLeapYear(2000)); // Divisible by 400
    try std.testing.expect(isLeapYear(2400)); // Divisible by 400
}

test "daysSinceEpoch" {
    // Unix epoch should be 0 days
    try std.testing.expectEqual(@as(u64, 0), daysSinceEpoch(1970, 1, 1));
    
    // Jan 2, 1970 should be 1 day
    try std.testing.expectEqual(@as(u64, 1), daysSinceEpoch(1970, 1, 2));
    
    // Jan 1, 1971 should be 365 days (1970 was not a leap year)
    try std.testing.expectEqual(@as(u64, 365), daysSinceEpoch(1971, 1, 1));
    
    // Jan 1, 1973 should be 365 + 365 + 366 = 1096 days (1972 was leap year)
    try std.testing.expectEqual(@as(u64, 1096), daysSinceEpoch(1973, 1, 1));
}