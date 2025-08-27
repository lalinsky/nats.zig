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

pub const ValidationError = error{
    EmptyName,
    NameTooLong,
    InvalidCharacter,
};

const MAX_NAME_LENGTH = 32;

/// Validates a NATS stream name according to NATS specifications.
/// Stream names must contain only alphanumeric characters (a-z, A-Z, 0-9),
/// hyphens (-), and underscores (_).
/// Length should be under 32 characters for filesystem compatibility.
pub fn validateStreamName(name: []const u8) ValidationError!void {
    if (name.len == 0) {
        return ValidationError.EmptyName;
    }
    
    if (name.len > MAX_NAME_LENGTH) {
        return ValidationError.NameTooLong;
    }
    
    for (name) |c| {
        if (!isValidNameCharacter(c)) {
            return ValidationError.InvalidCharacter;
        }
    }
}

/// Validates a NATS consumer name (including durable names) according to NATS specifications.
/// Consumer names must contain only alphanumeric characters (a-z, A-Z, 0-9),
/// hyphens (-), and underscores (_).
/// Length should be under 32 characters for filesystem compatibility.
pub fn validateConsumerName(name: []const u8) ValidationError!void {
    if (name.len == 0) {
        return ValidationError.EmptyName;
    }
    
    if (name.len > MAX_NAME_LENGTH) {
        return ValidationError.NameTooLong;
    }
    
    for (name) |c| {
        if (!isValidNameCharacter(c)) {
            return ValidationError.InvalidCharacter;
        }
    }
}

/// Checks if a character is valid for stream/consumer names.
/// Valid characters are: alphanumeric (a-z, A-Z, 0-9), hyphens (-), underscores (_)
fn isValidNameCharacter(c: u8) bool {
    return switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9', '-', '_' => true,
        else => false,
    };
}

// Tests
const testing = std.testing;

test "validateStreamName - valid names" {
    try validateStreamName("validName");
    try validateStreamName("VALID_NAME");
    try validateStreamName("valid-name");
    try validateStreamName("valid123");
    try validateStreamName("a");
    try validateStreamName("123");
    try validateStreamName("stream_name-123");
}

test "validateStreamName - empty name" {
    try testing.expectError(ValidationError.EmptyName, validateStreamName(""));
}

test "validateStreamName - name too long" {
    const long_name = "this_is_a_very_long_stream_name_that_exceeds_the_maximum_allowed_length";
    try testing.expectError(ValidationError.NameTooLong, validateStreamName(long_name));
}

test "validateStreamName - invalid characters" {
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid.name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid*name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid>name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid/name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid\\name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid\tname"));
    try testing.expectError(ValidationError.InvalidCharacter, validateStreamName("invalid\nname"));
}

test "validateConsumerName - valid names" {
    try validateConsumerName("validName");
    try validateConsumerName("VALID_NAME");
    try validateConsumerName("valid-name");
    try validateConsumerName("valid123");
    try validateConsumerName("a");
    try validateConsumerName("123");
    try validateConsumerName("consumer_name-123");
}

test "validateConsumerName - empty name" {
    try testing.expectError(ValidationError.EmptyName, validateConsumerName(""));
}

test "validateConsumerName - name too long" {
    const long_name = "this_is_a_very_long_consumer_name_that_exceeds_the_maximum_allowed_length";
    try testing.expectError(ValidationError.NameTooLong, validateConsumerName(long_name));
}

test "validateConsumerName - invalid characters" {
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid.name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid*name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid>name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid/name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid\\name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid name"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid\tname"));
    try testing.expectError(ValidationError.InvalidCharacter, validateConsumerName("invalid\nname"));
}

test "isValidNameCharacter - valid characters" {
    try testing.expect(isValidNameCharacter('a'));
    try testing.expect(isValidNameCharacter('z'));
    try testing.expect(isValidNameCharacter('A'));
    try testing.expect(isValidNameCharacter('Z'));
    try testing.expect(isValidNameCharacter('0'));
    try testing.expect(isValidNameCharacter('9'));
    try testing.expect(isValidNameCharacter('-'));
    try testing.expect(isValidNameCharacter('_'));
}

test "isValidNameCharacter - invalid characters" {
    try testing.expect(!isValidNameCharacter('.'));
    try testing.expect(!isValidNameCharacter('*'));
    try testing.expect(!isValidNameCharacter('>'));
    try testing.expect(!isValidNameCharacter('/'));
    try testing.expect(!isValidNameCharacter('\\'));
    try testing.expect(!isValidNameCharacter(' '));
    try testing.expect(!isValidNameCharacter('\t'));
    try testing.expect(!isValidNameCharacter('\n'));
    try testing.expect(!isValidNameCharacter('@'));
    try testing.expect(!isValidNameCharacter('#'));
    try testing.expect(!isValidNameCharacter('$'));
}