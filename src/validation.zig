// Validation functions according to ADR-6 naming rules
const std = @import("std");

/// filename-safe: printable ASCII except dot, asterisk, gt, fwd-slash, backslash
/// Maximum 255 characters
pub fn isFilenameSafe(c: u8) bool {
    // Must be printable ASCII (33-126)
    if (c < 33 or c > 126) {
        return false;
    }

    // Exclude prohibited characters
    return !(c == '.' or c == '*' or c == '>' or c == '/' or c == '\\');
}

/// restricted-term: A-Z, a-z, 0-9, dash, underscore only
pub fn isRestrictedTermChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '_';
}

/// limited-term: A-Z, a-z, 0-9, dash, underscore, fwd-slash, equals
pub fn isLimitedTermChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or
        c == '-' or c == '_' or c == '/' or c == '=';
}

/// term: printable ASCII except dot, asterisk or gt
pub fn isTermChar(c: u8) bool {
    // Must be printable ASCII (33-126)
    if (c < 33 or c > 126) {
        return false;
    }

    // Exclude dot, asterisk, gt
    return !(c == '.' or c == '*' or c == '>');
}

/// Validate stream name according to ADR-6: filename-safe, max 255 chars
pub fn validateStreamName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidStreamName;
    }

    if (name.len > 255) {
        return error.InvalidStreamName;
    }

    for (name) |c| {
        if (!isFilenameSafe(c)) {
            return error.InvalidStreamName;
        }
    }
}

/// Validate consumer name according to ADR-6: filename-safe, max 255 chars
pub fn validateConsumerName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidConsumerName;
    }

    if (name.len > 255) {
        return error.InvalidConsumerName;
    }

    for (name) |c| {
        if (!isFilenameSafe(c)) {
            return error.InvalidConsumerName;
        }
    }
}

/// Validate account name according to ADR-6: filename-safe, max 255 chars
pub fn validateAccountName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidAccountName;
    }

    if (name.len > 255) {
        return error.InvalidAccountName;
    }

    for (name) |c| {
        if (!isFilenameSafe(c)) {
            return error.InvalidAccountName;
        }
    }
}

/// Validate queue name according to ADR-6: term (no dots, asterisks, or gt)
pub fn validateQueueName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidQueueName;
    }

    if (name.len > 255) {
        return error.InvalidQueueName;
    }

    for (name) |c| {
        if (!isTermChar(c)) {
            return error.InvalidQueueName;
        }
    }
}

/// Validate subject according to ADR-6: term (dot term | asterisk)* (dot gt)?
pub fn validateSubject(subject: []const u8) !void {
    if (subject.len == 0) {
        return error.InvalidSubject;
    }

    if (subject.len > 255) {
        return error.InvalidSubject;
    }

    var tokens = std.mem.splitScalar(u8, subject, '.');
    var has_seen_greater = false;

    while (tokens.next()) |token| {
        if (has_seen_greater) {
            // No tokens allowed after ">"
            return error.InvalidSubject;
        }

        if (token.len == 0) {
            // Empty token (consecutive dots)
            return error.InvalidSubject;
        }

        // Check if token is ">"
        if (std.mem.eql(u8, token, ">")) {
            has_seen_greater = true;
            continue;
        }

        // Check if token is "*" (wildcard)
        if (std.mem.eql(u8, token, "*")) {
            continue;
        }

        // Regular token - validate each character
        for (token) |c| {
            if (!isTermChar(c)) {
                return error.InvalidSubject;
            }
        }
    }
}

/// Validate KV bucket name according to ADR-6: restricted-term
pub fn validateKVBucketName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidKVBucketName;
    }

    if (name.len > 255) {
        return error.InvalidKVBucketName;
    }

    for (name) |c| {
        if (!isRestrictedTermChar(c)) {
            return error.InvalidKVBucketName;
        }
    }
}

/// Validate KV key name according to ADR-6: limited-term (dot limited-term)*
/// No leading or trailing dots, keys starting with "_kv" are reserved
pub fn validateKVKeyName(key: []const u8) !void {
    if (key.len == 0) {
        return error.InvalidKVKeyName;
    }

    if (key.len > 255) {
        return error.InvalidKVKeyName;
    }

    // Check for leading or trailing dots
    if (key[0] == '.' or key[key.len - 1] == '.') {
        return error.InvalidKVKeyName;
    }

    // Check for reserved prefix
    if (std.mem.startsWith(u8, key, "_kv")) {
        return error.InvalidKVKeyName;
    }

    // Split by dots and validate each token
    var tokens = std.mem.splitScalar(u8, key, '.');

    while (tokens.next()) |token| {
        if (token.len == 0) {
            // Empty token (consecutive dots)
            return error.InvalidKVKeyName;
        }

        // Validate each character in the token
        for (token) |c| {
            if (!isLimitedTermChar(c)) {
                return error.InvalidKVKeyName;
            }
        }
    }
}

/// Validate Object Store bucket name according to ADR-6: restricted-term
pub fn validateOSBucketName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidOSBucketName;
    }

    if (name.len > 255) {
        return error.InvalidOSBucketName;
    }

    for (name) |c| {
        if (!isRestrictedTermChar(c)) {
            return error.InvalidOSBucketName;
        }
    }
}

/// Validate Object Store object name according to ADR-6: limited-term with dots, no leading/trailing slashes or dots
pub fn validateOSObjectName(name: []const u8) !void {
    if (name.len == 0) {
        return error.InvalidOSObjectName;
    }

    if (name.len > 255) {
        return error.InvalidOSObjectName;
    }

    // Check for leading or trailing slashes/dots
    if (name[0] == '/' or name[name.len - 1] == '/' or
        name[0] == '.' or name[name.len - 1] == '.')
    {
        return error.InvalidOSObjectName;
    }

    // Validate each character - allow limited-term chars plus dots
    for (name) |c| {
        const valid = isLimitedTermChar(c) or c == '.';
        if (!valid) {
            return error.InvalidOSObjectName;
        }
    }
}

// Tests

test "filename-safe characters" {
    // Valid characters
    try std.testing.expect(isFilenameSafe('A'));
    try std.testing.expect(isFilenameSafe('z'));
    try std.testing.expect(isFilenameSafe('0'));
    try std.testing.expect(isFilenameSafe('9'));
    try std.testing.expect(isFilenameSafe('-'));
    try std.testing.expect(isFilenameSafe('_'));
    try std.testing.expect(isFilenameSafe('='));
    try std.testing.expect(isFilenameSafe('|'));
    try std.testing.expect(isFilenameSafe('?'));
    try std.testing.expect(isFilenameSafe('&'));
    try std.testing.expect(isFilenameSafe(':'));
    try std.testing.expect(isFilenameSafe('"'));
    try std.testing.expect(isFilenameSafe('<'));

    // Invalid characters
    try std.testing.expect(!isFilenameSafe('.'));
    try std.testing.expect(!isFilenameSafe('*'));
    try std.testing.expect(!isFilenameSafe('>'));
    try std.testing.expect(!isFilenameSafe('/'));
    try std.testing.expect(!isFilenameSafe('\\'));
    try std.testing.expect(!isFilenameSafe(' ')); // space
    try std.testing.expect(!isFilenameSafe('\t')); // tab
    try std.testing.expect(!isFilenameSafe('\n')); // newline
    try std.testing.expect(!isFilenameSafe(0)); // null
    try std.testing.expect(!isFilenameSafe(127)); // DEL
}

test "restricted-term characters" {
    // Valid characters
    try std.testing.expect(isRestrictedTermChar('A'));
    try std.testing.expect(isRestrictedTermChar('z'));
    try std.testing.expect(isRestrictedTermChar('0'));
    try std.testing.expect(isRestrictedTermChar('9'));
    try std.testing.expect(isRestrictedTermChar('-'));
    try std.testing.expect(isRestrictedTermChar('_'));

    // Invalid characters
    try std.testing.expect(!isRestrictedTermChar('.'));
    try std.testing.expect(!isRestrictedTermChar('*'));
    try std.testing.expect(!isRestrictedTermChar('>'));
    try std.testing.expect(!isRestrictedTermChar('/'));
    try std.testing.expect(!isRestrictedTermChar('\\'));
    try std.testing.expect(!isRestrictedTermChar('='));
    try std.testing.expect(!isRestrictedTermChar(' '));
}

test "limited-term characters" {
    // Valid characters
    try std.testing.expect(isLimitedTermChar('A'));
    try std.testing.expect(isLimitedTermChar('z'));
    try std.testing.expect(isLimitedTermChar('0'));
    try std.testing.expect(isLimitedTermChar('9'));
    try std.testing.expect(isLimitedTermChar('-'));
    try std.testing.expect(isLimitedTermChar('_'));
    try std.testing.expect(isLimitedTermChar('/'));
    try std.testing.expect(isLimitedTermChar('='));

    // Invalid characters
    try std.testing.expect(!isLimitedTermChar('.'));
    try std.testing.expect(!isLimitedTermChar('*'));
    try std.testing.expect(!isLimitedTermChar('>'));
    try std.testing.expect(!isLimitedTermChar('\\'));
    try std.testing.expect(!isLimitedTermChar(' '));
}

test "validateStreamName" {
    // Valid names
    try validateStreamName("valid-stream_name");
    try validateStreamName("Stream123");
    try validateStreamName("name_with-dashes");
    try validateStreamName("UPPERCASE");
    try validateStreamName("mixed-Case_123");

    // 255 characters should be valid
    const max_name = "a" ** 255;
    try validateStreamName(max_name);

    // Invalid names
    try std.testing.expectError(error.InvalidStreamName, validateStreamName(""));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name.with.dots"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name*withasterisk"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name>withgt"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name/withslash"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name\\withbackslash"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name with space"));
    try std.testing.expectError(error.InvalidStreamName, validateStreamName("name\twith\ttab"));

    // 256 characters should be invalid
    const long_name = "a" ** 256;
    try std.testing.expectError(error.InvalidStreamName, validateStreamName(long_name));
}

test "validateConsumerName" {
    // Same rules as stream names
    try validateConsumerName("valid-consumer_name");
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName("invalid.name"));
    try std.testing.expectError(error.InvalidConsumerName, validateConsumerName(""));
}

test "validateSubject" {
    // Valid subjects
    try validateSubject("foo");
    try validateSubject("foo.bar");
    try validateSubject("foo.bar.baz");
    try validateSubject("foo.*.bar");
    try validateSubject("foo.>");
    try validateSubject(">");
    try validateSubject("foo.*");
    try validateSubject("foo.bar.>");

    // Invalid subjects
    try std.testing.expectError(error.InvalidSubject, validateSubject(""));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo..bar"));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo.>.bar"));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo.bar.>baz"));
    try std.testing.expectError(error.InvalidSubject, validateSubject(".foo"));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo."));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo bar"));
    try std.testing.expectError(error.InvalidSubject, validateSubject("foo\nbar"));
}

test "validateKVBucketName" {
    // Valid bucket names (restricted-term)
    try validateKVBucketName("valid_bucket-name");
    try validateKVBucketName("Bucket123");
    try validateKVBucketName("UPPERCASE");

    // Invalid bucket names
    try std.testing.expectError(error.InvalidKVBucketName, validateKVBucketName(""));
    try std.testing.expectError(error.InvalidKVBucketName, validateKVBucketName("bucket.with.dots"));
    try std.testing.expectError(error.InvalidKVBucketName, validateKVBucketName("bucket/with/slash"));
    try std.testing.expectError(error.InvalidKVBucketName, validateKVBucketName("bucket=equals"));
    try std.testing.expectError(error.InvalidKVBucketName, validateKVBucketName("bucket with space"));
}

test "validateKVKeyName" {
    // Valid keys
    try validateKVKeyName("valid_key-name");
    try validateKVKeyName("key/with/slashes");
    try validateKVKeyName("key=with=equals");
    try validateKVKeyName("valid.key.with.dots");

    // Invalid keys
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName(""));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("_kv_reserved"));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName(".leadingdot"));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("trailingdot."));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("key*withasterisk"));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("key>withgt"));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("key\\withbackslash"));
    try std.testing.expectError(error.InvalidKVKeyName, validateKVKeyName("key with space"));
}

test "validateOSBucketName" {
    // Valid bucket names (restricted-term)
    try validateOSBucketName("valid_bucket-name");
    try validateOSBucketName("Bucket123");
    try validateOSBucketName("UPPERCASE");

    // Invalid bucket names
    try std.testing.expectError(error.InvalidOSBucketName, validateOSBucketName(""));
    try std.testing.expectError(error.InvalidOSBucketName, validateOSBucketName("bucket.with.dots"));
    try std.testing.expectError(error.InvalidOSBucketName, validateOSBucketName("bucket/with/slash"));
    try std.testing.expectError(error.InvalidOSBucketName, validateOSBucketName("bucket=equals"));
    try std.testing.expectError(error.InvalidOSBucketName, validateOSBucketName("bucket with space"));
}

test "validateOSObjectName" {
    // Valid object names
    try validateOSObjectName("valid-object/name_123.txt");
    try validateOSObjectName("object_with-dashes");
    try validateOSObjectName("object/with/slashes");
    try validateOSObjectName("object=with=equals");
    try validateOSObjectName("object.with.dots");

    // Invalid object names
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName(""));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("/starts-with-slash"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("ends-with-slash/"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName(".starts-with-dot"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("ends-with-dot."));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("object*with*asterisk"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("object>with>gt"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("object\\with\\backslash"));
    try std.testing.expectError(error.InvalidOSObjectName, validateOSObjectName("object with space"));
}
