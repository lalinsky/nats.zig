const std = @import("std");
const Mutex = std.Thread.Mutex;

/// NUID (NATS Unique Identifier) implementation
/// Uses 12 bytes of crypto-generated prefix + 10 bytes of sequential data
/// Encoded in base36 for a total of 22 characters
const NUID_PREFIX_LEN = 12;
const NUID_SEQUENCE_LEN = 10;
const NUID_TOTAL_LEN = NUID_PREFIX_LEN + NUID_SEQUENCE_LEN;

const BASE = 36;
const DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// Maximum values for different parts
const MAX_PREFIX: u64 = 4738381338321616896; // 36^12
const MAX_SEQUENCE: u64 = 3656158440062976; // 36^10
const MIN_INCREMENT: u64 = 33;
const MAX_INCREMENT: u64 = 333;

/// Internal NUID structure
const Nuid = struct {
    prefix: [NUID_PREFIX_LEN]u8,
    sequence: u64,
    increment: u64,

    fn init() Nuid {
        var nuid = Nuid{
            .prefix = undefined,
            .sequence = 0,
            .increment = 0,
        };

        nuid.randomizePrefix();
        nuid.resetSequence();

        return nuid;
    }

    fn randomizePrefix(self: *Nuid) void {
        // Generate a random number up to MAX_PREFIX and encode it in base36
        var random_bytes: [8]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);

        const random_val = std.mem.readInt(u64, &random_bytes, .little) % MAX_PREFIX;
        encodeBase36(random_val, self.prefix[0..]);
    }

    fn resetSequence(self: *Nuid) void {
        // Generate random sequence start and increment
        var random_bytes: [16]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);

        const seq_random = std.mem.readInt(u64, random_bytes[0..8], .little);
        const inc_random = std.mem.readInt(u64, random_bytes[8..16], .little);

        self.sequence = seq_random % MAX_SEQUENCE;
        self.increment = MIN_INCREMENT + (inc_random % (MAX_INCREMENT - MIN_INCREMENT + 1));
    }

    fn next(self: *Nuid, buffer: *[NUID_TOTAL_LEN]u8) void {
        // Check if sequence would overflow
        if (self.sequence + self.increment >= MAX_SEQUENCE) {
            self.randomizePrefix();
            self.resetSequence();
        }

        self.sequence += self.increment;

        // Copy prefix
        @memcpy(buffer[0..NUID_PREFIX_LEN], &self.prefix);

        // Encode sequence
        encodeBase36(self.sequence, buffer[NUID_PREFIX_LEN..]);
    }
};

/// Thread-safe global NUID instance
const GlobalNuid = struct {
    mutex: Mutex = .{},
    nuid: ?Nuid = null,

    fn reset(self: *GlobalNuid) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.nuid = Nuid.init();
    }

    fn next(self: *GlobalNuid, buffer: *[NUID_TOTAL_LEN]u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.nuid == null) {
            self.nuid = Nuid.init();
        }

        self.nuid.?.next(buffer);
    }
};

/// Global NUID instance
var global_nuid: GlobalNuid = .{};

/// Encode a value in base36 into the provided buffer
fn encodeBase36(value: u64, buffer: []u8) void {
    var val = value;
    var i = buffer.len;

    // Fill buffer from right to left
    while (i > 0) {
        i -= 1;
        buffer[i] = DIGITS[val % BASE];
        val /= BASE;
    }
}

/// Generate the next NUID
pub fn next() [NUID_TOTAL_LEN]u8 {
    var buffer: [NUID_TOTAL_LEN]u8 = undefined;
    global_nuid.next(&buffer);
    return buffer;
}

/// Generate the next NUID as a string slice
pub fn nextString(allocator: std.mem.Allocator) ![]u8 {
    const nuid_bytes = next();
    return try allocator.dupe(u8, &nuid_bytes);
}

/// Reset the global NUID (useful for testing)
pub fn reset() void {
    global_nuid.reset();
}

test "nuid generation" {
    const testing = std.testing;

    // Test basic generation
    const nuid1 = next();
    const nuid2 = next();

    // Should be exactly 22 characters
    try testing.expectEqual(@as(usize, NUID_TOTAL_LEN), nuid1.len);
    try testing.expectEqual(@as(usize, NUID_TOTAL_LEN), nuid2.len);

    // Should be different
    try testing.expect(!std.mem.eql(u8, &nuid1, &nuid2));

    // Should only contain valid base36 characters
    for (nuid1) |char| {
        const valid = (char >= '0' and char <= '9') or (char >= 'A' and char <= 'Z');
        try testing.expect(valid);
    }
}

test "nuid string generation" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const nuid_str = try nextString(allocator);
    defer allocator.free(nuid_str);

    try testing.expectEqual(@as(usize, NUID_TOTAL_LEN), nuid_str.len);
}

test "nuid uniqueness" {
    const testing = std.testing;

    // Generate many NUIDs and ensure uniqueness
    var seen = std.HashMap([NUID_TOTAL_LEN]u8, void, struct {
        pub fn hash(self: @This(), key: [NUID_TOTAL_LEN]u8) u64 {
            _ = self;
            return std.hash_map.hashString(&key);
        }

        pub fn eql(self: @This(), a: [NUID_TOTAL_LEN]u8, b: [NUID_TOTAL_LEN]u8) bool {
            _ = self;
            return std.mem.eql(u8, &a, &b);
        }
    }, std.hash_map.default_max_load_percentage).init(testing.allocator);
    defer seen.deinit();

    for (0..1000) |_| {
        const nuid = next();
        const result = try seen.getOrPut(nuid);
        try testing.expect(!result.found_existing);
    }
}

test "base36 encoding" {
    const testing = std.testing;

    var buffer: [10]u8 = undefined;

    // Test encoding 0
    encodeBase36(0, &buffer);
    const expected_zero = "0000000000";
    try testing.expectEqualStrings(expected_zero, &buffer);

    // Test encoding MAX_SEQUENCE - 1
    encodeBase36(MAX_SEQUENCE - 1, &buffer);
    // Should be all Z's (35 in base36)
    const expected_max = "ZZZZZZZZZZ";
    try testing.expectEqualStrings(expected_max, &buffer);
}

test "thread safety" {
    const testing = std.testing;

    // Reset global state
    reset();

    const ThreadContext = struct {
        results: []?[NUID_TOTAL_LEN]u8,
        start_index: usize,
        count: usize,
    };

    const thread_count = 4;
    const nuids_per_thread = 100;
    const total_nuids = thread_count * nuids_per_thread;

    var results: [total_nuids]?[NUID_TOTAL_LEN]u8 = [_]?[NUID_TOTAL_LEN]u8{null} ** total_nuids;
    var threads: [thread_count]std.Thread = undefined;

    // Create threads
    for (&threads, 0..) |*thread, i| {
        const context = ThreadContext{
            .results = results[0..],
            .start_index = i * nuids_per_thread,
            .count = nuids_per_thread,
        };

        thread.* = try std.Thread.spawn(.{}, struct {
            fn run(ctx: ThreadContext) void {
                for (0..ctx.count) |j| {
                    const nuid = next();
                    ctx.results[ctx.start_index + j] = nuid;
                }
            }
        }.run, .{context});
    }

    // Wait for all threads
    for (&threads) |*thread| {
        thread.join();
    }

    // Check all NUIDs are unique
    var seen = std.HashMap([NUID_TOTAL_LEN]u8, void, struct {
        pub fn hash(self: @This(), key: [NUID_TOTAL_LEN]u8) u64 {
            _ = self;
            return std.hash_map.hashString(&key);
        }

        pub fn eql(self: @This(), a: [NUID_TOTAL_LEN]u8, b: [NUID_TOTAL_LEN]u8) bool {
            _ = self;
            return std.mem.eql(u8, &a, &b);
        }
    }, std.hash_map.default_max_load_percentage).init(testing.allocator);
    defer seen.deinit();

    for (results) |maybe_nuid| {
        if (maybe_nuid) |nuid| {
            const result = try seen.getOrPut(nuid);
            try testing.expect(!result.found_existing);
        }
    }

    try testing.expectEqual(@as(usize, total_nuids), seen.count());
}
