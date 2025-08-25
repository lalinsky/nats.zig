# NATS.zig Implementation Plan

## Overview
This document provides a comprehensive implementation plan for building a NATS client library in Zig 0.15. The implementation should follow the NATS protocol specification and provide a production-ready client with full Core NATS support.

## Critical Context for Implementation

### Zig 0.15 I/O Changes (IMPORTANT!)
Zig 0.15 introduced breaking changes called "Writergate". The old generic `std.io` is replaced with concrete `std.Io.Reader` and `std.Io.Writer` types that are ring buffers. See: `/refs/zig-0.15.1-release-notes.md` (lines 1455-1600)

**Key points:**
- No `std.Io` module exists - use `std.net.Stream` for TCP
- Readers/Writers are concrete types, not generic
- Buffers are in the interface, not implementation
- Always flush writers explicitly

### Reference Materials
1. **NATS Protocol Specification**: `/refs/nats-protocol.md` - Complete protocol documentation
2. **Client Development Guide**: `/refs/nats-client-dev.md` - Best practices for client implementation
3. **Reference Implementations**:
   - `/refs/nats.c/` - C implementation (closest to Zig)
   - `/refs/nats.go/` - Go implementation (most complete)
4. **Initial API Design**: `/design/api.zig` - Starting point (needs updates for Zig 0.15)

## Architecture Overview

### Module Structure
```
src/
├── nats.zig           # Main public API
├── connection.zig     # Connection management
├── parser.zig         # Protocol parser (zero-allocation)
├── protocol.zig       # Protocol constants and messages
├── subscription.zig   # Subscription management
├── message.zig        # Message types
├── options.zig        # Configuration options
├── errors.zig         # Error definitions
├── auth.zig          # Authentication handling
├── url.zig           # URL parsing
├── utils/
│   ├── buffer.zig    # Ring buffer utilities
│   ├── nuid.zig      # NUID generation
│   └── inbox.zig     # Inbox generation
└── test/
    └── *.zig         # Test files
```

## Implementation Phases

### Phase 1: Core Infrastructure (Priority: CRITICAL)

#### 1.1 Protocol Parser Module (`parser.zig`)
**Goal**: Zero-allocation parser for NATS protocol messages

```zig
pub const Parser = struct {
    state: enum {
        OpStart,
        OpI, OpIn, OpInf, OpInfo,
        OpM, OpMs, OpMsg,
        OpP, OpPi, OpPin, OpPing, OpPo, OpPon, OpPong,
        OpPlus, OpPlusO, OpPlusOk,
        OpMinus, OpMinusE, OpMinusEr, OpMinusErr,
        OpH, OpHm, OpHms, OpHmsg,
        // ... other states
    },
    scratch: [MAX_CONTROL_LINE_SIZE]u8,
    scratch_len: usize,
    msg_buf: []u8,
    msg_arg: MsgArg,
    awaiting_msg_payload: bool,
    
    pub fn parse(self: *Parser, data: []const u8) !ParseResult {
        // State machine implementation
        // Return slices into original buffer (zero-copy)
    }
};

pub const MsgArg = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    hdr: usize,
    size: usize,
};
```

**Implementation Notes**:
- Use state machine pattern (see `/refs/nats.c/src/parser.c`)
- Parse directly from network buffer without allocations
- Handle partial messages across buffer boundaries
- Support both MSG and HMSG (with headers)

#### 1.2 Connection Module (`connection.zig`)
**Goal**: TCP connection with buffered I/O using Zig 0.15's new system

```zig
pub const Connection = struct {
    allocator: Allocator,
    
    // Network
    stream: std.net.Stream,
    read_buffer: [16384]u8,  // 16KB read buffer
    write_buffer: [16384]u8, // 16KB write buffer
    
    // Parser
    parser: Parser,
    
    // Subscriptions
    subs: std.AutoHashMap(u64, *Subscription),
    next_sid: std.atomic.Value(u64),
    
    // State
    status: std.atomic.Value(ConnectionStatus),
    stats: Statistics,
    
    // Threads
    read_thread: ?std.Thread,
    ping_thread: ?std.Thread,
    flusher_thread: ?std.Thread,
    
    // Ping/Pong tracking
    pings_out: std.atomic.Value(u32),
    
    pub fn connect(url: []const u8, options: ConnectOptions) !*Connection {
        // 1. Parse URL
        // 2. TCP connect
        // 3. Read INFO
        // 4. Send CONNECT
        // 5. Start read loop
        // 6. Start ping timer
    }
    
    fn readLoop(self: *Connection) !void {
        var reader = self.stream.reader(&self.read_buffer);
        
        while (self.status.load(.acquire) == .connected) {
            // Read from network
            const data = try reader.readUntilDelimiterOrEof('\n', MAX_CONTROL_LINE_SIZE);
            if (data == null) break;
            
            // Parse protocol
            const result = try self.parser.parse(data.?);
            try self.processParseResult(result);
        }
    }
};
```

**Critical Implementation Details**:
- Must handle INFO message immediately after connect
- Send CONNECT with proper auth if required
- Start background threads for reading and ping/pong
- Use atomic operations for thread-safe state

### Phase 2: Publishing and Subscribing (Priority: HIGH)

#### 2.1 Publishing
```zig
pub fn publish(self: *Connection, subject: []const u8, data: []const u8) !void {
    // Format: PUB <subject> <size>\r\n<data>\r\n
    var writer = self.stream.writer(&self.write_buffer);
    try writer.print("PUB {s} {d}\r\n", .{subject, data.len});
    try writer.writeAll(data);
    try writer.writeAll("\r\n");
    try writer.flush(); // CRITICAL: Don't forget!
}

pub fn publishRequest(self: *Connection, subject: []const u8, reply: []const u8, data: []const u8) !void {
    // Format: PUB <subject> <reply> <size>\r\n<data>\r\n
}
```

#### 2.2 Subscription Management
```zig
pub const Subscription = struct {
    sid: u64,
    subject: []const u8,
    queue: ?[]const u8,
    callback: ?*const fn(*Message) void,
    pending: std.fifo.LinearFifo(*Message, .Dynamic),
    
    pub fn nextMsg(self: *Subscription) !*Message {
        // Block until message available
    }
    
    pub fn nextMsgTimeout(self: *Subscription, timeout_ms: u64) !?*Message {
        // Wait with timeout
    }
};
```

**Subscribe Implementation**:
```zig
pub fn subscribe(self: *Connection, subject: []const u8, callback: anytype) !*Subscription {
    const sid = self.next_sid.fetchAdd(1, .monotonic);
    
    // Send SUB protocol message
    var writer = self.stream.writer(&self.write_buffer);
    try writer.print("SUB {s} {d}\r\n", .{subject, sid});
    try writer.flush();
    
    // Create and store subscription
    const sub = try self.allocator.create(Subscription);
    sub.* = .{
        .sid = sid,
        .subject = try self.allocator.dupe(u8, subject),
        .callback = callback,
        // ...
    };
    
    try self.subs.put(sid, sub);
    return sub;
}
```

### Phase 3: Protocol Compliance (Priority: HIGH)

#### 3.1 PING/PONG Implementation
```zig
fn pingLoop(self: *Connection) !void {
    while (self.status.load(.acquire) == .connected) {
        std.time.sleep(self.options.ping_interval);
        
        // Send PING
        var writer = self.stream.writer(&self.write_buffer);
        try writer.writeAll("PING\r\n");
        try writer.flush();
        
        // Track outstanding pings
        const pings = self.pings_out.fetchAdd(1, .monotonic);
        if (pings >= self.options.max_pings_out) {
            self.status.store(.stale, .release);
            return error.StaleConnection;
        }
    }
}

fn handlePong(self: *Connection) void {
    self.pings_out.store(0, .release);
}
```

#### 3.2 Error Handling
Parse and handle -ERR messages:
```zig
fn handleError(self: *Connection, err_msg: []const u8) !void {
    // Parse error message
    if (std.mem.startsWith(u8, err_msg, "'Authorization Violation'")) {
        return error.AuthorizationViolation;
    } else if (std.mem.startsWith(u8, err_msg, "'Slow Consumer'")) {
        // Handle slow consumer
        return error.SlowConsumer;
    }
    // ... other error cases
}
```

### Phase 4: Advanced Features (Priority: MEDIUM)

#### 4.1 Request/Reply Pattern
```zig
pub fn request(self: *Connection, subject: []const u8, data: []const u8, timeout_ms: u64) !*Message {
    // 1. Create unique inbox
    const inbox = try self.newInbox();
    defer self.allocator.free(inbox);
    
    // 2. Subscribe to inbox
    const sub = try self.subscribeSync(inbox);
    defer sub.unsubscribe();
    
    // 3. Publish with reply-to
    try self.publishRequest(subject, inbox, data);
    
    // 4. Wait for response
    return try sub.nextMsgTimeout(timeout_ms);
}
```

#### 4.2 Reconnection Logic
```zig
fn reconnect(self: *Connection) !void {
    var attempts: u32 = 0;
    
    while (attempts < self.options.max_reconnect) {
        for (self.servers.items) |server| {
            if (try self.connectToServer(server)) {
                // Re-subscribe all subscriptions
                var iter = self.subs.iterator();
                while (iter.next()) |entry| {
                    const sub = entry.value_ptr.*;
                    try self.resubscribe(sub);
                }
                return;
            }
        }
        
        attempts += 1;
        const wait = self.options.reconnect_wait + 
                     (std.crypto.random.int(u64) % self.options.reconnect_jitter);
        std.time.sleep(wait);
    }
    
    return error.NoServersAvailable;
}
```

### Phase 5: Headers Support (Priority: LOW)

#### 5.1 HPUB/HMSG Implementation
```zig
pub fn publishWithHeaders(self: *Connection, msg: *PublishMessage) !void {
    // Format headers
    var headers_buf: [4096]u8 = undefined;
    var headers_len = try formatHeaders(&headers_buf, msg.headers);
    
    // HPUB <subject> [reply] <header_len> <total_len>\r\n
    var writer = self.stream.writer(&self.write_buffer);
    try writer.print("HPUB {s} ", .{msg.subject});
    if (msg.reply) |reply| {
        try writer.print("{s} ", .{reply});
    }
    try writer.print("{d} {d}\r\n", .{headers_len, headers_len + msg.data.len});
    try writer.writeAll(headers_buf[0..headers_len]);
    try writer.writeAll(msg.data);
    try writer.writeAll("\r\n");
    try writer.flush();
}
```

## Testing Strategy

### Unit Tests
1. **Parser Tests**: Test all protocol messages and edge cases
2. **URL Parser Tests**: Various URL formats including auth
3. **Buffer Tests**: Ring buffer edge cases
4. **Message Tests**: Encoding/decoding

### Integration Tests
1. **Basic Pub/Sub**: Connect, publish, subscribe, receive
2. **Request/Reply**: Synchronous request/response
3. **Queue Groups**: Load balancing test
4. **Reconnection**: Server restart handling
5. **Auth Tests**: Various authentication methods

### Protocol Compliance Tests
Use the official NATS server test suite:
```bash
# Start test server
nats-server -DV -p 4222

# Run compliance tests
zig test src/test/protocol_test.zig
```

## Performance Considerations

1. **Zero-Allocation Parser**: Parse directly from network buffers
2. **Buffer Sizes**: 16KB default, tunable via options
3. **Thread Pool**: One read thread, one ping thread per connection
4. **Lock-Free Operations**: Use atomics for stats and state
5. **Batch Operations**: Group small writes before flushing

## Common Pitfalls to Avoid

1. **Forgetting to Flush**: Always flush writers in Zig 0.15
2. **Verbose Mode**: Default to false for performance
3. **Parser State**: Handle partial messages correctly
4. **Thread Safety**: Use proper synchronization for shared state
5. **Memory Leaks**: Ensure proper cleanup in error paths

## Validation Checklist

- [ ] Can connect to NATS server
- [ ] Can publish messages
- [ ] Can subscribe and receive messages
- [ ] Handles PING/PONG correctly
- [ ] Reconnects on disconnect
- [ ] Supports all auth methods
- [ ] Queue groups work
- [ ] Request/Reply pattern works
- [ ] Headers (HPUB/HMSG) supported
- [ ] No memory leaks (test with valgrind)
- [ ] Thread-safe operations
- [ ] Performance meets expectations

## Example Usage (Final API)

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    
    // Connect
    const nc = try nats.connect("nats://localhost:4222", .{
        .name = "zig-client",
        .ping_interval = std.time.ns_per_s * 30,
    });
    defer nc.deinit();
    
    // Subscribe
    const sub = try nc.subscribe("foo", struct {
        fn handle(msg: *nats.Message) void {
            std.debug.print("Received: {s}\n", .{msg.data});
        }
    }.handle);
    defer sub.deinit();
    
    // Publish
    try nc.publish("foo", "Hello NATS!");
    
    // Request/Reply
    const reply = try nc.request("help", "please", std.time.ns_per_s * 5);
    defer reply.deinit();
    std.debug.print("Got help: {s}\n", .{reply.data});
}
```

## Resources and References

- NATS Protocol: https://docs.nats.io/reference/reference-protocols/nats-protocol
- NATS Clients: https://github.com/nats-io/
- Zig 0.15 Docs: https://ziglang.org/documentation/0.15.0/
- Test Server: https://github.com/nats-io/nats-server

## Notes for Implementation

1. Start with Phase 1 - get basic connectivity working
2. Use `/refs/nats.c/src/parser.c` as reference for parser state machine
3. Study `/refs/nats.go/nats.go` for API design patterns
4. Test against real NATS server frequently
5. Focus on Core NATS first, JetStream can come later
6. Keep the API simple and idiomatic to Zig

Good luck with the implementation!