# NATS.zig Comprehensive Testing Plan

## Overview
This document outlines a comprehensive testing strategy for the NATS.zig client library, based on approaches used by official NATS client libraries (nats.go and nats.c) and adapted for Zig's testing framework.

## Testing Infrastructure

### 1. Test Server Management

#### Docker Compose Setup (Existing)
Your current `docker-compose.yml` provides:
- NATS server v2.10 with JetStream enabled
- Ports: 4222 (client), 8222 (monitoring), 6222 (clustering)
- Debug mode enabled for detailed logging
- Persistent volume for data

#### Test Server Utilities
Create `test/server.zig` for managing test servers:

```zig
const std = @import("std");
const nats = @import("../src/root.zig");

pub const TestServer = struct {
    process: ?std.process.Child,
    port: u16,
    monitoring_port: u16,
    config_file: ?[]const u8,
    allocator: std.mem.Allocator,
    
    pub fn start(allocator: std.mem.Allocator, port: u16) !*TestServer {
        // Start embedded NATS server or connect to Docker
    }
    
    pub fn startWithConfig(allocator: std.mem.Allocator, config: []const u8) !*TestServer {
        // Start with custom configuration
    }
    
    pub fn shutdown(self: *TestServer) void {
        // Gracefully shutdown test server
    }
    
    pub fn getUrl(self: *TestServer) []const u8 {
        // Return connection URL
    }
};

pub const TestCluster = struct {
    servers: []TestServer,
    allocator: std.mem.Allocator,
    
    pub fn start(allocator: std.mem.Allocator, size: usize) !*TestCluster {
        // Start a cluster of NATS servers
    }
    
    pub fn shutdown(self: *TestCluster) void {
        // Shutdown all servers in cluster
    }
};
```

### 2. Test Helpers and Utilities

Create `test/helpers.zig`:

```zig
const std = @import("std");
const nats = @import("../src/root.zig");

// Wait for condition with timeout
pub fn waitFor(comptime T: type, expected: T, actual_fn: fn() T, timeout_ms: u64) !void {
    const start = std.time.milliTimestamp();
    while (std.time.milliTimestamp() - start < timeout_ms) {
        if (actual_fn() == expected) return;
        std.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

// Check for goroutine/thread leaks
pub fn checkNoThreadLeak(base_count: usize, action: []const u8) !void {
    const current = getThreadCount();
    if (current > base_count) {
        std.log.err("{d} threads still exist after {s}", .{current - base_count, action});
        return error.ThreadLeak;
    }
}

// Create test connection with default options
pub fn newTestConnection(allocator: std.mem.Allocator, url: []const u8) !*nats.Connection {
    var conn = nats.Connection.init(allocator, .{
        .ping_interval_ms = 500,
        .max_pings_out = 3,
    });
    try conn.connect(url);
    return conn;
}

// Verify message received
pub fn expectMessage(sub: *nats.Subscription, expected_data: []const u8, timeout_ms: u64) !void {
    const msg = try sub.nextMsgTimeout(timeout_ms);
    defer msg.deinit();
    try std.testing.expectEqualStrings(expected_data, msg.data);
}
```

## Test Categories

### 1. Unit Tests (No Server Required)

Located in `src/*_test.zig` files alongside implementation:

#### Parser Tests (`src/parser_test.zig`)
```zig
test "parse INFO message" {
    var parser = Parser.init(allocator);
    defer parser.deinit();
    
    const info = "INFO {\"server_id\":\"test\",\"version\":\"2.10.0\"}\r\n";
    const result = try parser.parse(info);
    try std.testing.expect(result.op == .INFO);
}

test "parse MSG with payload" {
    // Test MSG parsing with various payload sizes
}

test "parse partial messages" {
    // Test handling of messages split across buffers
}

test "parse HMSG with headers" {
    // Test header parsing
}
```

#### URL Parser Tests (`src/url_test.zig`)
```zig
test "parse URL with auth" {
    const url = try parseUrl("nats://user:pass@localhost:4222");
    try std.testing.expectEqualStrings("localhost", url.host);
    try std.testing.expect(url.port == 4222);
    try std.testing.expectEqualStrings("user", url.user.?);
}

test "parse multiple URLs" {
    const urls = "nats://host1:4222,nats://host2:4223";
    const parsed = try parseUrls(urls);
    try std.testing.expect(parsed.len == 2);
}
```

#### Message Tests (`src/message_test.zig`)
```zig
test "message creation and cleanup" {
    var msg = try Message.init(allocator, "test.subject", "payload");
    defer msg.deinit();
    
    try std.testing.expectEqualStrings("test.subject", msg.subject);
    try std.testing.expectEqualStrings("payload", msg.data);
}

test "message with headers" {
    var msg = try Message.init(allocator, "test.subject", "payload");
    defer msg.deinit();
    
    try msg.setHeader("X-Custom", "value");
    const val = msg.getHeader("X-Custom");
    try std.testing.expectEqualStrings("value", val.?);
}
```

### 2. Integration Tests (Server Required)

Located in `test/` directory:

#### Basic Connectivity Tests (`test/basic_test.zig`)
```zig
const std = @import("std");
const nats = @import("nats");
const helpers = @import("helpers.zig");
const TestServer = @import("server.zig").TestServer;

test "connect and close" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();
    
    try conn.connect(server.getUrl());
    try std.testing.expect(conn.getStatus() == .connected);
    
    conn.close();
    try std.testing.expect(conn.getStatus() == .closed);
}

test "connect with bad URL" {
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();
    
    const result = conn.connect("nats://badhost:4222");
    try std.testing.expectError(error.ConnectionRefused, result);
}

test "multiple connect calls" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();
    
    try conn.connect(server.getUrl());
    
    // Second connect should fail
    const result = conn.connect(server.getUrl());
    try std.testing.expectError(error.AlreadyConnected, result);
}
```

#### Publish/Subscribe Tests (`test/pubsub_test.zig`)
```zig
test "basic pub/sub" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = try helpers.newTestConnection(allocator, server.getUrl());
    defer conn.deinit();
    
    const sub = try conn.subscribe("test.subject");
    defer sub.deinit();
    
    try conn.publish("test.subject", "hello");
    try conn.flush();
    
    const msg = try sub.nextMsgTimeout(1000);
    defer msg.deinit();
    
    try std.testing.expectEqualStrings("hello", msg.data);
}

test "queue groups" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = try helpers.newTestConnection(allocator, server.getUrl());
    defer conn.deinit();
    
    // Create two queue subscribers
    const sub1 = try conn.queueSubscribe("test", "workers");
    defer sub1.deinit();
    
    const sub2 = try conn.queueSubscribe("test", "workers");
    defer sub2.deinit();
    
    // Publish 10 messages
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "msg-{d}", .{i});
        try conn.publish("test", data);
    }
    try conn.flush();
    
    // Each subscriber should get some messages
    var count1: u32 = 0;
    var count2: u32 = 0;
    
    while (sub1.nextMsgTimeout(100)) |msg| {
        msg.deinit();
        count1 += 1;
    } else |_| {}
    
    while (sub2.nextMsgTimeout(100)) |msg| {
        msg.deinit();
        count2 += 1;
    } else |_| {}
    
    // Total should be 10
    try std.testing.expect(count1 + count2 == 10);
    // Load should be distributed
    try std.testing.expect(count1 > 0 and count2 > 0);
}
```

#### Request/Reply Tests (`test/request_reply_test.zig`)
```zig
test "request/reply pattern" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = try helpers.newTestConnection(allocator, server.getUrl());
    defer conn.deinit();
    
    // Setup responder
    const sub = try conn.subscribe("help");
    defer sub.deinit();
    
    // Start responder thread
    const responder = try std.Thread.spawn(.{}, struct {
        fn respond(s: *nats.Subscription, c: *nats.Connection) void {
            const msg = s.nextMsg() catch return;
            defer msg.deinit();
            
            if (msg.reply) |reply| {
                c.publish(reply, "I can help!") catch return;
            }
        }
    }.respond, .{sub, conn});
    
    // Make request
    const reply = try conn.request("help", "please", 1000);
    defer reply.deinit();
    
    try std.testing.expectEqualStrings("I can help!", reply.data);
    
    responder.join();
}
```

### 3. Protocol Compliance Tests

#### PING/PONG Tests (`test/protocol_test.zig`)
```zig
test "ping/pong keepalive" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = nats.Connection.init(allocator, .{
        .ping_interval_ms = 100,
        .max_pings_out = 2,
    });
    defer conn.deinit();
    
    try conn.connect(server.getUrl());
    
    // Wait for several ping intervals
    std.time.sleep(500 * std.time.ns_per_ms);
    
    // Connection should still be alive
    try std.testing.expect(conn.getStatus() == .connected);
    
    // Get stats to verify pings were sent
    const stats = conn.getStats();
    try std.testing.expect(stats.pings_sent > 3);
}

test "stale connection detection" {
    // Test that connection is marked stale when max_pings_out exceeded
}
```

#### Error Handling Tests (`test/errors_test.zig`)
```zig
test "authorization error" {
    const config =
        \\authorization {
        \\  user: test
        \\  password: secret
        \\}
    ;
    
    var server = try TestServer.startWithConfig(allocator, config);
    defer server.shutdown();
    
    var conn = nats.Connection.init(allocator, .{});
    defer conn.deinit();
    
    // Connect without auth should fail
    const result = conn.connect(server.getUrl());
    try std.testing.expectError(error.AuthorizationViolation, result);
}

test "slow consumer handling" {
    // Test slow consumer detection and handling
}
```

### 4. Reconnection Tests

#### Reconnection Logic Tests (`test/reconnect_test.zig`)
```zig
test "automatic reconnection" {
    var cluster = try TestCluster.start(allocator, 3);
    defer cluster.shutdown();
    
    var conn = nats.Connection.init(allocator, .{
        .reconnect = .{
            .allow_reconnect = true,
            .max_reconnect = 5,
            .reconnect_wait_ms = 100,
        },
    });
    defer conn.deinit();
    
    // Add all servers
    for (cluster.servers) |server| {
        try conn.addServer(server.getUrl());
    }
    
    try conn.connect(cluster.servers[0].getUrl());
    
    // Subscribe
    const sub = try conn.subscribe("test");
    defer sub.deinit();
    
    // Kill current server
    cluster.servers[0].shutdown();
    
    // Wait for reconnection
    try helpers.waitFor(nats.ConnectionStatus, .connected, 
        struct {
            fn getStatus() nats.ConnectionStatus {
                return conn.getStatus();
            }
        }.getStatus, 2000);
    
    // Subscription should still work
    try conn.publish("test", "after reconnect");
    try conn.flush();
    
    const msg = try sub.nextMsgTimeout(1000);
    defer msg.deinit();
    try std.testing.expectEqualStrings("after reconnect", msg.data);
}

test "subscription resubscribe on reconnect" {
    // Test that subscriptions are automatically restored
}
```

### 5. Stress and Performance Tests

#### Load Tests (`test/stress_test.zig`)
```zig
test "high message throughput" {
    var server = try TestServer.start(allocator, 4222);
    defer server.shutdown();
    
    var conn = try helpers.newTestConnection(allocator, server.getUrl());
    defer conn.deinit();
    
    const message_count = 100_000;
    var received: std.atomic.Value(u32) = .{};
    
    // Async subscriber
    const sub = try conn.subscribe("perf.test");
    defer sub.deinit();
    
    sub.setCallback(struct {
        fn onMessage(msg: *nats.Message) void {
            msg.deinit();
            _ = received.fetchAdd(1, .monotonic);
        }
    }.onMessage);
    
    // Publish messages
    const start = std.time.milliTimestamp();
    for (0..message_count) |i| {
        var buf: [32]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "msg-{d}", .{i});
        try conn.publish("perf.test", data);
        
        // Flush periodically
        if (i % 1000 == 0) {
            try conn.flush();
        }
    }
    try conn.flush();
    
    // Wait for all messages
    try helpers.waitFor(u32, message_count, 
        struct {
            fn getCount() u32 {
                return received.load(.acquire);
            }
        }.getCount, 10000);
    
    const elapsed = std.time.milliTimestamp() - start;
    const msg_per_sec = @as(f64, message_count) / (@as(f64, elapsed) / 1000.0);
    
    std.log.info("Throughput: {d:.0} msg/sec", .{msg_per_sec});
    
    // Should handle at least 10k msg/sec
    try std.testing.expect(msg_per_sec > 10_000);
}

test "concurrent publishers" {
    // Test multiple threads publishing simultaneously
}

test "memory leak detection" {
    // Test for memory leaks in long-running scenarios
}
```

### 6. TLS/Security Tests

#### TLS Connection Tests (`test/tls_test.zig`)
```zig
test "TLS connection" {
    const tls_config =
        \\tls {
        \\  cert_file: "test/certs/server-cert.pem"
        \\  key_file: "test/certs/server-key.pem"
        \\}
    ;
    
    var server = try TestServer.startWithConfig(allocator, tls_config);
    defer server.shutdown();
    
    var conn = nats.Connection.init(allocator, .{
        .tls = .{
            .ca_file = "test/certs/ca.pem",
        },
    });
    defer conn.deinit();
    
    try conn.connect(server.getTlsUrl());
    try std.testing.expect(conn.getStatus() == .connected);
    
    // Verify TLS is active
    const tls_info = try conn.getTlsConnectionState();
    try std.testing.expect(tls_info != null);
}
```

## Test Execution Strategy

### 1. Build Configuration

Create `build.zig` test configuration:

```zig
pub fn build(b: *std.Build) void {
    // ... existing build config ...
    
    // Unit tests (no server required)
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    
    // Integration tests (server required)
    const integration_tests = b.addTest(.{
        .root_source_file = b.path("test/all_tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    
    // Test steps
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    
    const test_integration_step = b.step("test-integration", "Run integration tests");
    test_integration_step.dependOn(&run_integration_tests.step);
    
    const test_all_step = b.step("test-all", "Run all tests including integration");
    test_all_step.dependOn(&run_unit_tests.step);
    test_all_step.dependOn(&run_integration_tests.step);
}
```

### 2. CI/CD Integration

#### GitHub Actions Workflow (`.github/workflows/test.yml`)
```yaml
name: Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Zig
      uses: goto-bus-stop/setup-zig@v2
      with:
        version: 0.13.0
    
    - name: Start NATS Server
      run: docker-compose up -d
    
    - name: Wait for NATS
      run: |
        for i in {1..30}; do
          nc -z localhost 4222 && break
          sleep 1
        done
    
    - name: Run Unit Tests
      run: zig build test
    
    - name: Run Integration Tests
      run: zig build test-integration
    
    - name: Run Stress Tests
      run: zig build test-stress
    
    - name: Stop NATS Server
      run: docker-compose down
```

### 3. Local Development Testing

#### Quick Test Script (`scripts/test.sh`)
```bash
#!/bin/bash

# Start NATS if not running
if ! nc -z localhost 4222 2>/dev/null; then
    echo "Starting NATS server..."
    docker-compose up -d
    sleep 2
fi

# Run tests based on argument
case "$1" in
    unit)
        zig build test
        ;;
    integration)
        zig build test-integration
        ;;
    stress)
        zig build test-stress
        ;;
    all)
        zig build test-all
        ;;
    watch)
        # Watch mode for development
        while true; do
            clear
            zig build test
            inotifywait -r -e modify src/ test/ 2>/dev/null
        done
        ;;
    *)
        echo "Usage: $0 {unit|integration|stress|all|watch}"
        exit 1
        ;;
esac
```

## Test Coverage Goals

### Minimum Coverage Requirements
- **Core Functionality**: 90% coverage
  - Connection management
  - Publishing/Subscribing
  - Protocol parsing
  - Error handling
  
- **Advanced Features**: 80% coverage
  - Request/Reply
  - Queue groups
  - Reconnection
  - TLS support

- **Edge Cases**: 70% coverage
  - Network failures
  - Malformed messages
  - Race conditions
  - Memory management

### Coverage Reporting

Use Zig's built-in coverage tools:
```bash
zig build test -Dtest-coverage
zig build test-coverage-report
```

## Test Data and Fixtures

### Message Fixtures (`test/fixtures/messages.zig`)
```zig
pub const valid_info = "INFO {\"server_id\":\"test\",\"version\":\"2.10.0\"}\r\n";
pub const valid_msg = "MSG test.subject 1 5\r\nhello\r\n";
pub const valid_hmsg = "HMSG test.subject 1 22 27\r\nNATS/1.0\r\n\r\nhello\r\n";
pub const valid_ping = "PING\r\n";
pub const valid_pong = "PONG\r\n";
pub const valid_ok = "+OK\r\n";
pub const valid_err = "-ERR 'Authorization Violation'\r\n";
```

### Server Configurations (`test/configs/`)
- `basic.conf` - Basic server config
- `auth.conf` - Authentication testing
- `tls.conf` - TLS configuration
- `cluster.conf` - Clustering setup
- `limits.conf` - Connection limits

## Debugging Failed Tests

### Verbose Test Output
```zig
// In test files
const verbose = std.os.getenv("TEST_VERBOSE") != null;

test "example" {
    if (verbose) {
        std.log.info("Starting test with server at {s}", .{url});
    }
    // ... test code ...
}
```

### Test Isolation
Each test should:
1. Create its own server/connection
2. Use unique subjects to avoid conflicts
3. Clean up all resources
4. Not depend on other tests

## Performance Benchmarks

### Benchmark Suite (`test/bench/`)
```zig
// bench/pubsub_bench.zig
pub fn benchPublish(allocator: std.mem.Allocator) !void {
    // Benchmark publishing throughput
}

pub fn benchSubscribe(allocator: std.mem.Allocator) !void {
    // Benchmark message delivery
}

pub fn benchRequestReply(allocator: std.mem.Allocator) !void {
    // Benchmark request/reply latency
}
```

## Validation Checklist

Before considering the library production-ready:

- [ ] All unit tests pass
- [ ] All integration tests pass against real NATS server
- [ ] Stress tests show no memory leaks
- [ ] Reconnection tests prove reliability
- [ ] TLS tests verify secure connections
- [ ] Performance meets targets (>10k msg/sec)
- [ ] Thread safety validated
- [ ] Protocol compliance verified
- [ ] Error handling comprehensive
- [ ] Documentation complete
- [ ] CI/CD pipeline green

## Next Steps

1. **Immediate Priority**: Implement basic integration tests
   - Connection lifecycle
   - Basic pub/sub
   - Error handling

2. **Short Term**: Add protocol compliance tests
   - PING/PONG
   - Reconnection
   - Queue groups

3. **Long Term**: Complete test coverage
   - Stress testing
   - TLS support
   - Performance benchmarks

This testing plan ensures the NATS.zig library is robust, reliable, and ready for production use.