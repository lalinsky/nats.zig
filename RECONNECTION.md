# NATS.zig Reconnection Features

This document describes the automatic reconnection features implemented in NATS.zig, which provide robust connection resilience for production applications.

## Overview

NATS.zig implements automatic reconnection logic that handles connection failures gracefully by:

- Detecting connection failures (network errors, server disconnects, etc.)
- Attempting to reconnect to available servers with configurable backoff
- Preserving and replaying pending messages during reconnection
- Re-establishing all subscriptions automatically
- Providing callbacks for connection state changes

## Configuration

### Reconnection Options

Configure reconnection behavior through `ReconnectOptions`:

```zig
const options = nats.ConnectionOptions{
    .reconnect = .{
        .allow_reconnect = true,           // Enable/disable reconnection
        .max_reconnect = 60,              // Max attempts (-1 = unlimited)
        .reconnect_wait_ms = 2000,        // Base wait time (2 seconds)
        .reconnect_jitter_ms = 100,       // Random jitter (100ms)
        .reconnect_buf_size = 8 * 1024 * 1024, // 8MB pending buffer
        .custom_reconnect_delay_cb = null, // Custom delay function
    },
};
```

### Callback Configuration

Monitor connection state changes:

```zig
const options = nats.ConnectionOptions{
    .callbacks = .{
        .disconnected_cb = onDisconnected,
        .reconnected_cb = onReconnected,
        .closed_cb = onClosed,
        .error_cb = onError,
    },
};

fn onDisconnected(conn: *nats.Connection) void {
    std.log.warn("Connection lost, attempting reconnection...", .{});
}

fn onReconnected(conn: *nats.Connection) void {
    std.log.info("Successfully reconnected!", .{});
}

fn onClosed(conn: *nats.Connection) void {
    std.log.info("Connection permanently closed", .{});
}

fn onError(conn: *nats.Connection, error_msg: []const u8) void {
    std.log.err("Connection error: {s}", .{error_msg});
}
```

## Server Pool Management

### Adding Multiple Servers

Configure multiple NATS servers for failover:

```zig
var conn = nats.Connection.init(allocator, options);

// Add multiple servers
try conn.addServer("nats://server1.example.com:4222");
try conn.addServer("nats://server2.example.com:4222");
try conn.addServer("nats://server3.example.com:4222");

// Connect to first available server
try conn.connect("nats://server1.example.com:4222");
```

### Server Selection Algorithm

The reconnection logic:

1. **Round-Robin**: Cycles through servers in order
2. **Failure Tracking**: Tracks connection failures per server
3. **Rotation**: Moves failed servers to end of list
4. **Retry Logic**: Retries servers that haven't exceeded max attempts

## Reconnection Process

### Connection Failure Detection

Reconnection is triggered by:

- **Network I/O Errors**: Socket read/write failures
- **Connection Closure**: Server-initiated disconnects
- **Protocol Errors**: Malformed message parsing
- **Authentication Failures**: Auth token expiration
- **Timeout Events**: Ping/pong timeout (if implemented)

### Reconnection Algorithm

1. **Detect Failure**: Connection error triggers reconnection
2. **Mark Status**: Connection status changes to `reconnecting`
3. **Close Resources**: Current socket is closed
4. **Start Thread**: Dedicated reconnection thread begins
5. **Server Cycle**: Try each server in pool
6. **Backoff**: Wait with jitter between server cycles
7. **Re-establish**: On success, restore subscriptions and flush pending
8. **Give Up**: After max attempts, mark connection as closed

### Message Buffering

During reconnection:

- **Outbound Messages**: Buffered up to `reconnect_buf_size`
- **Subscription Recovery**: All active subscriptions re-sent
- **Pending Replay**: Buffered messages flushed on reconnect
- **Overflow Handling**: Returns error if buffer exceeds limit

## Usage Examples

### Basic Reconnection

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Enable reconnection with callbacks
    const options = nats.ConnectionOptions{
        .reconnect = .{
            .allow_reconnect = true,
            .max_reconnect = 10,
            .reconnect_wait_ms = 1000, // 1 second base wait
        },
        .callbacks = .{
            .disconnected_cb = struct {
                fn callback(conn: *nats.Connection) void {
                    _ = conn;
                    std.log.warn("Connection lost!", .{});
                }
            }.callback,
            .reconnected_cb = struct {
                fn callback(conn: *nats.Connection) void {
                    _ = conn;
                    std.log.info("Reconnected successfully!", .{});
                }
            }.callback,
        },
    };

    var conn = nats.Connection.init(allocator, options);
    defer conn.deinit();

    try conn.connect("nats://localhost:4222");

    // Your application logic here
    // Reconnection happens automatically
}
```

### Multi-Server with Custom Delay

```zig
fn customReconnectDelay(attempts: u32) u64 {
    // Exponential backoff: 1s, 2s, 4s, 8s, max 30s
    const base = std.math.min(1000 * (1 << attempts), 30000);
    
    // Add some jitter
    var rng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    return base + rng.random().uintLessThan(u64, 1000);
}

const options = nats.ConnectionOptions{
    .reconnect = .{
        .max_reconnect = -1, // Unlimited attempts
        .custom_reconnect_delay_cb = customReconnectDelay,
    },
};

var conn = nats.Connection.init(allocator, options);

// Configure server cluster
try conn.addServer("nats://server1:4222");
try conn.addServer("nats://server2:4222");
try conn.addServer("nats://server3:4222");

try conn.connect("nats://server1:4222");
```

### Monitoring Connection State

```zig
while (true) {
    const status = conn.getStatus();
    
    switch (status) {
        .connected => {
            // Normal operations
            try conn.publish("heartbeat", "alive");
        },
        .reconnecting => {
            // Reduced operations, messages are buffered
            std.log.debug("Reconnecting...", .{});
        },
        .closed => {
            // Connection permanently failed
            std.log.err("Connection closed, exiting", .{});
            break;
        },
        else => {
            // Transitional states
            std.time.sleep(std.time.ns_per_ms * 100); // 100ms
        },
    }
    
    std.time.sleep(std.time.ns_per_s); // 1 second
}
```

## Connection States

- **`disconnected`**: Initial state, not connected
- **`connecting`**: Establishing initial connection
- **`connected`**: Normal operating state
- **`reconnecting`**: Lost connection, attempting to reconnect
- **`closed`**: Permanently closed, no more reconnection attempts
- **`draining_subs`**: Gracefully closing subscriptions (reserved)
- **`draining_pubs`**: Gracefully closing publishers (reserved)

## Best Practices

### Production Configuration

```zig
const production_options = nats.ConnectionOptions{
    .reconnect = .{
        .allow_reconnect = true,
        .max_reconnect = -1,          // Never give up
        .reconnect_wait_ms = 2000,    // 2 second base wait
        .reconnect_jitter_ms = 500,   // Up to 500ms jitter
        .reconnect_buf_size = 32 * 1024 * 1024, // 32MB buffer
    },
    .callbacks = .{
        .disconnected_cb = logDisconnection,
        .reconnected_cb = logReconnection,
        .error_cb = logError,
    },
};
```

### Handling Backpressure

Monitor buffer usage during reconnection:

```zig
// Check connection status before publishing
if (conn.getStatus() == .reconnecting) {
    // Consider reducing publish rate
    // or implementing application-level queuing
}

try conn.publish(subject, data);
```

### Server Health Monitoring

```zig
// Add health check endpoints as servers
try conn.addServer("nats://primary:4222");
try conn.addServer("nats://secondary:4222");
try conn.addServer("nats://failover:4222");

// The client will automatically try servers in order
// and rotate on failures
```

## Limitations and Considerations

1. **Message Ordering**: Messages may arrive out of order after reconnection
2. **Buffer Limits**: Large message backlogs may exceed buffer limits
3. **Subscription Gaps**: Brief periods where messages may be lost during transition
4. **Thread Overhead**: Reconnection uses additional background threads
5. **Memory Usage**: Pending message buffer consumes memory during outages

## Testing Reconnection

Use the included test utility:

```bash
# Build and run reconnection test
zig build examples
./zig-out/bin/reconnection_test

# While test is running, stop/start NATS server:
# Terminal 1: nats-server -p 4222
# Terminal 2: ./zig-out/bin/reconnection_test  
# Terminal 1: Ctrl+C (stop server), then restart
```

The test will demonstrate:
- Automatic reconnection attempts
- Message buffering during outage
- Subscription recovery
- Connection state callbacks

## Advanced Topics

### Custom Reconnection Logic

Implement `custom_reconnect_delay_cb` for specialized backoff strategies:

```zig
fn adaptiveDelay(attempts: u32) u64 {
    // Implement circuit breaker, exponential backoff,
    // or network condition-based delays
    return custom_algorithm(attempts);
}
```

### Integration with Monitoring

Monitor reconnection metrics:

```zig
fn onReconnected(conn: *nats.Connection) void {
    // Update metrics/alerting
    metrics.increment("nats.reconnections");
    alerting.send("NATS connection restored");
}
```

This reconnection system provides enterprise-grade resilience for NATS applications while maintaining simplicity and performance.