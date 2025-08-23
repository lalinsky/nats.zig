# NATS.zig

A Zig client library for NATS, the cloud-native messaging system.

## Examples

### Connect and Publish

```zig
// Create connection
var nc = nats.Connection.init(allocator, .{});
defer nc.deinit();

// Connect to NATS server
try nc.connect("nats://localhost:4222");

// Publish a message
try nc.publish("hello", "Hello, NATS!");
```

### Synchronous Subscribe

```zig
// Create synchronous subscription
const sub = try nc.subscribeSync("hello");

// Wait for message with 5 second timeout
if (sub.nextMsg(5000)) |msg| {
    defer msg.deinit();
    std.debug.print("Received: {s}\n", .{msg.data});
}
```

### Asynchronous Subscribe with Callback

```zig
// Define message handler
fn messageHandler(msg: *nats.Message, counter: *u32) void {
    defer msg.deinit();
    counter.* += 1;
    std.debug.print("Message #{d}: {s}\n", .{ counter.*, msg.data });
}

// Subscribe with callback handler
var counter: u32 = 0;
const sub = try nc.subscribe("hello", messageHandler, .{&counter});
```

### JetStream Stream Management

```zig
// Create JetStream context
var js = nc.jetstream(.{});
defer js.deinit();

// Create a stream
const stream_config = nats.StreamConfig{
    .name = "ORDERS",
    .subjects = &.{"orders.*"},
    .retention = .limits,
    .storage = .file,
    .max_msgs = 1000,
};

var stream_info = try js.addStream(stream_config);
defer stream_info.deinit();
```

### JetStream Consumer Management

```zig
// Create a durable consumer
const consumer_config = nats.ConsumerConfig{
    .durable_name = "order_processor", 
    .ack_policy = .explicit,
    .deliver_policy = .all,
};

var consumer_info = try js.addConsumer("ORDERS", consumer_config);
defer consumer_info.deinit();
```

## Building

```bash
# Build the library
zig build

# Build examples
zig build examples
```

## Testing

The project includes both unit tests and end-to-end tests:

```bash
# Run all tests (unit + e2e)
zig build test

# Run only unit tests
zig build test-unit

# Run only end-to-end tests
zig build test-e2e
```

The end-to-end tests automatically start and stop the required NATS servers using Docker Compose.
