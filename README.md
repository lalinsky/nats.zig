# NATS.zig

A Zig client library for NATS, the cloud-native messaging system.

## Development Status

This project is under active development and is not yet complete. The goal is to be as feature complete as the official NATS client libraries. It is based on the NATS C and Go libraries.

## Installation

1) Add nats.zig as a dependency in your `build.zig.zon`:

```bash
zig fetch --save "git+https://github.com/lalinsky/nats.zig"
```

2) In your `build.zig`, add the `nats` module as a dependency to your program:

```zig
const nats = b.dependency("nats", .{
    .target = target,
    .optimize = optimize,
});

// the executable from your call to b.addExecutable(...)
exe.root_module.addImport("nats", nats.module("nats"));
```

## Examples

### Connect

```zig
var nc = nats.Connection.init(allocator, .{});
defer nc.deinit();

try nc.connect("nats://localhost:4222");
```

### Publish message

```zig
try nc.publish("hello", "Hello, NATS!");
```

### Subscribe synchronously

```zig
// Create synchronous subscription
var counter: u32 = 0;
const sub = try nc.subscribeSync("hello");

// Wait for message with 5 second timeout
while (true) {
    var msg = sub.nextMsg(5000) catch |err| {
        if (err == error.Timeout) continue;
        return err;
    };
    defer msg.deinit();

    counter += 1;
    std.debug.print("Message #{d}: {s}\n", .{ counter, msg.data });
}
```

### Subscribe asynchronously (with callback)

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

### Send request and wait for reply

```zig
// Send request and wait for reply with 5 second timeout
const reply = try nc.request("help", "need assistance", 5000);
defer reply.deinit();

std.debug.print("Received reply: {s}\n", .{reply.data});
```

### Send request and wait for multiple replies

```zig
// Request multiple responses from different responders
var messages = try nc.requestMany("services.status", "ping all", 5000, .{
    .max_messages = 10,    // Stop after 10 responses
    .stall_ms = 100,       // Stop if no new responses for 100ms
});

while (messages.pop()) |msg| {
    defer msg.deinit();
    std.debug.print("Response: {s}\n", .{msg.data});
}
```

### Handle requests

```zig
// Define request handler
fn echoHandler(msg: *nats.Message, context: *MyContext) !void {
    defer msg.deinit();
    
    // Send reply
    try msg.reply(msg.data);
}

// Subscribe to handle requests
var context = MyContext{};
const sub = try nc.subscribe("echo", echoHandler, .{&context});
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
