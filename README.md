# NATS.zig

A Zig client library for NATS, built on top of Zig's standard library I/O interface (`std.Io`).
It supports most of the functionality found in the official client libraries, and works with any
`std.Io` implementation (see [Selecting the I/O Backend](#selecting-the-io-backend)).

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
const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    var nc = nats.Connection.init(init.gpa, init.io, .{});
    defer nc.deinit();

    try nc.connect("nats://localhost:4222");
}
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
defer sub.deinit();

// Wait for message with 5 second timeout
while (true) {
    var msg = sub.nextMsgTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }) catch |err| {
        if (err == error.Timeout) continue;
        return err;
    };
    defer msg.deinit();

    counter += 1;
    std.debug.print("Message #{d}: {s}\n", .{ counter, msg.data });
}
```

Synchronous subscriptions also support indefinite, non-blocking, and batch
receives:

```zig
const msg = try sub.nextMsg(); // wait indefinitely
defer msg.deinit();

if (sub.tryNextMsg()) |available| { // never blocks
    defer available.deinit();
}

var batch: [64]*nats.Message = undefined;
{
    const count = try sub.nextMsgBatchTimeout(&batch, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    defer for (batch[0..count]) |batch_msg| batch_msg.deinit();
    // Process batch[0..count].
}

{
    const count = sub.tryNextMsgBatch(&batch);
    defer for (batch[0..count]) |batch_msg| batch_msg.deinit();
    // Process immediately available messages in batch[0..count].
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
defer sub.deinit();
```

Subscriptions are owned by whoever created them, not by the connection: every
subscription must be released with `sub.deinit()` before its connection is
destroyed. `deinit()` unsubscribes and, for asynchronous subscriptions, waits
for the handler task to finish, so it is safe to free anything the handler
captured once it returns. Destroying a connection that still has live
subscriptions panics with the number outstanding rather than leaving them
pointing at freed connection state.

### Send request and wait for reply

```zig
// Send request and wait for reply with 5 second timeout
const reply = try nc.request("help", "need assistance", .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
defer reply.deinit();

std.debug.print("Received reply: {s}\n", .{reply.data});
```

### Send request and wait for multiple replies

```zig
// Request multiple responses from different responders
var messages = try nc.requestMany("services.status", "ping all", .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } }, .{
    .max_messages = 10, // Stop after 10 responses
    .stall = .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }, // Stop if no new responses for 100ms
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
defer sub.deinit();
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

### JetStream Push Subscriptions

```zig
// Push subscription with callback handler
fn orderHandler(js_msg: *nats.JetStreamMessage, count: *u32) !void {
    defer js_msg.deinit();
    count.* += 1;
    try js_msg.ack(); // Acknowledge message
    std.debug.print("Order: {s}\n", .{js_msg.data});
}

var processed: u32 = 0;
var push_sub = try js.subscribe("orders.*", orderHandler, .{&processed}, .{
    .stream = "ORDERS",
    .durable = "order_processor",
});
defer push_sub.deinit();
```

### JetStream Pull Subscriptions

```zig
// Pull subscription (fetch messages manually)
var pull_sub = try js.pullSubscribe("orders.*", "batch_processor", .{
    .stream = "ORDERS",
});
defer pull_sub.deinit();

var batch = try pull_sub.fetch(10, .fromSeconds(5)); // Fetch up to 10 msgs, 5s timeout
defer batch.deinit();
for (batch.messages) |js_msg| {
    try js_msg.ack();
}
```

### Authentication

Username/password and token authentication can be configured through the
connection options or directly in the URL:

```zig
// Username and password from options
var nc = nats.Connection.init(init.gpa, init.io, .{
    .user = "alice",
    .password = "secret",
});

// Token from options
var nc2 = nats.Connection.init(init.gpa, init.io, .{
    .token = "s3cr3t-t0ken",
});

// Credentials embedded in the URL
try nc.connect("nats://alice:secret@localhost:4222");

// A URL username without a password is sent as a token
try nc2.connect("nats://s3cr3t-t0ken@localhost:4222");
```

Credentials in the server URL take precedence over the options, matching the
behavior of the official NATS clients.

For NKey authentication, provide the seed and the client signs the server's
connection nonce with the derived Ed25519 key:

```zig
var nc = nats.Connection.init(init.gpa, init.io, .{
    .nkey_seed = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY",
});
```

For decentralized (JWT) authentication, point the client at a credentials
file generated by `nsc`, or pass the JWT and seed directly:

```zig
// Credentials file: re-read on every reconnect, so rotated
// credentials are picked up automatically
var nc = nats.Connection.init(init.gpa, init.io, .{
    .user_creds = "/path/to/user.creds",
});

// Or inline: the JWT is presented as-is, the seed signs the nonce
var nc2 = nats.Connection.init(init.gpa, init.io, .{
    .user_jwt = user_jwt,
    .nkey_seed = user_seed,
});
```

To keep the seed out of the client process entirely, provide the public key
and a signing callback instead of the seed:

```zig
fn signNonce(nonce: []const u8) anyerror![64]u8 {
    // Delegate to an agent, HSM, or other external signer.
    return external_signer.sign(nonce);
}

var nc = nats.Connection.init(init.gpa, init.io, .{
    .nkey = "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4",
    .nkey_sign_cb = signNonce,
});
```

If a server rejects the client's credentials with the same authentication
error twice in a row, the client stops reconnecting to it instead of
exhausting the reconnect budget; reconnect-time authentication errors are
also reported through the `error_cb` callback.

### TLS

TLS is enabled with the `tls://` URL scheme, or explicitly through the
connection options:

```zig
// tls:// scheme, server certificate verified against the system trust store
try nc.connect("tls://demo.nats.io:4443");

// Custom CA bundle
var nc2 = nats.Connection.init(init.gpa, init.io, .{
    .tls = .{ .ca_file = "/path/to/ca.pem" },
});

// For servers that expect the TLS handshake before the initial INFO
// (handshake-first mode, TLS-terminating proxies)
var nc3 = nats.Connection.init(init.gpa, init.io, .{
    .tls = .{ .ca_file = "/path/to/ca.pem", .handshake_first = true },
});
```

Once enabled, TLS applies to every connection, including cluster members
discovered at runtime. Certificate files are re-read on every (re)connect,
so rotated certificates are picked up automatically. Setting
`insecure_skip_verify` disables server certificate verification entirely;
it is meant for testing only. Use `server_name` to set the name used for
SNI and certificate verification when servers are dialed by IP address:

```zig
var nc4 = nats.Connection.init(init.gpa, init.io, .{
    .tls = .{ .ca_file = "/path/to/ca.pem", .server_name = "nats.example.com" },
});
```

For mutual TLS, provide a client certificate and key:

```zig
var nc5 = nats.Connection.init(init.gpa, init.io, .{
    .tls = .{
        .ca_file = "/path/to/ca.pem",
        .cert_file = "/path/to/client-cert.pem",
        .key_file = "/path/to/client-key.pem",
    },
});
```

With the server's `verify_and_map` mode, the client certificate is also
the authentication: its identity is mapped to a configured user and no
other credentials are needed.

TLS support is provided by [tls.zig](https://github.com/ianic/tls.zig) and
can be compiled out with `-Duse_tls=false`, in which case TLS connections
fail with `error.TlsNotConfigured`.

## Selecting the I/O Backend

The examples above use `init.io`, the threaded I/O implementation from the stdlib. This is suitable for development
or applications with a small number of connections.

For production use, it's recommended to use [zio](https://github.com/lalinsky/zio), which provides a coroutine-based
async I/O runtime. Each connection runs several internal tasks (socket reader, flusher, async subscription handlers),
and with zio these are lightweight coroutines multiplexed over a few OS threads instead of dedicated threads.
In the future, you can also use `std.Io.Evented`, but that implementation is not finished yet, it's missing any
networking functionality, so use zio for now.

Add it as a dependency:

```sh
zig fetch --save "git+https://github.com/lalinsky/zio"
```

In `build.zig`, add the zio module:

```zig
const zio = b.dependency("zio", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zio", zio.module("zio"));
```

Then initialize zio's runtime and pass it to nats.zig:

```zig
const std = @import("std");
const zio = @import("zio");
const nats = @import("nats");

// Route std.log and std.debug.print through zio, so they don't block the event loop
pub const std_options_debug_io = zio.debug_io;

pub fn main(init: std.process.Init) !void {
    var rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var nc = nats.Connection.init(init.gpa, rt.io(), .{});
    defer nc.deinit();

    try nc.connect("nats://localhost:4222");
}
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
