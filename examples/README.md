# NATS.zig Examples

This directory contains example programs demonstrating various NATS patterns using the Zig client library.

## Building Examples

```bash
zig build examples
```

This will build all examples and place the executables in `zig-out/bin/`.

## Available Examples

### Basic Pub/Sub
- **pub.zig** - Simple publisher
- **sub.zig** - Simple subscriber

### Request/Reply Pattern
- **requestor.zig** - Sends requests and waits for replies (matches `refs/nats.c/examples/getstarted/requestor.c`)
- **replier.zig** - Listens for requests and sends replies (matches `refs/nats.c/examples/getstarted/replier.c`)

## Running Examples

**Prerequisites:** A NATS server must be running at `nats://localhost:4222`

Start a NATS server:
```bash
# Using Docker
docker run -p 4222:4222 nats:latest

# Or using nats-server binary
nats-server
```

### Request/Reply Demo

In one terminal, run the replier:
```bash
./zig-out/bin/replier
```

In another terminal, run the requestor:
```bash
./zig-out/bin/requestor
```

**Expected output:**

Replier terminal:
```
Listening for requests on subject 'help'
Waiting for requests...
Received msg: help - really need some
Sent reply to: _INBOX.abc123xyz...
```

Requestor terminal:
```
Publishes a message on subject 'help'
Received reply: here's some help
Request/reply completed successfully!
```

## Implementation Details

The request/reply examples demonstrate:
- ✅ Unique inbox generation with NUID (`_INBOX.<22-char-nuid>`)
- ✅ Synchronous request with timeout (5 seconds)
- ✅ Publishing with reply-to subjects
- ✅ Automatic subscription cleanup
- ✅ Proper error handling and connection management

## API Features Used

- `Connection.request()` - Send request with timeout
- `Connection.subscribe()` - Subscribe to subjects  
- `Connection.publish()` - Publish messages with reply-to
- `Subscription.nextMessage()` - Synchronous message retrieval
- `inbox.newInbox()` - Generate unique inbox subjects

These examples match the behavior and style of the reference C implementation in `refs/nats.c/examples/getstarted/`.