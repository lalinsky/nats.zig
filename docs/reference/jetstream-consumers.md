# JetStream Consumers

A Consumer is a stateful view of a Stream that tracks message delivery and acknowledgment. Consumers define how messages are delivered to applications and manage the consumption state independently from message storage.

## Consumer Types

### Pull Consumers (Recommended)
- Client requests message batches on demand
- Better scalability and flow control
- Explicit batch size and timeout control
- Recommended for new projects

### Push Consumers  
- Server automatically delivers messages to specified subjects
- Good for simple use cases and load balancing
- Supports flow control and acknowledgments
- Legacy pattern, pull consumers preferred

### Ordered Consumers
- Always ephemeral, no acknowledgments required
- Single-threaded message dispatching
- Automatic flow control and replay on errors
- Ideal for ordered processing requirements

## Consumer Configuration

### Core Settings
```json
{
  "durable_name": "processor",
  "description": "Order processing consumer",
  "deliver_policy": "all",
  "opt_start_seq": 0,
  "opt_start_time": "2023-01-15T10:30:00Z",
  "ack_policy": "explicit",
  "ack_wait": 30000000000,
  "max_deliver": 5,
  "backoff": [1000000000, 5000000000, 10000000000],
  "max_ack_pending": 1000,
  "replay_policy": "instant"
}
```

### Delivery Policies
- **all**: Deliver all available messages from the stream
- **last**: Deliver only the last message per subject
- **new**: Deliver only messages published after consumer creation  
- **by_start_sequence**: Start from specific sequence number
- **by_start_time**: Start from specific timestamp
- **last_per_subject**: Last message for each subject

### Acknowledgment Policies
- **explicit**: Messages must be explicitly acknowledged
- **none**: No acknowledgment required (fire-and-forget)
- **all**: Acknowledging one message acknowledges all prior messages

### Replay Policies
- **instant**: Deliver messages as fast as possible (default)
- **original**: Replay at original publishing rate

## Pull Consumer Protocol

### Fetch Messages
```bash
# Subject: $JS.API.CONSUMER.MSG.NEXT.ORDERS.processor
# Payload: batch size (number) or request object (JSON)
```

#### Simple Batch Request
```
5
```

#### Advanced Request
```json
{
  "batch": 10,
  "expires": 30000000000,
  "no_wait": false,
  "max_bytes": 1048576
}
```

### Response Handling
Messages are delivered with standard NATS MSG protocol:
```
MSG orders.created 1 $JS.ACK.ORDERS.processor.1.2.2.1642249800.0 145
{"order_id": "12345", "amount": 99.99}
```

### Message Acknowledgment
```bash
# Reply to the acknowledgment subject from the message
# Empty payload for simple ACK
# JSON payload for advanced ACK options
```

#### Acknowledgment Types
- **+ACK**: Positive acknowledgment (message processed successfully)
- **-NAK**: Negative acknowledgment (redelivery requested)  
- **+WPI**: Working/In-Progress (extend ack wait time)
- **+TERM**: Terminate redelivery (poison message handling)

## Push Consumer Configuration

### Push-Specific Settings
```json
{
  "deliver_subject": "orders.processed",
  "deliver_group": "processors",
  "flow_control": true,
  "idle_heartbeat": 30000000000,
  "headers_only": false,
  "max_waiting": 512
}
```

- **deliver_subject**: Subject to deliver messages to
- **deliver_group**: Queue group for load balancing
- **flow_control**: Enable flow control protocol
- **idle_heartbeat**: Send heartbeats when idle (nanoseconds)
- **headers_only**: Deliver only headers, not payload
- **max_waiting**: Maximum outstanding unacknowledged messages

## Consumer State

Consumers maintain detailed state information:

```json
{
  "stream_name": "ORDERS",
  "name": "processor", 
  "config": { ... },
  "delivered": {
    "consumer_seq": 1024,
    "stream_seq": 2048,
    "last_active": "2023-01-15T14:30:00Z"
  },
  "ack_floor": {
    "consumer_seq": 1020,
    "stream_seq": 2044,
    "last_active": "2023-01-15T14:29:45Z"
  },
  "num_ack_pending": 4,
  "num_redelivered": 2,
  "num_waiting": 0,
  "num_pending": 500
}
```

## Consumer Management

### Creating Consumers
```bash
# Ephemeral Consumer
# Subject: $JS.API.CONSUMER.CREATE.ORDERS
# Payload: Consumer configuration (no durable_name)

# Durable Consumer  
# Subject: $JS.API.CONSUMER.DURABLE.CREATE.ORDERS.processor
# Payload: Consumer configuration (with durable_name)
```

### Consumer Information
```bash
# Subject: $JS.API.CONSUMER.INFO.ORDERS.processor
# Payload: (empty)
```

### Deleting Consumers
```bash
# Subject: $JS.API.CONSUMER.DELETE.ORDERS.processor  
# Payload: (empty)
```

### Listing Consumers
```bash
# All consumer info
# Subject: $JS.API.CONSUMER.LIST.ORDERS

# Names only
# Subject: $JS.API.CONSUMER.NAMES.ORDERS
```

## Flow Control

### For Pull Consumers
- Built-in via request/response pattern
- Client controls message flow explicitly
- No additional flow control protocol needed

### For Push Consumers
When `flow_control: true`:
1. Consumer tracks pending messages
2. Sends flow control messages when approaching limits
3. Client must respond to flow control requests
4. Consumer resumes delivery after receiving response

### Flow Control Headers
```
NATS/1.0 100 FlowControl Request
```
Must be acknowledged by replying to the message.

## Error Handling

### Redelivery
- Failed messages are redelivered based on `max_deliver` setting
- `backoff` array defines increasing delays between attempts
- After max attempts, messages can be handled via:
  - Dropping (default)
  - Dead letter queue (custom implementation)
  - Manual intervention

### Consumer Stalls
- Pull consumers: Handle via request timeouts
- Push consumers: Use heartbeats and flow control
- Monitor `num_waiting` and `last_active` timestamps

## Best Practices

1. **Use pull consumers** for new applications
2. **Set appropriate ack_wait** timeouts  
3. **Configure max_deliver** for poison message handling
4. **Use durable consumers** for persistent processing
5. **Monitor consumer lag** via `num_pending`
6. **Handle flow control** messages in push consumers
7. **Design for idempotency** - messages may be redelivered
8. **Use appropriate batch sizes** for pull consumers (10-100 typical)