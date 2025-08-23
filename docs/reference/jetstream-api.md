# JetStream Wire API Reference

JetStream exposes administrative and operational APIs through standard NATS subjects. All APIs respond with JSON and follow consistent patterns.

## API Subject Patterns

### General Format
```
$JS.API.<RESOURCE>.<ACTION>[.<PARAMETERS>]
```

### Domain Support
```
$JS.<DOMAIN>.API.<RESOURCE>.<ACTION>[.<PARAMETERS>]
```

## Core API Categories

### Account Information
- `$JS.API.INFO` - Account statistics and limits
- `$JS.API.ACCOUNT.INFO` - Detailed account information

### Stream Management
- `$JS.API.STREAM.CREATE.<stream>` - Create a stream
- `$JS.API.STREAM.UPDATE.<stream>` - Update stream configuration
- `$JS.API.STREAM.DELETE.<stream>` - Delete a stream
- `$JS.API.STREAM.INFO.<stream>` - Get stream information
- `$JS.API.STREAM.LIST` - List all streams
- `$JS.API.STREAM.NAMES` - List stream names only

### Stream Message Operations
- `$JS.API.STREAM.MSG.GET.<stream>` - Get message by sequence
- `$JS.API.STREAM.MSG.DELETE.<stream>` - Delete a message
- `$JS.API.DIRECT.GET.<stream>.<subject>` - Direct message access

### Consumer Management
- `$JS.API.CONSUMER.CREATE.<stream>` - Create ephemeral consumer
- `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.<consumer>` - Create durable consumer
- `$JS.API.CONSUMER.DELETE.<stream>.<consumer>` - Delete a consumer
- `$JS.API.CONSUMER.INFO.<stream>.<consumer>` - Get consumer info
- `$JS.API.CONSUMER.LIST.<stream>` - List stream consumers
- `$JS.API.CONSUMER.NAMES.<stream>` - List consumer names only

### Message Consumption
- `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` - Pull next message(s)
- `$JS.ACK.<stream>.<consumer>.<delivered>.<stream-seq>.<consumer-seq>.<timestamp>.<pending>` - Acknowledgment subject pattern

## Response Format

All JetStream API responses include a `type` field indicating the JSON schema:

```json
{
  "type": "io.nats.jetstream.api.v1.stream_info_response",
  "config": { ... },
  "state": { ... }
}
```

### Error Responses
```json
{
  "type": "io.nats.jetstream.api.v1.error_response",
  "error": {
    "code": 404,
    "err_code": 10059,
    "description": "stream not found"
  }
}
```

## Flow Control

### Acknowledgments
Messages requiring acknowledgment have reply subjects like:
```
$JS.ACK.<stream>.<consumer>.<delivered>.<stream-seq>.<consumer-seq>.<timestamp>.<pending>
```

### Flow Control Messages
When flow control is enabled, messages may include the header:
```
NATS/1.0 100 FlowControl Request
```
These MUST be replied to, or the consumer may stall.

## Pull Consumer Protocol

### Request Next Messages
```
# Subject: $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>
# Payload: number of messages to fetch (or empty for 1)
1
```

### Batch Request
```json
{
  "batch": 10,
  "expires": 30000000000
}
```

## Common Configuration Fields

### Stream Configuration
```json
{
  "name": "ORDERS",
  "subjects": ["orders.*"],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "file",
  "num_replicas": 1,
  "duplicate_window": 120000000000
}
```

### Consumer Configuration
```json
{
  "durable_name": "processor",
  "deliver_policy": "all",
  "ack_policy": "explicit",
  "ack_wait": 30000000000,
  "max_deliver": -1,
  "filter_subject": "",
  "replay_policy": "instant",
  "max_ack_pending": 1000,
  "flow_control": true
}
```

## Authentication and Authorization

JetStream operations respect NATS authentication and authorization:
- Account isolation
- Subject-based permissions
- Resource limits per account
- User/role-based access control

## Implementation Notes

- All durations are in nanoseconds
- Sequence numbers start at 1
- Stream and consumer names are case-sensitive
- API subjects are found as constants in nats-server source code
- Response types enable JSON schema validation