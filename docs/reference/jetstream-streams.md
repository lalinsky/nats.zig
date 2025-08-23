# JetStream Streams

A Stream defines how messages are stored, retained, and replicated in JetStream. Streams are the fundamental storage abstraction that capture and persist messages published to configured subjects.

## Stream Configuration

### Required Fields
- **name**: Stream name (alphanumeric, dashes, underscores)
- **subjects**: List of subjects to capture (supports wildcards)

### Retention Policies
- **limits**: Retain until limits are reached (default)
- **interest**: Retain while consumers exist
- **workqueue**: Retain until acknowledged by any consumer

### Storage Configuration
- **storage**: `"file"` (persistent) or `"memory"` (fast, volatile)
- **max_msgs**: Maximum number of messages (-1 = unlimited)
- **max_bytes**: Maximum storage bytes (-1 = unlimited)  
- **max_age**: Maximum message age in nanoseconds (0 = unlimited)
- **max_msg_size**: Maximum individual message size (-1 = unlimited)

### Replication and Placement
- **num_replicas**: Number of replicas (1-5)
- **placement**: Cluster placement constraints
  ```json
  {
    "cluster": "us-east",
    "tags": ["ssd", "fast"]
  }
  ```

### Advanced Features
- **duplicate_window**: Deduplication time window (nanoseconds)
- **compression**: `"s2"` or `"none"`
- **sealed**: Make stream immutable after creation
- **deny_delete**: Prevent message deletion
- **deny_purge**: Prevent stream purging
- **allow_rollup_hdrs**: Enable message rollup headers

### Mirroring and Sourcing
```json
{
  "mirror": {
    "name": "source-stream",
    "opt_start_seq": 1000,
    "filter_subject": "orders.us.*"
  },
  "sources": [
    {
      "name": "stream1", 
      "filter_subject": "region.east.*"
    },
    {
      "name": "stream2",
      "filter_subject": "region.west.*"
    }
  ]
}
```

### Subject Transformation
Transform subjects when storing messages:
```json
{
  "subject_transform": {
    "src": "orders.>",
    "dest": "transformed.orders.{{wildcard(1)}}"
  }
}
```

## Stream State

Streams maintain operational state information:

```json
{
  "messages": 150432,
  "bytes": 248832000,
  "first_seq": 1,
  "first_ts": "2023-01-15T10:30:00Z",
  "last_seq": 150432,
  "last_ts": "2023-01-15T14:30:00Z",
  "consumer_count": 3,
  "deleted": [1001, 1002, 1150]
}
```

### State Fields
- **messages**: Total message count
- **bytes**: Total storage bytes used
- **first_seq/last_seq**: Message sequence range
- **first_ts/last_ts**: Timestamp range (RFC3339)
- **consumer_count**: Number of consumers
- **deleted**: List of deleted message sequences

## Stream Operations

### Creating a Stream
```bash
# Subject: $JS.API.STREAM.CREATE.ORDERS
# Payload: Stream configuration JSON
```

### Updating a Stream
```bash
# Subject: $JS.API.STREAM.UPDATE.ORDERS  
# Payload: Updated stream configuration
```

### Getting Stream Info
```bash
# Subject: $JS.API.STREAM.INFO.ORDERS
# Payload: (optional) deleted_details and subjects_filter flags
```

### Deleting a Stream
```bash
# Subject: $JS.API.STREAM.DELETE.ORDERS
# Payload: (empty)
```

### Purging Messages
```bash
# Subject: $JS.API.STREAM.PURGE.ORDERS
# Payload: (optional) purge filters
```

## Message Management

### Direct Message Access
```bash
# Subject: $JS.API.DIRECT.GET.ORDERS.{subject}
# Gets last message on subject from stream
```

### Get Message by Sequence
```bash
# Subject: $JS.API.STREAM.MSG.GET.ORDERS
# Payload: {"seq": 12345}
```

### Delete Message
```bash
# Subject: $JS.API.STREAM.MSG.DELETE.ORDERS
# Payload: {"seq": 12345, "no_erase": false}
```

## Subject Wildcards

Streams support NATS wildcards in subject configurations:
- `*` matches single token: `orders.*.created`
- `>` matches multiple tokens: `orders.>`

## Stream Limits and Quotas

Streams respect account-level limits:
- Maximum number of streams per account
- Maximum storage per account
- Maximum message size
- Maximum consumer connections

## Best Practices

1. **Use meaningful names** - Stream names should reflect their purpose
2. **Configure retention** - Set appropriate limits for your use case  
3. **Plan for growth** - Consider message rates and storage requirements
4. **Use replication** - For high availability requirements
5. **Monitor state** - Track message counts and storage usage
6. **Subject design** - Design subject hierarchies for efficient filtering
7. **Compression** - Use S2 compression for large messages when appropriate