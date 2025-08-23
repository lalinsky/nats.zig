# JetStream Key/Value Store

The JetStream Key/Value (KV) Store provides an immediately consistent, persistent key-value storage system built on top of JetStream streams. It offers a familiar map-like interface with NATS-native features like clustering, replication, and security.

## Architecture

### Implementation Details
- Each KV bucket is backed by a dedicated JetStream stream
- Stream names use prefix: `KV_<BucketName>`
- Message subjects follow pattern: `$KV.<BucketName>.<Key>`
- Key operations are stored as stream messages
- Values are message payloads, metadata in headers

### Consistency Model
- **Immediately consistent** within a single NATS server
- **Eventually consistent** across cluster replicas
- **No read-after-write guarantee** - reads may see different values across replicas
- **Monotonic writes** - values don't go backward in time

## Key Constraints

### Valid Key Format
Keys must match regex: `\A[-/_=\.a-zA-Z0-9]+\z`

**Allowed characters:**
- Letters: `a-z`, `A-Z`  
- Numbers: `0-9`
- Special: `-`, `/`, `_`, `=`, `.`

**Restrictions:**
- Cannot start or end with `.`
- Keys starting with `_kv` are reserved for internal use
- Case sensitive

### Bucket Names
Bucket names must match regex: `\A[a-zA-Z0-9_-]+\z`
- Alphanumeric characters only
- Underscores and hyphens allowed
- No dots or special characters

## Bucket Configuration

### Basic Configuration
```json
{
  "bucket": "user-sessions",
  "description": "User session data",
  "max_value_size": 1048576,
  "history": 5,
  "ttl": 3600000000000,
  "max_bucket_size": -1,
  "storage": "file",
  "num_replicas": 1
}
```

### Advanced Configuration
```json
{
  "bucket": "config-data",
  "compression": "s2",
  "metadata": {
    "owner": "platform-team",
    "environment": "production"
  },
  "republish": {
    "src": ">",
    "dest": "kv.config.{{wildcard(1)}}"
  },
  "placement": {
    "cluster": "us-west",
    "tags": ["ssd"]
  }
}
```

## Key Operations

### Put Operation
Store a value for a key:
```bash
# Subject: $KV.BUCKET.key
# Payload: value data
# Headers: optional metadata
```

#### Put with Revision Check
```json
{
  "operation": "PUT",
  "key": "user:12345",
  "value": "base64-encoded-data",
  "revision": 5
}
```

### Get Operation
Retrieve current value for a key:
```bash
# Subject: $KV.BUCKET.key  
# Use NATS request/reply pattern
```

#### Get Response
```json
{
  "bucket": "user-sessions",
  "key": "user:12345", 
  "value": "base64-encoded-data",
  "revision": 6,
  "created": "2023-01-15T10:30:00Z",
  "delta": 0
}
```

### Create Operation
Add key only if it doesn't exist:
```json
{
  "operation": "CREATE",
  "key": "new-key",
  "value": "initial-value"
}
```

### Update Operation  
Modify key only if revision matches:
```json
{
  "operation": "UPDATE", 
  "key": "existing-key",
  "value": "new-value",
  "revision": 3
}
```

### Delete Operation
Mark key as deleted (preserves history):
```bash
# Subject: $KV.BUCKET.key
# Payload: (empty)
# Headers: KV-Operation: DEL
```

### Purge Operation
Remove all history for a key:
```bash
# Subject: $KV.BUCKET.key
# Payload: (empty)  
# Headers: KV-Operation: PURGE
```

## Advanced Operations

### Watch Operations
Monitor changes to keys in real-time:

#### Watch Single Key
```bash
# Subject: $KV.BUCKET.specific-key
# Subscribe for real-time updates
```

#### Watch All Keys
```bash
# Subject: $KV.BUCKET.>
# Subscribe to all key changes in bucket
```

#### Watch with Filters
```bash
# Subject: $KV.BUCKET.users.*
# Watch all keys matching pattern
```

### History Operations
Retrieve historical values for keys:

```json
{
  "operation": "HISTORY",
  "key": "user:12345",
  "include_deleted": false
}
```

### List Keys
Enumerate all keys in bucket:
```json
{
  "operation": "LIST",
  "filter": "users.*"
}
```

## Bucket Management

### Create Bucket
```bash
# Subject: $JS.API.STREAM.CREATE.KV_BUCKET_NAME
# Payload: Stream configuration with KV-specific settings
```

### Get Bucket Status
```bash
# Subject: $JS.API.STREAM.INFO.KV_BUCKET_NAME  
# Payload: (empty)
```

### Delete Bucket
```bash
# Subject: $JS.API.STREAM.DELETE.KV_BUCKET_NAME
# Payload: (empty)
```

## Message Format

### KV Message Headers
```
NATS/1.0
KV-Operation: PUT
KV-Revision: 3
KV-Created: 2023-01-15T10:30:00.123456789Z
```

### Special Operations
- `KV-Operation: DEL` - Soft delete (preserves history)
- `KV-Operation: PURGE` - Hard delete (removes history)
- `KV-Revision` - Used for optimistic concurrency control

## Error Handling

### Common Error Codes
- **10037**: Key not found
- **10071**: Wrong last sequence (revision mismatch)  
- **10048**: Key already exists (on CREATE operation)

### Error Response Format
```json
{
  "type": "io.nats.jetstream.api.v1.error_response",
  "error": {
    "code": 404,
    "err_code": 10037, 
    "description": "key not found"
  }
}
```

## Performance Considerations

### Optimization Tips
1. **Batch operations** when possible
2. **Use appropriate history** limits (default: 64)
3. **Enable compression** for large values
4. **Monitor bucket size** and set limits
5. **Use TTL** for automatic cleanup
6. **Choose storage backend** based on performance needs

### Monitoring
- Track bucket size and key count
- Monitor operation latencies
- Watch for revision conflicts
- Alert on storage limits

## Best Practices

1. **Design key hierarchies** for efficient watching and filtering
2. **Use meaningful key names** with consistent naming conventions  
3. **Handle revision conflicts** gracefully in concurrent scenarios
4. **Set appropriate TTLs** for automatic cleanup
5. **Use compression** for large values to save space
6. **Implement retry logic** for temporary failures
7. **Monitor bucket usage** and set appropriate limits
8. **Choose replication level** based on availability requirements