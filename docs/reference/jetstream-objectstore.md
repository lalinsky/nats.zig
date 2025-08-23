# JetStream Object Store

The JetStream Object Store provides scalable storage for large objects by automatically chunking them into smaller pieces stored across JetStream streams. It supports metadata management, object versioning, and integrates with NATS security and clustering features.

## Architecture

### Design Principles
- Objects stored as chunked messages in JetStream streams
- Each object store backed by a dedicated stream
- Object metadata stored separately from object data
- Configurable chunk size for optimal performance
- Built on JetStream's persistence and replication

### Storage Structure
- **Object Info**: JSON metadata stored in stream messages
- **Object Chunks**: Binary data stored as message payloads
- **Chunk Subjects**: Follow pattern for ordered retrieval
- **Stream Names**: Use predictable naming conventions

## Object Metadata

### Object Information Structure
```json
{
  "name": "document.pdf",
  "description": "Project specification document", 
  "headers": {
    "Content-Type": "application/pdf",
    "Author": "platform-team"
  },
  "options": {
    "max_chunk_size": 131072,
    "ttl": 86400000000000,
    "compression": "s2"
  }
}
```

### Generated Fields
```json
{
  "nuid": "ABCDEF123456789",
  "size": 2048576,
  "chunks": 16,
  "digest": "SHA-256:a1b2c3d4...",
  "deleted": false,
  "revision": 3,
  "created": "2023-01-15T10:30:00Z",
  "modified": "2023-01-15T14:30:00Z"
}
```

## Object Operations

### Put Object
Store a new object:
```json
{
  "name": "report.json",
  "description": "Monthly analytics report",
  "data": "base64-encoded-object-data",
  "headers": {
    "Content-Type": "application/json",
    "Generated": "2023-01-15T10:30:00Z"
  },
  "options": {
    "max_chunk_size": 65536
  }
}
```

### Get Object
Retrieve complete object:
```json
{
  "name": "report.json"
}
```

Response includes reconstructed object data and metadata.

### Get Object Info
Retrieve only metadata without object data:
```json
{
  "name": "report.json",
  "info_only": true
}
```

### Update Metadata
Modify object metadata without changing data:
```json
{
  "name": "report.json",
  "description": "Updated monthly analytics report",
  "headers": {
    "Content-Type": "application/json",
    "Modified": "2023-01-15T15:00:00Z"
  }
}
```

### Delete Object
Mark object as deleted:
```json
{
  "name": "report.json",
  "purge": false
}
```

### List Objects
Enumerate objects in store:
```json
{
  "filter": "*.json",
  "include_deleted": false
}
```

## Object Store Configuration

### Basic Configuration
```json
{
  "bucket": "documents",
  "description": "Project document storage",
  "max_object_size": 10485760,
  "storage": "file", 
  "num_replicas": 1,
  "ttl": 0
}
```

### Advanced Configuration
```json
{
  "bucket": "media-assets",
  "description": "Media file storage", 
  "max_object_size": 104857600,
  "storage": "file",
  "num_replicas": 3,
  "compression": "s2",
  "placement": {
    "cluster": "storage-tier",
    "tags": ["high-capacity", "redundant"]
  },
  "metadata": {
    "department": "media",
    "retention_policy": "7-years"
  }
}
```

## Chunking Strategy

### Default Chunk Size
- **Standard**: 128KB chunks
- **Configurable** per object store or individual objects
- **Performance trade-offs**: Larger chunks = fewer messages, smaller chunks = more parallel processing

### Chunk Organization
- Chunks stored as sequential stream messages
- Chunk order preserved via message sequence
- Final chunk may be smaller than configured size
- Chunk subjects enable efficient retrieval

### Chunk Subjects Pattern
```
$OBJ.<bucket>.<object-nuid>.C.<chunk-number>
```

## API Operations

### Object Store Management
- Create object store: Configure backing stream
- Delete object store: Remove stream and all objects  
- Get store status: Stream information and statistics
- List object stores: Enumerate available stores

### Object Management
- Put: Store new object or update existing
- Get: Retrieve complete object data
- GetInfo: Retrieve metadata only
- Delete: Mark object as deleted
- List: Enumerate objects with filtering
- Watch: Monitor store changes
- UpdateMeta: Modify object metadata

## Advanced Features

### Compression
- **S2 compression**: Automatic compression of object chunks
- **Per-object**: Configurable compression settings
- **Storage efficiency**: Reduces storage requirements for text/JSON objects

### Linking
- **Object linking**: Reference relationships between objects
- **Bucket linking**: Cross-store references
- **Metadata links**: Store references in object headers

### Versioning
- **Revision tracking**: Automatic version numbers
- **Historical access**: Retrieve previous object versions
- **Version metadata**: Track changes and timestamps

### Security
- **Subject-based ACLs**: Control access to objects
- **Account isolation**: Objects scoped to NATS accounts
- **Encryption**: Application-level encryption supported
- **Audit trails**: Track object access and modifications

## Error Handling

### Common Error Codes
- **10404**: Object not found
- **10413**: Object too large  
- **10409**: Object already exists
- **10507**: Storage limit exceeded

### Error Response Format
```json
{
  "error": {
    "code": 404,
    "err_code": 10404,
    "description": "object not found"
  }
}
```

## Performance Optimization

### Chunk Size Tuning
- **Small objects** (< 128KB): Single chunk optimal
- **Large objects** (> 10MB): Consider larger chunks (512KB-1MB)
- **Network considerations**: Balance chunk size with network latency
- **Storage considerations**: Align with storage backend block sizes

### Concurrent Operations
- **Parallel chunk retrieval**: Reconstruct objects faster
- **Streaming**: Stream chunks during reconstruction
- **Caching**: Cache frequently accessed objects
- **Compression**: Enable for text-based objects

## Monitoring and Observability

### Key Metrics
- Object count and total storage usage
- Average object size and chunk count
- Put/Get operation rates and latencies  
- Storage efficiency (compression ratios)
- Error rates by operation type

### Stream Statistics
- Message count (total chunks + metadata)
- Storage bytes used
- Consumer activity
- Replication status across replicas

## Best Practices

1. **Choose appropriate chunk sizes** based on object types and network characteristics
2. **Enable compression** for text-based objects to save storage
3. **Use meaningful object names** with consistent naming conventions
4. **Set appropriate TTLs** for automatic cleanup of temporary objects
5. **Monitor storage usage** and set limits to prevent runaway growth
6. **Use replication** for critical objects requiring high availability
7. **Implement retry logic** for temporary failures during chunked operations
8. **Consider object lifecycle** - use metadata to track object stages
9. **Leverage NATS security** features for access control and audit trails
10. **Test with realistic object sizes** to validate performance characteristics