# JetStream Overview

JetStream is a distributed streaming platform built on top of NATS Core that provides:

- **Persistent message storage** with configurable retention policies
- **At-least-once delivery guarantees** with optional exactly-once semantics  
- **Horizontal scalability** with clustering and replication
- **Stream and consumer abstractions** for flexible message processing
- **Built-in Key/Value and Object stores** for data persistence

## Core Architecture

JetStream separates **storage** (Streams) from **consumption** (Consumers):

- **Stream**: Defines how messages are stored, retained, and replicated
- **Consumer**: Defines how messages are delivered and acknowledged

This separation allows multiple consumers with different processing requirements to consume from the same stream independently.

## Quality of Service

Unlike Core NATS which provides at-most-once delivery, JetStream provides:

- **At-least-once delivery** by default through message acknowledgments
- **Exactly-once delivery** when using message deduplication features
- **Persistent storage** survives server restarts and failures

## Key Features

### Streams
- Configurable retention: by limits, interest, or workqueue policy
- Storage backends: memory or file-based
- Replication: 1-5 replicas across cluster nodes
- Compression: S2 compression support
- Placement: geographical/resource constraints

### Consumers
- **Push consumers**: Server pushes messages to client subjects
- **Pull consumers**: Client requests message batches (recommended)
- **Ordered consumers**: Single-threaded with automatic flow control
- Acknowledgment policies: explicit, none, or all
- Delivery policies: all, last, new, by sequence/time

### Advanced Features
- Subject transformation and filtering
- Message deduplication with configurable time windows
- Direct message access without consumers
- Account-scoped isolation and resource limits
- Flow control and back-pressure handling

## Wire Protocol

JetStream uses NATS Core's text-based protocol with:

- **JSON-based APIs** on subjects like `$JS.API.*`
- **Standardized responses** with type indicators for JSON schema validation
- **Domain support** for multi-tenant deployments
- **Advisory messages** for system events and monitoring

## Getting Started

1. **Enable JetStream** on your NATS server
2. **Create a Stream** to define message storage
3. **Create a Consumer** to define message processing
4. **Publish messages** to stream subjects
5. **Consume messages** using pull or push patterns

For detailed protocol specifications, see the individual reference documents in this directory.