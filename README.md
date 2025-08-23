# NATS.zig

A Zig client library for NATS, the cloud-native messaging system.

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

# Run only end-to-end tests (requires Docker)
zig build test-e2e
```

### Prerequisites for E2E Tests

End-to-end tests require Docker and NATS servers running on ports 14222-14225:

```bash
docker compose -f docker-compose.test.yml up -d
```
