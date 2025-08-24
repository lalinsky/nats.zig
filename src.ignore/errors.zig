const std = @import("std");

pub const NatsError = error{
    // Connection errors
    ConnectionClosed,
    ConnectionTimeout,
    ConnectionRefused,
    NoServers,
    InvalidConnection,
    StaleConnection,
    Disconnected,

    // Authentication/Authorization
    AuthenticationRequired,
    AuthenticationFailed,
    AuthorizationViolation,
    AuthenticationExpired,

    // Protocol errors
    InvalidSubject,
    InvalidQueue,
    InvalidSid,
    InvalidProtocol,
    ParseError,
    MaxPayloadExceeded,
    MaxControlLineExceeded,

    // TLS errors
    TlsRequired,
    TlsHandshakeError,

    // Subscription errors
    InvalidSubscription,
    SlowConsumer,
    MaxConnectionsExceeded,
    MaxSubscriptionsExceeded,

    // Request/Reply errors
    NoResponders,
    RequestTimeout,

    // System errors
    OutOfMemory,
    Timeout,
    IoError,

    // URL parsing
    InvalidUrl,
    InvalidScheme,
    InvalidPort,
};
