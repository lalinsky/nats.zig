// Copyright 2025 Lukas Lalinsky
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const Io = std.Io;
const xsync = @import("xsync");
const message_mod = @import("message.zig");
const Message = message_mod.Message;
const MessageList = message_mod.MessageList;
const STATUS_CONTROL = message_mod.STATUS_CONTROL;
const STATUS_NOT_FOUND = message_mod.STATUS_NOT_FOUND;
const STATUS_TIMEOUT = message_mod.STATUS_TIMEOUT;
const STATUS_MAX_BYTES = message_mod.STATUS_MAX_BYTES;
const STATUS_NO_RESPONSE = message_mod.STATUS_NO_RESPONSE;
const STATUS_CONFLICT: u16 = 409;

/// A status frame saying the consumer behind a pull no longer exists. Two shapes: a NEW
/// pull request to a missing consumer answers 503 "No Responders"; a pull request already
/// OPEN when the consumer is deleted answers 409 "Consumer Deleted" (the Go client's
/// `ErrConsumerDeleted`). Since a fetch keeps requests open across calls (patches 22–24),
/// the second is the usual one: counted as an expiry, it hid a gone consumer until the
/// next new request (NATS_ZIG_NOTES §33).
fn consumerGone(msg: *Message) bool {
    if (msg.status_code == STATUS_NO_RESPONSE) return true;
    if (msg.status_code != STATUS_CONFLICT) return false;
    const description = msg.headerGet("Description") orelse return false;
    return std.ascii.startsWithIgnoreCase(description, "Consumer Deleted");
}

const Connection = @import("connection.zig").Connection;
const Subscription = @import("subscription.zig").Subscription;
const subscription_mod = @import("subscription.zig");
const jetstream_message = @import("jetstream_message.zig");
const io_util = @import("io_util.zig");
const inbox = @import("inbox.zig");
const parseTimestamp = @import("timestamp.zig").parseTimestamp;
const Result = @import("result.zig").Result;

const log = @import("log.zig").log;

// Helper function to replace jsonStringifyAlloc
fn jsonStringifyAlloc(allocator: std.mem.Allocator, value: anytype, options: std.json.Stringify.Options) ![]u8 {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();

    try buffer.writer.print("{f}", .{std.json.fmt(value, options)});
    return buffer.toOwnedSlice();
}

// Re-export JetStream message types
pub const JetStreamMessage = jetstream_message.JetStreamMessage;
pub const MsgMetadata = jetstream_message.MsgMetadata;
pub const SequencePair = jetstream_message.SequencePair;

const default_api_prefix = "$JS.API.";
const default_request_timeout: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } };

// JetStream publish headers
const MsgIdHdr = "Nats-Msg-Id";
const ExpectedStreamHdr = "Nats-Expected-Stream";
const ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence";
const ExpectedLastSubjSeqHdr = "Nats-Expected-Last-Subject-Sequence";
const ExpectedLastSubjSeqSubjectHdr = "Nats-Expected-Last-Subject-Sequence-Subject";
const ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";
const MsgTTLHdr = "Nats-TTL";

const jetstream_errors = @import("jetstream_errors.zig");
const validation = @import("validation.zig");

const ErrorResponse = struct {
    @"error": struct {
        /// HTTP like error code in the 300 to 500 range
        code: u16,
        /// A human friendly description of the error
        description: []const u8 = "",
        /// The NATS error code unique to each kind of error
        err_code: u16 = 0,
    },
};

const AccountInfoResponse = struct {
    /// Memory Storage being used for Stream Message storage
    memory: u64,
    /// File Storage being used for Stream Message storage
    storage: u64,
    // Number of active Streams
    streams: u32,
    /// Number of active Consumers
    consumers: u32,
};

pub const StreamConfig = struct {
    /// A unique name for the Stream
    name: []const u8,
    /// A short description of the purpose of this stream
    description: ?[]const u8 = null,
    /// A list of subjects to consume, supports wildcards
    subjects: []const []const u8,
    /// How messages are retained in the stream
    retention: enum { limits, interest, workqueue } = .limits,
    /// How many Consumers can be defined for a given Stream. -1 for unlimited.
    max_consumers: i64 = -1,
    /// How many messages may be in a Stream. -1 for unlimited.
    max_msgs: i64 = -1,
    /// For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit
    max_msgs_per_subject: i64 = -1,
    /// How big the Stream may be. -1 for unlimited.
    max_bytes: i64 = -1,
    /// Maximum age of any message in nanoseconds. 0 for unlimited.
    max_age: u64 = 0,
    /// The largest message that will be accepted. -1 for unlimited.
    max_msg_size: i32 = -1,
    /// The storage backend to use for the Stream
    storage: enum { file, memory } = .file,
    /// Optional compression algorithm used for the Stream
    compression: enum { none, s2 } = .none,
    /// How many replicas to keep for each message
    num_replicas: u8 = 1,
    /// Disables acknowledging messages that are received by the Stream
    no_ack: bool = false,
    /// When a Stream reaches its limits either old messages are deleted or new ones are denied
    discard: enum { old, new } = .old,
    /// With `discard = .new` and `max_msgs_per_subject`, refuse a new message on a subject
    /// that is full instead of on the whole stream (requires NATS 2.10+)
    discard_new_per_subject: ?bool = null,
    /// The time window to track duplicate messages for, in nanoseconds. 0 for default
    duplicate_window: u64 = 0,
    /// Allow direct get on any replica instead of just the leader
    allow_direct: ?bool = null,
    /// Allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message
    allow_rollup_hdrs: ?bool = null,
    /// Whether to allow delete operations on individual messages
    deny_delete: ?bool = null,
    /// TTL for delete markers when subject is deleted by TTL (nanoseconds)
    subject_delete_marker_ttl: ?u64 = null,
};

/// Response from $JS.API.STREAM.NAMES
const StreamNamesResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    streams: ?[]const []const u8,
};

/// Response from $JS.API.STREAM.LIST
const StreamListResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    streams: ?[]const StreamInfo,
};

/// Response from $JS.API.CONSUMER.NAMES
const ConsumerNamesResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    consumers: ?[]const []const u8,
};

/// Response from $JS.API.CONSUMER.LIST
const ConsumerListResponse = struct {
    total: u64,
    offset: u64,
    limit: u64,
    consumers: ?[]const ConsumerInfo,
};

const StreamState = struct {
    messages: u64,
    bytes: u64,
    first_seq: u64,
    first_ts: []const u8,
    last_seq: u64,
    last_ts: []const u8,
    consumer_count: u32,
};

pub const StreamInfo = struct {
    config: StreamConfig,
    state: StreamState,
    created: []const u8,
};

pub const ConsumerConfig = struct {
    /// A unique name for a consumer
    name: ?[]const u8 = null,
    /// A unique name for a durable consumer (deprecated, use name)
    durable_name: ?[]const u8 = null,
    /// A short description of the purpose of this consumer
    description: ?[]const u8 = null,
    /// The point in the stream to receive messages from, either 'all', 'last', 'new', 'by_start_sequence', 'by_start_time', 'last_per_subject'.
    /// Not sent when null, which the server reads as 'all'.
    deliver_policy: ?enum { all, last, new, by_start_sequence, by_start_time, last_per_subject } = null,
    /// Used with deliver_policy 'by_start_sequence' to define the sequence to start at
    opt_start_seq: ?u64 = null,
    /// Used with deliver_policy 'by_start_time' to define the time to start at
    opt_start_time: ?[]const u8 = null,
    /// The subject to deliver messages to, omit for pull consumers
    deliver_subject: ?[]const u8 = null,
    /// How messages are acknowledged, either 'none', 'all', or 'explicit'.
    /// Not sent when null, which the server reads as 'none' - but the subscribe
    /// entry points ask for 'explicit' when the caller did not choose, since a
    /// subscription that silently never acknowledges is not a useful default.
    ack_policy: ?enum { none, all, explicit } = null,
    /// How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery.
    /// Not sent when null; the server uses 30 seconds for the 'explicit' and
    /// 'all' ack policies.
    ack_wait: ?u64 = null,
    /// The number of times a message will be redelivered to consumers if not acknowledged in time.
    /// Not sent when null; the server uses -1, no limit.
    max_deliver: ?i64 = null,
    /// Filter the stream by a single subject
    filter_subject: ?[]const u8 = null,
    /// Filter the stream by multiple subjects
    filter_subjects: ?[]const []const u8 = null,
    /// How messages are sent, either 'instant' or 'original'.
    /// Not sent when null, which the server reads as 'instant'.
    replay_policy: ?enum { instant, original } = null,
    /// The rate at which messages will be delivered to clients, expressed in bit per second
    rate_limit_bps: ?u64 = null,
    /// The maximum number of messages without acknowledgement that can be outstanding.
    /// Not sent when null, which lets the stream's consumer limits apply, and
    /// where they set nothing the server uses 1000 for consumers that
    /// acknowledge and no limit for ack policy 'none'.
    max_ack_pending: ?i64 = null,
    /// If the Consumer is idle for more than this many nano seconds a empty message with Status header 100 will be sent
    idle_heartbeat: ?u64 = null,
    /// For push consumers this will regularly send an empty mess with Status header 100 and a reply subject
    /// (This field was moved to the end to avoid duplication)
    /// The number of pulls that can be outstanding on a pull consumer
    max_waiting: ?i64 = null,
    /// Delivers only the headers of messages in the stream and not the bodies
    headers_only: ?bool = null,
    /// The largest batch property that may be specified when doing a pull on a Pull Consumer
    max_batch: ?i64 = null,
    /// How long to allow pull requests to remain open
    max_expires: ?u64 = null,
    /// The number of replicas for this consumer's state
    num_replicas: ?u8 = null,
    /// Queue group for push consumers (load balancing)
    deliver_group: ?[]const u8 = null,
    /// Enable flow control protocol for push consumers
    flow_control: ?bool = null,
    /// LOCAL FIX (see NOTES.md): nanoseconds of inactivity after which the SERVER
    /// deletes the consumer. Honoured for durable consumers too on nats-server >= 2.9,
    /// which is what makes a named consumer reapable from a client whose grants
    /// exclude `$JS.API.CONSUMER.DELETE`. Null (the default) keeps upstream behaviour:
    /// the consumer persists until deleted.
    inactive_threshold: ?u64 = null,
};

pub const ConsumerInfo = struct {
    /// The name of the consumer
    name: []const u8,
    /// The name of the stream this consumer belongs to
    stream_name: []const u8,
    /// The consumer configuration
    config: ConsumerConfig,
    /// The last delivered sequence for this consumer
    delivered: struct {
        consumer_seq: u64,
        stream_seq: u64,
    },
    /// The last acknowledged message
    ack_floor: struct {
        consumer_seq: u64,
        stream_seq: u64,
    },
    /// The number of pending messages for this consumer
    num_ack_pending: u64,
    /// The number of redelivered messages
    num_redelivered: u64,
    /// The number of waiting pull requests
    num_waiting: u64,
    /// The number of pending pull requests
    num_pending: u64,
    /// Whether a push consumer already has a subscription bound to it
    push_bound: ?bool = null,
    /// When this consumer was created
    created: []const u8,
};

/// Request for $JS.API.STREAM.PURGE
pub const StreamPurgeRequest = struct {
    /// Restrict purging to messages that match this subject
    filter: ?[]const u8 = null,
    /// Purge all messages up to but not including the message with this sequence
    seq: ?u64 = null,
    /// Ensures this many messages are present after the purge
    keep: ?u64 = null,
};

/// Response from $JS.API.STREAM.PURGE
const StreamPurgeResponse = struct {
    /// Indicates if this response is an error
    success: bool,
    /// The number of messages purged
    purged: u64,
};

/// Request for both $JS.API.STREAM.MSG.GET and $JS.API.DIRECT.GET
pub const GetMsgRequest = struct {
    /// Stream sequence number of the message to retrieve
    seq: ?u64 = null,
    /// Retrieves the last message for a given subject
    last_by_subj: ?[]const u8 = null,
    /// Retrieves the first message for a given subject at or after seq
    next_by_subj: ?[]const u8 = null,
    /// Number of messages to retrieve in batch mode (direct get only)
    batch: ?u32 = null,
    /// Maximum bytes to retrieve in batch mode (direct get only)
    max_bytes: ?u64 = null,
    /// Start time for time-based queries (RFC3339 format, direct get only)
    start_time: ?[]const u8 = null,
    /// Multiple subjects for multi-subject queries (direct get only)
    multi_last: ?[]const []const u8 = null,
    /// Upper bound sequence for consistent multi-subject reads (direct get only)
    up_to_seq: ?u64 = null,
    /// Upper bound time for consistent multi-subject reads (RFC3339 format, direct get only)
    up_to_time: ?[]const u8 = null,
    /// Suppress headers in response (direct get only)
    no_hdr: ?bool = null,
};

/// Options for getMsg() method
pub const GetMsgOptions = struct {
    /// Stream sequence number of the message to retrieve
    seq: ?u64 = null,
    /// Retrieves the last message for a given subject
    last_by_subj: ?[]const u8 = null,
    /// Retrieves the first message for a given subject at or after seq
    next_by_subj: ?[]const u8 = null,
    /// Use direct get API ($JS.API.DIRECT.GET) instead of legacy API
    direct: bool = false,
};

/// Request for $JS.API.STREAM.MSG.DELETE
pub const DeleteMsgRequest = struct {
    /// Stream sequence number of the message to delete
    seq: u64,
    /// Only remove the message, don't securely erase data
    no_erase: ?bool = null,
};

/// Response from $JS.API.STREAM.MSG.DELETE
const MsgDeleteResponse = struct {
    /// Indicates if the deletion was successful
    success: bool,
};

/// Response from $JS.API.STREAM.MSG.GET
const GetMsgResponse = struct {
    message: StoredMessage,
};

/// Stored message data from JetStream
pub const StoredMessage = struct {
    subject: []const u8,
    seq: u64,
    time: []const u8,
    hdrs: ?[]const u8 = null,
    data: []const u8,
};

/// Request for fetching messages from a pull consumer
pub const FetchRequest = struct {
    /// Maximum number of messages to fetch
    batch: usize = 1,
    /// Maximum bytes to fetch (optional)
    max_bytes: ?u64 = null,
    /// Request timeout in nanoseconds (default: 30 seconds)
    expires: ?u64 = null,
    /// Don't wait if no messages are available immediately
    no_wait: ?bool = null,
    /// Heartbeat interval in nanoseconds for long requests
    idle_heartbeat: ?u64 = null,
};

/// Options for getMsgs() batch method (future implementation)
pub const GetMsgsOptions = struct {
    // TODO: Define batch options based on ADR-31 when implemented
};

/// Publishing acknowledgment from JetStream
pub const PubAck = struct {
    /// Stream that received the message
    stream: []const u8,
    /// Sequence number assigned by the stream
    seq: u64,
    /// Whether this was a duplicate message
    duplicate: bool = false,
    /// JetStream domain (optional)
    domain: ?[]const u8 = null,
};

/// Options for JetStream publishing
pub const PublishOptions = struct {
    /// Message deduplication ID
    msg_id: ?[]const u8 = null,
    /// Expected target stream name
    expected_stream: ?[]const u8 = null,
    /// Expected last sequence number for optimistic concurrency control
    expected_last_seq: ?u64 = null,
    /// Expected last sequence number per subject
    expected_last_subject_seq: ?u64 = null,
    /// Override subject used for the per-subject sequence check
    expected_last_subject_seq_subject: ?[]const u8 = null,
    /// Expected last message ID
    expected_last_msg_id: ?[]const u8 = null,
    /// Message time-to-live in nanoseconds
    msg_ttl: ?Io.Duration = null,
    /// Wait between retries after a NoResponders response.
    retry_wait: Io.Duration = .fromMilliseconds(250),
    /// Number of retries after the initial publish attempt.
    retry_attempts: u32 = 2,
};

/// Batch of messages returned from fetch operation
pub const MessageBatch = struct {
    /// Allocator used for cleanup
    allocator: std.mem.Allocator,
    /// Array of JetStream messages
    messages: []*JetStreamMessage,
    /// Any error that occurred during fetch
    /// ⚠️ DEAD since 2026-08-29 — always `null`. Terminal statuses (408 expiry, 404
    /// no-messages, 503 no-responders) are now RETURNED by `fetch` when the batch is
    /// empty, and a partial batch is a success (nats.go's legacy `Fetch` contract).
    /// The field is kept so existing callers still compile; it carries nothing, and
    /// reading it is a mistake the old contract invited (see `fetch`). Remove it in the
    /// next breaking release.
    err: ?anyerror = null,

    pub fn deinit(self: *MessageBatch) void {
        // Clean up each message individually
        for (self.messages) |msg| {
            msg.deinit();
        }
        // Free the messages array
        self.allocator.free(self.messages);
    }
};

/// JetStream pull subscription
/// How long `fetch` keeps collecting after its first message (see `fetch`). 1 ms: a
/// batch already in flight arrives back-to-back on the socket, so this collects it;
/// anything longer is pure latency on a single event — measured end to end in
/// ZeBridge's libzb, 10 ms here made a 13 ms row, 1 ms makes it 3 ms.
const fetch_idle_after_first: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } };

/// How long a pull request stays open after its last delivered message (see
/// `requestDelivered`): the server may still be sending what it queued before `expires`.
const request_grace: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(2000), .clock = .awake } };

/// One outstanding pull request of a `PullSubscription` (see `reqs`).
/// Patch 26: how many "Message Size Exceeds MaxBytes" ends one fetch answers with a new
/// request while it holds no message. A routine end is followed by messages at once; three
/// in a row with nothing mean the next message is larger than `max_bytes` itself.
const max_bytes_empty_ends: u8 = 3;

const PullRequest = struct {
    reply: [160]u8 = undefined,
    reply_len: usize = 0,
    /// Messages the request still owes (the server serves a consumer's requests oldest first).
    left: usize = 0,
    deadline: Io.Timeout = .none,
    order: u64 = 0,
    /// `Connection.reconnect_epoch` when the request was sent: a reconnect since means
    /// the server dropped it, whatever its deadline says (see `slotOpen`).
    epoch: u32 = 0,
};

pub const PullSubscription = struct {
    /// How long `fetch` keeps collecting after its first message — `fetch_idle_after_first`
    /// (1 ms) unless the owner says otherwise. A CDC tail wants the 1 ms: one event, at
    /// once. An OBJECT reader wants the opposite: its `batch` chunks are contiguous and
    /// always all coming, and returning after the first one over a link where they do not
    /// land within a millisecond (any real network) makes the caller ask for the next
    /// batch while the rest of this one is still in flight — measured from an iPhone on
    /// Wi-Fi: 59 requests in a few seconds, 513 chunks delivered, 42 MB pending on the
    /// server, `Slow Consumer (Pending Bytes)`, connection cut. Set it to the fetch
    /// timeout and a fetch returns with the batch, or the deadline.
    idle_after_first: Io.Timeout = fetch_idle_after_first,
    /// The most bytes one pull request may ask the server for (null: the server's own
    /// limit). The server queues a request's messages for this connection at once, and
    /// over a slow link overlapping requests exceed its per-connection `max_pending`
    /// (64 MB): it then CLOSES the connection and every delivery in flight is lost
    /// (measured: an iPhone on Wi-Fi, 100 × 250 KB messages per request, disconnected 66
    /// times as a slow consumer). A cap keeps the queue bounded.
    max_bytes: ?u64 = null,
    /// The ONE pull request this consumer has outstanding: its reply subject (length 0:
    /// none), the messages it still owes, and when the server expires it. A fetch that
    /// returns early (after its first message, `idle_after_first`) leaves the rest of its
    /// request in flight; asking again then — which every fetch did — stacked request on
    /// request, each queued in full by the server for this connection. Over Wi-Fi an
    /// iPhone polling once a second held a dozen 8 MB requests at once, passed the
    /// server's 64 MB per-connection queue and was disconnected as a slow consumer every
    /// few seconds, losing every delivery in flight (ZeBridge §10jc). One outstanding
    /// request bounds the bytes in flight by `max_bytes`, whatever the link.
    reqs: [2]PullRequest = .{ .{}, .{} },
    req_order: u64 = 0,
    /// How many requests may be outstanding at once, at most 2. 1: the caller's apply
    /// and the next transfer alternate. 2: the next request downloads while the caller
    /// applies the last batch — on an iPhone over Wi-Fi a catch-up whose transfer and
    /// apply each take ~1.5 s per 8 MB goes about twice as fast. Bytes in flight stay
    /// bounded by `pull_depth × max_bytes`.
    pull_depth: usize = 1,
    /// JetStream context
    js: JetStream,
    /// Consumer information
    consumer_info: Result(ConsumerInfo),
    /// Persistent wildcard inbox subscription
    inbox_subscription: *Subscription,
    /// Inbox prefix for reply subjects (e.g., "_INBOX.abc123.")
    inbox_prefix: []u8,
    /// Fetch ID counter for unique reply subjects
    fetch_id_counter: u64 = 0,
    /// Mutex for fiber safety
    mutex: xsync.Mutex = .init,
    /// False when `inbox_subscription`/`inbox_prefix` belong to a shared `PullInbox`.
    owns_inbox: bool = true,

    pub fn deinit(self: *PullSubscription) void {
        self.consumer_info.deinit();
        if (self.owns_inbox) {
            self.inbox_subscription.deinit(); // This will unsubscribe and release the user reference
            self.js.nc.allocator.free(self.inbox_prefix);
        }
        self.js.nc.allocator.destroy(self);
    }

    /// The pull API subject for this consumer, allocated from `a`.
    fn nextSubject(self: *PullSubscription, a: std.mem.Allocator) ![]u8 {
        return self.js.apiSubject(a, "CONSUMER.MSG.NEXT.{s}.{s}", .{ self.consumer_info.value.stream_name, self.consumer_info.value.name });
    }

    /// Fetch a batch of messages from the pull consumer
    /// A data message the server has ALREADY delivered to this consumer's inbox — the
    /// reply to a pull request still parked at the server after `fetch` returned early —
    /// without issuing a new request. Status frames are skipped; null at the deadline.
    ///
    /// What a clean stop needs: after the last `fetch`, its request stays parked for up
    /// to its `expires`, and messages delivered into that window reach a subscription
    /// nobody reads — in flight until their ack wait runs out. Reading the inbox for that
    /// window, then leaving, is the difference between a restart that replays and one
    /// that does not (ZeBridge NOTES §10ec).
    pub fn nextDelivered(self: *PullSubscription, timeout: Io.Timeout) !?*JetStreamMessage {
        if (!self.owns_inbox) return error.SharedInbox;
        try self.mutex.lock(self.js.nc.io);
        defer self.mutex.unlock(self.js.nc.io);
        const deadline = timeout.toDeadline(self.js.nc.io);
        while (true) {
            const raw = self.inbox_subscription.nextMsgTimeout(deadline) catch |err| switch (err) {
                error.Timeout => return null,
                else => return err,
            };
            if (raw.status_code > 0) {
                raw.deinit();
                continue;
            }
            return try jetstream_message.createJetStreamMessage(self.js.nc, raw);
        }
    }

    /// A request still owes messages and has not expired (an expired one is forgotten).
    fn slotOpen(self: *PullSubscription, r: *PullRequest) bool {
        if (r.reply_len == 0 or r.left == 0) return false;
        // Sent before a reconnect: the server forgot it, so it will never deliver or
        // expire. Holding the slot would leave the consumer without a request until the
        // deadline — with a long `PullInbox.request_expires_ms`, live changes would wait.
        if (r.epoch != self.js.nc.reconnect_epoch.load(.acquire)) {
            r.reply_len = 0;
            return false;
        }
        const d = r.deadline.toDurationFromNow(self.js.nc.io) orelse {
            r.reply_len = 0;
            return false;
        };
        if (d.raw.toNanoseconds() <= 0) {
            r.reply_len = 0;
            return false;
        }
        return true;
    }

    /// Fewer requests outstanding than `pull_depth` allows.
    fn wantsRequest(self: *PullSubscription) bool {
        var n: usize = 0;
        for (&self.reqs) |*r| {
            if (self.slotOpen(r)) n += 1;
        }
        return n < @min(@max(self.pull_depth, 1), self.reqs.len);
    }

    fn requestStart(self: *PullSubscription, reply: []const u8, batch: usize, deadline: Io.Timeout) void {
        for (&self.reqs) |*r| {
            if (self.slotOpen(r)) continue;
            const n = @min(reply.len, r.reply.len);
            @memcpy(r.reply[0..n], reply[0..n]);
            r.reply_len = n;
            r.left = batch;
            r.deadline = deadline;
            r.epoch = self.js.nc.reconnect_epoch.load(.acquire);
            self.req_order += 1;
            r.order = self.req_order;
            return;
        }
    }

    /// A data message: owed by the OLDEST open request.
    fn requestDelivered(self: *PullSubscription) void {
        var oldest: ?*PullRequest = null;
        for (&self.reqs) |*r| {
            if (r.reply_len == 0 or r.left == 0) continue;
            if (oldest == null or r.order < oldest.?.order) oldest = r;
        }
        if (oldest) |o| {
            o.left -= 1;
            if (o.left == 0) {
                o.reply_len = 0;
            } else {
                // Still arriving: the request is not over while its messages are on the
                // wire, whatever its `expires` said. The server queues a request's messages
                // at once; over a link slower than that (an 8 MB request at ~5 MB/s takes
                // 1.6 s, its `expires` was 0.9 s) the slot used to free while data was still
                // in flight, the next fetch asked again, and the bytes in flight grew past
                // `pull_depth × max_bytes` — one more slow-consumer disconnect on the phone.
                // Each delivery keeps the request open for `request_grace` more.
                const grace = request_grace.toDeadline(self.js.nc.io);
                const cur = o.deadline.toDurationFromNow(self.js.nc.io);
                const ext = grace.toDurationFromNow(self.js.nc.io);
                if (cur == null or (ext != null and ext.?.raw.toNanoseconds() > cur.?.raw.toNanoseconds())) o.deadline = grace;
            }
        }
    }

    fn hasReply(self: *const PullSubscription, subject: []const u8) bool {
        for (&self.reqs) |*r| {
            if (r.reply_len > 0 and std.mem.eql(u8, r.reply[0..r.reply_len], subject)) return true;
        }
        return false;
    }

    /// A terminal status for the request answering on `subject`.
    fn requestEndReply(self: *PullSubscription, subject: []const u8) void {
        for (&self.reqs) |*r| {
            if (r.reply_len > 0 and std.mem.eql(u8, r.reply[0..r.reply_len], subject)) {
                r.reply_len = 0;
                r.left = 0;
                return;
            }
        }
    }

    /// Sends pull requests while a slot is open (`pull_depth`, patch 23) — at the start of a
    /// fetch, and again inside it when a request ended with room for another (patch 26).
    fn sendRequests(self: *PullSubscription, batch: usize, timeout: Io.Timeout) !void {
        while (self.wantsRequest()) {
            self.fetch_id_counter += 1;
            const new_reply = try std.fmt.allocPrint(self.js.nc.allocator, "{s}{d}", .{ self.inbox_prefix, self.fetch_id_counter });
            defer self.js.nc.allocator.free(new_reply);
            const timeout_duration = timeout.toDurationFromNow(self.js.nc.io);
            const request = FetchRequest{
                .batch = batch,
                .max_bytes = self.max_bytes,
                .expires = if (timeout_duration) |duration| @intCast(duration.raw.toNanoseconds()) else null,
            };
            const request_json = try jsonStringifyAlloc(self.js.nc.allocator, request, .{
                .emit_null_optional_fields = false,
            });
            defer self.js.nc.allocator.free(request_json);
            const api_subject = try self.js.apiSubject(self.js.nc.allocator, "CONSUMER.MSG.NEXT.{s}.{s}", .{ self.consumer_info.value.stream_name, self.consumer_info.value.name });
            defer self.js.nc.allocator.free(api_subject);
            try self.js.nc.publishRequest(api_subject, new_reply, request_json);
            self.requestStart(new_reply, batch, timeout.toDeadline(self.js.nc.io));
        }
    }

    pub fn fetch(self: *PullSubscription, batch: usize, timeout: Io.Timeout) !MessageBatch {
        if (batch == 0) return error.InvalidBatchSize;
        // A shared inbox is read by `PullInbox.fetch` only: reading it here would
        // consume the other consumers' replies.
        if (!self.owns_inbox) return error.SharedInbox;

        try self.mutex.lock(self.js.nc.io);
        defer self.mutex.unlock(self.js.nc.io);

        // At most `pull_depth` requests outstanding (see `reqs`): a new one only while
        // fewer are open — delivered in full, ended with a status, or expired. The inbox
        // holds only what the open ones already sent, which this fetch collects first.
        try self.sendRequests(batch, timeout);
        // Collect messages
        var messages = std.ArrayList(*JetStreamMessage).empty;
        defer messages.deinit(self.js.nc.allocator);
        errdefer for (messages.items) |js_msg| js_msg.deinit();

        var batch_complete = false;
        var fetch_error: ?anyerror = null;
        // Patch 26: max-bytes ends met with nothing in hand, in a row (see the 409 branch).
        var empty_ends: u8 = 0;

        // Convert the timeout to an absolute deadline so that receiving a
        // message (or a heartbeat) does not re-arm the full timeout.
        //
        // ⚠️ Once the FIRST message has arrived the deadline shrinks to a short idle
        // window (`fetch_idle_after_first`): a fetch returns as soon as it has
        // something and the in-flight remainder has been collected, instead of
        // parking until `timeout` in the hope of filling the batch. This is nats.go's
        // legacy `Fetch` contract, and the difference between a caller seeing a row at
        // once and seeing it when the expiry fires (measured in libzb: one queued
        // message, a 3 s fetch for 100, returned at 3006 ms). The server keeps the
        // pull request parked until its own `expires`; its late 408 is attributed to
        // this reply subject and dropped by the guard below on a later fetch.
        var receive_deadline = timeout.toDeadline(self.js.nc.io);

        // Collect messages until batch is complete or timeout — the caller's batch, which
        // the queue's share and the request's share make up together.
        while (!batch_complete and messages.items.len < batch) {
            if (self.inbox_subscription.nextMsgTimeout(receive_deadline)) |raw_msg| {
                log.debug("Message: subject={s}, reply={s}, data='{s}'", .{ raw_msg.subject, raw_msg.reply orelse "none", raw_msg.data });

                // ⚠️ `inbox_subscription` is a WILDCARD (`<prefix>.*`), so it also receives
                // replies addressed to *earlier* fetches. A fetch that returns before its
                // `expires` leaves its request parked on the server until the server times
                // it out, and that late 408 then lands here while a newer fetch is waiting.
                //
                // Accepting it ends the new fetch early, the caller re-fetches at once, and
                // that leaves another parked request behind — self-sustaining, settling at
                // `expires / period` parked pulls. Measured against an idle stream: 500 ms
                // expiry, a new fetch every 48 ms, num_waiting=10, ~21 pull requests/sec
                // with nothing to deliver.
                //
                // ⚠️ Applies to STATUS frames only. JetStream delivers a *data* message
                // with its original subject (`test.foo`), routed to us by subscription id,
                // so matching those against the inbox would drop every real message — the
                // e2e suite catches exactly that. Status frames are addressed to the inbox
                // (`HMSG _INBOX.<prefix>.<fetch_id>`), so they can be attributed, and they
                // are the only ones this loop treats as terminal.
                if (raw_msg.status_code > 0 and !self.hasReply(raw_msg.subject)) {
                    raw_msg.deinit();
                    continue;
                }

                if (raw_msg.status_code > 0 and raw_msg.status_code != STATUS_CONTROL) self.requestEndReply(raw_msg.subject);
                if (raw_msg.status_code == STATUS_NOT_FOUND) {
                    // No messages available (404, a no_wait pull): "nothing before the
                    // deadline", the same answer as an expiry.
                    raw_msg.deinit();
                    if (messages.items.len == 0) fetch_error = error.Timeout;
                    batch_complete = true;
                    break;
                } else if (consumerGone(raw_msg)) {
                    // ⚠️ 503 "No Responders" (or 409 "Consumer Deleted" on a request that
                    // was already open, see consumerGone): nothing serves this consumer's pull subject —
                    // the consumer was deleted, reaped by its inactive threshold, or never
                    // created. Folding it into the unknown-status branch (as before) made
                    // a gone consumer look exactly like an idle one: the caller waited out
                    // the whole timeout and got `error.Timeout`. Verified against
                    // nats-server 2.14.5: `$JS.API.CONSUMER.MSG.NEXT.<stream>.<missing>`
                    // answers a bare 503.
                    raw_msg.deinit();
                    if (messages.items.len == 0) fetch_error = error.NoResponders;
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_TIMEOUT) {
                    // Request timeout
                    raw_msg.deinit();
                    if (messages.items.len == 0) {
                        fetch_error = error.Timeout;
                    }
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_MAX_BYTES) {
                    // ⚠️ 409 means several things; its Description says which. "Message
                    // Size Exceeds MaxBytes" is the ROUTINE end of one request that carries
                    // `max_bytes` (patch 21): the next message did not fit what the request
                    // had left. At a 2 MB cap and 256 KB CDC batches that is every few
                    // messages. Reported as an error it ended the caller's fetch with
                    // `ConsumerSequenceMismatch` whenever it arrived before any message —
                    // and a CLI follower exited on it (ZeBridge NOTES §10je). Now: with
                    // messages in hand the batch is complete, as for the expiry; with none,
                    // a replacement request goes out at once and the fetch keeps waiting.
                    // A message larger than `max_bytes` itself ends EVERY request this way
                    // with nothing delivered; re-asking would loop until the deadline. So a
                    // fetch re-asks at most `max_bytes_empty_ends` times in a row with
                    // nothing in hand, then reports it. (Telling the two apart per request
                    // does not work: which request a message answers is the server's
                    // business, and the client's oldest-first guess measured wrong.) Every
                    // other 409 (Consumer Deleted, Exceeded MaxWaiting, …) stays an error.
                    const routine = if (raw_msg.headerGet("Description")) |d|
                        std.mem.eql(u8, std.mem.trim(u8, d, " \t"), "Message Size Exceeds MaxBytes")
                    else
                        false;
                    raw_msg.deinit();
                    if (routine) {
                        if (messages.items.len > 0) {
                            batch_complete = true;
                            break;
                        }
                        if (empty_ends < max_bytes_empty_ends) {
                            empty_ends += 1;
                            try self.sendRequests(batch, timeout);
                            continue;
                        }
                    }
                    fetch_error = error.ConsumerSequenceMismatch;
                    batch_complete = true;
                    break;
                } else if (raw_msg.status_code == STATUS_CONTROL) {
                    // Heartbeat - continue waiting
                    raw_msg.deinit();
                    continue;
                } else if (raw_msg.status_code > 0) {
                    // Unknown status code - clean up and continue
                    raw_msg.deinit();
                } else {
                    // This is a regular message - convert to JetStream message
                    const js_msg_ptr = jetstream_message.createJetStreamMessage(self.js.nc, raw_msg) catch |err| {
                        raw_msg.deinit();
                        return err;
                    };
                    errdefer js_msg_ptr.deinit();

                    try messages.append(self.js.nc.allocator, js_msg_ptr);
                    self.requestDelivered();
                    // Something arrived: from here on wait only for what is already in flight.
                    if (messages.items.len == 1) {
                        receive_deadline = self.idle_after_first.toDeadline(self.js.nc.io);
                    }
                }
            } else |err| switch (err) {
                error.Timeout => {
                    // The CLIENT-side deadline — it fires before the server's 408 when
                    // the two are equal, so this is the usual way an idle fetch ends.
                    if (messages.items.len == 0) fetch_error = error.Timeout;
                    batch_complete = true;
                },
                error.Canceled, error.ConnectionClosed, error.SlowConsumer => {
                    // Propagate cancellation and closed connections.
                    // SlowConsumer cannot occur here (the inbox limits are
                    // sized to the consumer's ack window), but if it ever
                    // does, surfacing it beats silently losing a gap.
                    return err;
                },
            }
        }

        // ⚠️ nats.go's legacy `Fetch` contract, which fits a SYNCHRONOUS call: an empty
        // batch with a terminal status is an ERROR (`Timeout` for the 408 expiry,
        // `NoResponders` for the 503), and a partial batch is a success with `err`
        // null — the caller got messages, there is nothing to report. Parking the error
        // on the batch (the async `jetstream.MessageBatch` shape) let a caller `try` the
        // fetch and never look at `err`; every `catch error.Timeout` written against the
        // old behaviour was dead code.
        if (messages.items.len == 0) {
            if (fetch_error) |err| return err;
        }

        // Convert ArrayList to owned slice
        const messages_slice = try messages.toOwnedSlice(self.js.nc.allocator);

        return MessageBatch{
            .messages = messages_slice,
            .err = null,
            .allocator = self.js.nc.allocator,
        };
    }
};

/// What `PullInbox.fetch` returns: messages from ANY of the consumers asked (each
/// says which via `metadata.stream` / `metadata.consumer`), and per consumer whether
/// nobody served it (`gone[i]`: a 503 — deleted, reaped, never created). A partial
/// failure is the normal case here, which is why it is a field and not an error: one
/// consumer of ten going away must not hide the nine that answered.
pub const MultiBatch = struct {
    allocator: std.mem.Allocator,
    messages: []*JetStreamMessage,
    /// Indexed like the `subs` slice given to `fetch`.
    gone: []bool,

    pub fn deinit(self: *MultiBatch) void {
        for (self.messages) |m| m.deinit();
        self.allocator.free(self.messages);
        self.allocator.free(self.gone);
    }
};

/// One inbox, several pull consumers, ONE wait.
///
/// A `PullSubscription` owns a private wildcard inbox, so a client tailing N streams
/// can block on one consumer at a time and must alternate between them in slices —
/// measured in ZeBridge's libzb: two streams, 250 ms slices, a busy stream waits out
/// the idle one's slice. With a shared inbox every consumer answers into the same
/// subscription; `fetch` issues one pull per consumer and then waits on the single
/// queue, so the first message from ANY of them ends the wait.
///
/// Attribution is what makes this safe: a status frame (404/408/503) is addressed to
/// the reply subject of the pull it answers, and a data message carries its stream
/// and consumer in its ack subject (`metadata`). Replies to an EARLIER fetch — a
/// parked pull that expired after we returned, or its late messages — are therefore
/// either dropped (stale status) or delivered to the right consumer anyway (data).
///
/// Create with `JetStream.pullInbox`; pass to `pullSubscribe` as `.inbox`. Not
/// fiber-safe across concurrent `fetch` calls (one waiter per inbox, by design).
pub const PullInbox = struct {
    js: JetStream,
    inbox_subscription: *Subscription,
    inbox_prefix: []u8,
    fetch_id_counter: u64 = 0,
    mutex: xsync.Mutex = .init,
    /// As `PullSubscription.max_bytes`, for every consumer's request on this inbox.
    max_bytes: ?u64 = null,
    /// How long the server keeps each pull request waiting for messages, independent of
    /// how long one `fetch` waits locally (null: the fetch's own timeout, as before). A
    /// caller that fetches often with a short timeout otherwise sends a new request per
    /// consumer each time, whether or not anything arrived: ZeBridge clients polling every
    /// 250 ms sent ~6.5 pull requests a second per idle stream, about 13 a second per
    /// connected device. With a long expiry the request stays parked, delivers each
    /// message as it is stored, and the next fetch finds it in the inbox; a new request
    /// goes out only when one is filled or expires (`wantsRequest`), or after a reconnect.
    request_expires_ms: ?u32 = null,

    pub fn deinit(self: *PullInbox) void {
        self.inbox_subscription.deinit();
        self.js.nc.allocator.free(self.inbox_prefix);
        self.js.nc.allocator.destroy(self);
    }

    /// Pull up to `batch` messages from EACH of `subs` (all on this inbox), waiting
    /// at most `timeout` for the first message from any of them, then a short idle
    /// window for what is in flight. Returns `error.NoResponders` only when EVERY
    /// consumer answered 503; otherwise a `MultiBatch`, possibly with no messages
    /// (the expiry), whose `gone` says which consumers need re-creating.
    pub fn fetch(self: *PullInbox, subs: []const *PullSubscription, batch: usize, timeout: Io.Timeout) !MultiBatch {
        if (batch == 0) return error.InvalidBatchSize;
        if (subs.len == 0) return error.NoSubscriptions;
        const a = self.js.nc.allocator;

        try self.mutex.lock(self.js.nc.io);
        defer self.mutex.unlock(self.js.nc.io);

        self.fetch_id_counter += 1;
        const fetch_id = self.fetch_id_counter;

        // At most `pull_depth` requests outstanding per consumer (see
        // `PullSubscription.reqs`); a new request only while fewer are open. Reply subjects
        // `<prefix><fetch>x<i>y<k>` — one token, as the wildcard requires — so a status
        // frame names the request (and so the consumer) it answers.
        const cap = batch * subs.len;
        // The request's own lifetime on the server: the inbox's `request_expires_ms`, else
        // this fetch's timeout.
        const request_timeout: Io.Timeout = if (self.request_expires_ms) |ms|
            .{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } }
        else
            timeout;
        const request_duration = request_timeout.toDurationFromNow(self.js.nc.io);
        for (subs, 0..) |sub, i| {
            if (sub.inbox_subscription != self.inbox_subscription) return error.NotOnThisInbox;
            var k: usize = 0;
            while (sub.wantsRequest()) : (k += 1) {
                const new_reply = try std.fmt.allocPrint(a, "{s}{d}x{d}y{d}", .{ self.inbox_prefix, fetch_id, i, k });
                defer a.free(new_reply);
                const request_json = try jsonStringifyAlloc(a, FetchRequest{
                    .batch = batch,
                    .max_bytes = self.max_bytes,
                    .expires = if (request_duration) |d| @intCast(d.raw.toNanoseconds()) else null,
                }, .{ .emit_null_optional_fields = false });
                defer a.free(request_json);
                const api_subject = try sub.nextSubject(a);
                defer a.free(api_subject);
                try self.js.nc.publishRequest(api_subject, new_reply, request_json);
                sub.requestStart(new_reply, batch, request_timeout.toDeadline(self.js.nc.io));
            }
        }

        const gone = try a.alloc(bool, subs.len);
        errdefer a.free(gone);
        @memset(gone, false);
        const done = try a.alloc(bool, subs.len);
        defer a.free(done);
        @memset(done, false);
        var open: usize = subs.len;

        var messages = std.ArrayList(*JetStreamMessage).empty;
        defer messages.deinit(a);
        errdefer for (messages.items) |m| m.deinit();

        var receive_deadline = timeout.toDeadline(self.js.nc.io);

        while (open > 0 and messages.items.len < cap) {
            const raw_msg = self.inbox_subscription.nextMsgTimeout(receive_deadline) catch |err| switch (err) {
                error.Timeout => break,
                error.Canceled, error.ConnectionClosed, error.SlowConsumer => return err,
            };
            if (raw_msg.status_code > 0) {
                // Which request does this frame answer? One no longer open (an earlier,
                // finished one) is dropped.
                const idx: ?usize = for (subs, 0..) |sub, i| {
                    if (sub.hasReply(raw_msg.subject)) break i;
                } else null;
                const code = raw_msg.status_code;
                const is_gone = consumerGone(raw_msg); // read before the frame is freed
                if (idx) |hit| if (code != STATUS_CONTROL) subs[hit].requestEndReply(raw_msg.subject);
                raw_msg.deinit();
                const i = idx orelse continue;
                if (code == STATUS_CONTROL) continue; // heartbeat
                if (done[i]) continue;
                if (is_gone) gone[i] = true;
                // 404, 408, 409, 503 — all terminal for this consumer's pull.
                done[i] = true;
                open -= 1;
                continue;
            }
            const js_msg = jetstream_message.createJetStreamMessage(self.js.nc, raw_msg) catch |err| {
                raw_msg.deinit();
                return err;
            };
            errdefer js_msg.deinit();
            try messages.append(a, js_msg);
            // Owed by whichever consumer delivered it (its ack metadata names it).
            for (subs) |sub| if (std.mem.eql(u8, sub.consumer_info.value.name, js_msg.metadata.consumer)) {
                sub.requestDelivered();
                break;
            };
            // Something arrived: from here on wait only for what is already in flight.
            if (messages.items.len == 1) receive_deadline = fetch_idle_after_first.toDeadline(self.js.nc.io);
        }

        if (messages.items.len == 0) {
            var all_gone = true;
            for (gone) |g| all_gone = all_gone and g;
            if (all_gone) {
                // No explicit free: the errdefer on `gone`'s allocation runs on this
                // error return. Freeing here too was a DOUBLE FREE — and it needed every
                // consumer on the inbox to answer terminally-gone in one fetch to fire,
                // which is exactly what a deleted stream produces and nothing else did
                // (found by stream_wipe.py: free() abort inside PullInbox.fetch, 2026-09-03).
                return error.NoResponders;
            }
        }
        return MultiBatch{
            .allocator = a,
            .messages = try messages.toOwnedSlice(a),
            .gone = gone,
        };
    }
};

/// JetStream push subscription
pub const JetStreamSubscription = struct {
    /// Underlying NATS subscription
    subscription: *Subscription,
    /// JetStream context
    js: JetStream,
    /// Consumer information (Result wrapper)
    consumer_info: Result(ConsumerInfo),
    /// Owned deliver subject when auto-generated (null if provided by user)
    deliver_subject_owned: ?[]u8 = null,

    pub fn deinit(self: *JetStreamSubscription) void {
        self.consumer_info.deinit();
        self.subscription.deinit();
        if (self.deliver_subject_owned) |ds| {
            self.js.nc.allocator.free(ds);
        }
        self.js.nc.allocator.destroy(self);
    }

    /// Wait indefinitely for the next JetStream message.
    pub fn nextMsg(self: *JetStreamSubscription) !*JetStreamMessage {
        const msg = try self.subscription.nextMsg();
        errdefer msg.deinit();

        return try jetstream_message.createJetStreamMessage(self.js.nc, msg);
    }

    /// Wait up to `timeout` for the next JetStream message.
    pub fn nextMsgTimeout(self: *JetStreamSubscription, timeout: Io.Timeout) !*JetStreamMessage {
        // Get the next message from the underlying subscription
        const msg = try self.subscription.nextMsgTimeout(timeout);
        errdefer msg.deinit();

        // Convert to JetStream message
        return try jetstream_message.createJetStreamMessage(self.js.nc, msg);
    }
};

/// Options for push subscriptions (subscribe, subscribeSync, queueSubscribe)
pub const SubscribeOptions = struct {
    /// Stream name - auto-discovered from subject if null
    stream: ?[]const u8 = null,

    /// Durable consumer name - creates durable consumer if provided
    durable: ?[]const u8 = null,

    /// Full consumer configuration - merged with other options
    config: ?ConsumerConfig = null,

    /// Manual acknowledgment mode (default: false = auto-ack like nats.c)
    manual_ack: bool = false,
};

/// Options for pull subscriptions
pub const PullSubscribeOptions = struct {
    /// Stream name - auto-discovered from subject if null
    stream: ?[]const u8 = null,

    /// Full consumer configuration
    config: ?ConsumerConfig = null,

    /// Answer into a SHARED inbox (`JetStream.pullInbox`) instead of a private one.
    /// The subscription is then fetched through `PullInbox.fetch` together with
    /// every other subscription on that inbox — `PullSubscription.fetch` refuses it —
    /// and the inbox must outlive the subscription.
    inbox: ?*PullInbox = null,
};

pub const JetStreamOptions = struct {
    request_timeout: Io.Timeout = default_request_timeout,
    /// The JetStream domain to address. Null speaks to the server's own JetStream at
    /// `$JS.API.`; a name speaks to that domain at `$JS.<domain>.API.` — what a client
    /// on a leaf node uses to reach the hub's JetStream (nats.go's `nats.Domain`).
    domain: ?[]const u8 = null,
};

pub const JetStream = struct {
    nc: *Connection,
    opts: JetStreamOptions,

    pub fn init(nc: *Connection, options: JetStreamOptions) JetStream {
        return .{
            .nc = nc,
            .opts = options,
        };
    }

    /// An API subject under this context's prefix: `$JS.API.<tail>`, or
    /// `$JS.<domain>.API.<tail>` when a domain is set. Every request the context
    /// sends is built here, so the domain is decided in one place.
    pub fn apiSubject(self: JetStream, a: std.mem.Allocator, comptime tail_fmt: []const u8, args: anytype) ![]u8 {
        if (self.opts.domain) |d| return std.fmt.allocPrint(a, "$JS.{s}.API." ++ tail_fmt, .{d} ++ args);
        return std.fmt.allocPrint(a, default_api_prefix ++ tail_fmt, args);
    }

    fn sendRequest(self: JetStream, subject: []const u8, payload: []const u8) !*Message {
        const full_subject = try self.apiSubject(self.nc.allocator, "{s}", .{subject});
        defer self.nc.allocator.free(full_subject);

        return try self.nc.request(full_subject, payload, self.opts.request_timeout);
    }

    /// Parse an error response from the server, if present.
    fn maybeParseErrorResponse(msg: *Message) !void {
        var buf: [1024]u8 = undefined;
        var allocator = std.heap.FixedBufferAllocator.init(&buf);

        const response = std.json.parseFromSliceLeaky(ErrorResponse, allocator.allocator(), msg.data, .{
            .allocate = .alloc_if_needed,
            .ignore_unknown_fields = true,
        }) catch return;

        const info = response.@"error";
        // The caller gets this as a TYPED error and decides whether it is an incident: a
        // bucket that already exists on a create, a consumer not found on a
        // look-before-create, are answers, not failures. Logging them here at error
        // level reported every routine probe as an incident (NATS_ZIG_NOTES entry 10).
        log.debug("JetStream error: code={d} err_code={d} description={s}", .{ info.code, info.err_code, info.description });
        log.debug("Full response: {s}", .{msg.data});

        // Map specific JetStream error codes to proper error types
        return jetstream_errors.mapErrorCode(info.err_code);
    }

    /// Lookup stream name by subject pattern
    fn lookupStreamBySubject(self: JetStream, subject: []const u8) ![]const u8 {
        try validation.validateSubject(subject);

        // Build the subject filter request
        const request_payload = struct {
            subject: []const u8,
        }{ .subject = subject };

        const request_json = try jsonStringifyAlloc(self.nc.allocator, request_payload, .{
            .emit_null_optional_fields = false,
        });
        defer self.nc.allocator.free(request_json);

        const msg = try self.sendRequest("STREAM.NAMES", request_json);
        defer msg.deinit();

        const names_result = try self.parseResponse(StreamNamesResponse, msg);
        defer names_result.deinit();

        const streams = names_result.value.streams orelse return error.NoStreamMatches;

        if (streams.len == 0) {
            return error.NoStreamMatches;
        } else if (streams.len > 1) {
            return error.MultipleStreamMatches;
        }

        // Return a copy of the stream name that we own
        return try self.nc.allocator.dupe(u8, streams[0]);
    }

    /// Fill in the consumer config fields that follow from the subscribe call
    /// itself rather than from the caller's config.
    fn buildConsumerConfig(base_config: ?ConsumerConfig, subject: ?[]const u8, durable: ?[]const u8, is_pull: bool, queue: ?[]const u8) ConsumerConfig {
        // Start with base config or empty config
        var config = base_config orelse ConsumerConfig{};

        // Set consumer name from durable parameter if provided
        if (durable) |d| {
            config.name = d;
            config.durable_name = d; // For backward compatibility
        }

        // Set subject filter if provided
        if (subject) |s| {
            config.filter_subject = s;
        }

        // Set queue group for push consumers
        if (queue) |q| {
            config.deliver_group = q;
        }

        // A consumer created through subscribe acknowledges explicitly unless
        // asked otherwise: the server's own default for an omitted ack policy
        // is 'none', which silently gives up redelivery. nats.go applies the
        // same default in the same place (js.go:1877).
        if (config.ack_policy == null) config.ack_policy = .explicit;

        // Configure for pull vs push consumer
        if (is_pull) {
            config.deliver_subject = null;
            if (config.max_waiting == null) config.max_waiting = 512;
        } else {
            // Push consumer - remove pull-only fields
            config.max_waiting = null; // Don't send max_waiting for push consumers
            config.max_batch = null;
            config.max_expires = null;
        }

        return config;
    }

    fn optStrEql(a: ?[]const u8, b: ?[]const u8) bool {
        const lhs = a orelse return b == null;
        const rhs = b orelse return false;
        return std.mem.eql(u8, lhs, rhs);
    }

    fn configMismatch(comptime field: []const u8, requested: anytype, actual: anytype) error{ConsumerConfigMismatch} {
        log.err("existing consumer has " ++ field ++ " {any}, but {any} was requested", .{ actual, requested });
        return error.ConsumerConfigMismatch;
    }

    fn configMismatchStr(comptime field: []const u8, requested: ?[]const u8, actual: ?[]const u8) error{ConsumerConfigMismatch} {
        log.err("existing consumer has " ++ field ++ " {?s}, but {?s} was requested", .{ actual, requested });
        return error.ConsumerConfigMismatch;
    }

    /// Reject an existing consumer that does not deliver what the caller asked
    /// for, rather than silently attaching to it. Mirrors processConsInfo and
    /// checkConfig in nats.go, and _processConsInfo in nats.c.
    ///
    /// Every ConsumerConfig field the caller left null is left alone: null
    /// means "whatever this consumer already is", so only fields that were
    /// actually asked for can produce a mismatch.
    fn checkExistingConsumer(existing: *const ConsumerInfo, requested: ConsumerConfig, is_pull: bool, subject: ?[]const u8, queue: ?[]const u8) !void {
        const config = &existing.config;

        // A consumer whose filter does not cover the requested subject would
        // deliver nothing, or the wrong thing. Unlike nats.go, subscribing
        // without a subject is allowed against a filtered consumer: binding to
        // a durable by name alone is a normal way to use this API.
        if (subject) |s| {
            if (config.filter_subject) |filter| {
                if (!std.mem.eql(u8, s, filter)) return configMismatchStr("filter_subject", s, filter);
            }
        }

        // Pull and push consumers are not interchangeable, and the deliver
        // subject is what tells them apart.
        if (is_pull) {
            if (config.deliver_subject != null) return error.PullSubscribeToPushConsumer;
        } else {
            if (config.deliver_subject == null) return error.PushSubscribeToPullConsumer;

            if (config.deliver_group) |group| {
                // A consumer with a deliver group only delivers to members of
                // that group, so a plain subscription receives nothing.
                const q = queue orelse return configMismatchStr("deliver_group", null, group);
                if (!std.mem.eql(u8, q, group)) return configMismatchStr("deliver_group", q, group);
            } else if (queue) |q| {
                return configMismatchStr("deliver_group", q, null);
            } else if (existing.push_bound orelse false) {
                // Two plain subscriptions to one push consumer split its
                // delivery between them at random.
                return error.ConsumerAlreadyBound;
            }
        }

        if (requested.description) |v| {
            if (!optStrEql(v, config.description)) return configMismatchStr("description", v, config.description);
        }
        if (requested.deliver_policy) |v| {
            if (v != config.deliver_policy) return configMismatch("deliver_policy", v, config.deliver_policy);
        }
        if (requested.opt_start_seq) |v| {
            if (v != config.opt_start_seq) return configMismatch("opt_start_seq", v, config.opt_start_seq);
        }
        if (requested.opt_start_time) |v| {
            if (!optStrEql(v, config.opt_start_time)) return configMismatchStr("opt_start_time", v, config.opt_start_time);
        }
        if (requested.ack_policy) |v| {
            if (v != config.ack_policy) return configMismatch("ack_policy", v, config.ack_policy);
        }
        if (requested.ack_wait) |v| {
            if (v != config.ack_wait) return configMismatch("ack_wait", v, config.ack_wait);
        }
        if (requested.max_deliver) |v| {
            if (v != config.max_deliver) return configMismatch("max_deliver", v, config.max_deliver);
        }
        if (requested.replay_policy) |v| {
            if (v != config.replay_policy) return configMismatch("replay_policy", v, config.replay_policy);
        }
        if (requested.rate_limit_bps) |v| {
            if (v != config.rate_limit_bps) return configMismatch("rate_limit_bps", v, config.rate_limit_bps);
        }
        if (requested.max_ack_pending) |v| {
            if (v != config.max_ack_pending) return configMismatch("max_ack_pending", v, config.max_ack_pending);
        }
        if (requested.idle_heartbeat) |v| {
            if (v != config.idle_heartbeat) return configMismatch("idle_heartbeat", v, config.idle_heartbeat);
        }
        if (requested.max_waiting) |v| {
            if (v != config.max_waiting) return configMismatch("max_waiting", v, config.max_waiting);
        }
        if (requested.num_replicas) |v| {
            if (v != config.num_replicas) return configMismatch("num_replicas", v, config.num_replicas);
        }
        // Flow control the caller did not ask for is handled by the library
        // either way, so only an unmet request for it is a mismatch.
        if (requested.flow_control orelse false) {
            if (!(config.flow_control orelse false)) return configMismatch("flow_control", true, config.flow_control);
        }
    }

    /// Look up a named consumer, returning null when it is unnamed or does
    /// not exist yet.
    fn lookupConsumer(self: JetStream, stream_name: []const u8, name: ?[]const u8) !?Result(ConsumerInfo) {
        const consumer_name = name orelse return null;

        if (self.getConsumerInfo(stream_name, consumer_name)) |existing_info| {
            log.debug("Found existing consumer: {s}", .{consumer_name});
            return existing_info;
        } else |err| switch (err) {
            error.ConsumerNotFound => {
                log.debug("Consumer {s} not found, creating new one", .{consumer_name});
                return null;
            },
            else => return err, // Other errors should be propagated
        }
    }

    /// Take ownership of a looked up consumer info, leaving null behind so the
    /// errdefer guarding the lookup does not release it a second time.
    fn takeConsumerInfo(info: *?Result(ConsumerInfo)) ?Result(ConsumerInfo) {
        defer info.* = null;
        return info.*;
    }

    /// A delivery subscription together with the consumer it delivers from.
    const PushConsumerSubscription = struct {
        subscription: *Subscription,
        consumer_info: Result(ConsumerInfo),
    };

    /// Subscribe to a push consumer's deliver subject and then make sure the
    /// consumer exists, in that order: the SUB frame is queued ahead of the
    /// create request, so the server has interest in the deliver subject
    /// before it can push anything to it.
    ///
    /// `subscriber` performs the subscribe itself, which differs per entry
    /// point (callback or sync, with or without a queue group). It may be
    /// called twice, so it must not consume anything it is given.
    fn subscribeToPushConsumer(self: JetStream, stream_name: []const u8, subject: ?[]const u8, durable: ?[]const u8, base_config: ConsumerConfig, queue: ?[]const u8, subscriber: anytype) !PushConsumerSubscription {
        const config = buildConsumerConfig(base_config, subject, durable, false, queue);

        // The name addConsumer would target, which the caller can also have
        // supplied through the config rather than through `durable`. Both
        // official clients look the consumer up under either name.
        const consumer_name = config.name orelse config.durable_name;

        // When the consumer is already there, the server delivers to its
        // deliver subject rather than to the one we would have asked for.
        var existing = try self.lookupConsumer(stream_name, consumer_name);
        errdefer if (existing) |info| info.deinit();

        // Two passes at most: the second one always has a consumer to attach
        // to, so it returns before reaching addConsumer again.
        while (true) {
            const deliver_subject = if (existing) |info| blk: {
                try checkExistingConsumer(&info.value, base_config, false, subject, queue);
                break :blk info.value.config.deliver_subject.?;
            } else config.deliver_subject.?;

            const subscription = try subscriber.subscribe(deliver_subject);
            errdefer subscription.deinit();

            if (takeConsumerInfo(&existing)) |info| {
                return .{ .subscription = subscription, .consumer_info = info };
            }

            if (self.addConsumer(stream_name, config)) |info| {
                return .{ .subscription = subscription, .consumer_info = info };
            } else |err| switch (err) {
                // Another client created the consumer between our lookup and
                // this create, so it owns the deliver subject now. Attach to
                // the consumer that won and subscribe where it actually
                // delivers. Look it up before dropping the subscription, so a
                // failing lookup leaves exactly one cleanup to run.
                error.ConsumerAlreadyExists, error.ConsumerNameExist, error.ConsumerExistingActive => {
                    const winner = try self.lookupConsumer(stream_name, consumer_name) orelse return err;
                    subscription.deinit();
                    existing = winner;
                },
                else => return err,
            }
        }
    }

    /// Get existing consumer or create new one based on configuration
    fn getOrCreateConsumer(self: JetStream, stream_name: []const u8, subject: ?[]const u8, durable: ?[]const u8, base_config: ?ConsumerConfig, is_pull: bool, queue: ?[]const u8) !Result(ConsumerInfo) {
        const config = buildConsumerConfig(base_config, subject, durable, is_pull, queue);
        const name = config.name orelse config.durable_name orelse return try self.addConsumer(stream_name, config);

        // Create first, as "create": one request where asking CONSUMER.INFO first made two —
        // and INFO answers a missing consumer only after the meta-leader has looked, which
        // a fleet creating consumers by the thousand pays every time (a ZeBridge device
        // names every consumer anew: the INFO always said "not found"). An identical
        // consumer is a success; one with other settings is refused and left untouched,
        // and only then is it read and checked as before.
        if (self.createConsumerAs(stream_name, config, "create")) |created| {
            return created;
        } else |err| switch (err) {
            error.ConsumerAlreadyExists => {},
            else => return err,
        }
        if (try self.lookupConsumer(stream_name, name)) |existing_info| {
            errdefer existing_info.deinit();
            try checkExistingConsumer(&existing_info.value, base_config orelse .{}, is_pull, subject, queue);
            return existing_info;
        }
        // Deleted between the two requests.
        return try self.addConsumer(stream_name, config);
    }

    /// Parse a response from the server, handling errors if present.
    fn parseResponse(self: JetStream, comptime T: type, msg: *Message) !Result(T) {
        try maybeParseErrorResponse(msg);

        const parsed = std.json.parseFromSlice(T, self.nc.allocator, msg.data, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| {
            log.err("Failed to parse response: {}", .{err});
            log.debug("Full response: {s}", .{msg.data});
            return error.JetStreamParseError;
        };

        // Reuse the arena from std.json.Parsed in our Result
        return Result(T){
            .arena = parsed.arena,
            .value = parsed.value,
        };
    }

    // Retrieves stats and limits for the connected user's account.
    pub fn getAccountInfo(self: JetStream) !Result(AccountInfoResponse) {
        const msg = try self.sendRequest("INFO", "");
        defer msg.deinit();

        return try self.parseResponse(AccountInfoResponse, msg);
    }

    /// Retrieves a list of stream names.
    pub fn listStreamNames(self: JetStream) !Result([]const []const u8) {
        const msg = try self.sendRequest("STREAM.NAMES", "");
        defer msg.deinit();

        const page_result = try self.parseResponse(StreamNamesResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const streams = page_result.value.streams orelse &[_][]const u8{};
        std.debug.assert(page_result.value.total == streams.len);

        const result: Result([]const []const u8) = .{
            .arena = page_result.arena,
            .value = streams,
        };
        return result;
    }

    /// Retrieves a list of streams with full information.
    pub fn listStreams(self: JetStream) !Result([]const StreamInfo) {
        const msg = try self.sendRequest("STREAM.LIST", "");
        defer msg.deinit();

        const page_result = try self.parseResponse(StreamListResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const streams = page_result.value.streams orelse &[_]StreamInfo{};
        std.debug.assert(page_result.value.total == streams.len);

        const result: Result([]const StreamInfo) = .{
            .arena = page_result.arena,
            .value = streams,
        };
        return result;
    }

    /// Creates a new stream with the provided configuration.
    pub fn addStream(self: JetStream, config: StreamConfig) !Result(StreamInfo) {
        try validation.validateStreamName(config.name);
        for (config.subjects) |subject| {
            try validation.validateSubject(subject);
        }

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.CREATE.{s}", .{config.name});
        defer self.nc.allocator.free(subject);

        // Serialize the config to JSON
        const config_json = try jsonStringifyAlloc(self.nc.allocator, config, .{});
        defer self.nc.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Updates a stream with the provided configuration.
    pub fn updateStream(self: JetStream, config: StreamConfig) !Result(StreamInfo) {
        try validation.validateStreamName(config.name);
        for (config.subjects) |subject| {
            try validation.validateSubject(subject);
        }

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.UPDATE.{s}", .{config.name});
        defer self.nc.allocator.free(subject);

        // Serialize the config to JSON
        const config_json = try jsonStringifyAlloc(self.nc.allocator, config, .{});
        defer self.nc.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Deletes a stream.
    pub fn deleteStream(self: JetStream, stream_name: []const u8) !void {
        try validation.validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.DELETE.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        // Just check for errors, don't need to parse the response
        try maybeParseErrorResponse(msg);
    }

    /// Gets information about a specific stream.
    pub fn getStreamInfo(self: JetStream, stream_name: []const u8) !Result(StreamInfo) {
        try validation.validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.INFO.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(StreamInfo, msg);
    }

    /// Retrieves a list of consumer names for a stream.
    pub fn listConsumerNames(self: JetStream, stream_name: []const u8) !Result([]const []const u8) {
        try validation.validateStreamName(stream_name);

        const subject = try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.NAMES.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        const page_result = try self.parseResponse(ConsumerNamesResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const consumers = page_result.value.consumers orelse &[_][]const u8{};
        std.debug.assert(page_result.value.total == consumers.len);

        const result: Result([]const []const u8) = .{
            .arena = page_result.arena,
            .value = consumers,
        };
        return result;
    }

    /// Retrieves a list of consumers with full information for a stream.
    pub fn listConsumers(self: JetStream, stream_name: []const u8) !Result([]const ConsumerInfo) {
        try validation.validateStreamName(stream_name);

        const subject = try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.LIST.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        const page_result = try self.parseResponse(ConsumerListResponse, msg);
        errdefer page_result.deinit();

        // TODO: handle pagination
        const consumers = page_result.value.consumers orelse &[_]ConsumerInfo{};
        std.debug.assert(page_result.value.total == consumers.len);

        const result: Result([]const ConsumerInfo) = .{
            .arena = page_result.arena,
            .value = consumers,
        };
        return result;
    }

    /// Creates a new consumer with the provided configuration.
    /// Uses DURABLE endpoint only if durable_name is provided, otherwise creates ephemeral consumer.
    pub fn addConsumer(self: JetStream, stream_name: []const u8, config: ConsumerConfig) !Result(ConsumerInfo) {
        return self.createConsumerAs(stream_name, config, null);
    }

    /// `addConsumer` with the request's `action` (server >= 2.10): null creates or updates;
    /// "create" creates, succeeds unchanged on an identical consumer, and is refused with
    /// `ConsumerAlreadyExists` by one with other settings, which it leaves as it is.
    fn createConsumerAs(self: JetStream, stream_name: []const u8, config: ConsumerConfig, action: ?[]const u8) !Result(ConsumerInfo) {
        try validation.validateStreamName(stream_name);
        if (config.name) |n| try validation.validateConsumerName(n);
        if (config.durable_name) |n| try validation.validateConsumerName(n);

        log.debug("adding consumer", .{});
        // LOCAL FIX (see NOTES.md): a NAMED consumer uses the modern
        // CONSUMER.CREATE.<stream>.<name>[.<filter>] endpoint (server >= 2.9)
        // instead of the legacy CONSUMER.DURABLE.CREATE API. Least-privilege
        // grant sets cover the modern form (`CONSUMER.CREATE.<stream>` and
        // `CONSUMER.CREATE.<stream>.>`); the legacy subject is typically not
        // granted, and an unauthorized publish surfaces as a bare Timeout.
        const named: ?[]const u8 = config.name orelse config.durable_name;
        const subject = if (named) |n| blk: {
            if (config.filter_subject) |filt| {
                break :blk try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.CREATE.{s}.{s}.{s}", .{ stream_name, n, filt });
            }
            break :blk try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.CREATE.{s}.{s}", .{ stream_name, n });
        } else try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.CREATE.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        // Create request payload
        const request_payload = struct {
            stream_name: []const u8,
            config: ConsumerConfig,
            action: ?[]const u8 = null,
        }{ .stream_name = stream_name, .config = config, .action = action };

        const config_json = try jsonStringifyAlloc(self.nc.allocator, request_payload, .{
            .emit_null_optional_fields = false,
        });
        defer self.nc.allocator.free(config_json);

        const msg = try self.sendRequest(subject, config_json);
        defer msg.deinit();

        return try self.parseResponse(ConsumerInfo, msg);
    }

    /// Gets information about a specific consumer.
    pub fn getConsumerInfo(self: JetStream, stream_name: []const u8, consumer_name: []const u8) !Result(ConsumerInfo) {
        try validation.validateStreamName(stream_name);
        try validation.validateConsumerName(consumer_name);

        const subject = try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.INFO.{s}.{s}", .{ stream_name, consumer_name });
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        return try self.parseResponse(ConsumerInfo, msg);
    }

    /// Deletes a consumer.
    pub fn deleteConsumer(self: JetStream, stream_name: []const u8, consumer_name: []const u8) !void {
        try validation.validateStreamName(stream_name);
        try validation.validateConsumerName(consumer_name);

        const subject = try std.fmt.allocPrint(self.nc.allocator, "CONSUMER.DELETE.{s}.{s}", .{ stream_name, consumer_name });
        defer self.nc.allocator.free(subject);

        const msg = try self.sendRequest(subject, "");
        defer msg.deinit();

        // Just check for errors, don't need to parse the response
        try maybeParseErrorResponse(msg);
    }

    /// Purges messages from a stream.
    pub fn purgeStream(self: JetStream, stream_name: []const u8, request: StreamPurgeRequest) !Result(StreamPurgeResponse) {
        try validation.validateStreamName(stream_name);
        if (request.filter) |subject| try validation.validateSubject(subject);

        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.PURGE.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        const request_json = try jsonStringifyAlloc(self.nc.allocator, request, .{});
        defer self.nc.allocator.free(request_json);

        const msg = try self.sendRequest(subject, request_json);
        defer msg.deinit();

        return try self.parseResponse(StreamPurgeResponse, msg);
    }

    /// Internal function for getting messages from the stream using legacy API
    fn getMsgLegacy(self: JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        try validation.validateStreamName(stream_name);
        if (options.last_by_subj) |subject| try validation.validateSubject(subject);
        if (options.next_by_subj) |subject| try validation.validateSubject(subject);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.MSG.GET.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        // Create GetMsgRequest from options
        const request = GetMsgRequest{
            .seq = options.seq,
            .last_by_subj = options.last_by_subj,
            .next_by_subj = options.next_by_subj,
        };

        // Serialize the request to JSON, omitting null fields
        const request_json = try jsonStringifyAlloc(self.nc.allocator, request, .{
            .emit_null_optional_fields = false,
        });
        defer self.nc.allocator.free(request_json);

        const resp = try self.sendRequest(subject, request_json);
        defer resp.deinit();

        // Parse the response to extract the message
        const parsed_resp = try self.parseResponse(GetMsgResponse, resp);
        defer parsed_resp.deinit();

        const stored_msg = parsed_resp.value.message;

        // Create a new Message using the proper API
        const msg = try self.nc.newMsg();
        errdefer msg.deinit();

        // Set the subject
        try msg.setSubject(stored_msg.subject, true);

        // Set the sequence number
        msg.seq = stored_msg.seq;
        msg.time = try parseTimestamp(stored_msg.time);

        // Decode base64 data
        if (stored_msg.data.len > 0) {
            const decoder = std.base64.standard.Decoder;
            const data_len = try decoder.calcSizeForSlice(stored_msg.data);
            const decoded_data = try msg.arena.allocator().alloc(u8, data_len);
            try decoder.decode(decoded_data, stored_msg.data);
            try msg.setPayload(decoded_data, false);
        } else {
            try msg.setPayload("", false);
        }

        // Decode base64 headers if present
        if (stored_msg.hdrs) |hdrs_b64| {
            const decoder = std.base64.standard.Decoder;
            const hdrs_len = try decoder.calcSizeForSlice(hdrs_b64);
            const decoded_headers = try msg.arena.allocator().alloc(u8, hdrs_len);
            try decoder.decode(decoded_headers, hdrs_b64);
            try msg.setRawHeaders(decoded_headers, false);
        }

        return msg;
    }

    /// Gets a message from the stream using the specified options
    pub fn getMsg(self: JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        // Validate options combinations:
        // 1. seq only - get message by sequence
        // 2. last_by_subj only - get last message for subject
        // 3. seq + next_by_subj - get next message for subject at or after sequence
        const has_seq = options.seq != null;
        const has_last_by_subj = options.last_by_subj != null;
        const has_next_by_subj = options.next_by_subj != null;

        if (has_last_by_subj and (has_seq or has_next_by_subj)) {
            // last_by_subj cannot be combined with seq or next_by_subj
            return error.InvalidGetMessageOptions;
        } else if (has_next_by_subj and !has_seq) {
            // next_by_subj requires seq to be set
            return error.InvalidGetMessageOptions;
        } else if (!has_seq and !has_last_by_subj and !has_next_by_subj) {
            // At least one option must be set
            return error.InvalidGetMessageOptions;
        }

        if (options.direct) {
            return self.getMsgDirect(stream_name, options);
        } else {
            return self.getMsgLegacy(stream_name, options);
        }
    }

    /// Gets multiple messages from the stream (batch operation) - NOT IMPLEMENTED
    pub fn getMsgs(self: JetStream, stream_name: []const u8, options: GetMsgsOptions) !MessageList {
        _ = self;
        _ = stream_name;
        _ = options;
        return error.NotImplemented;
    }

    /// Internal function for direct get messages from any stream replica
    fn getMsgDirect(self: JetStream, stream_name: []const u8, options: GetMsgOptions) !*Message {
        log.debug("getMsgDirect: Starting with stream_name={s}", .{stream_name});
        try validation.validateStreamName(stream_name);
        if (options.last_by_subj) |subject| try validation.validateSubject(subject);
        if (options.next_by_subj) |subject| try validation.validateSubject(subject);
        log.debug("getMsgDirect: Stream name validation passed", .{});

        // LOCAL FIX (see NOTES.md): a last-by-subject lookup with no other
        // options uses the ADR-31 SUBJECT form — DIRECT.GET.<stream>.<subject>
        // with an empty body. Identical semantics to the JSON form, but it
        // works under least-privilege grants that scope direct gets PER KEY;
        // ZeBridge's tenant-scoped KV/object grants deliberately cover only
        // this form (a bucket-level grant on KV_generations would hand every
        // tenant every manifest).
        if (options.last_by_subj != null and options.seq == null and options.next_by_subj == null) {
            const subj_form = try std.fmt.allocPrint(self.nc.allocator, "DIRECT.GET.{s}.{s}", .{ stream_name, options.last_by_subj.? });
            defer self.nc.allocator.free(subj_form);
            const resp = try self.sendRequest(subj_form, "");
            errdefer resp.deinit();
            if (resp.status_code == STATUS_NOT_FOUND) {
                return error.MessageNotFound;
            } else if (resp.status_code == STATUS_TIMEOUT) {
                return error.BadRequest;
            } else if (resp.status_code == 413) {
                return error.TooManySubjects;
            }
            if (resp.headerGet("Nats-Subject")) |nats_subject| {
                try resp.setSubject(nats_subject, false);
            }
            if (resp.headerGet("Nats-Sequence")) |nats_seq_str| {
                resp.seq = std.fmt.parseInt(u64, nats_seq_str, 10) catch 0;
            }
            return resp;
        }

        // Build the subject for the direct get API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "DIRECT.GET.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        // Convert GetMsgOptions to GetMsgRequest
        const request = GetMsgRequest{
            .seq = options.seq,
            .last_by_subj = options.last_by_subj,
            .next_by_subj = options.next_by_subj,
        };

        // Serialize the request to JSON, omitting null fields
        const request_json = try jsonStringifyAlloc(self.nc.allocator, request, .{
            .emit_null_optional_fields = false,
        });
        defer self.nc.allocator.free(request_json);

        const resp = try self.sendRequest(subject, request_json);
        errdefer resp.deinit();

        // Check for error status codes
        if (resp.status_code == STATUS_NOT_FOUND) {
            return error.MessageNotFound;
        } else if (resp.status_code == STATUS_TIMEOUT) {
            return error.BadRequest;
        } else if (resp.status_code == 413) {
            return error.TooManySubjects;
        }

        // For direct get, extract metadata from JetStream headers
        if (resp.headerGet("Nats-Subject")) |nats_subject| {
            try resp.setSubject(nats_subject, false);
        }

        if (resp.headerGet("Nats-Sequence")) |nats_seq_str| {
            resp.seq = std.fmt.parseInt(u64, nats_seq_str, 10) catch 0;
        }

        return resp;
    }

    /// Internal function for deleting messages from the stream
    fn deleteMsgInternal(self: JetStream, stream_name: []const u8, request: DeleteMsgRequest) !bool {
        try validation.validateStreamName(stream_name);

        // Build the subject for the API call
        const subject = try std.fmt.allocPrint(self.nc.allocator, "STREAM.MSG.DELETE.{s}", .{stream_name});
        defer self.nc.allocator.free(subject);

        // Serialize the request to JSON
        const request_json = try jsonStringifyAlloc(self.nc.allocator, request, .{});
        defer self.nc.allocator.free(request_json);

        const msg = try self.sendRequest(subject, request_json);
        defer msg.deinit();

        const response = try self.parseResponse(MsgDeleteResponse, msg);
        defer response.deinit();

        return response.value.success;
    }

    /// Deletes a message from the stream (marks as deleted, doesn't erase from storage)
    pub fn deleteMsg(self: JetStream, stream_name: []const u8, seq: u64) !bool {
        return self.deleteMsgInternal(stream_name, DeleteMsgRequest{ .seq = seq, .no_erase = true });
    }

    /// Erases a message from the stream (securely removes from storage)
    pub fn eraseMsg(self: JetStream, stream_name: []const u8, seq: u64) !bool {
        return self.deleteMsgInternal(stream_name, DeleteMsgRequest{ .seq = seq, .no_erase = null });
    }

    /// Subscribe to a JetStream push consumer with callback handler
    /// Handle JetStream status messages (heartbeats and flow control)
    fn handleStatusMessage(msg: *Message, nc: *Connection) !void {
        // Debug: Print all headers to understand the actual format
        log.debug("Status message headers:", .{});
        var header_iter = msg.headers.iterator();
        while (header_iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const values = entry.value_ptr.*;
            for (values.items) |value| {
                log.debug("  {s}: {s}", .{ key, value });
            }
        }

        // Get the description header to distinguish between heartbeats and flow control
        if (msg.headers.get("Description")) |desc_values| {
            if (desc_values.items.len > 0) {
                const description = desc_values.items[0];

                if (std.mem.eql(u8, description, "Idle Heartbeat")) {
                    // This is an idle heartbeat - just log it (optional)
                    log.debug("Received idle heartbeat from JetStream", .{});
                    return;
                } else if (std.mem.eql(u8, description, "FlowControl Request")) {
                    // This is a flow control request - we need to respond
                    log.debug("Received flow control request from JetStream", .{});

                    if (msg.reply) |reply_subject| {
                        // Respond with empty message to acknowledge flow control
                        try nc.publish(reply_subject, "");
                        log.debug("Sent flow control response to: {s}", .{reply_subject});
                    } else {
                        log.warn("Flow control request missing reply subject", .{});
                    }
                    return;
                }

                // Unknown status message description
                log.warn("Unknown status message description: {s}", .{description});
            }
        } else {
            // Status message without description - treat as heartbeat
            log.debug("Received status message without description (likely heartbeat)", .{});
        }
    }

    /// Helper to generate a JetStream message handler wrapper
    fn JetStreamHandler(comptime handlerFn: anytype) type {
        return struct {
            fn wrappedHandler(msg: *Message, nc: *Connection, user_args: anytype, manual_ack: bool) anyerror!void {
                // Check for status messages (heartbeats and flow control)
                if (msg.status_code == STATUS_CONTROL) {
                    // Handle status message internally, don't pass to user callback
                    defer msg.deinit(); // Clean up status message
                    handleStatusMessage(msg, nc) catch |err| {
                        if (err == error.Canceled) return err;
                        log.err("Failed to handle status message: {}", .{err});
                    };
                    return;
                }

                // Create JetStream message wrapper for regular messages
                const js_msg = jetstream_message.createJetStreamMessage(nc, msg) catch |err| {
                    msg.deinit(); // Clean up on error
                    if (err == error.Canceled) return err;
                    log.err("Failed to wrap JetStream message: {}", .{err});
                    return;
                };
                // The handler may deinit it; the free waits for the auto-ack below.
                js_msg.lifetime.store(.in_handler, .release);
                defer js_msg.endHandler();

                // Call user handler with JetStream message - handler owns cleanup responsibility
                // Support both void and fallible handlers
                comptime {
                    const HandlerType = @TypeOf(handlerFn);
                    const type_info = @typeInfo(HandlerType);
                    if (type_info != .@"fn") {
                        @compileError("Handler must be a function, got: " ++ @typeName(HandlerType));
                    }
                }
                const ReturnType = @typeInfo(@TypeOf(handlerFn)).@"fn".return_type.?;

                // Only auto-ack if callback succeeds (like nats-py)
                var callback_success = false;
                if (ReturnType == void) {
                    @call(.auto, handlerFn, .{js_msg} ++ user_args);
                    callback_success = true;
                } else {
                    @call(.auto, handlerFn, .{js_msg} ++ user_args) catch |err| {
                        log.err("User handler failed: {}", .{err});
                        return; // Don't auto-ack on callback error
                    };
                    callback_success = true;
                }

                // Auto-acknowledge after successful user callback if not manual_ack
                if (!manual_ack and callback_success) {
                    js_msg.ack() catch |err| switch (err) {
                        jetstream_message.AckError.AlreadyAcked => {}, // Ignore already acked (like nats-py)
                        error.Canceled => |e| return e,
                        else => log.err("Auto-ack failed: {}", .{err}),
                    };
                }
            }
        };
    }

    /// Keep JetStream delivery subscriptions bounded, while matching
    /// nats.go's adjustment when the server's max-ack-pending window exceeds
    /// the default message limit. MaxAckPending is not a no-drop guarantee:
    /// byte limits, redeliveries, and control messages are independent of the
    /// number of unique unacknowledged messages.
    fn applyPendingLimits(subscription: *Subscription, consumer_config: *const ConsumerConfig) void {
        const configured = consumer_config.max_ack_pending orelse return;
        if (configured <= Subscription.default_pending_msgs_limit) return;

        const max_ack_pending: u64 = @intCast(configured);
        const msgs_limit: u32 = std.math.cast(u32, max_ack_pending) orelse std.math.maxInt(u32);
        const bytes_limit = @max(max_ack_pending *| @as(u64, 1024 * 1024), Subscription.default_pending_bytes_limit);
        subscription.setPendingLimits(msgs_limit, bytes_limit);
    }

    /// Subscribe to a JetStream push consumer with callback handler.
    pub fn subscribe(self: JetStream, subject: ?[]const u8, comptime handlerFn: anytype, handler_args: anytype, options: SubscribeOptions) !*JetStreamSubscription {
        // Resolve stream name
        const stream_name = if (options.stream) |s|
            s
        else if (subject) |s|
            try self.lookupStreamBySubject(s)
        else
            return error.StreamOrSubjectRequired;

        defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);

        // Prepare consumer config, generating deliver_subject if needed
        var config = options.config orelse ConsumerConfig{};
        const generated_deliver_subject = if (config.deliver_subject == null)
            try self.nc.newInbox(self.nc.allocator)
        else
            null;
        errdefer if (generated_deliver_subject) |ds| self.nc.allocator.free(ds);

        // Validate user-provided deliver_subject (not generated ones)
        if (config.deliver_subject != null) {
            try validation.validateSubject(config.deliver_subject.?);
        }

        if (generated_deliver_subject) |ds| {
            config.deliver_subject = ds;
        }

        // Use the reusable JetStream handler wrapper
        const JSHandler = JetStreamHandler(handlerFn);

        const subscriber = struct {
            nc: *Connection,
            handler_args: @TypeOf(handler_args),
            manual_ack: bool,

            fn subscribe(sub_ctx: @This(), deliver_subject: []const u8) !*Subscription {
                return sub_ctx.nc.subscribe(deliver_subject, JSHandler.wrappedHandler, .{ sub_ctx.nc, sub_ctx.handler_args, sub_ctx.manual_ack });
            }
        }{ .nc = self.nc, .handler_args = handler_args, .manual_ack = options.manual_ack };

        var push = try self.subscribeToPushConsumer(stream_name, subject, options.durable, config, null, subscriber);
        errdefer push.subscription.deinit();
        errdefer push.consumer_info.deinit();

        applyPendingLimits(push.subscription, &push.consumer_info.value.config);

        // Create JetStream subscription wrapper
        const js_sub = try self.nc.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = push.subscription,
            .js = self,
            .consumer_info = push.consumer_info,
            .deliver_subject_owned = generated_deliver_subject,
        };

        return js_sub;
    }

    /// Create a synchronous push subscription for manual message consumption
    pub fn subscribeSync(self: JetStream, subject: ?[]const u8, options: SubscribeOptions) !*JetStreamSubscription {
        // Resolve stream name
        const stream_name = if (options.stream) |s|
            s
        else if (subject) |s|
            try self.lookupStreamBySubject(s)
        else
            return error.StreamOrSubjectRequired;

        defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);

        // Prepare config with deliver_subject if needed
        var config = options.config orelse ConsumerConfig{};

        // Validate user-provided deliver_subject (not generated ones)
        if (config.deliver_subject != null) {
            try validation.validateSubject(config.deliver_subject.?);
        }

        // Generate deliver_subject if not provided in the config
        const generated_deliver_subject = if (config.deliver_subject == null)
            try self.nc.newInbox(self.nc.allocator)
        else
            null;
        errdefer if (generated_deliver_subject) |ds| self.nc.allocator.free(ds);

        if (generated_deliver_subject) |ds| {
            config.deliver_subject = ds;
        }

        // Subscribe synchronously, without a callback handler
        const subscriber = struct {
            nc: *Connection,

            fn subscribe(sub_ctx: @This(), deliver_subject: []const u8) !*Subscription {
                return sub_ctx.nc.subscribeSync(deliver_subject);
            }
        }{ .nc = self.nc };

        var push = try self.subscribeToPushConsumer(stream_name, subject, options.durable, config, null, subscriber);
        errdefer push.subscription.deinit();
        errdefer push.consumer_info.deinit();

        applyPendingLimits(push.subscription, &push.consumer_info.value.config);

        // Create JetStream subscription wrapper
        const js_sub = try self.nc.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = push.subscription,
            .js = self,
            .consumer_info = push.consumer_info,
            .deliver_subject_owned = generated_deliver_subject,
        };
        return js_sub;
    }

    /// Create a queue subscription for load balancing across multiple consumers
    pub fn queueSubscribe(self: JetStream, subject: ?[]const u8, queue: []const u8, comptime handlerFn: anytype, handler_args: anytype, options: SubscribeOptions) !*JetStreamSubscription {
        // Resolve stream name
        const stream_name = if (options.stream) |s|
            s
        else if (subject) |s|
            try self.lookupStreamBySubject(s)
        else
            return error.StreamOrSubjectRequired;

        defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);

        // Prepare config with deliver_subject if needed
        var config = options.config orelse ConsumerConfig{};

        // Validate user-provided deliver_subject (not generated ones)
        if (config.deliver_subject != null) {
            try validation.validateSubject(config.deliver_subject.?);
        }

        // Generate deliver_subject if not provided in the config
        const generated_deliver_subject = if (config.deliver_subject == null)
            try self.nc.newInbox(self.nc.allocator)
        else
            null;
        errdefer if (generated_deliver_subject) |ds| self.nc.allocator.free(ds);

        if (generated_deliver_subject) |ds| {
            config.deliver_subject = ds;
        }

        // Use the reusable JetStream handler wrapper
        const JSHandler = JetStreamHandler(handlerFn);

        const subscriber = struct {
            nc: *Connection,
            queue: []const u8,
            handler_args: @TypeOf(handler_args),
            manual_ack: bool,

            fn subscribe(sub_ctx: @This(), deliver_subject: []const u8) !*Subscription {
                return sub_ctx.nc.queueSubscribe(deliver_subject, sub_ctx.queue, JSHandler.wrappedHandler, .{ sub_ctx.nc, sub_ctx.handler_args, sub_ctx.manual_ack });
            }
        }{ .nc = self.nc, .queue = queue, .handler_args = handler_args, .manual_ack = options.manual_ack };

        var push = try self.subscribeToPushConsumer(stream_name, subject, options.durable, config, queue, subscriber);
        errdefer push.subscription.deinit();
        errdefer push.consumer_info.deinit();

        applyPendingLimits(push.subscription, &push.consumer_info.value.config);

        // Create JetStream subscription wrapper
        const js_sub = try self.nc.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = push.subscription,
            .js = self,
            .consumer_info = push.consumer_info,
            .deliver_subject_owned = generated_deliver_subject,
        };

        return js_sub;
    }

    /// Create a synchronous queue subscription for load balancing across multiple consumers
    pub fn queueSubscribeSync(self: JetStream, subject: ?[]const u8, queue: []const u8, options: SubscribeOptions) !*JetStreamSubscription {
        // Resolve stream name
        const stream_name = if (options.stream) |s|
            s
        else if (subject) |s|
            try self.lookupStreamBySubject(s)
        else
            return error.StreamOrSubjectRequired;

        defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);

        // Prepare config with deliver_subject if needed
        var config = options.config orelse ConsumerConfig{};

        // Validate user-provided deliver_subject (not generated ones)
        if (config.deliver_subject != null) {
            try validation.validateSubject(config.deliver_subject.?);
        }

        // Generate deliver_subject if not provided in the config
        const generated_deliver_subject = if (config.deliver_subject == null)
            try self.nc.newInbox(self.nc.allocator)
        else
            null;
        errdefer if (generated_deliver_subject) |ds| self.nc.allocator.free(ds);

        if (generated_deliver_subject) |ds| {
            config.deliver_subject = ds;
        }

        // Subscribe synchronously with a queue group, without a callback handler
        const subscriber = struct {
            nc: *Connection,
            queue: []const u8,

            fn subscribe(sub_ctx: @This(), deliver_subject: []const u8) !*Subscription {
                return sub_ctx.nc.queueSubscribeSync(deliver_subject, sub_ctx.queue);
            }
        }{ .nc = self.nc, .queue = queue };

        var push = try self.subscribeToPushConsumer(stream_name, subject, options.durable, config, queue, subscriber);
        errdefer push.subscription.deinit();
        errdefer push.consumer_info.deinit();

        applyPendingLimits(push.subscription, &push.consumer_info.value.config);

        // Create JetStream subscription wrapper
        const js_sub = try self.nc.allocator.create(JetStreamSubscription);
        js_sub.* = JetStreamSubscription{
            .subscription = push.subscription,
            .js = self,
            .consumer_info = push.consumer_info,
            .deliver_subject_owned = generated_deliver_subject,
        };

        return js_sub;
    }

    /// Create a pull subscription for the specified stream
    /// One inbox for several pull consumers — see `PullInbox`.
    pub fn pullInbox(self: JetStream) !*PullInbox {
        const inbox_base = try self.nc.newInbox(self.nc.allocator);
        defer self.nc.allocator.free(inbox_base);
        const inbox_prefix = try std.fmt.allocPrint(self.nc.allocator, "{s}.", .{inbox_base});
        errdefer self.nc.allocator.free(inbox_prefix);
        const wildcard_subject = try std.fmt.allocPrint(self.nc.allocator, "{s}*", .{inbox_prefix});
        defer self.nc.allocator.free(wildcard_subject);
        const inbox_subscription = try self.nc.subscribeSync(wildcard_subject);
        errdefer inbox_subscription.deinit();
        const shared = try self.nc.allocator.create(PullInbox);
        shared.* = .{ .js = self, .inbox_subscription = inbox_subscription, .inbox_prefix = inbox_prefix };
        return shared;
    }

    pub fn pullSubscribe(self: JetStream, subject: ?[]const u8, durable: []const u8, options: PullSubscribeOptions) !*PullSubscription {
        // Resolve stream name
        const stream_name = if (options.stream) |s|
            s
        else if (subject) |s|
            try self.lookupStreamBySubject(s)
        else
            return error.StreamOrSubjectRequired;

        defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);

        // A shared inbox: borrow its subscription and prefix, own nothing of them.
        if (options.inbox) |shared| {
            var consumer_info = try self.getOrCreateConsumer(stream_name, subject, durable, options.config, true, null);
            errdefer consumer_info.deinit();
            // The limits only ever go up, so several consumers on one inbox get the
            // largest of their windows.
            applyPendingLimits(shared.inbox_subscription, &consumer_info.value.config);
            const pull_subscription = try self.nc.allocator.create(PullSubscription);
            pull_subscription.* = PullSubscription{
                .js = self,
                .consumer_info = consumer_info,
                .inbox_subscription = shared.inbox_subscription,
                .inbox_prefix = shared.inbox_prefix,
                .owns_inbox = false,
            };
            return pull_subscription;
        }

        // Generate unique inbox prefix for this pull subscription
        const inbox_base = try self.nc.newInbox(self.nc.allocator);
        defer self.nc.allocator.free(inbox_base);

        const inbox_prefix = try std.fmt.allocPrint(self.nc.allocator, "{s}.", .{inbox_base});
        errdefer self.nc.allocator.free(inbox_prefix);

        // Create wildcard subscription subject
        const wildcard_subject = try std.fmt.allocPrint(self.nc.allocator, "{s}*", .{inbox_prefix});
        defer self.nc.allocator.free(wildcard_subject);

        // Create the persistent wildcard inbox subscription before the consumer
        // exists, so no response can be delivered without local interest.
        const inbox_subscription = try self.nc.subscribeSync(wildcard_subject);
        errdefer inbox_subscription.deinit();

        // Create or get consumer (pull consumers require a name)
        var consumer_info = try self.getOrCreateConsumer(stream_name, subject, durable, options.config, true, null);
        errdefer consumer_info.deinit();

        applyPendingLimits(inbox_subscription, &consumer_info.value.config);

        // Allocate PullSubscription
        const pull_subscription = try self.nc.allocator.create(PullSubscription);
        pull_subscription.* = PullSubscription{
            .js = self,
            .consumer_info = consumer_info,
            .inbox_subscription = inbox_subscription,
            .inbox_prefix = inbox_prefix,
        };

        return pull_subscription;
    }

    /// Publish a message to JetStream
    pub fn publish(self: JetStream, subject: []const u8, data: []const u8, options: PublishOptions) !Result(PubAck) {
        // Validate subject according to ADR-6
        try validation.validateSubject(subject);

        // Create a temporary message
        const msg = try self.nc.newMsg();
        defer msg.deinit();

        // Set message fields
        try msg.setSubject(subject, false);
        try msg.setPayload(data, false);

        // Use publishMsg to handle the actual publishing
        return self.publishMsgInternal(msg, options);
    }

    /// Publish a pre-constructed message to JetStream
    pub fn publishMsg(self: JetStream, msg: *Message, options: PublishOptions) !Result(PubAck) {
        try validation.validateSubject(msg.subject);
        return self.publishMsgInternal(msg, options);
    }

    /// Internal function to publish a message with header processing
    fn publishMsgInternal(self: JetStream, msg: *Message, options: PublishOptions) !Result(PubAck) {
        if (options.retry_wait.nanoseconds <= 0) return error.InvalidRetryWait;

        // Set JetStream-specific headers based on options
        if (options.msg_id) |id| {
            try msg.headerSet(MsgIdHdr, id);
        }
        if (options.expected_stream) |stream| {
            try msg.headerSet(ExpectedStreamHdr, stream);
        }
        if (options.expected_last_seq) |seq| {
            var buf: [256]u8 = undefined;
            const seq_str = try std.fmt.bufPrint(&buf, "{d}", .{seq});
            try msg.headerSet(ExpectedLastSeqHdr, seq_str);
        }
        if (options.expected_last_subject_seq) |seq| {
            var buf: [256]u8 = undefined;
            const seq_str = try std.fmt.bufPrint(&buf, "{d}", .{seq});
            try msg.headerSet(ExpectedLastSubjSeqHdr, seq_str);
        }
        if (options.expected_last_subject_seq_subject) |subject| {
            try msg.headerSet(ExpectedLastSubjSeqSubjectHdr, subject);
        }
        if (options.expected_last_msg_id) |id| {
            try msg.headerSet(ExpectedLastMsgIdHdr, id);
        }
        if (options.msg_ttl) |ttl| {
            var buf: [256]u8 = undefined;
            const ttl_str = try std.fmt.bufPrint(&buf, "{d}ns", .{ttl.toNanoseconds()});
            try msg.headerSet(MsgTTLHdr, ttl_str);
        }

        const deadline = io_util.deadline(self.nc.io, self.opts.request_timeout);
        var retries_left = options.retry_attempts;
        const resp = while (true) {
            break self.nc.requestMsg(msg, deadline) catch |request_err| switch (request_err) {
                error.NoResponders => {
                    if (retries_left == 0) return error.NoStreamResponse;
                    retries_left -= 1;

                    const remaining = io_util.remaining(self.nc.io, deadline) orelse return error.Timeout;
                    const retry_wait = if (remaining.nanoseconds < options.retry_wait.nanoseconds)
                        remaining
                    else
                        options.retry_wait;
                    try self.nc.io.sleep(retry_wait, .awake);
                    if (io_util.expired(self.nc.io, deadline)) return error.Timeout;
                    continue;
                },
                else => return request_err,
            };
        };

        defer resp.deinit();

        // Parse the publish acknowledgment using parseResponse for consistency
        const parsed_resp = self.parseResponse(PubAck, resp) catch |err| {
            if (err == error.JetStreamParseError) {
                return error.InvalidJSAck;
            }
            return err;
        };

        return parsed_resp;
    }

    /// Create a KV manager for bucket operations
    pub fn kvManager(self: JetStream) @import("jetstream_kv.zig").KVManager {
        const jetstream_kv = @import("jetstream_kv.zig");
        return jetstream_kv.KVManager.init(self);
    }

    /// Open an existing KV bucket
    pub fn kvBucket(self: JetStream, bucket_name: []const u8) !@import("jetstream_kv.zig").KV {
        var manager = self.kvManager();
        return try manager.openBucket(bucket_name);
    }

    /// `kvBucket` without the stream lookup (`KVManager.bindBucket`): for a credential
    /// granted exact keys only.
    pub fn kvBind(self: JetStream, bucket_name: []const u8) !@import("jetstream_kv.zig").KV {
        var manager = self.kvManager();
        return try manager.bindBucket(bucket_name);
    }

    /// Create an object store manager
    pub fn objectStoreManager(self: JetStream) @import("jetstream_objstore.zig").ObjectStoreManager {
        const jetstream_objstore = @import("jetstream_objstore.zig");
        return jetstream_objstore.ObjectStoreManager.init(self);
    }
};

test "apiSubject: no domain is $JS.API., a domain is $JS.<domain>.API." {
    const a = std.testing.allocator;
    const plain = JetStream{ .nc = undefined, .opts = .{} };
    const s1 = try plain.apiSubject(a, "CONSUMER.MSG.NEXT.{s}.{s}", .{ "CDC_kilo", "c1" });
    defer a.free(s1);
    try std.testing.expectEqualStrings("$JS.API.CONSUMER.MSG.NEXT.CDC_kilo.c1", s1);

    const hub = JetStream{ .nc = undefined, .opts = .{ .domain = "hub" } };
    const s2 = try hub.apiSubject(a, "{s}", .{"INFO"});
    defer a.free(s2);
    try std.testing.expectEqualStrings("$JS.hub.API.INFO", s2);
}
