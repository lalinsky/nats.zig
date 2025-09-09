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

// Re-export key types and functions
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionOptions = @import("connection.zig").ConnectionOptions;
pub const ConnectionStatus = @import("connection.zig").ConnectionStatus;
pub const ConnectionError = @import("connection.zig").ConnectionError;
pub const PublishError = @import("connection.zig").PublishError;
pub const Message = @import("message.zig").Message;
pub const Subscription = @import("subscription.zig").Subscription;
pub const MsgHandler = @import("subscription.zig").MsgHandler;
pub const createMsgHandler = @import("subscription.zig").createMsgHandler;
pub const Dispatcher = @import("dispatcher.zig").Dispatcher;
pub const DispatcherPool = @import("dispatcher.zig").DispatcherPool;
pub const ServerPool = @import("server_pool.zig").ServerPool;
pub const Server = @import("server_pool.zig").Server;
pub const Socket = @import("socket.zig").Socket;
pub const inbox = @import("inbox.zig");

// JetStream types
pub const JetStream = @import("jetstream.zig").JetStream;
pub const JetStreamOptions = @import("jetstream.zig").JetStreamOptions;
pub const JetStreamError = @import("jetstream_errors.zig").JetStreamError;
pub const PublishOptions = @import("jetstream.zig").PublishOptions;
pub const StreamConfig = @import("jetstream.zig").StreamConfig;
pub const StreamInfo = @import("jetstream.zig").StreamInfo;
pub const StreamPurgeRequest = @import("jetstream.zig").StreamPurgeRequest;
pub const ConsumerConfig = @import("jetstream.zig").ConsumerConfig;
pub const ConsumerInfo = @import("jetstream.zig").ConsumerInfo;
pub const AckError = @import("jetstream_message.zig").AckError;

// JetStream Push Subscription types
pub const JetStreamMessage = @import("jetstream.zig").JetStreamMessage;
pub const MsgMetadata = @import("jetstream.zig").MsgMetadata;
pub const SequencePair = @import("jetstream.zig").SequencePair;
pub const JetStreamSubscription = @import("jetstream.zig").JetStreamSubscription;

// JetStream Pull Subscription types
pub const PullSubscription = @import("jetstream.zig").PullSubscription;
pub const FetchRequest = @import("jetstream.zig").FetchRequest;
pub const MessageBatch = @import("jetstream.zig").MessageBatch;

// JetStream KV types
pub const KV = @import("jetstream_kv.zig").KV;
pub const KVManager = @import("jetstream_kv.zig").KVManager;
pub const KVEntry = @import("jetstream_kv.zig").KVEntry;
pub const KVStatus = @import("jetstream_kv.zig").KVStatus;
pub const KVConfig = @import("jetstream_kv.zig").KVConfig;
pub const PutOptions = @import("jetstream_kv.zig").PutOptions;
pub const KVOperation = @import("jetstream_kv.zig").KVOperation;
pub const KVError = @import("jetstream_kv.zig").KVError;
// Validation functions
pub const validateStreamName = @import("validation.zig").validateStreamName;
pub const validateConsumerName = @import("validation.zig").validateConsumerName;
pub const validateSubject = @import("validation.zig").validateSubject;
pub const validateQueueName = @import("validation.zig").validateQueueName;
pub const validateKVBucketName = @import("validation.zig").validateKVBucketName;
pub const validateKVKeyName = @import("validation.zig").validateKVKeyName;

// Utility types
pub const Result = @import("result.zig").Result;

// Removed top-level connect functions - use Connection.init() and Connection.connect() directly

test {
    std.testing.refAllDecls(@This());
    _ = @import("jetstream.zig");
    _ = @import("jetstream_message.zig");
    _ = @import("jetstream_kv.zig");
    _ = @import("pending_msgs_test.zig");
}
