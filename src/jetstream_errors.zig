// Generated from _refs/nats-server/server/errors.json
// DO NOT EDIT MANUALLY - regenerate using scripts/generate_jetstream_errors.py

const std = @import("std");

/// JetStream error codes and their corresponding Zig error types
pub const JetStreamError = error{
    /// resource limits exceeded for account (code: 10002)
    AccountResourcesExceeded,
    /// bad request (code: 10003)
    BadRequest,
    /// incomplete results (code: 10004)
    ClusterIncomplete,
    /// {err} (code: 10005)
    ClusterNoPeers,
    /// JetStream not in clustered mode (code: 10006)
    ClusterNotActive,
    /// JetStream cluster not assigned to this server (code: 10007)
    ClusterNotAssigned,
    /// JetStream system temporarily unavailable (code: 10008)
    ClusterNotAvail,
    /// JetStream cluster can not handle request (code: 10009)
    ClusterNotLeader,
    /// JetStream clustering support required (code: 10010)
    ClusterRequired,
    /// tags placement not supported for operation (code: 10011)
    ClusterTags,
    /// {err} (code: 10012)
    ConsumerCreate,
    /// consumer name already in use (code: 10013)
    ConsumerNameExist,
    /// consumer not found (code: 10014)
    ConsumerNotFound,
    /// deliver subject not valid (code: 10015)
    SnapshotDeliverSubjectInvalid,
    /// consumer expected to be durable but no durable name set in subject (code: 10016)
    ConsumerDurableNameNotInSubject,
    /// consumer name in subject does not match durable name in request (code: 10017)
    ConsumerDurableNameNotMatchSubject,
    /// consumer expected to be durable but a durable name was not set (code: 10018)
    ConsumerDurableNameNotSet,
    /// consumer expected to be ephemeral but detected a durable name set in subject (code: 10019)
    ConsumerEphemeralWithDurableInSubject,
    /// consumer expected to be ephemeral but a durable name was set in request (code: 10020)
    ConsumerEphemeralWithDurableName,
    /// stream external api prefix {prefix} must not overlap with {subject} (code: 10021)
    StreamExternalApiOverlap,
    /// stream external delivery prefix {prefix} overlaps with stream subject {subject} (code: 10022)
    StreamExternalDelPrefixOverlaps,
    /// insufficient resources (code: 10023)
    InsufficientResources,
    /// stream external delivery prefix {prefix} must not contain wildcards (code: 10024)
    StreamInvalidExternalDeliverySubj,
    /// invalid JSON: {err} (code: 10025)
    InvalidJSON,
    /// maximum consumers limit reached (code: 10026)
    MaximumConsumersLimit,
    /// maximum number of streams reached (code: 10027)
    MaximumStreamsLimit,
    /// insufficient memory resources available (code: 10028)
    MemoryResourcesExceeded,
    /// {err} (code: 10029)
    MirrorConsumerSetupFailed,
    /// stream mirror must have max message size >= source (code: 10030)
    MirrorMaxMessageSizeTooBig,
    /// stream mirrors can not also contain other sources (code: 10031)
    MirrorWithSources,
    /// stream mirrors can not have both start seq and start time configured (code: 10032)
    MirrorWithStartSeqAndTime,
    /// stream mirrors can not contain filtered subjects (code: 10033)
    MirrorWithSubjectFilters,
    /// stream mirrors can not contain subjects (code: 10034)
    MirrorWithSubjects,
    /// account not found (code: 10035)
    NoAccount,
    /// not currently supported in clustered mode (code: 10036)
    ClusterUnSupportFeature,
    /// no message found (code: 10037)
    NoMessageFound,
    /// expected an empty request payload (code: 10038)
    NotEmptyRequest,
    /// JetStream not enabled for account (code: 10039)
    NotEnabledForAccount,
    /// peer not a member (code: 10040)
    ClusterPeerNotMember,
    /// {err} (code: 10041)
    RaftGeneral,
    /// JetStream unable to subscribe to restore snapshot {subject}: {err} (code: 10042)
    RestoreSubscribeFailed,
    /// sequence {seq} not found (code: 10043)
    SequenceNotFound,
    /// server is not a member of the cluster (code: 10044)
    ClusterServerNotMember,
    /// {err} (code: 10045)
    SourceConsumerSetupFailed,
    /// stream source must have max message size >= target (code: 10046)
    SourceMaxMessageSizeTooBig,
    /// insufficient storage resources available (code: 10047)
    StorageResourcesExceeded,
    /// {err} (code: 10048)
    StreamAssignment,
    /// {err} (code: 10049)
    StreamCreate,
    /// {err} (code: 10050)
    StreamDelete,
    /// {err} (code: 10051)
    StreamGeneralError,
    /// {err} (code: 10052)
    StreamInvalidConfig,
    /// {err} (code: 10053)
    StreamLimits,
    /// message size exceeds maximum allowed (code: 10054)
    StreamMessageExceedsMaximum,
    /// stream mirror configuration can not be updated (code: 10055)
    StreamMirrorNotUpdatable,
    /// stream name in subject does not match request (code: 10056)
    StreamMismatch,
    /// {err} (code: 10057)
    StreamMsgDeleteFailed,
    /// stream name already in use with a different configuration (code: 10058)
    StreamNameExist,
    /// stream not found (code: 10059)
    StreamNotFound,
    /// expected stream does not match (code: 10060)
    StreamNotMatch,
    /// Replicas configuration can not be updated (code: 10061)
    StreamReplicasNotUpdatable,
    /// restore failed: {err} (code: 10062)
    StreamRestore,
    /// expected stream sequence does not match (code: 10063)
    StreamSequenceNotMatch,
    /// snapshot failed: {err} (code: 10064)
    StreamSnapshot,
    /// subjects overlap with an existing stream (code: 10065)
    StreamSubjectOverlap,
    /// {err} (code: 10066)
    StreamTemplateCreate,
    /// {err} (code: 10067)
    StreamTemplateDelete,
    /// template not found (code: 10068)
    StreamTemplateNotFound,
    /// {err} (code: 10069)
    StreamUpdate,
    /// wrong last msg ID: {id} (code: 10070)
    StreamWrongLastMsgID,
    /// wrong last sequence: {seq} (code: 10071)
    StreamWrongLastSequence,
    /// JetStream unable to open temp storage for restore (code: 10072)
    TempStorageFailed,
    /// template name in subject does not match request (code: 10073)
    TemplateNameNotMatchSubject,
    /// replicas > 1 not supported in non-clustered mode (code: 10074)
    StreamReplicasNotSupported,
    /// peer remap failed (code: 10075)
    PeerRemap,
    /// JetStream not enabled (code: 10076)
    NotEnabled,
    /// {err} (code: 10077)
    StreamStoreFailed,
    /// consumer config required (code: 10078)
    ConsumerConfigRequired,
    /// consumer deliver subject has wildcards (code: 10079)
    ConsumerDeliverToWildcards,
    /// consumer in push mode can not set max waiting (code: 10080)
    ConsumerPushMaxWaiting,
    /// consumer deliver subject forms a cycle (code: 10081)
    ConsumerDeliverCycle,
    /// consumer requires ack policy for max ack pending (code: 10082)
    ConsumerMaxPendingAckPolicyRequired,
    /// consumer idle heartbeat needs to be >= 100ms (code: 10083)
    ConsumerSmallHeartbeat,
    /// consumer in pull mode requires explicit ack policy on workqueue stream (code: 10084)
    ConsumerPullRequiresAck,
    /// consumer in pull mode requires a durable name (code: 10085)
    ConsumerPullNotDurable,
    /// consumer in pull mode can not have rate limit set (code: 10086)
    ConsumerPullWithRateLimit,
    /// consumer max waiting needs to be positive (code: 10087)
    ConsumerMaxWaitingNegative,
    /// consumer idle heartbeat requires a push based consumer (code: 10088)
    ConsumerHBRequiresPush,
    /// consumer flow control requires a push based consumer (code: 10089)
    ConsumerFCRequiresPush,
    /// consumer direct requires a push based consumer (code: 10090)
    ConsumerDirectRequiresPush,
    /// consumer direct requires an ephemeral consumer (code: 10091)
    ConsumerDirectRequiresEphemeral,
    /// consumer direct on a mapped consumer (code: 10092)
    ConsumerOnMapped,
    /// consumer filter subject is not a valid subset of the interest subjects (code: 10093)
    ConsumerFilterNotSubset,
    /// {err} (code: 10094)
    ConsumerInvalidPolicy,
    /// failed to parse consumer sampling configuration: {err} (code: 10095)
    ConsumerInvalidSampling,
    /// stream not valid (code: 10096)
    StreamInvalid,
    /// header size exceeds maximum allowed of 64k (code: 10097)
    StreamHeaderExceedsMaximum,
    /// workqueue stream requires explicit ack (code: 10098)
    ConsumerWQRequiresExplicitAck,
    /// multiple non-filtered consumers not allowed on workqueue stream (code: 10099)
    ConsumerWQMultipleUnfiltered,
    /// filtered consumer not unique on workqueue stream (code: 10100)
    ConsumerWQConsumerNotUnique,
    /// consumer must be deliver all on workqueue stream (code: 10101)
    ConsumerWQConsumerNotDeliverAll,
    /// consumer name is too long, maximum allowed is {max} (code: 10102)
    ConsumerNameTooLong,
    /// durable name can not contain '.', '*', '>' (code: 10103)
    ConsumerBadDurableName,
    /// error creating store for consumer: {err} (code: 10104)
    ConsumerStoreFailed,
    /// consumer already exists and is still active (code: 10105)
    ConsumerExistingActive,
    /// consumer replacement durable config not the same (code: 10106)
    ConsumerReplacementWithDifferentName,
    /// consumer description is too long, maximum allowed is {max} (code: 10107)
    ConsumerDescriptionTooLong,
    /// consumer with flow control also needs heartbeats (code: 10108)
    ConsumerWithFlowControlNeedsHeartbeats,
    /// invalid operation on sealed stream (code: 10109)
    StreamSealed,
    /// {err} (code: 10110)
    StreamPurgeFailed,
    /// {err} (code: 10111)
    StreamRollupFailed,
    /// invalid push consumer deliver subject (code: 10112)
    ConsumerInvalidDeliverSubject,
    /// account requires a stream config to have max bytes set (code: 10113)
    StreamMaxBytesRequired,
    /// consumer max request batch needs to be > 0 (code: 10114)
    ConsumerMaxRequestBatchNegative,
    /// consumer max request expires needs to be >= 1ms (code: 10115)
    ConsumerMaxRequestExpiresTooSmall,
    /// max deliver is required to be > length of backoff values (code: 10116)
    ConsumerMaxDeliverBackoff,
    /// subject details would exceed maximum allowed (code: 10117)
    StreamInfoMaxSubjects,
    /// stream is offline (code: 10118)
    StreamOffline,
    /// consumer is offline (code: 10119)
    ConsumerOffline,
    /// no JetStream default or applicable tiered limit present (code: 10120)
    NoLimits,
    /// consumer max ack pending exceeds system limit of {limit} (code: 10121)
    ConsumerMaxPendingAckExcess,
    /// stream max bytes exceeds account limit max stream bytes (code: 10122)
    StreamMaxStreamBytesExceeded,
    /// can not move and scale a stream in a single update (code: 10123)
    StreamMoveAndScale,
    /// stream move already in progress: {msg} (code: 10124)
    StreamMoveInProgress,
    /// consumer max request batch exceeds server limit of {limit} (code: 10125)
    ConsumerMaxRequestBatchExceeded,
    /// consumer config replica count exceeds parent stream (code: 10126)
    ConsumerReplicasExceedsStream,
    /// Consumer name can not contain path separators (code: 10127)
    ConsumerNameContainsPathSeparators,
    /// Stream name can not contain path separators (code: 10128)
    StreamNameContainsPathSeparators,
    /// stream move not in progress (code: 10129)
    StreamMoveNotInProgress,
    /// stream name already in use, cannot restore (code: 10130)
    StreamNameExistRestoreFailed,
    /// Consumer create request did not match filtered subject from create subject (code: 10131)
    ConsumerCreateFilterSubjectMismatch,
    /// Consumer Durable and Name have to be equal if both are provided (code: 10132)
    ConsumerCreateDurableAndNameMismatch,
    /// replicas count cannot be negative (code: 10133)
    ReplicasCountCannotBeNegative,
    /// consumer config replicas must match interest retention stream's replicas (code: 10134)
    ConsumerReplicasShouldMatchStream,
    /// consumer metadata exceeds maximum size of {limit} (code: 10135)
    ConsumerMetadataLength,
    /// consumer cannot have both FilterSubject and FilterSubjects specified (code: 10136)
    ConsumerDuplicateFilterSubjects,
    /// consumer with multiple subject filters cannot use subject based API (code: 10137)
    ConsumerMultipleFiltersNotAllowed,
    /// consumer subject filters cannot overlap (code: 10138)
    ConsumerOverlappingSubjectFilters,
    /// consumer filter in FilterSubjects cannot be empty (code: 10139)
    ConsumerEmptyFilter,
    /// duplicate source configuration detected (code: 10140)
    SourceDuplicateDetected,
    /// sourced stream name is invalid (code: 10141)
    SourceInvalidStreamName,
    /// mirrored stream name is invalid (code: 10142)
    MirrorInvalidStreamName,
    /// stream mirrors can not have first sequence configured (code: 10143)
    MirrorWithFirstSeq,
    /// source with multiple subject transforms cannot also have a single subject filter (code: 10144)
    SourceMultipleFiltersNotAllowed,
    /// source transform source: {err} (code: 10145)
    SourceInvalidSubjectFilter,
    /// source transform: {err} (code: 10146)
    SourceInvalidTransformDestination,
    /// source filters can not overlap (code: 10147)
    SourceOverlappingSubjectFilters,
    /// consumer already exists (code: 10148)
    ConsumerAlreadyExists,
    /// consumer does not exist (code: 10149)
    ConsumerDoesNotExist,
    /// mirror with multiple subject transforms cannot also have a single subject filter (code: 10150)
    MirrorMultipleFiltersNotAllowed,
    /// mirror transform source: {err} (code: 10151)
    MirrorInvalidSubjectFilter,
    /// mirror subject filters can not overlap (code: 10152)
    MirrorOverlappingSubjectFilters,
    /// consumer inactive threshold exceeds system limit of {limit} (code: 10153)
    ConsumerInactiveThresholdExcess,
    /// mirror transform: {err} (code: 10154)
    MirrorInvalidTransformDestination,
    /// stream transform source: {err} (code: 10155)
    StreamTransformInvalidSource,
    /// stream transform: {err} (code: 10156)
    StreamTransformInvalidDestination,
    /// pedantic mode: {err} (code: 10157)
    Pedantic,
    /// duplicate message id is in process (code: 10158)
    StreamDuplicateMessageConflict,
    /// Setting PriorityPolicy requires at least one PriorityGroup to be set (code: 10159)
    ConsumerPriorityPolicyWithoutGroup,
    /// Provided priority group does not exist for this consumer (code: 10160)
    ConsumerInvalidPriorityGroup,
    /// Group name cannot be an empty string (code: 10161)
    ConsumerEmptyGroupName,
    /// Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters (code: 10162)
    ConsumerInvalidGroupName,
    /// expected last sequence per subject temporarily unavailable (code: 10163)
    StreamExpectedLastSeqPerSubjectNotReady,
    /// wrong last sequence (code: 10164)
    StreamWrongLastSequenceConstant,
    /// invalid per-message TTL (code: 10165)
    MessageTTLInvalid,
    /// per-message TTL is disabled (code: 10166)
    MessageTTLDisabled,
    /// too many requests (code: 10167)
    StreamTooManyRequests,
    /// message counters is disabled (code: 10168)
    MessageIncrDisabled,
    /// message counter increment is missing (code: 10169)
    MessageIncrMissing,
    /// message counter has payload (code: 10170)
    MessageIncrPayload,
    /// message counter increment is invalid (code: 10171)
    MessageIncrInvalid,
    /// message counter is broken (code: 10172)
    MessageCounterBroken,
    /// stream mirrors can not also calculate counters (code: 10173)
    MirrorWithCounters,
    /// atomic publish is disabled (code: 10174)
    AtomicPublishDisabled,
    /// atomic publish sequence is missing (code: 10175)
    AtomicPublishMissingSeq,
    /// atomic publish batch is incomplete (code: 10176)
    AtomicPublishIncompleteBatch,
    /// atomic publish unsupported header used: {header} (code: 10177)
    AtomicPublishUnsupportedHeaderBatch,
    /// priority groups can not be used with push consumers (code: 10178)
    ConsumerPushWithPriorityGroup,
    /// atomic publish batch ID is invalid (code: 10179)
    AtomicPublishInvalidBatchID,
    /// min last sequence (code: 10180)
    StreamMinLastSeq,
    /// consumer ack policy invalid (code: 10181)
    ConsumerAckPolicyInvalid,
    /// consumer replay policy invalid (code: 10182)
    ConsumerReplayPolicyInvalid,
    /// consumer ack wait needs to be positive (code: 10183)
    ConsumerAckWaitNegative,
    /// consumer backoff needs to be positive (code: 10184)
    ConsumerBackOffNegative,
    /// JetStream minimum api level required (code: 10185)
    RequiredApiLevel,
    /// stream mirrors can not also schedule messages (code: 10186)
    MirrorWithMsgSchedules,
    /// stream source can not also schedule messages (code: 10187)
    SourceWithMsgSchedules,
    /// message schedules is disabled (code: 10188)
    MessageSchedulesDisabled,
    /// message schedules pattern is invalid (code: 10189)
    MessageSchedulesPatternInvalid,
    /// message schedules target is invalid (code: 10190)
    MessageSchedulesTargetInvalid,
    /// message schedules invalid per-message TTL (code: 10191)
    MessageSchedulesTTLInvalid,
    /// message schedules invalid rollup (code: 10192)
    MessageSchedulesRollupInvalid,
    /// missing sequence for expected last sequence per subject (code: 10193)
    StreamExpectedLastSeqPerSubjectInvalid,
    /// stream is offline: {err} (code: 10194)
    StreamOfflineReason,
    /// consumer is offline: {err} (code: 10195)
    ConsumerOfflineReason,
    /// consumer can not have priority groups when policy is none (code: 10196)
    ConsumerPriorityGroupWithPolicyNone,
    /// PinnedTTL cannot be set when PriorityPolicy is none (code: 10197)
    ConsumerPinnedTTLWithoutPriorityPolicyNone,
    /// Unknown or unmapped error code
    UnknownError,
};

/// Map JetStream API error codes to Zig errors
pub fn mapErrorCode(error_code: u32) JetStreamError {
    return switch (error_code) {
        10002 => JetStreamError.AccountResourcesExceeded,
        10003 => JetStreamError.BadRequest,
        10004 => JetStreamError.ClusterIncomplete,
        10005 => JetStreamError.ClusterNoPeers,
        10006 => JetStreamError.ClusterNotActive,
        10007 => JetStreamError.ClusterNotAssigned,
        10008 => JetStreamError.ClusterNotAvail,
        10009 => JetStreamError.ClusterNotLeader,
        10010 => JetStreamError.ClusterRequired,
        10011 => JetStreamError.ClusterTags,
        10012 => JetStreamError.ConsumerCreate,
        10013 => JetStreamError.ConsumerNameExist,
        10014 => JetStreamError.ConsumerNotFound,
        10015 => JetStreamError.SnapshotDeliverSubjectInvalid,
        10016 => JetStreamError.ConsumerDurableNameNotInSubject,
        10017 => JetStreamError.ConsumerDurableNameNotMatchSubject,
        10018 => JetStreamError.ConsumerDurableNameNotSet,
        10019 => JetStreamError.ConsumerEphemeralWithDurableInSubject,
        10020 => JetStreamError.ConsumerEphemeralWithDurableName,
        10021 => JetStreamError.StreamExternalApiOverlap,
        10022 => JetStreamError.StreamExternalDelPrefixOverlaps,
        10023 => JetStreamError.InsufficientResources,
        10024 => JetStreamError.StreamInvalidExternalDeliverySubj,
        10025 => JetStreamError.InvalidJSON,
        10026 => JetStreamError.MaximumConsumersLimit,
        10027 => JetStreamError.MaximumStreamsLimit,
        10028 => JetStreamError.MemoryResourcesExceeded,
        10029 => JetStreamError.MirrorConsumerSetupFailed,
        10030 => JetStreamError.MirrorMaxMessageSizeTooBig,
        10031 => JetStreamError.MirrorWithSources,
        10032 => JetStreamError.MirrorWithStartSeqAndTime,
        10033 => JetStreamError.MirrorWithSubjectFilters,
        10034 => JetStreamError.MirrorWithSubjects,
        10035 => JetStreamError.NoAccount,
        10036 => JetStreamError.ClusterUnSupportFeature,
        10037 => JetStreamError.NoMessageFound,
        10038 => JetStreamError.NotEmptyRequest,
        10039 => JetStreamError.NotEnabledForAccount,
        10040 => JetStreamError.ClusterPeerNotMember,
        10041 => JetStreamError.RaftGeneral,
        10042 => JetStreamError.RestoreSubscribeFailed,
        10043 => JetStreamError.SequenceNotFound,
        10044 => JetStreamError.ClusterServerNotMember,
        10045 => JetStreamError.SourceConsumerSetupFailed,
        10046 => JetStreamError.SourceMaxMessageSizeTooBig,
        10047 => JetStreamError.StorageResourcesExceeded,
        10048 => JetStreamError.StreamAssignment,
        10049 => JetStreamError.StreamCreate,
        10050 => JetStreamError.StreamDelete,
        10051 => JetStreamError.StreamGeneralError,
        10052 => JetStreamError.StreamInvalidConfig,
        10053 => JetStreamError.StreamLimits,
        10054 => JetStreamError.StreamMessageExceedsMaximum,
        10055 => JetStreamError.StreamMirrorNotUpdatable,
        10056 => JetStreamError.StreamMismatch,
        10057 => JetStreamError.StreamMsgDeleteFailed,
        10058 => JetStreamError.StreamNameExist,
        10059 => JetStreamError.StreamNotFound,
        10060 => JetStreamError.StreamNotMatch,
        10061 => JetStreamError.StreamReplicasNotUpdatable,
        10062 => JetStreamError.StreamRestore,
        10063 => JetStreamError.StreamSequenceNotMatch,
        10064 => JetStreamError.StreamSnapshot,
        10065 => JetStreamError.StreamSubjectOverlap,
        10066 => JetStreamError.StreamTemplateCreate,
        10067 => JetStreamError.StreamTemplateDelete,
        10068 => JetStreamError.StreamTemplateNotFound,
        10069 => JetStreamError.StreamUpdate,
        10070 => JetStreamError.StreamWrongLastMsgID,
        10071 => JetStreamError.StreamWrongLastSequence,
        10072 => JetStreamError.TempStorageFailed,
        10073 => JetStreamError.TemplateNameNotMatchSubject,
        10074 => JetStreamError.StreamReplicasNotSupported,
        10075 => JetStreamError.PeerRemap,
        10076 => JetStreamError.NotEnabled,
        10077 => JetStreamError.StreamStoreFailed,
        10078 => JetStreamError.ConsumerConfigRequired,
        10079 => JetStreamError.ConsumerDeliverToWildcards,
        10080 => JetStreamError.ConsumerPushMaxWaiting,
        10081 => JetStreamError.ConsumerDeliverCycle,
        10082 => JetStreamError.ConsumerMaxPendingAckPolicyRequired,
        10083 => JetStreamError.ConsumerSmallHeartbeat,
        10084 => JetStreamError.ConsumerPullRequiresAck,
        10085 => JetStreamError.ConsumerPullNotDurable,
        10086 => JetStreamError.ConsumerPullWithRateLimit,
        10087 => JetStreamError.ConsumerMaxWaitingNegative,
        10088 => JetStreamError.ConsumerHBRequiresPush,
        10089 => JetStreamError.ConsumerFCRequiresPush,
        10090 => JetStreamError.ConsumerDirectRequiresPush,
        10091 => JetStreamError.ConsumerDirectRequiresEphemeral,
        10092 => JetStreamError.ConsumerOnMapped,
        10093 => JetStreamError.ConsumerFilterNotSubset,
        10094 => JetStreamError.ConsumerInvalidPolicy,
        10095 => JetStreamError.ConsumerInvalidSampling,
        10096 => JetStreamError.StreamInvalid,
        10097 => JetStreamError.StreamHeaderExceedsMaximum,
        10098 => JetStreamError.ConsumerWQRequiresExplicitAck,
        10099 => JetStreamError.ConsumerWQMultipleUnfiltered,
        10100 => JetStreamError.ConsumerWQConsumerNotUnique,
        10101 => JetStreamError.ConsumerWQConsumerNotDeliverAll,
        10102 => JetStreamError.ConsumerNameTooLong,
        10103 => JetStreamError.ConsumerBadDurableName,
        10104 => JetStreamError.ConsumerStoreFailed,
        10105 => JetStreamError.ConsumerExistingActive,
        10106 => JetStreamError.ConsumerReplacementWithDifferentName,
        10107 => JetStreamError.ConsumerDescriptionTooLong,
        10108 => JetStreamError.ConsumerWithFlowControlNeedsHeartbeats,
        10109 => JetStreamError.StreamSealed,
        10110 => JetStreamError.StreamPurgeFailed,
        10111 => JetStreamError.StreamRollupFailed,
        10112 => JetStreamError.ConsumerInvalidDeliverSubject,
        10113 => JetStreamError.StreamMaxBytesRequired,
        10114 => JetStreamError.ConsumerMaxRequestBatchNegative,
        10115 => JetStreamError.ConsumerMaxRequestExpiresTooSmall,
        10116 => JetStreamError.ConsumerMaxDeliverBackoff,
        10117 => JetStreamError.StreamInfoMaxSubjects,
        10118 => JetStreamError.StreamOffline,
        10119 => JetStreamError.ConsumerOffline,
        10120 => JetStreamError.NoLimits,
        10121 => JetStreamError.ConsumerMaxPendingAckExcess,
        10122 => JetStreamError.StreamMaxStreamBytesExceeded,
        10123 => JetStreamError.StreamMoveAndScale,
        10124 => JetStreamError.StreamMoveInProgress,
        10125 => JetStreamError.ConsumerMaxRequestBatchExceeded,
        10126 => JetStreamError.ConsumerReplicasExceedsStream,
        10127 => JetStreamError.ConsumerNameContainsPathSeparators,
        10128 => JetStreamError.StreamNameContainsPathSeparators,
        10129 => JetStreamError.StreamMoveNotInProgress,
        10130 => JetStreamError.StreamNameExistRestoreFailed,
        10131 => JetStreamError.ConsumerCreateFilterSubjectMismatch,
        10132 => JetStreamError.ConsumerCreateDurableAndNameMismatch,
        10133 => JetStreamError.ReplicasCountCannotBeNegative,
        10134 => JetStreamError.ConsumerReplicasShouldMatchStream,
        10135 => JetStreamError.ConsumerMetadataLength,
        10136 => JetStreamError.ConsumerDuplicateFilterSubjects,
        10137 => JetStreamError.ConsumerMultipleFiltersNotAllowed,
        10138 => JetStreamError.ConsumerOverlappingSubjectFilters,
        10139 => JetStreamError.ConsumerEmptyFilter,
        10140 => JetStreamError.SourceDuplicateDetected,
        10141 => JetStreamError.SourceInvalidStreamName,
        10142 => JetStreamError.MirrorInvalidStreamName,
        10143 => JetStreamError.MirrorWithFirstSeq,
        10144 => JetStreamError.SourceMultipleFiltersNotAllowed,
        10145 => JetStreamError.SourceInvalidSubjectFilter,
        10146 => JetStreamError.SourceInvalidTransformDestination,
        10147 => JetStreamError.SourceOverlappingSubjectFilters,
        10148 => JetStreamError.ConsumerAlreadyExists,
        10149 => JetStreamError.ConsumerDoesNotExist,
        10150 => JetStreamError.MirrorMultipleFiltersNotAllowed,
        10151 => JetStreamError.MirrorInvalidSubjectFilter,
        10152 => JetStreamError.MirrorOverlappingSubjectFilters,
        10153 => JetStreamError.ConsumerInactiveThresholdExcess,
        10154 => JetStreamError.MirrorInvalidTransformDestination,
        10155 => JetStreamError.StreamTransformInvalidSource,
        10156 => JetStreamError.StreamTransformInvalidDestination,
        10157 => JetStreamError.Pedantic,
        10158 => JetStreamError.StreamDuplicateMessageConflict,
        10159 => JetStreamError.ConsumerPriorityPolicyWithoutGroup,
        10160 => JetStreamError.ConsumerInvalidPriorityGroup,
        10161 => JetStreamError.ConsumerEmptyGroupName,
        10162 => JetStreamError.ConsumerInvalidGroupName,
        10163 => JetStreamError.StreamExpectedLastSeqPerSubjectNotReady,
        10164 => JetStreamError.StreamWrongLastSequenceConstant,
        10165 => JetStreamError.MessageTTLInvalid,
        10166 => JetStreamError.MessageTTLDisabled,
        10167 => JetStreamError.StreamTooManyRequests,
        10168 => JetStreamError.MessageIncrDisabled,
        10169 => JetStreamError.MessageIncrMissing,
        10170 => JetStreamError.MessageIncrPayload,
        10171 => JetStreamError.MessageIncrInvalid,
        10172 => JetStreamError.MessageCounterBroken,
        10173 => JetStreamError.MirrorWithCounters,
        10174 => JetStreamError.AtomicPublishDisabled,
        10175 => JetStreamError.AtomicPublishMissingSeq,
        10176 => JetStreamError.AtomicPublishIncompleteBatch,
        10177 => JetStreamError.AtomicPublishUnsupportedHeaderBatch,
        10178 => JetStreamError.ConsumerPushWithPriorityGroup,
        10179 => JetStreamError.AtomicPublishInvalidBatchID,
        10180 => JetStreamError.StreamMinLastSeq,
        10181 => JetStreamError.ConsumerAckPolicyInvalid,
        10182 => JetStreamError.ConsumerReplayPolicyInvalid,
        10183 => JetStreamError.ConsumerAckWaitNegative,
        10184 => JetStreamError.ConsumerBackOffNegative,
        10185 => JetStreamError.RequiredApiLevel,
        10186 => JetStreamError.MirrorWithMsgSchedules,
        10187 => JetStreamError.SourceWithMsgSchedules,
        10188 => JetStreamError.MessageSchedulesDisabled,
        10189 => JetStreamError.MessageSchedulesPatternInvalid,
        10190 => JetStreamError.MessageSchedulesTargetInvalid,
        10191 => JetStreamError.MessageSchedulesTTLInvalid,
        10192 => JetStreamError.MessageSchedulesRollupInvalid,
        10193 => JetStreamError.StreamExpectedLastSeqPerSubjectInvalid,
        10194 => JetStreamError.StreamOfflineReason,
        10195 => JetStreamError.ConsumerOfflineReason,
        10196 => JetStreamError.ConsumerPriorityGroupWithPolicyNone,
        10197 => JetStreamError.ConsumerPinnedTTLWithoutPriorityPolicyNone,
        else => JetStreamError.UnknownError,
    };
}

// Total JetStream errors: 196
