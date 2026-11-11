# Local changes to nats.zig

Fixes made while migrating my app onto this library.
Each entry: what broke, how it showed up, what changed. Upstream candidates unless noted.

The submodule sits on an upstream commit (`b8ffdd9` since 2026-10-07, §35; `d4cd40d`
before); the changes live in its working tree and, since 2026-09-23, as an **ordered
series** of patches here, `01-…` onwards, each the exact diff between two consecutive
states. They apply in numeric order and only in that order; a number may be missing (03,
dropped in §35). Two scripts keep it honest:

    nats.zig-patch/check-series.sh          upstream + series == working tree, or say where not
    nats.zig-patch/new-patch.sh <topic>     cut the next patch as diff(upstream + series, working tree)

Never cut a patch from `git -C nats.zig diff` with hunks filtered by hand — §17 is what
that produced. Entry numbers below are chronological and are not the series numbers;
the table in §17 maps one onto the other.

---

## 1. Use-after-free in `pullSubscribe` when the stream is derived from the subject

**How it appeared**

A pull subscription created without an explicit `.stream`, then fetched:

```zig
const sub = try js.pullSubscribe("zb.spike.one", "zb_spike_worker", .{
    .config = .{ .ack_policy = .explicit, .max_deliver = 3, .filter_subject = "zb.spike.one" },
});
var batch = try sub.fetch(1, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
```

```
error: InvalidSubject
  src/validation.zig:144  validateSubject
  src/connection.zig:661  publishRequest
  src/jetstream.zig:468   fetch
```

Misleading symptom: the subject was not malformed, it was **freed**.
`validateSubject` was reading whatever the allocator had put back in that memory.

**Cause**

`pullSubscribe` resolves the stream name, and frees it on return when it had to look it up:

```zig
const stream_name = if (options.stream) |s| s
    else if (subject) |s| try self.lookupStreamBySubject(s)   // heap-allocated
    else return error.StreamOrSubjectRequired;

defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);
```

but then stores that same pointer in the `PullSubscription`, which outlives the call:

```zig
pull_subscription.* = PullSubscription{ .stream_name = stream_name, ... };
```

`fetch` later builds `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` from it.
Passing `.stream` explicitly avoids the path entirely, which is why it is easy to miss.

**Change**

`src/jetstream.zig`:

* `pullSubscribe` — `defer free` → `errdefer free`.
  Ownership moves to the subscription once it is constructed; the `errdefer` still covers the failure paths before that.
* `PullSubscription` — new `owns_stream_name: bool`, set from `options.stream == null and subject != null`. Needed because by `deinit` time the caller's `options` are long gone, so the struct has to remember whether the memory is its own.
* `PullSubscription.deinit` — frees `stream_name` when it owns it.

⚠️ **`subscribe` (push) has the identical line and it is correct there** — that function only uses `stream_name` within its own body. I patched it first by matching on the text and had to revert: it would have leaked on every push subscription. The two lines look the same; only the lifetime differs.

**Verified**

```bash
cd spike && zig build && ./zig-out/bin/spike     # 12/12, on the path that used to crash
cd nats.zig && zig build test                    # 104 + 167 tests pass
```

This survived 249 commits.


---

## 2. `UNSUB` on a closed connection logged at error level

**How it appeared**

Every clean shutdown of the bridge, once per subscription:

```
info(nats): Closing connection
info(nats): Disconnected
error(nats): Failed to send UNSUB for sid 1: error.Closed
error(nats): Failed to send UNSUB for sid 1: error.Closed
error(nats): Failed to send UNSUB for sid 1: error.Closed
```

Note the ordering: the errors arrive *after* the connection is reported closed, which isthe clue.
Three of them because my app has three subscribers.

**Cause**

`Connection.unsubscribe` (connection.zig ~:914) treats every failure of `unsubscribeInternal` as an error worth reporting.
But sending `UNSUB` on a socket that is already closed cannot succeed *and does not need to* — the server drops all subscriptions for a connection when it goes away.
It is an expected step in an ordinary shutdown, not a fault.

Harmless in itself; the cost is that an operator learns to ignore `error(nats)`.

**Change**

`src/connection.zig` — `error.Closed` now logs at `debug` with the reason, `error.Canceled`
keeps its `recancel()` path, everything else still logs at error level.

**Verified**

```bash
kill -TERM <bridge>     # 0 occurrences, was 3
cd nats.zig && zig build test    # 104 + 167 tests pass
```

---

## 3. A wildcard inbox makes every pull request spawn another

**How it appeared**

An idle stream — nothing published, nothing to deliver — with two pull consumers, each
looping on `fetch(1, 500ms)`. Measured against nats-server 2.14.4:

| | |
| --- | --- |
| pull requests issued | **20.7/s** on one consumer, 3.6/s on the other |
| period between requests | **48 ms**, against a requested `expires` of **500 ms** |
| `num_waiting` on the consumer | **10** and **2** — parked requests, not one in flight |
| NATS traffic, whole server, idle | 17.3 msg/s in *and* out |
| fetch ids reached | 5,911 in 286 s, monotonic — genuinely new requests, not retries |

The server was answering each abandoned request with `408 Request Timeout` at its expiry, so
the debug log filled with 6,658 status frames in under five minutes on a stream with no data.

**Why**

`PullSubscription.fetch` mints a unique reply subject per request:

```zig
const reply_subject = try std.fmt.allocPrint(..., "{s}{d}", .{ self.inbox_prefix, fetch_id });
```

…then reads replies from `self.inbox_subscription`, which is a **wildcard** (`<prefix>.*`).
So it also receives replies addressed to *earlier* fetches. A fetch that returns before its
own `expires` leaves its request parked server-side until the server times it out, and that
late `408` then lands here while a **newer** fetch is waiting. The newer fetch treats it as
its own timeout, returns immediately, the caller re-fetches at once — and that request is
abandoned in turn. Self-sustaining, settling at `expires ÷ period` parked requests, which is
exactly the 10 and 2 observed against a 500 ms expiry.

Nothing is lost, because the wildcard also picks up the real replies. The costs are the
request storm, the CPU, and `num_waiting` climbing toward `max_waiting` (512).

**The fix** (`nats.zig-jetstream-stale-408.patch`, hunk at `@@ -483`)

Discard a frame whose subject is not this fetch's reply subject, and keep waiting:

```zig
if (raw_msg.status_code > 0 and !std.mem.eql(u8, raw_msg.subject, reply_subject)) {
    raw_msg.deinit();
    continue;
}
```

⚠️ **Status frames only — this is the part that bites.** The first attempt matched *every*
frame against `reply_subject` and broke `jetstream_pull_test` "basic fetch" (expected 2
messages, got 0). JetStream delivers a **data** message with its *original* subject
(`test.foo`), routed to the inbox by subscription id; only status frames are addressed to the
inbox itself (`HMSG _INBOX.<prefix>.<fetch_id>`). Matching data messages against the inbox
therefore drops every real message. The existing comment — *"the timestamp in the ACK subject
ensures messages belong to this fetch request"* — reasons about data messages and does not
cover status frames.

After the fix, same idle stream: **3.8 msg/s** server-wide (from 17.3), and `num_waiting`
sits at **1** on both consumers instead of 10 and 2.

`zig build test-unit` 104/104 and `zig build test-e2e` 167/167 pass with it applied.

**Reproducing without ZeBridge**

Create a pull consumer on an empty stream, loop `fetch(1, 500ms)`, and watch
`nats consumer info <stream> <consumer>`. `num_waiting` should stay at 1; it climbs to
`expires ÷ actual_period` instead. A caller cannot detect this from the API: `fetch` reports
408 in `MessageBatch.err` rather than returning an error, so `catch { continue; }` never fires
and an empty `messages` slice looks like a normal idle result.

---

## 5. The deliberate close logged like an incident

**How it appeared**

Every generation-producer tick — the producer opens a fresh connection per tick and
closes it on the way out — the bridge log gained:

```
info(nats): Closing connection
info(nats): Disconnected
```

Reported from review: an operator saw the pair recurring, cross-checked
`nats_reconnects=0` on /status, and reasonably asked whether NATS was reconnecting
silently or the counter was broken. Neither — the counter was right and nothing was
wrong, which is exactly the problem: a routine, caller-initiated close should not read
as a connection event worth investigating.

**Cause**

`Connection.close()` logs "Closing connection" at info, and the connection loop's exit
logs "Disconnected" at info whenever it ends without an error — including after a
deliberate `close()`. Both lines are about an event the CALLER caused.

**Change**

`src/connection.zig` — both lines drop to `debug`. Real failures keep their voice:
an unexpected drop logs `Connection failed: {err}` (still info, inside the same exit
path but only when an error is present) and the reconnect machinery reports at warn.
A server-initiated clean EOF also lands on the quieted line, but the reconnect path
announces itself immediately after, so the signal is not lost.

`nats.zig-quiet-deliberate-close.patch` (hunks 1 and 3 of the connection.zig diff; the
middle hunk is entry 2's).

**Verified**

```bash
cd nats.zig && zig build test        # 132 + 183 tests pass
# live bridge, 65s at LOG_LEVEL=info: 0 occurrences of the pair (was 2 per cadence tick)
```

---

## 4. Submodule bumped to upstream `d4cd40d` (2026-08-24)

`b3684bd` ("Retry JetStream publishes on no responders", #148) → `d4cd40d`
("Type-check the public surface", #157) — 11 upstream commits. None of them contain
fixes 1–3 above, so both local patches were re-applied; both applied cleanly with no
re-fit (`git apply` straight from `nats.zig-connection.patch` and
`nats.zig-jetstream-stale-408.patch`).

What the bump brings, relevant to ZeBridge:

* **#153/#154/#156** — lock-order inversion and deinit races around `subs_mutex` in
  `connection.zig`, the same shutdown region fix 2 touches.
* **TLS** (#150 series, incl. mutual TLS) — the client can now speak `tls://`. Does not
  change the v1.0 colocation decision, but the "no TLS client exists" premise behind it
  is no longer true.
* **#157** — type-checks the public surface; three API functions never compiled before.

Verified after the bump: `zig build` and `zig build test` clean (Debug and ReleaseFast),
bridge boots against the Docker stack, `keys.py` passes 7/7 (exercises the pull-fetch
loop fix 3 guards), and the `MUTATIONS` durable shows `num_waiting: 0` after the run —
no parked-pull storm.

**TLS verified (2026-08-24).** The library's own e2e TLS suite passes 8/8 against real
`nats-server` containers (`TEST_FILTER="tls e2e" zig build test-e2e`): CA-verified
connect, verification failure without the CA, insecure-skip-verify, handshake-first,
mutual TLS with `verify_and_map` as the authentication, rejection without/with an
unmapped client cert, and reconnect over TLS. On top of that, a standalone probe ran the
bridge's workload shape — `addStream`, acked `js.publish`, durable `pullSubscribe` +
`fetch` + ack — against a host `nats-server` with only a `tls{}` listener
(JetStream on, CA-verified, no insecure flags): all green. That probe is now permanent:
`scripts/scenarios/tls.py` provisions the server, builds the probe against the
submodule, and asserts both the verified round trip and that a connect *without* the CA
is refused (`CertificateIssuerNotFound`). So `tls://` is real for
both core NATS and JetStream, which reopens the door the colocation constraint closed —
a bridge talking to a remote NATS (e.g. a per-tenant leaf) over TLS is now a client
capability, not a wish.

## 6. The terminal auth verdict, retrievable (2026-09-04)
`Connection.final_auth_error` + `lastAuthError()`. When the server states an auth
verdict — `-ERR 'User Authentication Expired'`, revoked, or violation — the reader and
the two-strikes reconnect path both record it, so a caller that later finds the
connection closed can NAME the cause instead of reporting a bare `ConnectionClosed`.
libzb's C ABI consults it on any failed poll (ZeBridge NOTES §10cj). Patch:
nats.zig-auth-verdict.patch. 132+183 tests pass.

---

## 7. Three standing patches that never got their entry (ledgered 2026-09-04)

All three shipped with libzb's live-tailing work (ZeBridge NOTES §10bh) and were
referenced from there but never written up HERE — found when a review asked
whether a submodule diff was recorded. Nothing new in the diff; this closes the
bookkeeping gap.

**`nats.zig-fetch-early-return-503.patch`** — `PullSubscription.fetch` parked
terminal status frames (503 and friends) in `MessageBatch.err` and returned an
EMPTY batch, so a caller's `catch` never fired and "the consumer is gone" looked
identical to "nothing to read". A terminal status is now a returned error; the
dead `batch.err` arm in libzb's drain loop became live the same day (see the
comment at libzb client.zig's bounded drain).

**`nats.zig-consumer-inactive-threshold.patch`** — `ConsumerConfig` gains
`inactive_threshold`: the SERVER reaps an idle consumer after the given
nanoseconds. libzb names its tail consumers and lets the server own their
lifetime (30 s past any real fetch gap) instead of deleting them client-side —
a client that dies uncleanly leaves nothing behind.

**`nats.zig-shared-pull-inbox.patch`** — `PullInbox`: ONE wait over many pull
subscriptions — one pull per tail into a shared inbox, first message from any
of them ends the wait. Replaced libzb's 250 ms round-robin whose cost was a
FIXED idle slice per stream (measured 265 ± 1 ms added latency on every write).
Carries `owns_inbox` so a subscription on a shared inbox does not free it, and
the addendum hunk: the all-consumers-gone path freed `gone` twice (errdefer +
explicit) — a macOS SIGTRAP found by stream_wipe.py, 2026-09-03.

---

## 8. Double release of the meta message on a deleted object (2026-09-06)

**How it appeared**

libzb's host (a Python process over the C ABI) aborted at `zb_client_close` after a
seed that had failed on a chain object the producer had just deleted:

```
malloc: *** error for object 0x...: pointer being freed was not allocated
```

Found with `lldb -o "breakpoint set -n malloc_error_break"`: the second free came from
`ObjectStore.get`, on the meta message of an object whose info said `deleted: true`.

**Cause**

`get()` and `infoIncludingDeleted()` fetch the object's meta message, and on the
deleted path release it explicitly before returning `error.ObjectNotFound` — while an
`errdefer meta_msg.deinit()` above releases it again on that same error return.
Reachable by any caller that reads an object between the producer's delete and the
manifest swap, which the generation chain does on every prune.

**Change** (`nats.zig-objstore-double-release.patch`)

`src/jetstream_objstore.zig`: an owned flag on the meta message —
`var meta_msg_owned = true; errdefer if (meta_msg_owned) meta_msg.deinit();` — cleared
right after the explicit release, in both functions.

`src/root.zig`: exports `KVWatcher` and `WatchOptions`, which libzb's live schema
watch needs (ZeBridge NOTES §10de); the types existed, only the re-export was missing.

**Verified**

```bash
cd libzb && zig build test                    # unit tests
libzb/python/migrate_reseed.py, migrate_rekey.py   # the seed-after-delete path, PASS
```

Upstream checked 2026-09-06 (`git fetch`, `d4cd40d` still the tip): none of entries 1–8
are upstream. A PR bundling 1, 3, and 8 would carry the least ZeBridge-specific
context; 6 and 7 are API additions and need a discussion first.
## 9. The routine connect logged twice, the consumer add logged like a decision (2026-09-07)

**How it appeared**

Every fleet-monitor tick — the monitor opens a fresh connection per pass, like the
generation producer, and lists the `live` bucket's keys — the bridge log gained:

```
info(nats): Connected successfully to nats://127.0.0.1:4222
info(nats): Connected successfully
info(nats): adding consumer
```

Reported from the web-consumer session: an operator saw the trio recur every minute,
next to a `JetStream error … 10058` that turned out to be the bridge's own
create-then-open of the bucket (fixed on the bridge side, NOTES §10dr), and asked
whether the bridge was reconnecting. It was not — it was connecting, on purpose, on
schedule — and entry 5's reasoning applies unchanged: a routine, caller-initiated event
should not read as one worth investigating.

**Cause**

`connect()` announces success twice at info: once with the URL in the per-server
connect path, once more in the wait loop that returns to the caller. `addConsumer`
logs "adding consumer" at info before every CONSUMER.CREATE, which a KV `keys()` issues
each time it is called.

**Change**

`src/connection.zig` — both "Connected successfully" lines drop to `debug`.
`src/jetstream.zig` — "adding consumer" drops to `debug`. Real events keep their
voice: a drop and the re-connect after it are reported by the reconnect machinery at
warn, and a JetStream API refusal stays at err (the caller gets the typed error too).

`nats.zig-quiet-routine-connect.patch`.

**Verified**

```bash
cd nats.zig && zig build test
# bridge at LOG_LEVEL=info: the trio gone from the fleet tick; a forced drop still logs the reconnect
```

---
## 10. Every JetStream API refusal logged as an incident (2026-09-08)

**How it appeared**

Two lines at error level on every libzb open and on every fleet-monitor tick:

```
error(nats): JetStream error: code=404 err_code=10014 description=consumer not found
error(nats): JetStream error: code=400 err_code=10058 description=stream name already in use with a different configuration
```

The first is `pullSubscribe`'s look-before-create of the client's consumer, the
second was the bridge's create-then-open of a bucket (fixed on the bridge side,
NOTES §10dr). Both callers handle the typed error and go on; the log said otherwise.

**Cause**

`jetstream.zig`'s API response handler logs `JetStream error: …` at error level for
every error response before mapping it to a typed error. The library cannot know
whether the caller treats the answer as a failure, so the level is wrong by
construction: a 404 on a probe is an answer.

**Change**

`src/jetstream.zig` — the line drops to `debug`. The typed error still reaches the
caller, which reports at the level it means (the bridge's `ensureStream` logs its own
"not reachable" at error, for instance).

`nats.zig-jetstream-error-level.patch`.

**Verified**

```bash
cd nats.zig && zig build test        # unit steps; the integration suite needs its services
# libzb open + fleet tick at LOG_LEVEL=info: both lines gone
```

---
## 11. `PullSubscription.nextDelivered` — what a parked pull still delivers (2026-09-08)

**How it appeared**

A clean stop of ZeBridge's mutation listener left one or two writes "in flight": the
next instance found them pending on the durable consumer and waited an ack wait for
their redelivery (ZeBridge NOTES §10eb). The batch in hand had been committed and
acked; what stayed open was the last pull request.

**Cause**

`fetch` returns as soon as it has something (entry 3's early return, nats.go's legacy
contract), and the server keeps the request parked until its `expires`. Messages
delivered into that window land on the wildcard inbox, which the NEXT `fetch` reads —
fine while fetches keep coming. At shutdown nobody reads it, so those messages are
delivered and unacked. Pulling "until empty" does not close the window either: under
a steady publisher every pull returns something and parks in turn.

**Change**

`src/jetstream.zig` — `PullSubscription.nextDelivered(timeout)`: a data message already
delivered to the consumer's inbox, without a new request; status frames skipped; null
at the deadline. A caller stopping reads the inbox until it has been silent for longer
than the parked request can live, then leaves with nothing in flight.

`nats.zig-pull-next-delivered.patch`.

**Verified**

```bash
# bridge stopped under a 15 writes/s publisher: "N write(s) the parked pull still
# delivered, judged before exit"; the next boot: nothing in flight.
```

---

## 12. Which TLS versions the ClientHello offers is now the caller's choice (2026-09-17)

**How it appeared**

A question, not a fault: "what would it take to move the bridge's NATS hop to TLS 1.3,
and would the shorter handshake help?" Measured before touching anything, against a
throwaway `nats-server` with a `tls{}` listener and the server's own `/connz`:

```json
{"name": "nats.zig", "lang": "zig", "tls_version": "1.3", "tls_cipher_suite": "TLS_AES_128_GCM_SHA256"}
```

It was TLS 1.3 already. `connection.zig` builds `tls.nonblock.Client.init(.{ ... })`
without naming `cipher_suites`, so tls.zig's default applies — `cipher_suites.all`:
the 1.3 suites first, then 1.2's recommended ones, then 1.2's CBC ones — and a Go
`nats-server` picks the highest both sides speak. The handshake saving is per
*connection*, and the bridge holds one for the life of the process; steady-state cost
is record encryption, which 1.3 does not change. No performance work to do.

What the library could not do was say *"1.3 only"*: `TlsOptions` had no field for the
suites, so the ClientHello always carried the 1.2 fallback, CBC suites included, and a
downgrade could always be offered. Hygiene, not speed — but real, and one field away.

**Change** (`nats.zig-tls13-ciphers.patch`, 4 hunks)

`src/connection.zig` — `pub const CipherSuites = enum { default, secure, tls13 }` and a
`cipher_suites: CipherSuites = .default` field on `TlsOptions`, mapped in the
handshake to `tls.config.cipher_suites.{all, secure, tls13}`. An enum rather than
tls.zig's own `[]const CipherSuite`: `TlsOptions` is public surface and is compiled
with `use_tls` off too, where `tls` is `tls_stub.zig` — a type from the dependency
would leak into every caller's build. `.default` maps to exactly the slice tls.zig
used when nothing was said, so nothing changes unless asked.

`src/tls_stub.zig` — the mirror of `config.CipherSuite`, `config.cipher_suites` and
the `Client.cipher_suites` field, nine lines, so the stub still type-checks.

ZeBridge does not set it yet: the colocated `nats-server` speaks 1.3, but pinning the
bridge's hop is a deployment decision (an old TLS-terminating proxy would refuse), not
a library one. When it is taken, it is `.tls = .{ .cipher_suites = .tls13 }`.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 132/132, 20 consecutive runs on a quiet machine
# probe client with .cipher_suites = .tls13 against a tls{} nats-server, read on /connz:
#   tls_version "1.3", tls_cipher_suite "TLS_AES_128_GCM_SHA256"
```

One caution recorded rather than hidden: over 30 runs with the patch and 30 without,
`tls: server key updates are handled and answered` (a 5 s timing test) failed once —
with the patch, on a run that shared the machine with the scenario battery. The
patch cannot reach that test (`.default` is the same slice), so it is filed as the
timing flake it looks like, and watched.

Not in this change, and worth knowing before anyone asks for resumption: tls.zig
supports TLS 1.3 session tickets (`Options.session_resumption`, reachable from the
nonblock path), nats.zig passes none — and in the probe the server issued no ticket,
so check `nats-server` before building on it. Go's TLS server implements no 0-RTT.

---

## 13. The inbox prefix is now the caller's choice (2026-09-19)

**How it appeared**

A read boundary that could not be drawn. JetStream does not deliver a pulled message, a
KV answer or an object chunk on the subject the reader filtered on — it delivers to the
reader's **inbox**, and the subject ACL is never consulted for it. Every NATS client
generates inboxes under one shared `_INBOX.`, so every principal on an account listens in
the same space. Measured again on the dev stack while writing this, with a plain client
credential subscribed to `_INBOX.>`:

```
_INBOX.8YR56W4T5TBBWKEN1IK4C4.147640x0      # another principal's pull deliveries
_INBOX.U8XIEDIS07ZC1BCLU2KT6V.147632x0      # and another's
_INBOX.M7BDIF0FBG61D08SDGXW1N.31298
```

The fix is one grant per principal, `_INBOX.<name>.>`. The library could not be a client
of it: `_INBOX.` was a constant in `inbox.zig` and again in `response_manager.zig`, where
it also sized an inline buffer at compile time.

Not an opinion, an omission: nats.go has `CustomInboxPrefix` (validated: non-empty, no
wildcard, no trailing dot), nats.js has `inboxPrefix` — its own documentation says
"useful for clients with limited subject permissions" — and nats-py has `inbox_prefix`.
nats.zig was the outlier.

**Change** (`nats.zig-inbox-prefix.patch`, 15 hunks, 6 files)

`src/inbox.zig` — `default_prefix` ("_INBOX", no dot), `max_prefix_len` (64),
`validatePrefix` with nats.go's rules plus the length bound, and
`newInboxWithPrefix(allocator, prefix)`. `newInbox(allocator)` keeps its signature and
its output.

`src/connection.zig` — `ConnectionOptions.inbox_prefix`, borrowed like `name` and `user`
rather than duplicated; validated once in `connect()`, so a bad prefix is an error at the
API boundary instead of a silent permissions violation on the first reply; and
`Connection.newInbox(allocator)`, the nats.go shape, which everything inside the library
now calls.

`src/response_manager.zig` — the reply prefix is built from the connection's prefix. The
inline buffer stays inline: its size comes from `max_prefix_len` instead of the literal,
so a custom prefix is bounded rather than allocated (the default uses 30 of those bytes).
The `bufPrint` that could not fail before now returns `error.InvalidInboxPrefix` rather
than `unreachable`, for a manager built by hand.

`src/jetstream.zig`, `src/jetstream_kv.zig`, `src/jetstream_objstore.zig` — the four
generated consumer delivery subjects, the two `PullInbox` bases, the KV watcher and the
two object-store reads go through `Connection.newInbox`. That is what makes the option
cover more than request/reply.

**Verified**

```bash
cd nats.zig && zig build test-unit         # 134/134 (2 new), 17 consecutive runs
git -C nats.zig apply -R … && apply …      # the patch reverses and re-applies cleanly
```

Live, against the dev stack, with the `requestor` example temporarily pointed at it (the
file was restored afterwards):

| prefix | reply subject on the wire |
|---|---|
| default | `_INBOX.O2EZJ87992JNROFX1DMEA2.0` |
| `_INBOX.zbprobe` | `_INBOX.zbprobe.36N9KD6RM95JQR47OKXJDB.0` |

Both replies arrived. ZeBridge adopted it the same day: every client sets
`_INBOX.<principal>` and the account's client and responder templates grant
`_INBOX.{{name()}}.>` instead of `_INBOX.>` (NOTES §10hm).

One run of the 134 failed immediately after the edit, before any of the 17 that followed,
and the failing test was not captured. Recorded rather than hidden; the suite has a known
timing test that flaked once under load in entry 12.

---

## 14. An object store can expire its objects (2026-09-20)

**How it appeared**

An answer too large for one NATS message goes to an object store and the asker reads it
(ZeBridge §10hq). Such a store holds TRANSIENT objects: read once, then rubbish. The
backing stream already has `max_age` — `ObjectStoreConfig` had no way to set it, so
`createStore` sent a stream with no age at all and the only way to bound the bucket was
`max_bytes` or a sweeper of one's own.

**Change** (`nats.zig-objstore-max-age.patch`, 2 hunks, 1 file)

`src/jetstream_objstore.zig` — `max_age_ns: u64 = 0` on `ObjectStoreConfig`, passed to
the stream config `createStore` already builds. Zero keeps an object for ever, which is
what every store created before this got, so nothing changes unless asked. nats.go
spells the same thing `TTL` on its ObjectStoreConfig.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 134/134
git -C nats.zig apply -R … && apply …       # the patch reverses and re-applies cleanly
```

Live: ZeBridge's responders create `res-<tenant>` with 600 s and the server reports it —
`OBJ_res-_default max_age 600 s` in `scripts/scenarios/serve.py`, alongside a 1.24 MB
answer that travelled as an object and arrived whole in both client libraries.

---

## 15. The JetStream domain is now the caller's choice (2026-09-23)

**How it appeared**

A client on a NATS leaf node reaches the hub's JetStream only under `$JS.<domain>.API.`
(ZeBridge §10hl: every `$JS.API` request from a leaf gets NoResponders). `JetStream` built
every API subject from a `const default_api_prefix = "$JS.API."` at three sites —
`sendRequest`, and the pull consumer's `CONSUMER.MSG.NEXT` in `nextSubject` and `fetch` —
so a caller had no way to say which JetStream it meant. nats.go spells this `nats.Domain`,
nats.js `{ domain }` on `jetstream()`/`jetstreamManager()`.

**Change** (`nats.zig-jetstream-domain.patch`, 5 hunks, 1 file)

`src/jetstream.zig` — `domain: ?[]const u8 = null` on `JetStreamOptions`; a public
`JetStream.apiSubject(a, tail_fmt, args)` that renders `$JS.API.<tail>` or
`$JS.<domain>.API.<tail>`; the three sites call it. KV and object stores need nothing:
their `$KV.` and `$O.` subjects are data, not API, and their API calls already go through
`sendRequest`. Null keeps every existing caller on the plain prefix. One unit test.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 135/135 (+1)
git -C nats.zig apply -R --check …          # reverses cleanly
```

Live, ZeBridge §10is: against a server with `jetstream { domain: hub }` and `-DV`, a
`JetStream` with `.domain = "hub"` published `PUB $JS.hub.API.STREAM.INFO.KV_tenants` and
got the hub's answer; with `.domain = "other"` the server returned `503` with
`Nats-Subject: $JS.other.API.STREAM.INFO.KV_tenants`.

---

## 16. The two fixes from before the ledger (2026-08-27, ledgered 2026-09-23)

Found by the regeneration in §17: two changes in the working tree that no patch carried
and no entry here described. Both are in the submodule's old in-tree `NOTES.md` (untracked,
from before this ledger moved to `nats.zig-patch/`), and both say "LOCAL FIX (see
NOTES.md)" at the code. Ledgered here from that file; the patches are the series' first two.

**`01-nats.zig-direct-get-subject-form.patch`** — `getMsgDirect` published every
request to the bucket-level API (`$JS.API.DIRECT.GET.<stream>`) with a JSON body. For a
`last_by_subj`-only lookup it uses the ADR-31 subject form — `DIRECT.GET.<stream>.<subject>`
with an empty body — and keeps the JSON form for `seq` and `next_by_subj`. The two forms
are identical for last-by-subject, but only the subject form works under grants that scope
direct gets PER KEY, which is what makes ZeBridge's tenant-scoped KV and object grants
possible at all. Measured 2026-08-27 against a live JWT-mode server: `KV.get` and the
object store's meta lookups failed with a Permissions Violation under client credentials
before, pass after.

**`02-nats.zig-consumer-create-named.patch`** — a consumer with a name went through the
legacy `CONSUMER.DURABLE.CREATE.<stream>.<name>`; it now uses the modern
`CONSUMER.CREATE.<stream>.<name>[.<filter>]` (server ≥ 2.9) whenever `name` or
`durable_name` is set. Least-privilege grants cover the modern form and typically not the
legacy one, and under JWT auth an unauthorised API publish is dropped, so the failure was a
bare `Timeout`. Measured the same day: durable pull consumers timed out under client
credentials before, create cleanly after.

---

## 17. The patches regenerated as an ordered series (2026-09-23)

**How it appeared**

Asked whether the patches were up to date, the honest check was run for the first time:
does clean upstream `d4cd40d` plus the patch files rebuild the working tree? It does not,
in any order. 7 of 15 never applied to clean upstream; unwinding from the working tree
freed 10 and left 5 interlocked (`inbox-prefix`, `shared-pull-inbox`, `connection`,
`quiet-deliberate-close`, `stale-408`); 14 pairs of patches carried each other's lines
(cut from a full `git diff` with hunks filtered by hand, as the workflow note warned);
the `tests/jetstream_pull_test.zig` change (+163, the fetch-contract tests of entry 7)
was in no patch; two fixes were in no patch and no entry (§16). `apply -R --check` had
passed for 13 of 15 all along — it passes for a patch whose lines a later patch also
carries, so it never proved anything.

**Change** (every patch file replaced; the working tree untouched)

The 83 zero-context hunks of the full working-tree diff were attributed to a topic by
their changed lines (twelve read by hand; two hunks staged, because a later topic rewrote
a line an earlier one added: `nextSubject` — `shared-pull-inbox`, then `jetstream-domain`
— and `inbox_base` — `shared-pull-inbox`, then `inbox-prefix`); seventeen states were
rebuilt from upstream by cumulative hunk subsets; each patch is the exact diff between two
consecutive states. Chronological order:

## 18. `fetch` waits for the batch when the caller says so (2026-09-24)

**How it appeared**

`fetch` returns 1 ms after its first message (§13's contract: one CDC event, at once).
An object reader pulls `batch` chunks that are contiguous and always all coming, and
over any real network they do not land within a millisecond of each other. ZeBridge's
libzb on an iPhone over Wi-Fi: `fetch(8)` returned with one chunk, the reader asked for
eight more while seven were still in flight, and so on — nats-server's `connz` showed
59 requests in a few seconds, 513 chunks delivered, 42 MB pending on the phone's
connection, then `Slow Consumer (Pending Bytes)` and the connection cut, three times
in a row, on a 100 MB object it never got a third of. Loopback never showed it: there
the eight chunks arrive inside the idle window every time.

**Change** (`18-nats.zig-fetch-idle-after-first.patch`, 2 hunks, 1 file)

`src/jetstream.zig` — `idle_after_first: Io.Timeout = fetch_idle_after_first` on
`PullSubscription`, and `fetch` reads it instead of the constant. Every existing caller
keeps the 1 ms; an object reader sets it to its fetch timeout and a fetch returns with
the batch, or the deadline. libzb's `ObjectPull` does (ZeBridge §10iy).

**Verified**

```bash
nats.zig-patch/new-patch.sh fetch-idle-after-first
# wrote 18-nats.zig-fetch-idle-after-first.patch (2 hunks, 1 files)
# ✅ 18 patches: upstream d4cd40d + series == working tree (src, tests)
```

Live: the iPhone seed that was cut three times ran through — ZeBridge §10iy has the
number.

| series | ledger | topic |
| --- | --- | --- |
| 01 | §16 | direct-get-subject-form (2026-08-27) |
| 02 | §16 | consumer-create-named (2026-08-27) |
| 03 | §1 | jetstream-stale-408 |
| 04 | §2 | connection (UNSUB on a closed connection) |
| 05 | §7 | fetch-early-return-503 — now carries its tests |
| 06 | §7 | consumer-inactive-threshold |
| 07 | §3, §7 | shared-pull-inbox |
| 08 | §5 | quiet-deliberate-close |
| 09 | §6 | auth-verdict |
| 10 | §8 | objstore-double-release |
| 11 | §9 | quiet-routine-connect |
| 12 | §10 | jetstream-error-level |
| 13 | §11 | pull-next-delivered |
| 14 | §12 | tls13-ciphers |
| 15 | §13 | inbox-prefix |
| 16 | §14 | objstore-max-age |
| 17 | §15 | jetstream-domain |
| 18 | §18 | fetch-idle-after-first |
| 19 | §19 | creds-content |

Hunk and file counts quoted in older entries describe the old files; the series' own
counts are in each file. `check-series.sh` and `new-patch.sh` are the workflow from here.

**Verified**

```bash
nats.zig-patch/check-series.sh
# ✅ 18 patches: upstream d4cd40d + series == working tree (src, tests)
nats.zig-patch/new-patch.sh probe
# nothing to cut: the working tree equals upstream + the series
```

Every intermediate state compiles and passes its unit tests — `zig build test-unit` after
each patch in turn: 132/132 through 14, 134/134 at 15 (inbox-prefix adds two), 135/135 at
17 (domain adds one). The first cut did not: states 07–14 failed on `self.nc.newInbox`,
which `inbox-prefix` introduces at 15 — the old `shared-pull-inbox.patch` had been re-cut
after inbox-prefix and carried the newer line; staging `inbox_base` fixed it, and that is
the kind of anachronism only a per-state build catches. One unit test is flaky: it failed
once in ~4–6 runs, at a different state each time and on the unchanged final tree too, so
it is not the series; six further runs passed and it never printed its name. Recorded, not
hidden.

## 19. Credentials from memory, not only from a file (2026-09-25)

**How it appeared**

ZeBridge gives both of its clients one option vocabulary (§10ja), and zb-client-ts
takes the credentials as text: a browser or a phone app holds what `/enroll` returned
and has no file to point at. libzb could only pass `user_creds`, a path, so the Flutter
and React Native apps wrote the creds to disk first just to have one. nats.zig parses a
.creds file's content already (`creds.parse`); only the option to hand it over was
missing.

**Change** (`19-nats.zig-creds-content.patch`, 4 hunks, 2 files)

`src/connection.zig` — `user_creds_content: ?[]const u8`: the content of a .creds file,
owned by the caller and kept alive for the connection (every handshake reads it). Used
when `user_creds` is not set, in both places `user_creds` is read: the up-front check
(a malformed content fails before dialing, like an unreadable file) and the handshake.

`tests/auth_test.zig` — "jwt credentials content authentication" (the fixture's bytes,
against the JWT server) and "malformed credentials content fails before connecting".

**Verified**

```bash
nats.zig-patch/new-patch.sh creds-content
# wrote 19-nats.zig-creds-content.patch (4 hunks, 2 files)
# ✅ 19 patches: upstream d4cd40d + series == working tree (src, tests)
```

`zig build test-unit`: 135/135 on the first run; on a second run one TLS reconnect test
timed out (a handshake timeout under load, nothing of this patch). The e2e suite did not
run: on OrbStack its `waitForHealthyServices` timed out at 10 s with all ten containers
reporting healthy, and `beforeAll`'s `docker compose up -d` started nothing until run by
hand — an environment problem, recorded, not fixed here. The two new tests are
therefore unrun; the live proof is ZeBridge's: libzb connecting to the dev stack with
`"creds": <bob.creds text>` synced as bob on tenant globex, and garbage content was
refused before any socket (`MissingUserJwt`).


## 20. A pull fetch asks only for what the inbox does not already hold (2026-09-26)

**How it appeared**

libzb's live tail, following a table written at 75k–100k events/s from an already-live
replica (ZeBridge §10jb), reported `SlowConsumer` from `PullInbox.fetch` in two runs of
four, each time followed by a gap and a re-seed. The inbox depth, traced before every
pull, explained it: 231–234 messages (~45 MB of 256 KB CDC messages) queued before 154
of 163 fetches. `fetch` returns once messages stop for `idle_after_first` (patch 18); the
server keeps sending the rest of that request into the inbox; the next fetch requested a
full batch on top of it. Steady state: more than two batches queued, a hair under the
subscription's 64 MB pending limit — and past that limit the connection DROPS the
message, which a JetStream reader can only see as a hole.

**Change** (`20-nats.zig-pull-deficit.patch`, 4 hunks, 1 file)

`src/jetstream.zig` — `PullSubscription.fetch` and `PullInbox.fetch` read the inbox
subscription's `pending_msgs` first and request `batch − queued` (the shared inbox splits
`queued` evenly over its consumers); when the queue alone fills the batch no request is
sent and the fetch returns from the queue. The collection loop's bound is the caller's
`batch`, which the queue's share and the request's share make up together. Status frames
count as queued too — harmless: the idle window returns the fetch as before.

**Verified**

```bash
nats.zig-patch/new-patch.sh pull-deficit
# wrote 20-nats.zig-pull-deficit.patch (4 hunks, 1 files)
# ✅ 20 patches: upstream d4cd40d + series == working tree (src, tests)
```

`zig build test-unit`: 135/135. The live proof is ZeBridge's firehose at 37.5k and 50k
rows/s with both clients live (§10jb): the inbox depth before a pull, the poll errors and
the re-seed count, before and after.

## 21. A pull request can cap its bytes (2026-09-26)

**How it appeared**

libzb on an iPhone over Wi-Fi: nats-server disconnected it 66 times as a "Slow Consumer
(Pending Bytes)". A pull request of 100 CDC messages is 20–25 MB; the server queues a
request's messages for the connection at once, and requests overlap (a fetch returns after
its first message, patch 18; the next poll asks again, patch 20 counting only what already
ARRIVED). Over a link of a few MB/s the queue passed the server's per-connection
`max_pending` (64 MB) and the server closed the connection — every delivery in flight lost,
re-sent only after `ack_wait`, out of order.

**Change** (`21-nats.zig-pull-max-bytes.patch`, 4 hunks, 1 file)

`src/jetstream.zig` — `PullSubscription.max_bytes` and `PullInbox.max_bytes` (default null:
unchanged), sent as the request's `max_bytes`. The server then stops a request at that many
bytes (409 "Message Size Exceeds MaxBytes" ends it, handled as before).

**Verified**: `zig build test-unit` 135/135; libzb sets 8 MB on its tail inbox and its drain
consumers (ZeBridge §10jc).

## 22. One outstanding pull request per consumer (2026-09-26)

**How it appeared**

Patch 21's byte cap bounded one request, not how many were open. `fetch` returns after its
first message (patch 18) and the next fetch asked again (patch 20 counts only what already
ARRIVED), so an iPhone polling once a second held a dozen 8 MB requests at once over Wi-Fi:
still 44 disconnects as a slow consumer after patch 21.

**Change** (`22-nats.zig-pull-one-request.patch`, src/jetstream.zig)

`PullSubscription` keeps its one outstanding request (reply subject, messages still owed,
deadline). `PullSubscription.fetch` and `PullInbox.fetch` send a new request only when the
last has delivered everything, ended with a status (404/408/409/503), or expired; data
messages are counted against the consumer their ack metadata names. Patch 20's deficit
arithmetic is gone with it (the inbox now only ever holds the current request's rest).

**Verified**: `zig build test-unit` 135/135; ZeBridge's delivery-loss test exact on the Mac
(138 simulated losses); the iPhone at 40k events/s with four SIGKILLs: 0 disconnects (the
server's slow-consumer count unchanged), 153 of 153 batches exact.

## 23. Up to two outstanding pull requests (2026-09-26)

**How it appeared**

With one request at a time (patch 22) a phone's catch-up alternated transfer and apply —
~1.5 s each per 8 MB on an iPhone 12 over Wi-Fi: a 1.3M-row first seed took 91.8 s.

**Change** (`23-nats.zig-pull-depth.patch`, src/jetstream.zig)

`PullSubscription.pull_depth` (default 1: unchanged; at most 2). The one tracked request
becomes two slots; a fetch issues requests while fewer than `pull_depth` are open; a data
message is owed by the OLDEST open request (the server serves a consumer's requests in
order); a status frame ends the request whose reply subject it names. `PullInbox` reply
subjects become `<prefix><fetch>x<i>y<k>`. Bytes in flight: at most `pull_depth × max_bytes`.

**Verified**: `zig build test-unit` 135/135 (three runs; one earlier run had the known flaky
TLS test fail); ZeBridge's delivery-loss test exact; the iPhone's 1.3M-row first seed
53.7 s (was 91.8 s), exact. One slow-consumer disconnect remained during that seed's
drain — being measured.

## 24. A pull request stays open while its messages arrive (2026-09-26)

**How it appeared**

After patch 23 one slow-consumer disconnect remained, during a phone's drain. A request's
slot freed at its `expires` (0.9 s for libzb's drain), but the server queues a request's
messages at once and 8 MB over ~5 MB/s Wi-Fi takes 1.6 s: the slot freed with data still on
the wire, the next fetch asked again, and the bytes in flight passed `pull_depth × max_bytes`.

**Change** (`24-nats.zig-pull-grace.patch`, src/jetstream.zig)

Each delivered message extends its request's deadline to at least now + `request_grace`
(2 s). A request frees when fully delivered, ended by a status (408 at expiry, 409 at the
byte cap), or silent for the grace after its expiry.

**Verified**: `zig build test-unit` 135/135; ZeBridge's delivery-loss test exact; the iPhone at
40k events/s with four SIGKILLs: 0 slow-consumer disconnects, 0 connection resets, 153/153
batches exact, converged 170 s after the load (285 s before patches 21-24).

## 25. It compiles for 32-bit ARM (2026-09-26)

**How it appeared**

A moto e20 (Android 11 Go, 1.8 GB) runs a 32-bit userspace (`abilist`: armeabi-v7a,
armeabi): Android loads the armeabi-v7a Flutter engine and needs a 32-bit libzb, which never
compiled — `std.atomic.Value(u64)` is refused on 32-bit ARM by Zig 0.16 whatever the CPU
("expected 32-bit integer type or smaller"), and three u64/usize mixes.

**Change** (`25-nats.zig-arm32.patch`)

`src/atomic_u64.zig` — `AtomicU64`: `std.atomic.Value(u64)` where usize is 64-bit, else a
spin-locked u64 with the four operations used (load, store, fetchAdd, fetchSub); the six
64-bit counters of `connection.zig` and `subscription.zig` use it. `nuid.zig`,
`jetstream_kv.zig`, `jetstream_objstore.zig`: explicit casts (an object larger than a 32-bit
address space fails as OutOfMemory). A test for the locked form.

**Verified**: `zig build test-unit` 136/136; libzb builds for `arm-linux-androideabi` with
`-Dcpu=cortex_a7` (libzb itself needed no change); the Flutter app starts on the moto e20.

## 26. A "Message Size Exceeds MaxBytes" end is routine (2026-09-27)

**How it appeared**

ZeBridge's delivery-loss test (a libzb follower, every 41st delivery discarded, killed four
times under 20k events/s) stopped converging: the follower exited with
`error: ConsumerSequenceMismatch` a few batches after recreating its consumer. A diagnostic
printed every 409's Description: all 19 were "Message Size Exceeds MaxBytes". Since patch 21
a request carries `max_bytes` (libzb: 2 MB) and CDC batch messages are up to 256 KB, so the
server ends a request with that 409 every few messages — the routine end of a request, not a
consumer problem. `fetch` mapped every 409 to `ConsumerSequenceMismatch` and returned it
whenever it arrived before any message of the current fetch (the previous fetch took the
request's messages; its 409 opened the next one).

**Change** (`26-nats.zig-max-bytes-end.patch`)

`src/jetstream.zig`: the request-sending loop moves into `sendRequests` so `fetch` can send
one mid-loop. A 409 whose Description is "Message Size Exceeds MaxBytes" completes the batch
when the fetch holds messages; with none, a replacement request goes out and the fetch keeps
waiting — at most `max_bytes_empty_ends` (3) times in a row, because a message larger than
`max_bytes` itself ends every request that way and re-asking would loop until the deadline.
Every other 409 (Consumer Deleted, Exceeded MaxWaiting, …) is still an error.

A first version told the cases apart per request (did the ended request deliver anything?)
using the oldest-first attribution of patch 22: the diagnostic showed requests ending
max-bytes with "nothing delivered" by that count, and the follower still exited. Which
request a message answers is the server's business; the bounded count needs no guess.

**Verified**: `zig build test-unit` passes; ZeBridge's delivery-loss test PASS — 62/62
batches exact (1,282,000 rows), 104 deliveries lost and recovered, four kills, no
`ConsumerSequenceMismatch` (before: the follower exited twice per run and the run never
converged).

## 27. A KV bucket can be bound without STREAM.INFO (2026-09-28)

**Problem.** `KVManager.openBucket` (and `JetStream.kvBucket`) call `getStreamInfo` to check
the bucket exists before returning it. A credential granted only exact keys of a bucket
(ZeBridge's clients: `$KV.tenants.<self>`, `$KV.generations.<tenant>.*`, by direct get) must
then also hold `$JS.API.STREAM.INFO.KV_<bucket>` — and STREAM.INFO takes a
`subjects_filter` in its request BODY, which no subject grant can scope: the holder lists
every key of the bucket (measured in ZeBridge: 164 principal names, every tenant's
manifest keys — NOTES §10jr).

**Change** (`27-nats.zig-kv-bind.patch`)

`src/jetstream_kv.zig`: `KVManager.bindBucket` returns `KV.init` without the lookup — what
nats.js's `Kvm.open` does (bindOnly). `src/jetstream.zig`: `JetStream.kvBind`, the
`kvBucket` counterpart. A missing bucket shows at the first read instead of at open.

**Verified**: `zig build test` in libzb; live, with STREAM.INFO removed from the client
templates: libzb seeds a fresh replica from the chain, reads its tenants key, and the
bucket it can no longer describe answers its exact keys.

## 28. An nkey signature can be verified (2026-09-28)

**Problem.** `nkeys.zig` could sign (`SeedKeyPair.sign`) but not verify: decoding a public
key text needs the private `base32Decode` and `verifyChecksum`. ZeBridge's `/renew` must
check that a request was signed by the key a device enrolled with.

**Change** (`28-nats.zig-nkeys-verify.patch`): `pub fn verify(expected: KeyType,
public_text, msg, signature: [64]u8) Error!bool` — decode, checksum, prefix, Ed25519 verify;
false for a bad signature, an error for a key that does not decode. Tested: a seed's
signature verifies against its key, not against another key, not over another message,
and an account-type expectation refuses a user key.

## 29. In-memory creds can be replaced for the next handshake (2026-09-28)

**Problem.** `user_creds_content` is set once in the options, and every handshake parses
it. A renewed JWT (same seed) must reach the NEXT reconnect without rebuilding the
connection, and the reconnect may run while the host swaps the bytes.

**Change** (`29-nats.zig-creds-swap.patch`): `Connection.setCredsContent(content)` replaces
the slice under the connection mutex, which the handshake holds — a reconnect sees the old
bytes or the new ones. The live socket is untouched; the server ends it at the old JWT's
expiry ("User Authentication Expired") and the reconnect presents the new one.

**Verified** (with 28): libzb polled for 100 s across 40 s JWTs — two renewals, two
server cut-offs each followed by a reconnect with the new JWT, no poll error, and a row
written after the first expiry arrived.

## 30. A stream can refuse new messages per subject (2026-09-30)

**How it appeared.** The bridge now creates MUTATIONS itself when it is missing (ZeBridge
NOTES §10kk). That stream caps each principal's queued writes with `max_msgs_per_subject`
and `discard new` per subject, and `StreamConfig` had no field for the per-subject part:
without it a full principal would refuse writes for the whole stream.

**Change** (`30-nats.zig-discard-new-per-subject.patch`): `StreamConfig` gains
`discard_new_per_subject: ?bool = null`, the server's `discard_new_per_subject`. Null is
omitted from the JSON, so every existing caller sends what it sent before.

**Verified:** a bridge booted against an empty nats-server 2.15 created MUTATIONS with
`discard: new`, `max_msgs_per_subject: 777` (from `MUTATION_BACKLOG_PER_PRINCIPAL`) and
`discard_new_per_subject: true`, read back with `nats stream info -j`; `check-series.sh`
green over 30 patches.

## 31. A subscription can wake another one's reader (2026-10-02)

**How it appeared.** A libzb service (ZeBridge `examples/10-airports`) answers questions
on `serve` subscriptions while its `poll` waits on the tail inbox for CDC. A question that
arrived during that wait sat in its own subscription until the wait ran out: the answer
took one poll interval (50 ms polls → ~52 ms round trip; 5 ms polls → ~9 ms, at 1–2% of a
core idle). Nothing in the library let one thread wait on one subscription and be told
another had something.

**Change** (`31-nats.zig-wake-reader.patch`):
- `ConcurrentQueue.wake()`: ends the current (or the next) wait for data as a timeout
  would, without queuing anything. A pending wake is consumed by any wait; data already
  queued is still returned.
- `Subscription.wake_on_message: ?*Subscription`: when a message is queued here, the
  dispatch calls `peer.messages.wake()`. The peer must outlive the subscription; the
  caller sets it before messages flow.
- `PullInbox.fetch` needs nothing: a woken wait reads as `Timeout`, which ends its receive
  loop with what has arrived; open pull requests stay registered for the next fetch.

**Verified:** `queue.test.wake…` (a wake given before the wait, one from another thread
during a 30 s wait — returned in ~24 ms, and queued data not hidden); nats.zig 138/138.
libzb wires every serve subscription to its tail inbox (which lives as long as the
client). The airport service on the default 250 ms poll: round trip 6.4 ms (was 9.3 ms at
5 ms polls), 0.1% CPU idle (was 1–2%), CDC still applied. `check-series.sh` green over 31.

## 32. TCP_NODELAY on every connection (2026-10-04)

**How it appeared.** A libzb client connected to a NATS leaf (`leaf1.zebridge.eu`, in the
`hub` domain) asked a service on the hub: median 205–289 ms a request, in every run, while
the same client straight to the hub took ~31 ms, and NATS's JavaScript client through the
same leaf ~33 ms (over IPv4 and IPv6 alike). Every slow run had connected to the leaf over
IPv6. Inside libzb, `conn.request` itself took the time (363 ms against 31 ms), not the
host's queue or the poll. nats.zig never set `TCP_NODELAY`: Nagle's algorithm held a small
write until the previous segment was acknowledged, and a delayed ACK on the far side turned
that into ~200 ms per request. The Go and JS clients set NODELAY by default; whether Nagle
bites depends on timing, which is why it showed on this path and not on others.

**Change** (`32-nats.zig-tcp-nodelay.patch`): `net_util.setNoDelay(socket, enabled)`,
beside `setKeepAlive`, through the raw handle (`IPPROTO.TCP`, `TCP.NODELAY`; no-op on
Windows); `establishConnection` calls it after `setKeepAlive`, so every connection and
every reconnect has it. The IP-literal socket test calls it too.

**Verified:** nats.zig unit tests 138/138; the integration step fails with and without
the patch (`autounsubscribe async basic functionality`: a subscription still live at
`Connection.deinit()` — pre-existing, unrelated). libzb rebuilt, the same client through
the leaf over IPv6, 12 runs of 10 requests: medians 32–55 ms in 11 runs, one at 213 ms
(was 205–289 ms in all 6 runs before). `check-series.sh` green over 32.

## 33. A pull open when its consumer is deleted reports it gone (2026-10-04)

**How it appeared.** `test-e2e` failed one test on every run, 184/185: "JetStream shared
pull inbox: one wait over two streams, partial failure per consumer". After consumer A was
deleted, the next `PullInbox.fetch` waited its whole 300 ms and reported
`gone=[false, false]`. Since patches 22–24 a fetch keeps a consumer's pull request open
across calls and sends a new one only for the deficit: A's request from the previous fetch
was still open when A was deleted, and the server answered it `409 Consumer Deleted` —
not the 503 a NEW request to a missing consumer gets, which is the only status `fetch`
read as gone. The test was written before 22–24 and had not been run since (it needs the
Docker cluster). Effect beyond the test: libzb reopens a tail whose consumer went away
(inactive threshold, a stream recreated) when `fetch` says gone — one poll late, after a
new request finally drew the 503.

**Change** (`33-nats.zig-consumer-deleted-409.patch`): `consumerGone(msg)` — 503, or 409
whose Description starts with "Consumer Deleted" (the Go client's `ErrConsumerDeleted`) —
used by `PullInbox.fetch` (`gone[i]`, read before the frame is freed) and by
`PullSubscription.fetch` (`error.NoResponders`, as for a 503).

**Verified:** the server's frame is `NATS/1.0 409 Consumer Deleted` (seen in the e2e
output); `test-e2e` 185/185 twice, the test now reporting `gone=[true, false]`; unit tests
green; libzb 38/40 (2 skipped) and the bridge 342/342 rebuilt on it. `check-series.sh`
green over 33.

**Also seen, not fixed:** once today, `autounsubscribe async basic functionality`
panicked in `Connection.deinit()` with one subscription still live; not in the four e2e
runs after it. Intermittent, timing-dependent; to look at if it returns.

## 34. Host names are dialled Happy Eyeballs style (2026-10-04)

**How it appeared.** An audit after §32 (what Go's runtime does that a port must do by
hand). `tcpConnectToHost` dialled a host name's addresses one at a time, in resolver order
(IPv6 first), with no limit per address — Zig 0.16 cannot bound one connect
(`ConnectOptions.timeout` panics "TODO implement netConnectIpPosix with timeout"). On a
network that silently drops IPv6, the IPv6 attempt waits for the OS's TCP timeout before
IPv4 is tried, while the connection's handshake limit (`Options.timeout`, 5 s) has long
expired, and every reconnect starts with IPv6 again: such a client never connects to a
dual-stack name. Go's dialer races the families (300 ms); nats.c dials one at a time but
gives each address its share of the timeout (`timeoutPerIP = totalTimeout / numIPs`) and
lets the order be chosen (`46`, `64`).

Reproduced on a real host: a port on the leaf VPS (`leaf1.zebridge.eu:4299`) allowed by its
firewall for IPv4 only, so IPv6 to it is dropped silently. `tcpConnectToHost` connected
over IPv4 after **75,024 ms** (macOS's TCP timeout).

**Change** (`34-nats.zig-happy-eyeballs.patch`), RFC 8305 simplified: `resolveAll` (every
address, at most 16), `interleaveFamilies` (alternating, the resolver's first family
first, order kept within each), then `race`: an `Io.Select` dials the first address and
starts a 250 ms delay; the delay or a failure starts the next address beside the ones
still trying; the first success wins, the rest are cancelled and a late success closed.
IP literals still connect directly. Tests: `interleaveFamilies`; "localhost" (::1 and
127.0.0.1) connecting to an IPv4-only listener.

**Verified:** the same port, three runs: connected over IPv4 after 285–288 ms (the 250 ms
head start plus a round trip) — cancelling the hanging IPv6 dial returns at once; the
leaf's open port over IPv6 in 24–31 ms as before; the hub (IPv4 only) 48 ms. nats.zig unit
140/140, e2e 185/185; libzb 38/40 (2 skipped), the bridge 342/342; a libzb client through
the leaf 36 ms median, straight to the hub 33–41 ms. `check-series.sh` green over 34.

## 35. The series rebased onto upstream `b8ffdd9` (2026-10-07)

**How it appeared.** Before porting to Zig 0.17, a comparison of the 34 patches with
upstream. Upstream had rewritten its history: `d4cd40d` is no longer an ancestor of
`main`, and survives on GitHub only on the branch `fix/subscription-handler-self-join`.
Its tree is identical to upstream's `6fadc18` (#157). Since then upstream has 11
commits, in `jetstream.zig`, `jetstream_kv.zig`, `subscription.zig` and the tests:
#181 (join a subscription's handler before releasing it), #178 (consumer create races,
unset config fields left to the server), #183 `d9b7074` (a pull subscription reads its
stream and consumer names from its consumer info), #184 `913455a` (ignore a status
addressed to an earlier fetch), #185 `e59cdde` (KV entries removed by the server are
deletes), #186 (nats-server 2.15 in CI).

**Change.** The series re-cut on `b8ffdd9`, one commit per patch, with the same file names:

- **03 (stale-408) is dropped.** #183 and #184 fix both of its halves, the freed stream
  name and the late 408, with tests.
- **05** keeps its contract, so upstream's new test "ignores a status left over from an
  earlier request" expects `error.Timeout` from its empty first fetch instead of an empty
  batch. The two test sets sit side by side.
- **07** creates the consumer inside its shared-inbox branch, because #178 moved
  `getOrCreateConsumer` after the inbox subscribe. It reads the names from
  `consumer_info.value`, because #183 removed `stream_name`, `owns_stream_name` and
  `consumer_name`. On error it calls `inbox_subscription.deinit()`, #181's idiom.
- **17, 20, 22** build the `CONSUMER.MSG.NEXT` subject from `consumer_info.value.*`. 22
  matches a delivered message to its consumer by `consumer_info.value.name`.
- **23** keeps our `hasReply` over #184's single `reply_subject`: several requests stay
  open across fetches, and a status for any of them is ours.
- **Unchanged in content:** 04, 08, 09, 10, 14, 16, 19, 28, 29, 32, 34 are
  byte-identical. The other 16 only moved: same lines, new line numbers and context.

ZeBridge's libzb read `PullSubscription.consumer_name`; it reads
`consumer_info.value.name` now.

**Verified** (Zig 0.16.0): `check-series.sh` green over 33 on `b8ffdd9`. nats.zig unit
141/141; e2e 191/191 against nats-server 2.15 (the suite's compose cluster through
OrbStack). The bridge 352/352; libzb 39/41 (2 skipped, live). A second port, made
separately, gave the same `src/`, apart from two comments and the error path of 07, which
there still called `nc.unsubscribe`.


## 36. Patch 35: tls.zig pinned to a commit that builds on 0.16 and 0.17 (2026-10-09)

**How it appeared.** The Zig 0.17 evaluation (2026-10-08, a scratch copy): the pinned tls.zig
(`ef6780e`) does not build on 0.17. lalinsky/tls.zig `60d0ced` ("Build with both Zig 0.16 and
0.17", 2026-10-04) does.

**Change** (`35-nats.zig-tls-zig-017.patch`): `build.zig.zon` only — the `tls` dependency's URL
and hash, still lazy. The first patch of the series outside `src/` and `tests/`: cut from
`git -C nats.zig diff build.zig.zon` (new-patch.sh cuts those two directories only), and
`check-series.sh` applies it with the rest but compares `src/` and `tests/` only.

**Verified on 0.16.0:** nats.zig test-unit green, the bridge 354/354, libzb 39/41 (2 skipped);
the same pin builds on 0.17.0 and on master 0.18.0-dev.120. Not verified live: a TLS
connection through it (the dev stack was stopped) — the next start of the dev bridge, which
speaks tls:// to nats-server, is that check.

## 37. A shared pull inbox can park its requests on the server (2026-10-19)

**How it appeared.** A count of the JetStream API requests one ZeBridge device sends to the
hub (every `$JS.API.>` subject for 40 s, against a baseline without the device): idle, it
sent ~6.5 `CONSUMER.MSG.NEXT` a second on each of its two streams — about 13 a second per
connected device, 6,500 a second for 500 devices doing nothing. `PullInbox.fetch` sent each
request with `expires` = the fetch's own timeout, and libzb's host polls every 250 ms: each
request lived one poll, and every poll asked again (two per stream, `pull_depth` 2). The
slots of patches 22–24 already stop a fetch from asking while a request is open; only the
request's lifetime was tied to the caller's wait.

**Change** (`36-nats.zig-long-pull-requests.patch`):
- `jetstream.zig`: `PullInbox.request_expires_ms` — when set, a request asks the server to
  keep it that long and its slot's deadline follows; each `fetch` still waits only its own
  timeout locally and reads what the parked request delivered. Null: as before.
- `jetstream.zig`: `PullRequest.epoch`, set at `requestStart`; `slotOpen` drops a request
  sent before the last reconnect. A server that crashed, or a client whose network vanished,
  never answers the parked request: without this the slot stays "open" until its deadline
  and the consumer has no request — live changes would wait up to `request_expires_ms`.
- `connection.zig`: `Connection.reconnect_epoch`, bumped when a reconnect completes (after
  subscriptions are restored, before the reconnected callback).
- `tests/jetstream_pull_test.zig`: a 10 s expiry, ten 100 ms fetches on an idle stream —
  `num_waiting` is 1, and a message published after is delivered through it.
- `tests/reconnection_test.zig`: a 30 s request parked, the three-node cluster KILLED
  (SIGKILL: no status frame to the request) and started; after the reconnect, ONE 200 ms
  fetch must park a new request (`num_waiting` ≥ 1) and the stored message arrives.

**Verified:** `test-e2e` 193/193. Negative control: with the epoch check disabled the
reconnect test fails — 0 requests waiting after the crash (with a `restart` instead of a
kill it passed either way: a clean shutdown sends the parked request a status frame, which
frees the slot by itself — hence the kill). libzb sets `request_expires_ms = 25_000` on its
tail inbox (its `tail_watch_ms`); the same capture on the hub: `CONSUMER.MSG.NEXT` from one
idle device over 40 s went from 162 + 176 to 2 + 2. libzb 43/45 (2 skipped).
`check-series.sh` green over 36.

## 38. A consumer is created before it is looked up (2026-10-19)

**How it appeared.** The same request count as §37: every `pullSubscribe` sent
`CONSUMER.INFO` then `CONSUMER.CREATE`. `getOrCreateConsumer` looked the consumer up first
— the pattern Synadia's "JetStream Anti-Patterns" names: INFO answers a missing consumer
only after the meta-leader has looked, and a fleet creating consumers by the thousand pays
it each time. A ZeBridge device names every consumer anew, so the INFO always answered "not
found": 8 of its ~37 open requests, plus one per seed object and per reopened tail.

**Change** (`37-nats.zig-create-before-info.patch`):
- `jetstream.zig`: `createConsumerAs(stream, config, action)` — `addConsumer` with the
  request's `action`; `addConsumer` sends none, as before. `getOrCreateConsumer` for a
  named consumer creates first with `action: "create"` (server ≥ 2.10): an identical
  consumer is a success, one with other settings is refused (`ConsumerAlreadyExists`) and
  left untouched, and only then is it looked up and checked (`checkExistingConsumer`) as
  before. Not `action` null: that is create-OR-UPDATE, and would have rewritten another
  application's consumer instead of refusing to bind to it.
- `tests/jetstream_pull_test.zig`: a second connection hears the API: two `pullSubscribe`
  to one durable send 2 `CONSUMER.CREATE` and 0 `CONSUMER.INFO`.

**Verified:** `test-e2e` 194/194, the mismatch tests of `jetstream_sync_test.zig` (filter,
queue group, pull against push) still refused — they take the fallback path. Negative
control: with the lookup put back first, the new test fails (1 create, 2 info). On the hub,
one device's open: `CONSUMER.INFO` 8 → 4 (the 4 left are libzb's own health checks); the
device's open plus 34 s idle is 38 requests, against 375 before §37. `check-series.sh`
green over 37.

## 39. Failed endpoints preserve the configured connection order (2026-10-21)

**How it appeared.** Ordered leaf-to-hub fallback exposed `ServerPool.getNextServer` using swap-removal to rotate or exhaust a failed server. With three or more configured endpoints, removing the first moved the last ahead of the second, so the retry pool no longer followed enrollment order.

**Change** (`38-nats.zig-ordered-server-pool.patch`): both removals use ordered removal. Retried servers rotate to the end without reordering the others; an exhausted server leaves the remaining order intact. Two four-endpoint unit tests cover retry rotation and exhaustion.

**Verified:** before the fix, the two new tests fail (8 pass, 2 fail); after it, the pool tests pass 10/10 in Debug and ReleaseSafe. The SDK `test-unit` suite passes 143 tests with 2 skipped in both modes. The endpoint scenario exercises an unreachable first address followed by a real TLS leaf and hub, reads the live native pool at initial connect, observes a real asynchronous INFO, then kills the leaf and checks reconnection to the hub with its distinct TLS name. `check-series.sh` reproduces the working source from upstream plus all 37 patch files. The submodule pointer remains unchanged.

## 40. Patch 39: xsync pinned to v1.0.1, which builds on 0.16 and 0.17 (2026-10-23)

**How it appeared.** The Zig 0.17 port (parked on branch `zig-0.17`) carried xsync as a local patch: 0.17 made `Io.Threaded.mutexLock` cancelable, and xsync v1.0.0 calls it. xsync v1.0.1 (2026-10-13) uses `mutexLockUncancelable` where it exists, so one source builds on both compilers.

**Change** (`39-nats.zig-xsync-1.0.1.patch`): `build.zig.zon` only — the `xsync` dependency's URL and hash, from `zig fetch --save=xsync "git+https://github.com/lalinsky/xsync.zig?ref=v1.0.1"`. Cut as patch 35 was: the diff of `build.zig.zon` between upstream plus the series and the working tree (`new-patch.sh` cuts `src/` and `tests/` only).

**Verified on 0.16.0:** nats.zig `test-unit` 143/143, the bridge 410/410, libzb 48 passed and 2 skipped. One libzb `zig build test` run failed under the build runner and passed when run directly and on two reruns: recorded as a flake. `check-series.sh` green. The switch to 0.17 no longer needs an xsync patch; the submodule pointer is unchanged.

## 41. Patch 40: the reconnect epoch is 32 bits, so Android armv7 builds again (2026-10-23)

**How it appeared.** A build-only run of `publish.yml` on `main`: the Maven and React Native jobs failed compiling libzb for `arm-linux-androideabi` with "expected 32-bit integer type or smaller; found 64-bit integer type". Patch 36 (long pull requests, 2026-10-19) added `Connection.reconnect_epoch` as a `std.atomic.Value(u64)`, and Zig has no 64-bit atomics on a 32-bit target. CI builds no armv7, so only the release jobs saw it.

**Change** (`40-nats.zig-epoch-u32.patch`): `src/connection.zig` (`reconnect_epoch` is `std.atomic.Value(u32)`) and `src/jetstream.zig` (`PullRequest.epoch: u32`). The epoch is only compared for equality, so wrapping after 4 billion reconnects changes nothing. libzb's copy (`outbox_reconnect_epoch`) follows, outside this series.

**Verified on 0.16.0:** libzb builds for `arm-linux-androideabi` (cortex_a7, NDK 28); nats.zig `test-unit` 143/143, the bridge 410/410, libzb 48 passed and 2 skipped; `check-series.sh` green over 39 files. A 32-bit Moto e20 ran the 10-airports Flutter app on a libzb carrying this change (built with 0.17, 2026-10-23).

## 42. Patch 41: the Zig 0.17 port (branch `zig-0.17-port`, 2026-10-23)

**How it appeared.** The 0.17 port, parked on 2026-10-09 as patch 36 on a branch, redone on `main` after `receipts-locks` merged. `main` had meanwhile taken number 36 (long pull requests) and added 37–39, so the edits are cut again as the last patch.

**Change** (`41-nats.zig-zig-017.patch`, 8 files, 12 hunks): `build.zig` (`std.zon.parse.fromSlice` with its options struct), `src/net_util.zig` (`writeVec` through `io.operate(.net_write)`, `Io.VTable.netWrite` is gone), `src/nuid.zig` (`mutexLockUncancelable`), `src/root.zig`, `src/timestamp.zig`, `src/validation.zig`, `src/inbox.zig` (`**` → `@splat`), `test_runner.zig`. Cut as the diff of those files between upstream plus patches 01–40 and the working tree: `new-patch.sh` cuts `src/` and `tests/` only, and this patch also touches `build.zig` and `test_runner.zig`. The xsync patch the parked branch carried is gone: patch 39 pins xsync v1.0.1, which builds on both.

**Verified on 0.17.0:** nats.zig `test-unit` 143/143, the bridge 410/410, libzb 48 passed and 2 skipped. `check-series.sh` green over 40 files, and a clean apply reproduces `build.zig`, `build.zig.zon` and `test_runner.zig` byte for byte. The switch is a clean break: this patch does not build on 0.16.

## 43. Patch 42: a subject may be longer than 255 bytes (2026-10-27)

**How it appeared.** ZeBridge's troubleshooting work (a tenant id of 93 bytes got rows on its CDC stream and no generation chain ever) traced to `$JS.API.CONSUMER.CREATE.OBJ_gen-<tenant>.<name>.$O.gen-<tenant>.C.>`, which carries the tenant twice and passed 255 bytes. The limit was nats.zig's, not the server's: `validateSubject` applied the 255 of the NAME rules (ADR-6: stream, consumer and bucket names) to subjects.

**Measured against nats-server 2.15.1** (nats-py, an isolated server): a core subject of 3000 bytes is delivered; an object store with a 240-byte tenant (bucket `gen-<tenant>`) puts and gets; a named consumer created with a 547-byte API subject is created. The server bounds the protocol line by `max_control_line` (4096 by default).

**Change** (`src/validation.zig`, `src/root.zig`): `max_subject_len = 1024` (exported, so a caller can derive its own limits from it) replaces the literal 255 in `validateSubject`; names keep 255. A test pins a 300-byte subject, the consumer-create subject for a 128-byte tenant, and the bound itself.

## 44. Patch 43: Zig 0.17.1 in CI, the floor at 0.17.0, `@backingInt` (2026-11-11)

**How it appeared.** ZeBridge moved to Zig 0.17.1 (its `minimum_zig_version`, CI and release builds). The fork still declared `minimum_zig_version = "0.16.0"`, wrong since patch 41 (the 0.17 port), and its CI installed 0.16.0; that CI runs only on `main` and pull requests, so the `zebridge` branch never exercised it. `zig fmt --check` under 0.17.1 failed on three files: its formatter rewrites `@intFromEnum` on an enum value to `@backingInt`.

**Change**: `build.zig.zon` `minimum_zig_version = "0.17.0"` (the floor the code needs; `@backingInt` exists in 0.17.0, not in 0.16.0), `.github/workflows/ci.yml` installs 0.17.1, and `zig fmt` applied to `src/nkeys.zig`, `tests/tls_test.zig` and `tests/utils.zig` (nine `@intFromEnum` → `@backingInt`, nothing else).

**Verified:** `test-unit` 144/144 and `zig build examples` on both 0.17.1 and 0.17.0; `zig fmt --check` clean on every tracked `.zig` file (the untracked `zig-pkg/` dependency copies are not ours); the ZeBridge bridge and libzb build against it.

**Found, not fixed:** `zig build test-e2e` (nats-server 2.15.2, the docker compose servers) crashes with a segmentation fault at `0x5555555555555575` in `JetStreamMessage.sendAck` (`jetstream_message.zig:95`), reached from the push subscription's auto-ack in `jetstream.zig` (`js_msg.ack()` after the user handler). The address is the fill of freed (undefined) memory, and the handler is documented as owning the message's cleanup: the auto-ack touches a message the handler may already have freed. It reproduces on the unchanged patch 42 on both 0.17.1 and 0.17.0, so it predates this patch. ZeBridge is not on this path: the bridge and libzb use only `pullSubscribe` and synchronous core subscriptions (`subscribeSync`, `queueSubscribeSync`), never `JetStream.subscribe` with a handler.
