const std = @import("std");
const nats = @import("nats");
const utils = @import("utils.zig");

const Subscriber = struct {
    conn: *nats.Connection,
    stop: std.atomic.Value(bool) = .init(false),
    ok: std.atomic.Value(u32) = .init(0),
    failed: std.atomic.Value(u32) = .init(0),

    /// Subscribe in a tight loop so that at any given moment this task is
    /// likely to be inside subscribeInternal, holding the connection mutex
    /// and waiting on subs_mutex. That is the half of the AB-BA pair that
    /// drain() used to complete by taking the two in the opposite order.
    fn run(self: *Subscriber) void {
        var buf: [64]u8 = undefined;
        var i: u32 = 0;
        while (!self.stop.load(.acquire)) : (i += 1) {
            const subject = std.fmt.bufPrint(&buf, "drain.race.{d}", .{i}) catch return;
            const sub = self.conn.subscribeSync(subject) catch {
                _ = self.failed.fetchAdd(1, .monotonic);
                return;
            };
            _ = self.ok.fetchAdd(1, .monotonic);
            sub.deinit();
        }
    }
};

// Regression test for the drain()/subscribe() lock-order inversion.
//
// drain() used to hold subs_mutex across Subscription.drain(), which sends
// UNSUB via unsubscribeInternal and therefore takes the connection mutex -
// the reverse of subscribeInternal's mutex-then-subs_mutex order. A drain
// racing a subscribe deadlocked both tasks permanently, wedging the
// connection including its own shutdown path.
//
// Note the failure mode: a regression here hangs rather than fails, and is
// caught by the CI job timeout rather than by an assertion.
test "connection drain concurrent with subscribe does not deadlock" {
    const io = std.testing.io;

    for (0..10) |_| {
        const conn = try utils.createDefaultConnection(io);
        defer utils.closeConnection(conn);

        // Seed subscriptions so drain() has real work to do in the window
        // where it used to hold subs_mutex.
        var seeded: [32]*nats.Subscription = undefined;
        for (&seeded, 0..) |*slot, k| {
            var buf: [64]u8 = undefined;
            const subject = try std.fmt.bufPrint(&buf, "drain.race.seed.{d}", .{k});
            slot.* = try conn.subscribeSync(subject);
        }
        defer for (seeded) |sub| sub.deinit();

        var subscriber = Subscriber{ .conn = conn };
        var group: std.Io.Group = .init;
        try group.concurrent(io, Subscriber.run, .{&subscriber});
        // Stop the subscriber by flag and join it, rather than cancelling:
        // cancelling mid-subscribe makes registerSubscription fail, and this
        // test is about the lock order, not about the unwind.
        defer {
            subscriber.stop.store(true, .release);
            group.await(io) catch {};
        }

        // Give the subscriber time to reach the contended window.
        try io.sleep(.fromMilliseconds(2), .awake);

        try conn.drain();
        // Assert completion rather than swallowing it: the drain has to finish,
        // not merely avoid deadlocking. A subscription drain that never reports
        // completion leaves drain_subscription_count above zero and parks this
        // call until it times out, which a discarded result would hide.
        try conn.waitForDrainCompletion(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    }
}
