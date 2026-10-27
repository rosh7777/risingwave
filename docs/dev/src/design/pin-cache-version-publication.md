# Pin cache refill and version publication

## Responsibilities

The event handler is the only writer of shared `TableCacheMetadata`: table cache
policies, the effective Pin table set, role and streaming/serving vnode ownership.
Foyer borrows it during submission. The Pin worker receives the existing
`ReadOnlyRwLockRef`; each asynchronous attempt retains immutable task inputs.

The handler separately owns optional `PinCacheMembership` in
`event_handler::pin_cache_membership`. It contains the derived object reference index and the published /
received version ledger, without a shared lock or runtime. The index borrows the
current Pin table set; it does not keep another policy copy. Neither refiller reads
or writes this lifecycle state.

`CacheRefiller` constructs the independent backends, captures event-scoped Pin
candidate intent under the submission-time table policy and combines their conditions.
`FoyerRefiller` owns candidate admission, task projections, spawning, timeout and
result handling. `PinCacheRefiller` owns the pending intents, batch selection,
table-level admission and batch completion. The existing executor owns physical
downloads and cancellation. Both backends share the data concurrency limit;
Foyer alone uses metadata preparation permits.

Received versions register Pin membership before backend submission, even when a
request waits behind an active batch or no download is admitted. Publication retires
removals while preserving references in received unpublished versions. Pin policy
changes rebuild membership across published and received snapshots; they do not
create download intent. The reference index counts logical SST references in the
latest received version; backend registration protects the version union.

Metadata replacement calls `invalidate_pin_refills` with the changed tables and
current Pin table set. It only narrows pending intent after Pin withdrawal and
cancels submitted attempts through the executor; it does not update metadata or
membership.
Vnode changes do not sweep published files. Version retirement and Pin-policy
withdrawal unregister objects through the existing backend. Ownership-driven
reclamation is separate work; this PR does not guarantee reclamation when only
vnode ownership changes. After recovery, a storage-owned GC worker runs one full
pass for startup garbage and periodic minor passes for withdrawn files whose
readers have exited. Storage shutdown aborts this worker.

The metadata lock never covers I/O or an await. Policy/vnode replacement, membership
policy rebuild and cancellation share its writer guard so admission cannot observe
an intermediate policy transition. Version receipt and publication mutate the
handler-exclusive lifecycle without a metadata write lock. Retirement unregisters
objects before cancelling their executor waiters; backend tokens prevent stale
publication. Lock order is metadata, Pin pending queue, executor/cache locks.
Selection and executor submission complete under the same metadata read guard,
so a metadata update cannot miss a selected but not-yet-submitted attempt.

The handler constructs the refiller with read-only metadata and passes both to its
inner constructor. Foyer directly creates tasks at submission; controlled task
injection is test-only. Backend tests live in `refiller/foyer/tests.rs` and
`refiller/pin/tests.rs`; composition/publication tests remain in `refiller/tests.rs`.
Shared fixtures remain in `refiller/test_utils.rs`. `version_update` stays independent
of metadata policy and backend implementations.

The version update framework applies received deltas once to construct unpublished
version snapshots. Refill planning can advance beyond the version visible to readers.

Foyer and Pin own their admission, execution, timeout and cancellation. Foyer starts
an independent attempt for each received event. Pin may merge already queued adjacent
events, up to `pin_cache_max_batch_size`. One consumer task lives with the Pin refiller
on the existing Tokio runtime and parks on `Notify` when idle. Submission only enqueues
intent and calls `notify_one`; all batch selection and admission happen in the consumer.
There is no batching timer or wait for a future compaction. Events received before
the worker's first selection can share its first batch. An active batch is not
retroactively rebatched.

Pending intents can only lose tables when Pin policy is withdrawn; a later SET cannot
create intent for an older delta. Vnode ownership is read from current CN metadata
when the worker selects a batch. After submission, Pin withdrawal or losing all local vnodes for an admitted table
cancels its downloads instead of restarting them. A nonempty-to-nonempty vnode
change does not invalidate coarse table admission. Thus an unpolled worker has pending intents,
not an already-admitted first batch.

The only shared scheduling state is the pending queue. Under the metadata read
guard the worker selects a target, merges plans, checks table ownership, and submits
admitted objects to the existing executor. It then releases the guards and executor
handle before awaiting download results under the batch timeout. There is no
metadata-preparation stage, active-batch cancellation state or ownership snapshot.

Pin admission requires a surviving Pin table projection with any locally owned
vnode. It does not inspect block metadata or refine SST key ranges: an object may
be downloaded even if none of its actual rows belong locally. This intentionally
trades bandwidth and capacity for a simpler first implementation; finer admission
is separate work. Empty ownership and withdrawn Pin intent still reject candidates
before allocating executor jobs. Foyer's existing block-level admission is unchanged.

`Notify` carries no requests. If a submit races the consumer's empty-queue check,
`notify_one` retains a permit for its next wait. Busy submissions may coalesce wakeups;
their requests remain in the single pending queue. The owner retains the only task
handle and aborts it at shutdown; there is no startup/idle-exit handoff protocol.
Empty plans still enter the queue because their target can retire earlier candidates.

Pin starts its next batch when its current attempt ends, regardless of Foyer or
publication progress. Publication notifies the CN membership owner; it never schedules
refill work. Existing shared I/O limits may still cause resource contention.

## Publication conditions

A condition waits for an event's independent Foyer attempt and its Pin batch attempt
to settle, then returns whether that event's target may be a publication endpoint:

| Condition state | Meaning |
| --- | --- |
| Pending | A prerequisite attempt is still outstanding. |
| Ready(true) | Attempts settled and this target is an allowed endpoint. |
| Ready(false) | Attempts settled, but a selected batch subsumed this target. |

`false` does not mean refill failed. Backend errors and backend-owned timeouts allow
remote fallback. They preserve the selected batch's endpoint decisions.

For example, a commit at v11 adds A and a compaction at v12 replaces A with B. If
both events queue behind an earlier Pin attempt, Pin can merge them and discard A
against the selected v12 snapshot before download allocation.
While the batch runs, both Pin conditions wait. Once it settles, Pin's v11 predicate
is false and v12's is true: v11 must not become an independent published head after
its refill was skipped. A later unselected version must not influence this pruning.

While the handler is running, each selected finite Pin batch has exactly one true
predicate, at its final event, including on error, timeout or cancellation.
The existing executor catches physical-download panics and completes their waiters;
the batch worker only awaits these results. All selected nonfinal events return false, even if no SST
was ultimately pruned. Handler shutdown drops both refillers and the publication
queue; completion channels may close without delivering endpoint predicates.
No-Pin events return true after their Foyer attempt ends, preserving legacy
task-error behavior.

## Queue and publication

The queue caches each condition result and examines the settled FIFO prefix. It
publishes through the last true result in that prefix. It stops at the first Pending
condition: a later Foyer task cannot substitute for an unfinished earlier attempt.
A false suffix stays queued until a later true endpoint settles. Canceling a queue
wait neither removes events nor causes completed futures to be polled again.

The queue receives no Pin batch IDs or required version ranges. It has no deadline,
backlog force-release or retry policy. It retains main's 16-event publication bound
when each event is an endpoint. If the boundary falls inside a subsumed batch, it
continues to the next allowed endpoint so that a selected Pin batch is never split.

Publication retires the prefix together and advances the current version to its final
target. Existing history and per-event local read-version processing are retained;
each local read version applies the prefix under its own write lock. Watch readers
and the uploader receive the final target. Historical snapshots can still read A
through remote fallback; neither historical cache coverage nor global atomicity
across different read holders is promised.

This contract covers batched Pin and per-event Foyer. Adding another independently
batched backend requires reviewing endpoint compatibility and progress; simply
ANDing arbitrary batching predicates is not established by this design.
