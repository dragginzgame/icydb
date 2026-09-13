# Application-Owned Multi-Canister Workflows

Use durable application records around IcyDB's existing synchronous batches.
IcyDB commits local changes; the application owns remote calls, authorization,
idempotency, retries and compensation. There is no transaction across an
`await`, across stores, or across canisters.

This recipe describes one source canister enrolling a user on one target.
Repeat the same boundary for additional placement steps; do not hold a database
session or accepted-schema snapshot across the remote call.

## Minimal Records And Ownership

Keep the following information in journaled storage, not heap-only state:

| Owner | Retained information | Purpose |
| --- | --- | --- |
| Source intent | Operation ID, authenticated subject, selected target, exact request, pending/terminal outcome | Resume the same operation after interruption |
| Target receipt | Source identity and operation ID, exact request binding, terminal outcome | Replay the result without repeating the effect |
| Source result | Accepted receipt and completed local business state | Return the same result to repeated client requests |

These are responsibilities, not necessarily three new entities. An existing
directory or identity row can retain the intent or receipt if its immutable
fields establish the complete request binding. Keep mutations that must commit
together in the same registered store. Use unique keys/indexes for operation
identity and for domain uniqueness, such as one identity per principal.

Generate or accept an operation ID once, before its first remote effect; retain
it and reuse it on every retry. Scope target duplicate detection to the
authenticated source and operation ID. Bind that identity to the subject,
target, operation kind and all effect-affecting arguments. Comparing retained
typed fields is sufficient; a second hash/encoding authority is unnecessary.
An ID is not an authorization credential. Check the caller and its authority
before exposing receipts or applying effects.

For one-time enrollment, a source can return an existing subject's operation
instead of accepting a new client ID. Make this explicit, and continue using
the retained request and ID; do not substitute new arguments into old work.
Do not redirect an uncertain operation to another target: the original target
may already have committed. Placement changes need their own application rule.

## Three Local Commit Boundaries

1. **Source preparation:** authorize, read current state, and atomically retain
   the immutable intent with any local reservation. Commit before calling the
   target. If an intent already exists, validate its request binding and resume
   it or return its terminal result.
2. **Target application:** authenticate the source, read its receipt and current
   business rows, then atomically commit the business effect and receipt. A
   duplicate exact request returns the retained outcome. The same identity with
   changed arguments is a conflict, not a new operation.
3. **Source finalization:** after the reply, re-read the intent and local business
   rows using current authority. Validate the reply against the retained
   operation and atomically commit local finalization with the terminal result.

Each read/check/compute/batch sequence is synchronous, with no `await` between
its final reads and write. Do not implement “effect, then receipt” as two
independent writes. Returning an application `Err` after a successful batch
does not roll back that batch. A saved-row adapter failure after publication
also does not prove the batch failed; re-read retained state before retrying.

Use `session.trusted_typed_write_batch()`, its generated `push(...)` inputs and
`execute()` for typed writes, or
`session.execute_trusted_structural_mutation_batch(...)` for structural
mutations. Both use the existing local commit owner. The
[public facade example](public-facade-api.md) and
[complete typed enrollment compile fixture](../../crates/icydb/tests/pass/typed_enrollment.rs)
show the concrete API. The [transaction contract](../contracts/TRANSACTION_SEMANTICS.md)
defines current batch limits and result semantics.

### Control Flow Example

The following is application pseudocode, not additional IcyDB methods or a
distributed-transaction API. `local` means a fresh synchronous database scope;
each `commit_batch` uses one of the existing batch surfaces above.

```text
source.enroll(caller, request):
    intent = local:
        authorize(caller, request)
        read existing intent and business rows
        if existing: validate/resume its retained request
        else: commit_batch(new intent + local reservation)
        return retained intent
    if intent is terminal: return retained result

    reply = await call(intent.target, intent.id, intent.request)

    return local:
        current = read intent and business rows again
        require current identity, target and request match the sent intent
        if current is terminal: return retained result
        if reply is uncertain or retryable: leave pending; return Pending(id)
        validate terminal receipt identity and request binding
        if receipt is Applied:
            commit_batch(local finalization + Completed(receipt))
        if receipt is Rejected:
            commit_batch(release local reservation + Rejected(receipt))
        return retained result

target.apply(caller, id, request):
    return local:
        authorize source caller
        receipt = read receipt keyed by (caller, id)
        if receipt exists:
            require exact request binding; return retained outcome
        validate request and read current business rows
        if temporarily unavailable: return Retryable(id)
        if domain refusal is final:
            commit_batch(Rejected receipt bound to request)
        else:
            commit_batch(business effect + Applied receipt bound to request)
        return retained terminal receipt
```

Terminal outcomes are typed application values, for example `Applied(result)`
and `Rejected(reason)`. `Retryable(reason)` and a transport failure are not
terminal receipts. A terminal rejection must fence later application of the
same operation; an early validation error without such a fence cannot justify
releasing a reservation for an earlier, possibly in-flight attempt.

## Lost Replies, Reentry And Compensation

A failed or missing reply can leave the remote outcome unknown. Keep the
intent pending and retry the *same* target, ID and request. A repeated target
call must return its retained outcome, not reapply the effect. A receipt lookup
can help, but `NotFound` alone does not prove an earlier attempt cannot still
commit. Do not infer terminal failure from a timeout or an ordinary stale read.

Two retries may be in flight simultaneously. Before any post-`await` write,
re-read and compare current progress. A callback for an earlier step must not
move `Completed` back to pending, change a retained assignment, or overwrite a
later step. Returning a terminal result or treating an already completed step
as a no-op is enough; a new lock/lease subsystem is not required for this
synchronous local boundary. For multiple steps, compare the retained step as
well as the operation identity before advancing it.

If the target committed but source finalization fails, leave the source intent
recoverable and retry finalization using the same receipt. Do not issue a new
remote operation just because local finalization or result decoding failed.

Compensation is a separate domain decision, not rollback. Only release a local
reservation when authoritative protocol state makes that safe. If an applied
effect must be undone, use a separately identified, idempotent compensation
operation linked to the original result. It can also fail or need retry; retain
that obligation. If the effect is irreversible, expose that fact and require
domain recovery rather than pretending the workflow was atomic.

## Restart, Bounded Progress And Retention

Wait for database startup `Ready`, then resume application-owned pending work.
IcyDB recovery finishes local database commits; it does not resend remote calls
or restart suspended application futures. A client retry or an application
worker can resume the intent using the same control flow.

Index pending work so each worker invocation reads a bounded page and attempts
a bounded number of operations. Bound admitted pending intents and request/
receipt sizes at the application boundary. Keep retry scheduling separate from
domain identity, and revisit unfinished work fairly. Derived UI projections
can converge in bounded pages; do not use a lagging projection as authorization,
duplicate-detection or compensation authority.

Retain request-binding and duplicate-detection evidence for as long as retries
remain admissible. Keeping that evidence on a permanent identity row is often
the simplest enrollment design. A TTL or source acknowledgment alone is not
permission to delete the receipt and accept the old ID again. If receipt
storage must be reclaimed, the protocol must first reject old operations
durably, for example through an authenticated sequence cutoff that also
handles delayed calls and gaps. That is additional application protocol work,
not a built-in IcyDB expiry guarantee.

An ordinary compatible upgrade may resume persisted intents. An incompatible
pre-1.0 database recreation does not preserve them automatically. Coordinate
outstanding operations and prevent identity reuse before restarting a source
with empty state; do not rely on deleted receipts after resetting a target.

## Qualification Checklist

Test the application protocol at message boundaries, not only its happy path:

| Interruption or conflict | Required result |
| --- | --- |
| Source preparation rejects | No remote effect starts |
| Intent commits, call never starts | Restart/retry uses the retained request |
| Target commits, reply is lost | Same request returns the same receipt; one effect |
| Same ID, changed subject/target/arguments | Conflict; no second effect or receipt disclosure |
| Wrong source caller | No effect and no unauthorized receipt access |
| Source finalization rejects after target success | Pending intent remains recoverable; no repeated effect |
| Two callers resume the same intent | One local result; stale callbacks cannot regress progress |
| Terminal rejection followed by delayed retry | Rejection stays terminal; reservation release remains safe |
| Compatible upgrade between any two steps | Startup readiness, then application resumption |
| Reclaimed receipt or recreated database | Old IDs cannot become newly applicable operations |
| Pending backlog exceeds one worker page | Bounded work and eventual revisiting; no unbounded scan |

Use native tests for local batch/replay rules and a canister integration harness
for lost replies, upgrades and reordered callbacks. A native idempotency test
alone is not evidence that the distributed workflow is interruption-safe.
