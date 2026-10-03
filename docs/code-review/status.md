# Saved code review status

Updated 2026-10-03. This document tracks verification and repair of the saved
29 September 2026 review of commit `994715134` (`0.261.16`) against the current
`0.264` line. The original HTML review remains unchanged.

The review is open. Its 345 raw findings contain 307 distinct, non-refuted
items. An original status such as “verified” or “reproduced” describes the old
snapshot; it is not proof that the current code is fixed. The inventory below
keeps every distinct ID visible without declaring unchecked findings resolved.

Source: [saved review](IcyDB%20Code%20Review.html).
Release owner: [0.264 tracker](../design/0.264-signed-index-admission/0.264-status.md).

## Current progress

| Current status | Findings |
| --- | ---: |
| Verified fixed | 34 |
| In progress | 0 |
| Open | 0 |
| Partial | 1 |
| Needs verification | 272 |

“Verified fixed” requires current code evidence and focused semantic validation.
“Open” means the reviewed defect remains visible in current source; reproduction
is recorded separately. “Partial” identifies an implemented correction that
does not cover the whole original finding. “Needs verification” means no current
closure verdict has been established; it does not mean the finding is a confirmed
current defect. Performance claims require raw Wasm, IC cycles or instructions.

## Authorized work queue

The user authorized continuing the confirmed examples and maintaining this
status document on 2026-10-02. One bounded outcome is one handoff under the
repository rules. Generic continuation takes the next queued outcome in 0.264;
this queue does not authorize starting a different minor line.

| Order | Finding | Bounded outcome | Status |
| --- | --- | --- | --- |
| 1 | `value-types-error-4` | Normalize decimal multiplication operands in the existing schema numeric owner; qualify typed and checked results and SQL stored-field reads | Complete as A13 |
| 2 | `index-access-2` | Preserve signed timestamp ordering through current index encoding, decoding and range reads | Complete as A14 |
| 3 | `index-access-3` | Converge index suffix bounds on the maintained composite primary-key contract, including writes and reads | Complete as A15 |
| 4 | `r2-covering-projection-1` | Make hybrid component admission and decoding agree on supported kinds, with current projection results | Complete as A16 |
| 5 | `executor-aggregate-1` | Resolve owned group keys through existing hash buckets before creating groups | Complete as A17 |
| 6 | `query-intent-1` | Project canonical scalar sort requirements and exact primary-key candidate bounds into read admission | Complete as A18 |
| 7 | `query-intent-2` | Distinguish selective index access from the whole-index ordering fallback in public admission | Complete as A19 |
| 8 | `query-expr-4` | Preserve missing-path no-match semantics while composing boolean filters through the shared expression owner | Complete as A20 |
| 9 | `executor-stream-2` | Reuse disjoint multi-lookup access identity instead of imposing primary-key deduplication on branch order | Complete as A21 |
| 10 | Unordered DISTINCT observation | Admit current unordered SQL projection through global DISTINCT while retaining cursor order authority | Complete as A22; outside saved inventory |
| 11 | `query-plan-4` / `query-plan-5` | Preserve residual expressions after exact-key stripping and require the existing residual proof for index-only terminal eligibility | Complete as A23; explicitly authorized together after overlap scan |
| 12 | Mixed-filter cache observation | Preserve separately appended predicate semantics alongside expression identity in ordinary shared keys | Complete as A24; outside saved inventory |
| 13 | Simultaneous-residual observation | Enforce uncovered expressions and independently remaining predicates through the existing effective runtime filter | Complete as A25; outside saved inventory |
| 14 | `cli-3` | Preserve SQL endpoint errors through the existing command result into nonzero one-shot exit status and stderr; retain interactive continuation | Complete as A26 |
| 15 | `cli-4` | Qualify migration terminal receipts and make run/advance/abort command status reflect the requested operation's outcome | Complete as A27 |
| 16 | `model-schema-crates-1` | Distinguish excess fractional precision from true magnitude overflow in the shared Decimal multiplication owner; qualify operators, powers and checked runtime consumers | Complete as A28 |
| 17 | `model-schema-crates-4` | Compute exact remainder after wide scale alignment in the shared Decimal owner; qualify primitive, application and checked consumers | Complete as user-selected A29 |
| 18 | `model-schema-crates-3` and direct shared-boundary fallout | Qualify fitting arithmetic results before primitive magnitude saturation in the existing Decimal owner | Complete as user-selected A30 |
| 19 | `executor-aggregate-3` | Preserve route direction at the generic grouped sort boundary through the shared grouped comparator; qualify related key/window paths | Complete as A31 |
| 20 | Filtered-index predicate cluster | Replace persisted SQL authority with one accepted bound predicate representation through intake, codec, runtime, identity and rename consumers | Complete as A32 |
| 21 | `data-2` / `r2-recursive-bounds-1` | Converge borrowed and materializing current value-storage traversal on canonical enum framing and one accepted recursive-depth authority | Complete as user-selected A33 |
| 22 | `facade-3` | Preserve memory grant/admission causes at the existing public startup/error boundary; qualify generated access and document explicit fresh-allocation grants | Complete — A34; six bounded diagnostic leaves and public startup/db qualification |

The original five queued outcomes are complete. The scoped read-only
[closeout audit](closeout-audit.md) verifies another existing fix, reproduces the
remaining admission and missing-path defects, and proposes the next independent
corrections within 0.264. User-authorized A18–A22 complete the two admission
corrections, missing-path expression semantics, branch-ordered DISTINCT and the
unordered DISTINCT observation. A23 completes the subsequently authorized
residual-preservation pair. The user subsequently authorized its reproduced
mixed-filter cache follow-up as A24 and the simultaneous-residual execution
follow-up as A25, both now complete. After that completed queue, a scoped source
audit confirms `cli-3` and reports it before correction. The user's request to fix
the next saved-review bug extends 0.264 with this one bounded outcome as A26.
The user subsequently authorizes migration command status (`cli-4`); A27 completes
its independent receipt qualification. The planned queue is complete; generic
continuation performs a scoped read-only closeout audit within 0.264 before
selecting another correction. Dynamic and diagnostic gaps remain visible in the
audit. The subsequent numeric audit verifies the existing division-panic
correction and reproduces excess-precision multiplication; the user's next
continuation selects A28, now complete. The queue is again complete; generic
continuation starts a scoped audit before another correction. That audit now
reproduces the independent alignment/saturation and remainder findings. The next
user selection authorizes A29, now complete across the shared remainder boundary
and its direct consumers. Subsequent user-selected A30 completes the shared
alignment/saturation correction and directly related boundary cases. The
queue is complete; generic continuation performs a scoped read-only audit before
another correction. The saved review remains open; this is not a closeout verdict
for all findings.

The next scoped arithmetic audit proposes A30: qualify representable results
before primitive saturation in the shared Decimal owner. It reproduces the then-open
alignment/saturation finding and related signed-subtraction, checked-division and
true multiplication-overflow cases. The proposal is recorded before correction;
the audit changes no runtime code or inventory counters. The subsequent user
selection authorizes A30, now complete; details are in its validation record below.

The requested [quick overlap scan](overlap-triage.md) screens all 291 unchecked
findings and records seven candidate groups containing 21 distinct reports.
Filtered-index predicate authority is the strongest seven-item structural
cluster; residual preservation is the smallest clear two-item candidate.
Other groups need qualification or splitting. Existing SQL NOT and checked
decimal-division corrections are verification candidates, not new code work.
The scan itself changes no inventory counters or implementation authorization.
The user subsequently selected the residual pair; A23 closes its two saved IDs.
Qualification reproduced a separate mixed-filter cache identity defect, recorded
in the audit and subsequently authorized as A24 within 0.264.

Other findings retain their individual verification state below. In particular,
`model-schema-crates-1` (decimal excess precision incorrectly saturating) is a
separate outcome from operand padding; A13 must not claim it resolved.

## Standing repair discipline

User requirement recorded 2026-10-02: address incomplete boundary qualification
and facts drifting between owners alongside the individual bugs. A local example
fix alone is insufficient when the defect belongs to a shared boundary family.

- Identify the maintained contract, its canonical owner and all affected
  consumers before implementation. Reuse that authority instead of duplicating
  type support, ordering, access, equality or budget facts in another owner.
- Qualify the relevant combinations of accepted types, execution routes, order,
  limits, continuation, missing/null values and repeated keys. Choose a bounded
  matrix for the demonstrated defect; do not run the full repository suite.
- Compare equivalent optimized and general/trusted execution where appropriate.
  Include admitted controls and typed rejection/error cases at the same boundary.
- Verify facts survive projection into admission, diagnostics and execution.
  Record covered surfaces and gaps, and close aliases only when they share the
  proven correction. Separate independently reviewable outcomes into later slices.

## Validation record

The initial check passed 19 focused regressions for all five distinct critical
findings, plus three hybrid admission/ownership tests. Rust formatting and diff
checks passed. A disposable probe reproduced `value-types-error-4`: padded
scale-18 `20 × 20` returned `None`, while normalized values returned `400`.
The decimal division overflow probe returned `None` without a panic; this is
narrow evidence for `value-types-error-3`, not complete closure of that item.

A13 completes `value-types-error-4`. Operand normalization prevents padding
overflow without changing accepted stored field scales. All 44 focused tests
pass: 26 decimal tests, 17 runtime numeric tests and one SQL regression that
verifies admitted scale-18 storage and exact cold/warm multiplication results.
Two new decimal regressions and the SQL regression failed before the correction.
True magnitude overflow retains its existing typed errors and saturation.

Required maintainer Clippy recovery and strict schema/core all-feature
library/test lint pass. The first locked lint attempt met a concurrent
`ic-timers` update; the offline maintainer check then needed its new cached
version. Copying the already-downloaded dependency into the repository-local
cache resolved this without changing manifests or lockfile. The 44-test gate
was refreshed against the current lockfile. Formatting, schema/format guards,
documentation references, inventory uniqueness and diff checks pass.

Eight owned files add approximately 540 net lines, mostly this complete
inventory and direct regression coverage. Production arithmetic adds four
lines; the implementation shape is neutral, with no new behavior axis. Existing
hybrid cleanup and memory/timer dependency changes are preserved. A13 handed back with `index-access-2` queued;
other review findings retain their individual verification state below.

A14 completes `index-access-2`. Timestamp index components reuse the existing
signed 64-bit transform. The signed-byte check, primitive pairwise order check
and stored-range regression failed before correction: a full signed range omitted
all negative timestamps. Unique and non-unique accepted indexes now pass equality,
signed extrema, duplicate timestamp ordering, strict/inclusive ranges, ASC/DESC,
total limits and every issued continuation suffix. Full row reads verify decoded
timestamps against their inserted values; planner checks prove index-range
admission. Primary-key timestamp round trips retain their signed representation.
The separate hybrid unsupported-component finding is still queued.

All 41 distinct focused tests pass (42 executions across timestamp, ordered-key
and numeric range selections). Strict all-feature core library/test Clippy,
formatting, schema/format guards, documentation references and diff checks pass.
An initial fixture reset failed when changing index uniqueness under one retained
registry; separate unique/non-unique test cases provide isolated accepted schemas.
No production workaround was needed.

This is a pre-1.0 hard cut of current version-1 index bytes. Reinstall/recreate
stores with timestamp index components, regenerate indexes and discard retained
continuations. Opaque prior timestamp payloads have no encoding discriminator;
in-place retention is unsupported. No compatibility decoder or new format/mode
is added. Nine owned files add approximately 370 net lines, mainly regression
coverage and documentation. Production changes one expression with no line
increase; the implementation shape is neutral. Existing dirty work is preserved.
A14 handed back with `index-access-3` queued.

A15 completes `index-access-3`. The strict index codec reuses the primary-key
owner’s 254-byte composite bound. Four new regressions fail before correction:
maximum suffix encoding, maximum stable sizing and indexed inserts with two
full-width principals (64 bytes) or four accounts (254 bytes). All 48 focused
tests pass against the current lockfile. The 46 codec tests cover user/system
suffixes, complete borrowed decode, row witnesses, stable reopen, current maximum
sizes, scan bounds and malformed/truncated rejection. Two session regressions
prove accepted index prefix/range admission, indexed inserts, typed unique
conflicts, cold/warm lookups, full rows, ASC/DESC and every continuation suffix.

Required maintainer lint recovery and strict all-feature core library/test lint
pass. Formatting, schema/format guards, documentation references, inventory and
diff checks pass. Recovery corrected test-helper const/closure warnings and an
order-type import error. Concurrent memory/testkit updates required copying their
existing downloads into the local offline cache; current-lockfile checks pass,
and dependency edits remain outside this slice.

The stable key bound grows by 191 bytes. Reinstall/recreate persisted index stores
with current indexes and fresh continuations; in-place retention is unsupported.
Tuple and primary-key representations retain their current shape and version-1
posture, without a compatibility bridge. Nine owned files add approximately 440
net lines, chiefly regression coverage and notes. Production replaces the bound
expression and adjusts its import (+2 net lines); complexity is neutral with
one shared authority. Existing dirty work is preserved. The next queued finding
at that handoff was `r2-covering-projection-1` (hybrid unsupported-component
admission/decoding).

A16 completes `r2-covering-projection-1`. Unsupported components propagate the
shared decoder's decline through both hybrid branches to the scalar projection
reader. No supported-kind matrix or catalog-independent enum decoder is added.
All 42 focused tests pass against the current lockfile: 21 accepted-session
type cases, three component-boundary cases and 18 maintained covering,
ownership, missing-row and materialization/budget regressions. The matrix covers
fourteen unsupported scalar families, accepted unit enums and all six supported
tags, with plain SELECT, cold/warm reads, admitted prefixes/ranges, multi-prefix
membership, ASC/DESC, LIMIT/OFFSET and supported scalar parameter constants.
Unsupported decoding after a supported component declines the whole projection;
malformed supported payloads retain their typed errors.

Required maintainer lint recovery and strict all-feature core library/test lint
pass. Initial test fixture/import errors and an unnecessary binding clone are
resolved. Enum SQL parameters remain outside the maintained binding surface;
the enum regression verifies accepted projection through the category predicate.
Formatting, schema/format guards, documentation references, inventory and diff
checks pass. Eight owned files add approximately 580 net lines, mainly tests and
documentation; production adds ten lines to propagate the existing contract,
with neutral complexity and no new state/format/route. Concurrent dependency
updates are preserved. A16 handed back with `executor-aggregate-1` queued.

A16 qualification also observed an independent pure-covering failure:
`SELECT DISTINCT operand, category FROM PlannerRow WHERE category = 3` returns
`InvariantViolation(19)` without an explicit order, including with supported Bool
and Nat64 operands in the accepted `(category, operand)` index fixture. Hybrid
projections of `operand, label` pass after the current correction. Keep the
unordered pure-covering DISTINCT observation for isolated verification and a
separate outcome; it is not part of the saved review's 307-item inventory or an
A16 closure claim. At that handoff, `executor-aggregate-1` was the next queue item.

A17 completes `executor-aggregate-1`. Owned grouped keys now probe the existing
stable-hash bucket and compare canonical `GroupKey` identity before new-group
admission. Five new accepted-session regressions reproduced one group per input
row for unit, list, set, map and accepted enum keys; the borrowed scalar control
passed before the correction. All 66 focused tests pass: six accepted-session
cases, three new bundle cases, 33 existing grouped/key/cursor/composition tests
and 24 hybrid component regressions from A16. Nonadjacent repeated single- and
multi-field keys combine COUNT/SUM results. Trusted and indexed public reads
succeed with group limits smaller than the input row count. Dedicated COUNT
results, collisions, malformed bucket errors, nested Decimal normalization and
group/state accounting retain the existing authorities.

Strict all-feature core library/test lint, formatting, schema/format guards and
documentation/inventory/diff checks pass. A unit fixture's cross-tag numeric
assumption was corrected to the maintained Decimal scale-normalization contract;
no production equality change was needed. Eight files add approximately 440 net
lines relative to the A16 handoff, mainly tests and notes. Production adds 14
lines using existing lookup and identity owners, with neutral complexity and no
new state, route or format. Earlier dirty work and dependency updates are
preserved. The authorized queue is exhausted; the next turn is a read-only
closeout audit rather than another implementation slice.

The read-only closeout audit verifies `xc-security-1` against current IC entropy
source and five passing native entropy/cursor tests; its two duplicate source
aliases close with the canonical finding. Disposable current-source probes
reproduce `query-intent-1`, `query-intent-2`, `query-expr-4`, `executor-stream-2`
and the separate unordered DISTINCT observation. A composite branch-set DISTINCT
control succeeds; the single-field multi-lookup family retains the saved defect.
See the audit for fixture controls, shared
owners, remaining verification limits and the proposed follow-up queue.

A18 completes `query-intent-1`. Scalar admission projects the executor route
owner's post-access sort rule, including residual filters, and reports exact
primary-key candidate bounds rather than authored output limits. Indexed sorts
with unknown candidate bounds reject with typed SortRequiresMaterialization;
exact ByKey/ByKeys exceptions, ordered index reads and grouped limits retain
their maintained policies. Text and JSON EXPLAIN expose the same facts.

All 44 focused tests pass: five accepted-session regressions and 39 maintained
admission/input, route, page-limit, secondary-order and grouped cases. Four new
regressions fail before correction. Fixture-only private-method, order-name and
unordered-intent issues were corrected to use maintained accepted preparation
and effective page order. Required maintainer lint recovery and strict all-feature
core library/test lint pass after removing a redundant test-helper closure.
Formatting, schema/format guards, documentation/inventory and diff checks pass.
Eleven files add approximately 400 net lines relative to A17 and its read-only
audit handoff; production adds 60 lines. Complexity increases slightly for the
shared sort fact and summary construction, with no new mode, state or format.
Existing dirty work and dependency changes are preserved. The next queued outcome
is `query-intent-2`; this slice does not claim its access proof corrected.

A19 completes `query-intent-2` and source alias `xc-security-3`: one canonical
inventory closure, two raw reports. An empty equality prefix and two unbounded
range endpoints now project to the existing logical FullScan admission class.
The physical index name remains available to diagnostics; existing public
policy owns rejection for every consuming surface. An output limit, index
ordering or grouped-state cap cannot replace the missing access constraint.
No new route, mode, enum variant, format or persisted fact is added.

All 54 focused tests pass: five new accepted-session cases, eighteen combinations
in the new range-bound projection test, and maintained admission/input, route,
sort, scalar page-limit, secondary-order, owned-key/grouped and sparse-index
cases. Session qualification covers no filter, residual predicates and field
expressions, ASC/DESC, small limits, cold/warm plans, authenticated live and
exhaustive resume, fresh exhaustive/grouped rejection, and text/JSON EXPLAIN.
Selective prefix and inclusive/exclusive range controls compare complete public
and trusted page results. Live/exhaustive rejection observes zero visited rows;
this is a functional admission invariant, not a performance measurement.

Four session regressions reproduce the defect before correction. The resumed
fixture initially hit the trusted/public envelope mismatch; rebinding the
current authenticated token through the canonical encoder reproduces actual
public admission. Two sparse-index regressions exposed stale expectations from
A18: they now qualify typed implicit-sort rejection and retained completeness
through supported full composite-index order, including nullable rows and
continuation. No production ordering rule is weakened.

Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Thirteen incremental files
add approximately 480 net lines; production adds seven lines, including comments.
Implementation shape stays neutral: the existing projection now carries the
missing fact to one policy owner. The standing repair discipline is recorded
above and linked from AGENTS.md. Existing dirty work, dependency edits and
published notes are preserved. The next proposed outcome is `query-expr-4`;
unrelated plan-cache, EXPLAIN and DISTINCT findings retain their status.

A20 completes `query-expr-4` through the shared compiled expression owner.
Only `MissingFieldPathValue` becomes UNKNOWN at AND/OR operand boundaries;
required-slot, reader and persisted-decode failures retain their typed errors.
Direct missing comparison, descendant IS NULL, NOT and value-level COALESCE
keep their maintained leaf contracts, while projections still materialize NULL.
No second evaluator, filter mode, value tag, route or persisted state is added.

All 72 focused tests pass. Seven new regressions cover sixty combinations of
missing/null/matched/unmatched descendants, true/false/null siblings, AND/OR and
both operand orders; nested NOT/CASE/COALESCE; projection controls and typed
reader errors. Accepted-schema fixtures qualify cold/warm admitted structural
reads in both execution lanes, ASC/DESC, SQL reads, public/trusted grouped
continuation, and exact UPDATE/DELETE scopes and before images. Three expression
regressions and the SQL mutation regression fail before correction. The other
sixty-five tests protect surrounding compiled functions, canonicalization,
CASE preparation, aggregate filters and scalar/grouped pagination.

Dynamic dotted-field filters separately expose literal/coercion rejection and
loss of descendant traversal during lowering. These remain open observations
in the [closeout audit](closeout-audit.md), outside the saved inventory pending
mapping. Fixtures use maintained SQL lowering into structural FieldPath IR;
A20 does not claim qualification of dynamic nested scalar live/exhaustive calls.
The historical missing-slot portion of `data-4` also remains Partial.

Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Twelve incremental files
add approximately 800 net lines; production adds twenty-five lines, including
comments. Local dispatch grows slightly through one typed helper and arm; all
consumers retain one semantic owner. Existing dirty work, Cargo edits and
published notes are preserved. Branch-ordered DISTINCT (`executor-stream-2`)
is the next proposed outcome in 0.264.

A21 completes `executor-stream-2` by reusing the canonical access contract:
strict leading equality values are a set, and each row belongs to one disjoint
multi-lookup prefix. The planner no longer applies a primary-key-monotonic key
DISTINCT adapter to this secondary order. Composite union/intersection key
deduplication remains ordered; projected-value DISTINCT continues through the
existing adjacent/global accumulators and execution budgets. The unused
materialized key strategy, diagnostic node and access predicate are deleted;
no retained key set, new mode, route or format is added.

All 104 focused tests pass. Eight new regressions qualify decreasing primary
keys across branches, repeated/reordered literals, duplicate projected rows,
nonadjacent expression duplicates, ASC/DESC, cold/warm calls, LIMIT/OFFSET,
identity and primary-order controls, composite access, and unique/non-unique
numeric controls. Public/trusted live pages preserve complete page unions and
every saved resume suffix; exhaustive pages preserve proof-bound continuation.
Four zero-budget controls preserve typed DISTINCT entry/state and row/storage
failures with the exact budget-resource facts. The five initial regressions
fail with the saved invariant before correction. Ninety-six surrounding tests
protect key combinators, monotonic/error controls, projected DISTINCT state and
cursor boundaries, multi-lookup admission, aggregate filters and diagnostics.

Initial clippy warnings are repaired. Required workspace/feature `make clippy`,
strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Fifteen incremental files
add approximately 550 net lines; production removes twenty lines including
comments. Implementation shape gets simpler through an existing access owner
and deletion of an unnecessary strategy. Existing Cargo edits, standing repair
instructions and published notes are preserved.

DESC EXPLAIN separately reports no materialized sort in its admission summary
but emits a materialized-sort descriptor for the projected DISTINCT boundary.
That observation remains recorded in the [audit](closeout-audit.md), alongside
dynamic frontend gaps. No additional inventory finding is closed by inference.
The next proposed outcome is unordered DISTINCT within 0.264.

Full repository suites remain user-owned. Raw Wasm, IC cycles and instruction
deltas are unmeasured. No network lifecycle actions have been taken.

### A22 — Unordered projected DISTINCT

The scoped-audit observation is verified fixed outside the saved inventory.
Ordinary SQL without ORDER BY keeps the planner's absent order and uses the
existing global projected-value accumulator. Cursor emission still requires
resolved order, supplied by accepted primary-key metadata at the cursor frontend.
Adjacent DISTINCT remains justified by resolved-order/group-seek proofs; no
hidden SQL sort or independent strategy, state, route, mode or format is added.

All 56 focused tests pass, including six new regressions and fifty surrounding
projection, retained-output, branch-order, page-limit, grouped-aggregate,
order-contract, SQL lowering and corruption controls. Five initial regressions
fail before correction; the default-order cursor control already passes. The
new matrix covers covering/hybrid-eligible and row-backed selections through
DISTINCT's shared scalar projection flow, residual/empty/full-row results,
expression and canonical NULL/collection keys, cold/warm plans, ordered windows,
public/trusted live suffixes and exhaustive continuation, missing cursor-order
rejection, exact scan ceilings and typed state/row/storage budget facts.

Scalar SQL windows retain the typed unordered-pagination rejection, including
LIMIT 0; ordered controls apply windows after deduplication. Initial fixture API
and unordered-window assumptions were corrected without changing that policy.
Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. The inventory remains
fifteen verified fixes, one partial and 291 unchecked findings; A22 adds no
saved finding by inference.

Ten incremental files add approximately 600 net lines; production adds nine
including comments. Local dispatch grows slightly while preserving one order
authority and the existing accumulators; no state-space axis is added. Full
suites remain user-owned; raw Wasm, IC cycles and instruction deltas are
unmeasured. Cargo edits, standing instructions and published notes are preserved.
No network lifecycle action was taken. Dynamic frontend and DESC EXPLAIN
observations remain separate; further corrections require an audit handoff.

### A23 — Preserve uncovered filters across optimization boundaries

`query-plan-4` and `query-plan-5` are verified fixed. Exact primary-key access
proves only the predicate subset; existing intent coverage now decides whether
the complete expression may be removed. Partial expressions remain active,
while predicate-only and fully covered expression controls retain stripping.
Index-only existing-row aggregate eligibility consumes the existing residual
compatibility proof even when the predicate is absent. That helper currently
feeds aggregate EXPLAIN; its COUNT/EXISTS descriptors are qualified separately
from runtime reductions, rather than claiming a new executor route correction.

All 87 distinct focused tests pass: seven new regressions and eighty surrounding
intent, planning, residual ownership, ordering, continuation, SQL NULL, aggregate
and diagnostic controls. Six new regressions fail before production correction;
the complete-coverage control already passes. The accepted COUNT reproducer
counts four candidates instead of two. The corrected matrix covers both mixed
append orders, ByKey/ByKeys, repeated literals, matching/nonmatching/NULL rows,
public/trusted reads, cold/warm reuse within each scope, ASC/DESC LIMIT/OFFSET,
empty reductions and mutation selection/SQL UPDATE/DELETE controls.

Changing separately appended predicate-only scopes exposed an independent
cache-key defect: an expression-present key omits predicate identity. It can
reuse the prior exact-key plan even across new request sessions. At the A23
handoff, tests explicitly cleared the shared cache between these scopes and
qualified warm reuse within each scope. The [audit](closeout-audit.md) records
the failed reads/counts and source cause. A24 separately fixes cross-scope cache
identity and removes that workaround; no saved cache finding is closed by inference.

Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Twelve incremental files
add approximately 675 net lines; production adds one line including comments.
Implementation reuses existing coverage/residual authorities and removes a
shortcut; no mode, route, proof representation, configuration, state or format
is added. Cargo edits, standing instructions and published notes are preserved.
Full suites remain user-owned; raw Wasm, IC cycles and instructions are
unmeasured. No network lifecycle action was taken.

### A24 — Mixed-filter cache identity

The reproduced audit observation is verified fixed outside the saved inventory.
The ordinary structural key retains the existing normalized predicate fingerprint
alongside expression identity, so separately appended predicate semantics survive
projection into shared preparation. Fully covered parameter templates retain the
existing contract and current-value binding flow. One key builder owns the fix;
an expression-present shortcut is removed without another route, mode,
configuration, state or format.

All 81 distinct focused tests pass: three new regressions, two strengthened
A23 cases and seventy-six surrounding key, intent, template, budget, schema,
read, aggregate and diagnostic controls. Key identity and cross-request reuse
regressions plus the strengthened read and COUNT cases fail before correction.
The matrix qualifies both append orders, changed/revisited equality and multi-key
scopes, forward/reverse sequences, reordered/repeated membership operands,
matching/nonmatching/NULL/missing rows, fresh requests/query syntax, retained
and disabled cache policies, public/trusted reads, COUNT and mutation selection.
The earlier between-scope clearing workaround is removed. An identical revisited
scope remains reusable; a different scope requires its own plan.

At the A24 handoff, a separate singleton-IN control still returned a nonmatching
row with caching disabled and with the fixed key. The plan retained its expression,
but effective runtime preparation chose the remaining predicate alone. The audit
records the reproducer and owner. A25 separately fixes this simultaneous-residual
execution boundary; A24's original equality and multi-key controls did not claim it.

Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Nine incremental files add
approximately 300 net lines; production removes four including comments.
Implementation gets simpler and state-space stays unchanged. Existing dirty
work, Cargo edits and published notes are preserved. The inventory stays at
seventeen verified fixes, one partial and 289 unchecked findings. Full suites
remain user-owned; raw Wasm, IC cycles and instructions are unmeasured. No network
lifecycle action was taken. Its proposed runtime residual correction is
subsequently completed as A25.

### A25 — Complete simultaneous-residual execution

The audit observation is verified fixed outside the saved inventory. Existing
intent coverage controls whether a native predicate can replace the expression.
An uncovered expression otherwise retains any independent residual predicate
inside the existing effective-filter program, and the shared structural/cow
evaluators enforce both. Slot requirements and retained ownership include both
parts. Predicate-only capability projection declines combined programs.

All 116 distinct focused tests pass: three new regressions, two strengthened
singleton-IN cases and 111 surrounding filter, key, template, budget, storage,
NULL, window, continuation, aggregate and diagnostic controls. The new scan and
COUNT cases plus strengthened reads/mutation selections fail before correction.
The matrix includes both append orders, disabled/retained cache policies,
fresh requests, single/multi-key and scan scopes, repeated operands, nonmatching,
NULL and missing rows, counts, windows and mutation selection. Scan marker
controls require both conjuncts, so choosing only the expression also fails.
Cow-reader controls protect TRUE-only admission, rejection short-circuiting,
required expression-reader failures and both required slot sets. Existing
complete-coverage predicate and expression-only controls remain qualified.

Strict all-feature core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Twelve incremental files
add approximately 375 net lines; production adds twenty-one including comments.
Implementation grows modestly to represent a demonstrated conjunction within
one filter authority: valid compiled forms grow from two to three. No user mode,
execution route, configuration, persisted state or format is added. Prior dirty
work, Cargo edits and published notes are preserved. The saved inventory stays
at seventeen verified fixes, one partial and 289 unchecked findings; no saved
coercion/expression finding is closed by inference. Full suites remain
user-owned; raw Wasm, IC cycles and instructions are unmeasured. No network
lifecycle action was taken. This proposed correction is complete; remaining
review findings and earlier dynamic/diagnostic observations retain their status.

### A26 — SQL command failure status

`cli-3` is verified fixed. SQL query, DDL and UPDATE endpoint rejections retain
their error result through existing command execution. The process entrypoint
owns nonzero exit status and stderr; the interactive loop reports the failed
statement and continues. Error decoration stays at these output boundaries,
avoiding a duplicate prefix. No additional mode or dispatch path is introduced.

All 32 focused tests pass: four new process regressions, 24 existing shell
controls and four SQL argument/help tests. The process regressions cover 27
cases: two typed endpoint rejections across all three call lanes and both
one-shot argument forms, successful responses, malformed Candid and transport
errors, and interactive continuation after query/DDL/UPDATE rejection. Calls are
verified against the maintained endpoint names and query/update transport;
rejections require exit 1, empty stdout and a diagnostic on stderr. The endpoint
rejection regression fails before correction with exit 0. Test-local ICP fixtures
use actual child processes without changing global environment or using a network.

Strict all-target/all-feature CLI lint, formatting, documentation references,
inventory and diff checks pass. Nine incremental files add approximately 350 net
lines, primarily process tests and documentation. The implementation gets simpler: one production
file removes one net line including comments and preserves existing result and
output owners. The additional process tests and documentation grow the worktree
without adding runtime state. Prior dirty work, Cargo edits and published notes
are preserved. The inventory is now eighteen verified fixes, one partial and
288 unchecked findings. Full suites remain user-owned; raw Wasm, IC cycles and
instructions are unmeasured. No network lifecycle action was taken. `cli-4` is
the next planned independent outcome; this correction does not close it.

### A27 — Migration command outcome status

`cli-4` is verified fixed. One existing migration dispatcher prints the returned
status and findings, then projects the deployed phase into command success.
`run` requires `Applied`; `abort` requires `Aborted`; bounded `advance` accepts
progress or `Applied` and rejects `Rejected`/`Aborted`. Status inspection remains
successful for every phase. Adoption, explicit confirmation, exact loop identity
and missing-plan/endpoint errors retain their existing contracts. Abort keeps
cleaning through identical rejected pages whose private staging cursor is absent
from public status. No lifecycle state or runtime format changes.

All 14 focused tests pass: seven new migration process regressions, four existing
SQL process regressions and three migration argument/wire controls. The shared
command-status target covers 100 process cases: 73 migration cases and 27 SQL
cases. Migration qualification covers every status phase, bounded progress,
new/existing terminal results, already-applied abort, repeated cleanup pages,
database/plan mismatches, no-progress run, missing plans, confirmation, adoption,
remote rejections and invalid/transport replies on both read and update legs.
Real child status, stdout/stderr, findings and query/update call lanes are checked
without a live network. Three new regressions first fail on unchanged production
with exit 0 for rejected run/advance and already-applied abort.

Strict all-target/all-feature CLI lint, formatting, documentation references,
inventory and diff checks pass. Nine logical files, including the test-target
rename, add approximately 520 net lines, primarily tests and documentation.
One production file adds nineteen net lines,
including comments. Implementation shape stays neutral: one dispatcher owns
outcome checks and output, while helpers return the existing typed status page.
No new mode, configuration, enum variant, execution route, persisted state or
format is added. The SQL process fixture is reused in the renamed command-status
target. Prior dirty work, Cargo edits and published notes are preserved. The
inventory now contains nineteen verified fixes, one partial and 287 unchecked
findings. Full suites remain user-owned; raw Wasm, IC cycles and instructions
are unmeasured. No network lifecycle action was taken. The planned queue is
complete; other findings need their own scoped audit and qualification.

### Validation follow-up — Executor test layout

The user's complete invariant run exposed test-only files classified as
production by the panic scanner. Renaming missing-path and combined-residual
modules to the maintained test-file convention resolves it. The next invariant
check exposed semantic index encoders inside the hybrid component test block;
that block now uses the established test-directory layout. Regression assertions,
runtime logic and both production checks are unchanged. This is direct validation
fallout from A16/A20/A25, not closure of another saved finding. The saved inventory
remains nineteen verified fixes, one partial and 287 unchecked findings.

The complete invariant gate, nine focused projection tests, strict all-feature
core library/test lint and formatting pass. Source comparison verifies unchanged
regression bodies and hybrid runtime functions. Runtime complexity stays neutral;
only test layout and documentation change. Full repository suites remain
user-owned; raw Wasm, IC cycles and instructions are unmeasured.

### Validation follow-up — Public read fixtures

Full validation exposed two stale whole-index read expectations after A19:
catalogue seeding and hidden-order live pagination returned admission code 173.
The catalogue fixture shares a finite key range across setup, typed, staged and
selected reads; the live-page regression constrains its indexed order field.
The regression passes with no default features and with all features; five
admission controls and the 16/128-row PocketIC catalogue regression also pass.
Strict core/canister lint passes. Engine policy and inventory counts are unchanged.
Fixture code grows nineteen net lines with neutral implementation complexity.
SQL fixture raw Wasm grows 508 bytes, from 4,329,400 to 4,329,908. Cycle/instruction
deltas are unavailable because the prior fixture rejected before measurement.
Temporary local PocketIC servers were cleaned up; full suites remain user-owned.

### Scoped numeric audit after published 0.264.4

The completed repair queue requires a read-only audit on generic continuation.
All 43 maintained decimal/numeric tests pass. A disposable probe qualifies division
at all 29 scales and both original text inputs; the maintained core regression
verifies typed overflow. `value-types-error-3` is verified fixed by existing code.
The same audit reproduces `model-schema-crates-1` across four multiplication
operators and checked powers; this finding is Open and proposed as A28.
Boundary controls, consumer limits and the next correction's scope are in the
[audit record](closeout-audit.md#numeric-audit-after-published-02644).
Only audit/status documentation changes, with no runtime correction or release
entry. Raw Wasm, IC cycles and instructions are unmeasured; full suites are user-owned.

### A28 — Decimal multiplication precision

The user selected the numeric audit's shared correction. An exact temporary
256-bit product and the existing rounding owner distinguish precision loss from
magnitude overflow. Results round half away from zero to the greatest fitting
scale, at most 28, using the original product on each retry. Primitive operators,
products, powers and checked runtime arithmetic now agree. Formats and stored
field scales are unchanged; unrelated arithmetic findings remain unverified.

All 51 focused tests pass: 30 schema decimal, 18 core numeric and three accepted
SQL binding/parity tests. The numeric tests also pass without default features.
Six new regressions qualify all 3,364 scale/sign combinations, wide signed
mantissas, rounding ties/underflow, double-rounding avoidance, operator/power
parity and 16 cold/warm stored-field SQL outcomes. True multiplication and
division overflow retain typed errors. Three precision tests failed before
correction. Strict schema/core lint, formatting and documentation/invariant
checks pass. Production adds 23 net lines: local arithmetic is more complex,
but one rounding owner and one flow are retained, with no new behavior axis.
Raw Wasm, IC cycles and instructions are unmeasured; full suites remain user-owned.

### Decimal alignment and remainder audit after A28

The completed queue requires a scoped read-only audit on continuation. Current
source and a disposable probe reproduce two further findings, now marked Open.
Addition/subtraction saturate at the aligned scale after intermediate overflow;
`1e30 + 1e-28` becomes about `1.7e10`, and the negative subtraction control even
changes sign. True division overflow `1e37 / 1e-28` saturates to about `1.7e20`.
Assignment and iterator consumers inherit the same owner. These remain an
independent correction from remainder; A28 does not cover them.

For `5.0000000000000000000000000001 % 10000000000000`, the exact remainder is
the dividend. Current `checked_rem` returns `None`, `%`/`%=` return zero, and the
application `MultipleOf` validator records no issue. The probe covers 112
scale/sign cases at scales 1–28: twelve cases at scales 26–28 falsely accept a
non-multiple, while the other hundred and ordinary/zero-divisor controls pass.
Checked core arithmetic and accepted rules share `checked_rem` but fail rather
than falsely accepting; this is source evidence, not a new SQL execution receipt.

Proposed A29 fixes this boundary in the existing Decimal arithmetic owner,
reusing exact temporary wide arithmetic and narrowing the exact remainder after
alignment. One operand remains unscaled, and remainder magnitude is bounded by
both operands, so nonzero admitted divisors have representable exact remainders.
Qualify primitive/assignment, application validation, checked numeric and
accepted-rule consumers, signed limits and zero divisors. No validator-specific
arithmetic, mode, format or state is needed.
Addition/subtraction/division saturation remains separately Open.

The current schema/model libraries build, and all 54 focused maintained tests
pass: 30 schema decimal, four application numeric validators, 18 core numeric
and two accepted-rule multiple-of controls. Documentation/inventory checks pass.
Probe source and receipt are `/tmp/icydb-decimal-alignment-probe.rs` and
`/tmp/icydb-decimal-alignment-probe.log`. Its successful assertions confirm
defects, not correct product behavior. Only the audit, this status and the 0.264
tracker change in this handoff; earlier dirty A28 work is preserved. Runtime
complexity stays unchanged. Raw Wasm, IC cycles and instructions are unmeasured;
full suites remain user-owned.

### A29 — Exact decimal remainder

The user selects the proposed shared correction. Decimal aligns operands in
existing temporary wide arithmetic, computes the exact remainder, and narrows
only that result. Its scale is the greater operand scale and its sign follows
the dividend. One operand remains unscaled and bounds the remainder, so admitted
nonzero divisors cannot cause representation overflow. Primitive/assignment,
application validation, accepted-rule numeric and checked runtime consumers
share the corrected owner. No consumer-specific workaround or new behavior
axis is added; the alignment/saturation finding remains separately Open.

All 64 focused tests pass: 32 schema decimal, five application numeric validators,
19 core numeric, three accepted-rule numeric controls, four SQL binding/parity
tests and one targeted-rule evaluation control. The numeric tests also pass
without default features. Six new regressions fail before correction and pass
afterwards, covering 25,230 scale/mantissa pairs against an independent BigInt
oracle, signs, signed extrema, exact multiples, zero divisors and 16 cold/warm
stored-field SQL remainder outcomes with NULL controls. Strict schema/model/core
lint, formatting and direct authority/format/panic/documentation guards pass.

Production adds six net lines in one remainder flow; implementation shape stays
neutral with no mode, format or state added. Earlier dirty A28/audit work and
Cargo versions are preserved. No network lifecycle action occurs. Raw Wasm,
IC cycles and instructions are unmeasured; full suites remain user-owned.
The inventory now has 22 Verified fixed, one Open, one Partial and 283 Needs
verification. A29 is complete; the repair queue is complete at this handoff.
A29 touches twelve files, approximately +360 net lines, chiefly tests/docs;
the complete dirty worktree also retains earlier A28 and audit changes.

### Arithmetic result qualification audit after A29

The completed queue requires a scoped audit. `model-schema-crates-3` remains
Open: original addition/subtraction and true division-overflow examples still
reproduce. Further boundary controls find `MIN - MIN` returns a -1 mantissa at
all 29 scales, despite an exact zero result; checked subtraction returns `None`.
Intermediate addition overflow can also lose a representable cancellation.
Dividing signed MAX at scale zero by the same signed magnitude at scales 1–28
fails checked division in all 112 sign/scale cases, although exact quotients
are signed powers of ten. The primitive fallback substitutes an unrelated bound.
Four true-overflow multiplication sign cases also clamp near 17 billion because
the fallback keeps scale 28 instead of the global magnitude bound. That last
observation is outside the saved finding's original scope; A28 still closes its
excess-fractional-precision defect, not this independent fallback-scale defect.

Proposed A30 is one result-qualification correction in the Decimal owner: use
existing temporary wide arithmetic before narrowing addition/subtraction and
division, reuse the current rounding owner to select the greatest fitting scale,
and clamp primitive true magnitude overflow at scale zero across arithmetic.
Addition/subtraction follow multiplication's maintained half-away rounding and
28-digit ceiling; division keeps its 18-digit ceiling while retrying failed
rounded-result narrowing. Qualify checked/primitive/assignment/iterator consumers,
running SUM/AVG helpers and stored-field SQL, with signed limits, cancellation,
ties, underflow and typed overflow/zero-divisor controls. No new mode, format,
state or parallel arithmetic owner is needed. This is a proposed semantic change;
the audit does not implement it or change existing rejection behavior.

All 55 maintained tests pass: 32 schema decimal, 19 core numeric and four SQL
binding/parity tests. They do not cover these additional defects. The disposable
probe `/tmp/icydb-a30-saturation-audit.rs` links the current schema library;
`/tmp/icydb-a30-saturation-audit.log` retains its successful defect observations
and ordinary/zero-divisor controls. New core/SQL defect cases are source evidence,
not new end-to-end query receipts. Only three audit/status documents change;
earlier dirty A28/A29 work is preserved. Runtime complexity stays unchanged.
Raw Wasm, IC cycles and instructions are unmeasured; full suites remain user-owned.
Counters remain 22 Verified fixed, one Open, one Partial and 283 Needs verification.

### A30 — Arithmetic result qualification

The user selects the proposed correction. Addition/subtraction align in the
shared wide owner and reuse multiplication's greatest-fitting rounding helper.
Division uses wide operands and retries rounded-result narrowing up to its
maintained 18-digit ceiling. Primitive true magnitude overflow has one scale-zero
bound authority; subtraction reuses exact decimal ordering for the bound's sign,
including the asymmetric MIN boundary. The separate multiplication fallback and
addition sign/scale branches are removed. No mode, format, state or error variant
is added. Running SUM/AVG and stored-field SQL consume the same checked results.

All 71 focused tests pass: 36 schema decimal, 21 core numeric, five SQL binding/
parity, five application numeric validators, three accepted-rule numeric and one
targeted-rule evaluation control. The 21 numeric tests also pass without default
features. Seven new and four updated regressions fail before correction and pass
afterwards. The independent BigInt oracle checks 58,870 addition/subtraction/
division outcomes across all 841 scale pairs, including signed limits, rounding,
cancellation and underflow. A strengthened property checks arbitrary mantissas;
24 new cold/warm stored-field SQL cases cover arithmetic, SUM/AVG and typed
overflow/zero-divisor errors. Existing remainder, power and multiplication
precision matrices stay qualified through the shared owners.

The initial strict lint reports one oracle-test style warning. Required maintainer
Clippy recovery, strict schema/model/core lint and the rerun focused gate pass
after correcting it. Formatting, direct authority/format/panic/schema-model and
documentation/inventory checks pass. Production removes 23 net lines across two
files; implementation gets simpler by sharing alignment, fitting and global
saturation and deleting the separate fallback branches. Earlier dirty A28/A29
work and Cargo versions are preserved; no network lifecycle action occurs.
Raw Wasm, IC cycles and instructions are unmeasured; full suites remain user-owned.
The inventory now has 23 Verified fixed, one Partial and 283 Needs verification.
A30 is complete; the queue is complete at this handoff.
A30 touches eleven files, approximately +420 net lines, chiefly tests/docs;
the full dirty worktree also retains earlier A28/A29 and audit changes.

### Scoped grouped-order audit after A30

The queue is complete, so generic continuation performs the required read-only
audit in 0.264. The filtered-index representation family still spans persisted
metadata and needs a separate design; no claim is made that one small change
closes its seven reports. A smaller shared-owner correction is available:
`executor-aggregate-3` remains visible in current generic grouped finalization.
`finalize_unbounded` requests sorted groups, but `into_sorted_groups` always
uses ascending canonical comparison. Direction is already carried to candidate
construction and is lost at the preceding extraction/sort boundary.

Proposed A31 carries the existing route direction into that sort and reuses
`compare_grouped_boundary_values`, already used by the dedicated COUNT path and
cursor boundaries. No new ordering mode, route, format or state is needed.
Qualify ASC/DESC, bounded/unbounded, generic/dedicated, cold/warm public reads,
compound same-direction keys, offset/HAVING and maintained budget/cursor controls.
Mixed-direction order qualification is a separate finding and remains unchecked.
The [audit](closeout-audit.md#grouped-order-audit-after-a30) owns reproduction
receipts and limits. Nine maintained grouping tests and one defect-observation
probe pass on the disposable source snapshot; the passing probe asserts the
incorrect output and does not close the finding. Documentation/inventory and
whitespace checks pass. No runtime correction or release entry is made here;
earlier dirty work is preserved. The inventory has 23 Verified fixed, one Open,
one Partial and 282 Needs verification. Three audit/status docs change; runtime
complexity is unchanged. Raw Wasm, cycles and instructions are unmeasured;
full suites remain user-owned. A31 is proposed for the next user selection.

### A31 — Grouped sort direction authority

The user selects the proposed correction and requests shared-boundary cleanup.
The demonstrated need is lost route direction before generic unbounded sorting.
The simplest alternative passes that existing fact into the bundle sorter and
reuses `compare_grouped_boundary_values`, already owning ordered group transitions,
bounded candidate ordering, COUNT windows and cursor comparisons. A second output
sort or a DESC-only reversal would leave separate ordering authorities. No mode,
state, format, enum variant or execution route is added; state-space delta is zero.
Text/signed and compound uniform-direction keys, bounded/unbounded output,
public cold/warm reads, cursor suffixes and SQL HAVING/offset are qualified together.
Existing unit/list/set/map/enum tests also compare implicit canonical ordering
between dedicated and generic folds. Explicit list ORDER BY is rejected by the
maintained planner; the exploratory list matrix was corrected to signed scalars.
Mixed-direction admission is independent and is not silently claimed fixed.

A31 is complete. Direction now reaches bundle sorting through the existing
extraction flow; its ascending-only comparison/import is replaced by the shared
grouped comparator. Sorting remains charged once before row shaping; bounded
heap, dedicated COUNT, ordered transitions and continuation comparisons retain
their maintained owner and behavior. One canonical fact reaches every relevant
uniform-direction boundary, with no extra sort or condition for a special query.

All 37 distinct focused tests pass (13 owned-group, 22 grouped-fold and four
planner-order selections, with two overlapping tests counted once). Four new
regressions cover 144 cold/warm public query sequences across text/signed,
single/compound keys, COUNT/SUM/both, ASC/DESC, bounded/unbounded and every returned
cursor suffix; 48 SQL runs qualify projection, HAVING, offset and empty results.
Six zero-resource controls preserve typed sort-entry/comparison/temporary-byte
errors. Maintained unit/list/set/map/enum tests gain implicit-order parity.
Two meaningful regressions fail before the correction. The exploratory explicit
list-order test instead exposes maintained typed rejection, so its matrix uses
signed scalar keys; no list-order admission is added.

Initial strict lint finds three test-style warnings; the first maintainer rerun
still finds the helper two lines over its limit. A direct allocation-free zipped
assertion resolves them. Final required maintainer lint, strict all-feature core
lint and the rerun focused gate pass. Formatting and direct panic/layer/persisted/
schema-model guards pass; documentation/inventory and whitespace checks pass.
Receipts are `/tmp/icydb-a31-before.log`, `/tmp/icydb-a31-final-*.log` and
`/tmp/icydb-a31-clippy-recovery-final.log`.

Production adds four net lines across two files; one shared comparison authority
replaces the ascending-only sort. The implementation gets simpler in ownership,
with no new behavior axis. A31 touches eight files, approximately +385 net
lines, primarily tests and docs. Earlier dirty arithmetic work and Cargo versions
are preserved; no commit, push or network lifecycle action occurs. Raw Wasm,
cycles and instructions are unmeasured; full suites remain user-owned. The
inventory has 24 Verified fixed, one Partial and 282 Needs verification. The
queue is complete; generic continuation performs a read-only audit in 0.264.
Mixed-direction admission/order qualification remains a separate follow-up.

### Filtered-index cluster audit after A31

The user requests further bugs with more time spent rethinking clusters. The
completed queue makes this a scoped read-only audit within 0.264. Four saved
findings remain visible at one representation boundary: typed literal loss,
text-based contract identity, unrelated generated-predicate rewriting and
sequential rename capture. Three disposable observation tests reproduce their
literal/comparison and rewrite seams, with six maintained tests passing alongside
them. These are boundary receipts, not full query, uniqueness, startup or migration
receipts, and passing defect assertions do not mean product correctness.

The [cluster design](filtered-index-cluster.md) records per-finding evidence,
qualification gaps and a proposed accepted predicate owner. Reuse the accepted
literal/path and executable semantics authorities, but do not replace supported
filtered DDL with the narrower existing CHECK tree. That tree lacks the maintained
nested-path and prefix LIKE/ILIKE/coercion vocabulary. Raw text canonicalization
alone leaves the other symptoms. Proposed A32 replaces text authority end-to-end
under current version-1 hard-cut rules; no parallel representation, fallback,
ordering mode or new predicate cache is planned. Each cluster member needs its
own closure proof; three remaining reports stay unchecked.

Source-copy comparison, documentation/inventory and whitespace checks pass.
Five audit/design/status docs change; runtime code, dirty A28–A31 fixes, versions
and active changelog remain intact. Cost metrics are unmeasured, no network action
occurs and full suites remain user-owned. Current counts are 24 Verified fixed,
four Open, one Partial and 278 Needs verification. A32 is proposed for the next
selection in 0.264; this handoff implements no representation correction.

## Finding inventory

All 307 distinct, non-refuted findings are listed once. Duplicate aliases and the
four refuted raw findings remain in the source review and are excluded here.
Counts cover inventory states, not implementation or release readiness.

| Finding | Review severity | Current status | Review subject and current evidence |
| --- | --- | --- | --- |
| `index-access-1` | critical | Verified fixed | Filtered (partial) unique index: conflict re-check ignores the index predicate, so a committed batch cannot be folded or replayed (wedge risk) **Current evidence:** C76; four filtered-unique online fold/restart regressions pass. |
| `query-plan-1` | critical | Verified fixed | Two-sided PK range `pk >= a AND pk < b` has its predicate stripped although KeyRange end is inclusive: the row with pk == b is returned, counted, updated or deleted **Current evidence:** C72; five endpoint/read/count/mutation/pagination regressions pass. |
| `r2-cardinality-freshness-1` | critical | Verified fixed | Migration staging/abort folds the live overlay into canonical outside the journal protocol: durable prefix counts drift (rows vanish from indexed reads) and later folds wedge on count underflow **Current evidence:** C74; four migration staging/abort overlay regressions pass. |
| `r2-upgrade-lifecycle-1` | critical | Verified fixed | Renaming or moving a journaled store's Rust type is treated as STORE_CORRUPTION and permanently bricks the canister; reverting does not clear it **Current evidence:** C79; three store-path rejection and rollback regressions pass. |
| `schema-catalogs-1` | critical | Verified fixed | Record-member rename is metadata-only, but stored record values use member names as keys, so every existing row with that record becomes undecodable **Current evidence:** C77; three populated record rename/abort/recovery regressions pass. |
| `commit-1` | high | Needs verification | Online background folding routes batches through the LIVE accepted catalog, so a metadata-only entity rename published with retained debt makes older batches permanently unfoldable |
| `data-1` | high | Verified fixed | Scalar fast path reports corruption for rows filled by a non-null historical default (ADD COLUMN ... DEFAULT) **Current evidence:** Existing required_historical_scalar reuses accepted materialization for absent slots. Six maintained regressions pass across Nat64/Int64/Bool/Text/Blob, scalar/structural codecs, NULL/non-null fills, repeated scalar/projection reads, SQL filters and byte length before row rewrite, and malformed/rejected fills. This audit verifies an existing fix; it adds no runtime correction. See [row-boundary audit](closeout-audit.md#row-value-boundary-audit-after-a32). |
| `data-2` | high | Verified fixed | Value-storage walker misreads canonical enum envelopes (tag 0x84), so nested record-path reads fail for records that contain an enum **Current evidence:** A33 routes borrowed selection and owned materialization through canonical current enum framing. Unit/payload/nested enums and scalar siblings in both wire orders pass; published public/trusted full reads match nested SQL projection, filters and grouped keys on repeated calls. See [A33 qualification](#a33-current-value-traversal-authority--2026-10-03). |
| `data-4` | high | Partial | Byte-level readers treat a legitimately absent historical slot as corruption (resumable UPDATE, nested field paths) **Current evidence:** C101 fixes resumable UPDATE; nested field-path readers remain unverified. |
| `executor-aggregate-1` | high | Verified fixed | Generic hash GROUP BY 'DirectOwned' probe path never looks up existing groups: one group per row for enum/unit/collection/composite keys **Current evidence:** A17 reuses canonical owned keys in existing hash buckets; all 66 focused tests pass, including accepted unit/enum/list/set/map COUNT/SUM, single/multi-field limits, nested canonical values, collision and malformed-bucket checks. |
| `executor-aggregate-3` | high | Verified fixed | Unbounded generic grouped finalization ignores DESC: groups always come back in ascending key order **Current evidence:** A31 carries route direction into the shared grouped comparator before generic bundle sorting. All 37 distinct focused tests pass, including text/signed and compound uniform-direction keys, COUNT/SUM/both, bounded/unbounded, public cold/warm queries, cursor suffixes, SQL HAVING/offset and typed sort-budget errors. Mixed-direction ordering remains separate. |
| `executor-aggregate-5` | high | Needs verification | Zero-key global DISTINCT aggregate fails with an invariant error on any NULL; its NULL semantics also diverge from the per-group path |
| `executor-stream-1` | high | Needs verification | Resumed secondary-order IN-list pages cap each branch at limit+1 with no resume anchor, silently dropping rows |
| `executor-stream-5` | high | Needs verification | Resumed non-PK-ordered pages never reposition physical streams: each page rescans from the range start (quadratic traversal, deep pages exhaust the budget) |
| `index-access-2` | high | Verified fixed | Timestamp index components use unbiased two's-complement bytes, so negative timestamps sort after positive ones **Current evidence:** A14 reuses signed encoding; primitive order and accepted unique/non-unique equality/range/pagination regressions pass. |
| `index-access-3` | high | Verified fixed | Index keys cap the primary-key suffix at 63 bytes but composite PKs can encode up to 254 bytes, so inserts fail on indexed entities **Current evidence:** A15 shares the 254-byte primary-key bound; full-width principal/account inserts, unique conflicts, admitted index ranges, resumed reads, bounded decode and stable reopen tests pass. |
| `model-schema-crates-1` | high | Verified fixed | Decimal `Mul`/`MulAssign`/`Product`/`powu` return ~±1.7e10 when the exact product needs >28 fractional digits **Current evidence:** A28 keeps an exact temporary wide product and shares maintained rounding across operators, powers and checked arithmetic. All 51 focused tests pass, including 3,364 scale/sign combinations, wide signed mantissas, ties, underflow, final-scale rounding, stored-field SQL and true-overflow controls; core numeric tests also pass without default features. |
| `query-expr-2` | high | Needs verification | The FALSE set of a SQL NOT is compiled as a two-valued predicate NOT, so rows where the inner comparison is UNKNOWN (NULL operand) pass |
| `query-expr-4` | high | Verified fixed | In expression-lane filters, a missing nested path aborts the whole row, so an OR with a true sibling still rejects it **Current evidence:** A20 converts only missing-path boolean operands to UNKNOWN in the compiled owner. The 60-case truth matrix, leaf/projection contracts, typed reader errors, accepted structural/SQL reads, grouped continuation and UPDATE/DELETE scopes pass. Separate dynamic dotted-field frontend gaps remain open observations. |
| `query-intent-1` | high | Verified fixed | SortRequiresMaterialization can never fire: the admission summary always reports materialized_sort=false, so public reads admit materialized ORDER BY **Current evidence:** A18 projects the canonical scalar executor sort rule and exact primary-key candidate bounds. Public indexed sorts reject; bounded exact-key exceptions and truthful EXPLAIN pass focused validation. |
| `query-intent-2` | high | Verified fixed | PublicRead counts every secondary-index route as bounded, including the planner's unbounded whole-index fallback, so public pages can full-scan the table **Current evidence:** A19 projects whole-index ranges to logical FullScan while retaining physical index diagnostics; all 54 focused tests pass, including public live/exhaustive/grouped rejection, authenticated resume and selective prefix/range controls. Source alias xc-security-3 shares this closure. |
| `query-intent-3` | high | Needs verification | The shared plan-cache key identifies literals only by an XXH3-128 digest with a public seed when a filter has no parameter template, so a forged collision would serve one caller another caller's plan |
| `query-plan-3` | high | Needs verification | Grouped canonical ORDER BY with mixed per-term directions is admitted, but grouped output is sorted with a single direction |
| `r2-covering-projection-1` | high | Verified fixed | Hybrid covering fails valid SELECTs with an invariant error whenever a projected index component is anything other than Bool/Int/Nat(<=64)/Text/Ulid/Unit **Current evidence:** A16 propagates unsupported-component decline through both hybrid branches; all 42 focused tests pass, including 21 accepted scalar/unit-enum projection cases and typed malformed-component errors. |
| `r2-cross-message-concurrency-2` | high | Needs verification | Each migration Validating or abort page rewrites and heap-snapshots the whole index store, so large stores make both Advance and Abort trap while the database is gated |
| `r2-identity-allocation-1` | high | Needs verification | Dense field-ID renumbering orphans the Identity allocator: removing a field that sorts before the PK restarts Identity::next at 1 (reuse on empty entities; permanent insert failure and integrity corruption after a physical migration) |
| `r2-upgrade-lifecycle-2` | high | Needs verification | Store retirement or addition is committed irreversibly before the same release's schema reconciliation: store-key changes, entity moves and storage-mode switches leave neither the new nor the old wasm usable |
| `r2-upgrade-lifecycle-3` | high | Needs verification | Any store-topology change re-keys every TargetStoreIdentity, orphaning the entity source-lineage catalog; later migrations fail with Unadopted, and adoption is impossible once any entity is past version 1 |
| `schema-application-1` | high | Needs verification | Missing cardinality is treated as corruption; during upgrade startup this persists a terminal failure that blocks the rebuild that would clear it |
| `schema-application-2` | high | Needs verification | Capacity eviction can drop the deployed generated submission's receipt, flipping a Ready database to Recovering with no watchdog running |
| `schema-application-3` | high | Needs verification | Generated reconciliation rejects entities with any live activation, including SQL-DDL-owned ones, and persists it as terminal while the only way to finish the activation needs Ready |
| `schema-migration-1` | high | Needs verification | Entity-local `page.rows == 0` oversize check under a page-global leftover budget deterministically wedges RewritingRows/FinalValidation (non-abortable) and Validating |
| `schema-migration-2` | high | Needs verification | Lineage accepted_head is not re-stamped by SQL DDL, constraint-activation abort, or non-migration builds; every later migration/adoption fails with StaleAcceptedHead |
| `schema-mutation-1` | high | Needs verification | Aborting a SQL unique-index activation leaves staged candidate index entries behind for good, and they later break index DDL and generated schema removal |
| `schema-store-1` | high | Needs verification | Schema publication retain/position sweeps delete or overlay every cardinality record in a single message (unbounded work in DDL and in recovery fold) |
| `session-sql-1` | high | Needs verification | ILIKE/LOWER text predicates mean different things in the expression and predicate lanes; resumable UPDATE uses the expression lane and silently updates the wrong set |
| `value-types-error-3` | high | Verified fixed | Decimal::checked_div panics on i128::MIN / -1 (integer division overflow), which callers can trigger through SQL arithmetic **Current evidence:** Checked wide quotient/remainder avoid the panic. A30 preserves typed true overflow at scale zero and returns a fitting rounded result for the original scale-18 input; maintained schema/core and stored-field SQL regressions pass. The arithmetic oracle qualifies signed extrema across all 29 scales. The original panic remains closed. |
| `value-types-error-4` | high | Verified fixed | Decimal multiplication does not normalize its operands, so fixed-scale (e.g. e18) decimal fields overflow on tiny products like 20 × 20 **Current evidence:** A13 normalizes operands; all 44 focused tests and strict lint pass, including admitted scale-18 SQL reads. |
| `xc-architecture-1` | high | Verified fixed | Filtered-index predicates are persisted as name-based SQL text and re-parsed by a second grammar, losing typed literals: membership, planner implication and uniqueness disagree **Current evidence:** A32 retains canonical typed literals through encoding and shared execution. Nat64/Int128/Decimal membership and actual unique collisions pass; typed Nat64 reads use the eligible filtered index and match a primary scan across continuations and repeated calls. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `xc-security-1` | high | Verified fixed | Cursor HMAC key is deterministic on wasm32/IC, so continuation tokens can be forged **Current evidence:** IC raw_rand feeds boot admission; pending entropy blocks cursors and each boot replaces the key. Five focused entropy/session tests pass; source aliases value-types-error-1 and data-10 share this closure. |
| `canisters-testing-ci-1` | medium | Needs verification | The two Tier A SQLite and mutation oracle lanes match zero tests and pass on every PR |
| `canisters-testing-ci-2` | medium | Needs verification | PR CI never runs, or even compiles, the PocketIC tests for recovery, upgrade, migration, durable jobs and guard authorization |
| `canisters-testing-ci-3` | medium | Needs verification | The dependency_msrv job actually builds with 1.98.1, because rust-toolchain.toml overrides the toolchain the action sets |
| `canisters-testing-ci-4` | medium | Needs verification | Instruction-budget assertions can never fail (the 40B IC limit) or rely on a stale baseline; the 30B recovery allocation is not enforced for trapped recovery |
| `cli-1` | medium | Needs verification | `schema migration run` treats identical status pages as stalled, but core returns identical pages during legitimate multi-page FinalValidation and Idle journal draining |
| `cli-2` | medium | Needs verification | `canister refresh` reinstalls (wipes stable memory) with `--yes` on any environment, including one implicitly selected via ICP_ENVIRONMENT |
| `cli-3` | medium | Verified fixed | One-shot `icydb sql` prints canister-returned errors to stdout and exits 0 **Current evidence:** A26 preserves endpoint errors through existing command results. All 32 focused tests pass, including actual process status/streams across query, DDL and UPDATE, both one-shot forms, malformed/transport controls and interactive continuation. |
| `cli-4` | medium | Verified fixed | Migration `run`/`advance`/`abort` exit 0 on Rejected, and abort exits 0 when the migration was already Applied **Current evidence:** A27 projects returned phases into command outcomes in the existing dispatcher. Fourteen focused tests pass, including 73 migration process cases covering run/advance/abort, phase/finding output, bounded and rejected cleanup progress, identity/error/confirmation controls and successful status inspection. |
| `cli-5` | medium | Needs verification | Interactive shell executes a half-typed statement on Ctrl-D even though the banner advertises Ctrl-D as quit |
| `cli-9` | medium | Needs verification | Migration command Candid text leaves numeric literals untyped (`revision = N`), relying on icp-cli to recover types from canister metadata |
| `executor-aggregate-2` | medium | Needs verification | Grouped page cursor boundary is taken from the projected row, not the canonical group key |
| `executor-aggregate-6` | medium | Needs verification | Grouped continuation never seeks the access stream; resumed pages re-fold and re-count all pre-cursor groups against the cumulative max_groups |
| `executor-aggregate-7` | medium | Needs verification | Zero-key grouped aggregates return no row on empty input, while global-DISTINCT and SQL global aggregates return one row |
| `executor-aggregate-8` | medium | Needs verification | Grouped FIRST/LAST depend on traversal direction and access path (ORDER BY on group keys changes aggregate values) |
| `executor-stream-2` | medium | Verified fixed | DISTINCT over a branch-ordered IN-list stream trips the primary-key monotonicity invariant ('HashMaterialize' is implemented as adjacent dedup) **Current evidence:** A21 reuses disjoint canonical multi-lookup prefixes and removes the redundant key DISTINCT strategy. All 104 focused tests pass, including projected duplicates, direction, windows, cold/warm calls, public/trusted live/exhaustive continuation, typed budget facts and numeric/composite controls. Unordered DISTINCT and DESC EXPLAIN remain separate observations. |
| `executor-stream-3` | medium | Needs verification | IntersectOrderedKeyStream reports no page access bound, so PK-ordered live pages over a general intersection fail with an invariant error |
| `facade-2` | medium | Needs verification | Public SQL renderers print stored user text raw, allowing terminal-escape injection and forged table rows in operator tooling |
| `index-access-4` | medium | Needs verification | Principal index-component order (content-lexicographic) differs from Principal::cmp (length first) |
| `index-access-5` | medium | Needs verification | Oversized equality/range literal on an indexed column returns InvariantViolation instead of a correct result |
| `index-access-6` | medium | Needs verification | Stable index B-tree page size is derived from the 16,477-byte maximum key (~133 KiB of stable memory per node) |
| `index-access-7` | medium | Needs verification | Heap-resident exact prefix-cardinality metadata grows with every distinct index prefix and is never trimmed while the canister runs |
| `integrity-relations-1` | medium | Needs verification | Durable startup failure receipts are bound only to database state, so a fixed upgrade cannot clear a failure caused by a code bug and the canister stays wedged |
| `integrity-relations-3` | medium | Needs verification | Delete-restrict re-projects and re-charges the same surviving source row once per deleted target, so 'update-away plus delete targets' batches hit the relation budget quadratically |
| `integrity-relations-4` | medium | Needs verification | Updates charge both old and new images against a batch limit equal to the per-image limit, so rows with more than about 2,730 references can be inserted but never updated |
| `integrity-relations-5` | medium | Needs verification | prove_empty_reverse_relation_domain scans the entire target index store, capped at 262,144 entries, so relation or entity removal fails on any large target store |
| `integrity-relations-6` | medium | Needs verification | Deep index and reverse phases livelock on an oversized (corrupt) source row: pages report InProgress forever with an unchanged checkpoint |
| `journal-jobs-1` | medium | Needs verification | Any catalog-lookup failure (migration gate, recovery-pending, internal error) permanently ends a mutation job as AcceptedSchemaChanged |
| `journal-jobs-2` | medium | Needs verification | Direct progress-store writes skip recovery admission and can invalidate a pending marker's MutationProgress `before`, making recovery fail permanently |
| `journal-jobs-3` | medium | Needs verification | Durable access-state revision rises by 2 on every startup, so no resumable job or exhaustive cursor over journaled stores survives an upgrade |
| `journal-jobs-4` | medium | Needs verification | Heap-store read-set revisions reset on upgrade (ABA), so a pre-upgrade exhaustive proof and cursor can be accepted against different data |
| `model-macros-1` | medium | Needs verification | Declared normalizers/validators are silently never run in many accepted positions (ty on entity/record/enum/tuple; item normalizers on list/set/map/tuple/enum payloads; item validators on tuple and enum payloads) |
| `model-macros-2` | medium | Needs verification | `IS TRUE` / `IS FALSE` in generated CHECK predicates are lowered to `= TRUE` / `= FALSE`, which lets NULL through under the three-valued check evaluator |
| `model-schema-crates-2` | medium | Needs verification | Account treats `subaccount: None` and `Some([0;32])` as different accounts; core stores and indexes both, and text parsing collapses them |
| `model-schema-crates-3` | medium | Verified fixed | Decimal Add/Sub/Div saturate at the wrong scale when scale alignment overflows, returning values smaller than the dominant operand **Current evidence:** A30 shares wide alignment, greatest-fitting rounding and global primitive bounds. All 71 focused tests pass, including 58,870 exact-oracle arithmetic outcomes, signed limits/cancellation, original examples, checked SUM/AVG and cold/warm stored-field SQL with typed true-overflow/zero-divisor controls. The same owner fixes the directly related multiplication fallback-scale observation. Production removes 23 net lines; no mode or format is added. |
| `model-schema-crates-4` | medium | Verified fixed | Decimal `Rem` returns ZERO on scale-alignment overflow, so the MultipleOf application validator accepts non-multiples **Current evidence:** A29 aligns in exact temporary wide arithmetic and narrows only the remainder. All 64 focused tests pass, including an independent oracle at 25,230 scale/mantissa combinations, primitive/assignment, application validation, accepted-rule numeric, checked runtime and cold/warm stored-field SQL with signed limits, NULL and typed zero-divisor controls. Six regressions fail before correction; numeric tests also pass without default features. |
| `model-schema-crates-5` | medium | Needs verification | Nested relation lowering silently drops relation leaves that are reachable only through a recursive type, contradicting the authoring guide |
| `model-schema-crates-6` | medium | Needs verification | Fragment and migration plan are never composed at build time; a mismatch appears only on the canister and blocks all DB work |
| `model-schema-crates-7` | medium | Needs verification | Numeric validator/normalizer constructors silently replace unrepresentable bounds with 0 (Clamp can rewrite every value to 0) |
| `query-expr-3` | medium | Needs verification | Rewriting a scalar-WHERE CASE into AND/OR drops CASE's lazy evaluation, so a CASE-guarded division errors on the rows it was meant to skip |
| `query-expr-9` | medium | Needs verification | REPLACE with an empty search string inserts the replacement between every character, and text functions can amplify output without being charged to the execution budget (including during plan-time folding) |
| `query-intent-4` | medium | Needs verification | The AND-constraint simplifier restarts at index 0 after every operator replacement, doing caller-driven O(R*N^2) work with no metering before admission |
| `query-plan-2` | medium | Needs verification | Residual stripping discharges case-insensitive `Ne` / `NotIn` using strict equality, removing live filter clauses |
| `r2-candid-stability-1` | medium | Needs verification | icydb_schema returns every entity's full description in one reply with no size guard or paging; a valid large schema makes every call trap |
| `r2-cardinality-freshness-3` | medium | Needs verification | Derived-cardinality inconsistency aborts the authoritative journal fold (STORE_CORRUPTION, terminal), although the 0.230 contract says it only makes evidence unavailable |
| `r2-covering-projection-2` | medium | Needs verification | Pure covering over an undecodable component kind scans and buffers the whole index range, throws it away, then the scalar path scans again under the same hard budget |
| `r2-cross-message-concurrency-3` | medium | Needs verification | A Building cardinality generation restarts on every journal fold, so it never finishes on large, busy stores; the watchdog runs forever and later stores never get a build |
| `r2-persisted-sql-text-1` | medium | Verified fixed | DDL RENAME COLUMN reorders field-to-field predicates in generated filtered indexes, so the next generated schema release fails startup reconciliation **Current evidence:** A32 preserves FieldIds and canonical field-comparison identity through rename. The complete migration fixture reloads current encoded catalog bytes and generated reconciliation returns no changes. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `r2-query-call-caches-2` | medium | Needs verification | Folding a schema batch after C1 was rebuilt leaves the store bundle cache empty; C1 hits never refill it, so admitted-root cardinality evidence is silently unavailable and query calls re-decode the bundle |
| `r2-recursive-bounds-1` | medium | Verified fixed | Value-storage materializing decoder counts two depth units per nesting level, so nested-path reads reject (as Corruption) values the canonical write path accepted up to depth 64 **Current evidence:** A33 deletes the duplicate recursive decoder and its private limit. Borrowed validation, canonical and runtime materialization share the accepted depth owner; lists/maps/enum payloads pass at the exact limit and reject one level beyond it. A published 48-level list projects correctly alongside enum siblings. See [A33 qualification](#a33-current-value-traversal-authority--2026-10-03). |
| `r2-recursive-bounds-2` | medium | Needs verification | Recursive row decoders re-skip every subtree at each nesting level: decode cost is O(bytes x depth), up to ~63x write cost, while all row/page budgets are byte-based |
| `r2-replay-cost-1` | medium | Needs verification | Migration journal fold reloads and fully re-decodes the durable migration record twice per journal record, so large migrations produce rewrite pages that can never be folded |
| `r2-replay-cost-2` | medium | Needs verification | Unique-validation page fold re-reads, re-hashes, decodes and re-fingerprints the whole canonical accepted-schema bundle once per staged index key |
| `r2-replay-cost-3` | medium | Needs verification | DDL user-index replacement batches: the fold re-fingerprints the entity snapshot for every 64-key chunk, and admitted batches can hold 2x the 65,536-effect maximum that the convergence evidence proves and measures |
| `r2-upgrade-lifecycle-4` | medium | Needs verification | Deterministic registry-reconciliation rejections are non-terminal: the startup watchdog retries every second forever, re-reading the full control slot, while startup_state() reports Recovering |
| `r2-upgrade-lifecycle-5` | medium | Needs verification | A metadata-only migration touching only heap stores never wakes the stopped startup watchdog, so the canister stays Recovering after Applied until another upgrade |
| `repro-migration-1` | medium | Needs verification | Physical migration fails at Validating whenever dense field removal renumbers a retained pre-existing field |
| `schema-application-4` | medium | Needs verification | Pending application needs an exact precomputed final head; any intervening publication leaves the job unfinishable and unabortable, holding a record slot forever |
| `schema-application-6` | medium | Needs verification | Admission does not enforce the entity-name rules (64-byte limit, case-insensitive uniqueness) that the runtime root requires; accepted schemas can fail every runtime-root compile |
| `schema-application-7` | medium | Needs verification | Nested-leaf expansion during lowering has no budget; a record DAG with wide reuse can trap before any size check |
| `schema-catalogs-2` | medium | Needs verification | DESCRIBE / SHOW COLUMNS expand composite types with no depth limit or cycle check, so recursive (or heavily reused) composite types trap the call |
| `schema-migration-3` | medium | Needs verification | Plan-less generated source changes silently desynchronize lineage version/digest, later blocking unrelated migrations with MissingMigration |
| `schema-migration-4` | medium | Needs verification | Dangling lineage entries (removed or omitted entities) make current_proposal_lineage_is_applied permanently false: status never reports Applied and the successor fails startup with Downgrade |
| `schema-migration-5` | medium | Needs verification | A Rejected migration cannot be retried after fixing data: exact retry stays Aborted, generated submission identity cannot change, and findings beyond the first page are unobservable |
| `schema-mutation-2` | medium | Needs verification | Complete-domain staging charges its budget for every index entry and scans every row in the store, not just the target entity's |
| `schema-mutation-3` | medium | Needs verification | CHECK INTEGRITY reports a pending targeted-rule replacement as corruption of the accepted rule |
| `schema-mutation-4` | medium | Needs verification | Verify phase restarts on any write to any entity in the store, so activations can starve while unique activations block inserts |
| `schema-mutation-5` | medium | Needs verification | Check activations that depend on more than 32 fields can never persist findings: VALIDATE fails with a corruption error |
| `schema-store-3` | medium | Needs verification | SchemaStore::init_journaled uses StableBTreeMap::init, which silently reinitializes a non-empty schema allocation with a bad header |
| `schema-store-4` | medium | Needs verification | Heap-store identity allocation fully decodes, re-verifies and rewrites the whole live checkpoint (bundle up to 16 MiB plus identity inventory) on every write |
| `session-sql-2` | medium | Needs verification | SQL compiled-command cache is bounded by entry count only and keeps full SQL text twice plus literal payloads, so heap can be exhausted |
| `session-sql-3` | medium | Needs verification | Resumable job gets permanently stuck in Active when one matching row deterministically fails write admission (e.g. a row-local CHECK) |
| `session-write-3` | medium | Needs verification | Replacing an existing row regenerates non-PK generated Ulid/Timestamp fields and turns identical replaces into logical changes |
| `sql-parser-1` | medium | Needs verification | ORDER BY arithmetic sub-parser recurses on parentheses with no depth guard (stack overflow instead of ExpressionDepthLimit) |
| `sql-parser-2` | medium | Needs verification | Scope normalization rewrites a record path whose inner segment matches the entity or alias name to a top-level field (wrong column in SELECT/UPDATE/DELETE) |
| `sql-parser-3` | medium | Needs verification | Numbers in scientific or hex notation are silently split into a number plus an implicit projection alias |
| `sql-parser-4` | medium | Needs verification | DDL accepts dotted multi-segment column names in ADD COLUMN / RENAME COLUMN, persisting top-level fields that SQL cannot address consistently |
| `sql-parser-5` | medium | Verified fixed | Filtered-index predicate identity is raw token text: IF NOT EXISTS and duplicate-contract detection break on formatting and after any RENAME COLUMN **Current evidence:** A32 compares canonical bound trees. Maintained DDL tests cover parentheses, spacing, case and repeated guards through IF NOT EXISTS and active/candidate duplicate contracts, with differing predicates remaining conflicts. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `value-types-error-2` | medium | Needs verification | Numeric-widening compare/eq sends floats (and Nat128 values ≥ 2^127) through a lossy i128 Decimal; out-of-range values compare as None and rows silently drop out of WHERE filters |
| `value-types-error-6` | medium | Needs verification | Persisted LOWER/UPPER index expression keys depend on the Rust toolchain's Unicode tables, with no version pinning |
| `xc-performance-1` | medium | Needs verification | Every numeric ORDER BY / top-K / MIN-MAX comparison goes through Decimal, with u128 digit-string expansion (and float-to-text-to-parse for Float64) |
| `xc-performance-2` | medium | Needs verification | Bounded top-K window is O(N·K) and charges K+1 SortComparisons per row, so moderately large LIMIT/OFFSET without an index-backed order fails deterministically |
| `xc-performance-3` | medium | Needs verification | Validated full-row decode decodes each slot 2-3 times (validation results thrown away), against the row-contract rule |
| `xc-performance-5` | medium | Needs verification | Write amplification: each saved row is fully validated 4-6 times, re-encoded to canonical form at commit, and its bytes copied repeatedly |
| `xc-robustness-2` | medium | Needs verification | Scalar page kernel preallocates Vec::with_capacity(OFFSET+LIMIT+1) unclamped, so large OFFSET/LIMIT traps before any budget check |
| `xc-security-2` | medium | Needs verification | Startup watchdog replans deterministic migration-planning failures every round with no backoff, log or receipt |
| `canisters-testing-ci-10` | low | Needs verification | The SQL coverage manifest accepts `#[ignore]`d tests, and tests never run in CI, as satisfied evidence |
| `canisters-testing-ci-11` | low | Needs verification | The persisted-format version scan matches on names, so `*_POLICY_REVISION = 2` counters folded into a persisted identity slip through |
| `canisters-testing-ci-5` | low | Needs verification | The frozen canister endpoint policy checks only `icydb_*` exports; 'production' artifacts of test canisters still export unauthenticated trusted-write/SQL methods |
| `canisters-testing-ci-6` | low | Needs verification | CI downloads and runs PocketIC and actionlint with no checksum pinning, and the release-artifact job installs unpinned tools |
| `canisters-testing-ci-7` | low | Needs verification | The pre-commit hook re-stages only the root Cargo.toml, and CI's format gate skips the cargo-sort checks that the local fmt-check requires |
| `canisters-testing-ci-8` | low | Needs verification | TESTING.md's 'authoritative' taxonomy is stale and contradicts the repository |
| `canisters-testing-ci-9` | low | Needs verification | Contract tests assert source-file text, which TESTING.md prohibits and which can pass while the live constant has changed |
| `cli-10` | low | Needs verification | Statement splitter treats backslash as an escape inside strings, but the IcyDB lexer does not |
| `cli-11` | low | Needs verification | Substring error classification: any 'replica' error becomes 'local network not reachable', and other icp failures become 'not created' with stderr discarded |
| `cli-12` | low | Needs verification | Refresh silently downgrades from reinstall to upgrade when the status probe errors |
| `cli-13` | low | Needs verification | Dead string-matching recovery hint contradicts the 'don't match on error strings' rule |
| `cli-14` | low | Needs verification | NULL rendering is ambiguous (text 'null' shown as SQL NULL) and inconsistent between the query and UPDATE…RETURNING paths |
| `cli-15` | low | Needs verification | INSTALLING.md contradicts the code on the default environment and on refresh with a missing fixture method |
| `cli-16` | low | Needs verification | Diagnostic artifact lookups use binary search, but validation never enforces sorted IDs |
| `cli-17` | low | Needs verification | Shell history I/O errors are fatal and happen before the statement runs |
| `cli-18` | low | Needs verification | Abort loop has no iteration bound or progress guard |
| `cli-19` | low | Needs verification | Live-schema resolution failure in `diagnostic --canister` aborts the command instead of falling back |
| `cli-20` | low | Needs verification | CLI parsing tests depend on the ambient ICP_ENVIRONMENT variable |
| `cli-6` | low | Needs verification | Interactive line normalization rewrites the content of multi-line string literals before execution |
| `cli-8` | low | Needs verification | `canister upgrade` installs a wasm from a hard-coded, CWD-relative, environment-independent path instead of what `icp build` produced |
| `commit-3` | low | Needs verification | Marker encoding never checks the bytes it wrote against the precomputed envelope lengths |
| `commit-4` | low | Needs verification | CommitGuard has no Drop safety net, so the rule 'retained marker => wake-up registered' holds only by caller discipline, and one early return already bypasses it |
| `commit-6` | low | Needs verification | The schema-fingerprint check in commit preparation is tautological, and replay never validates recorded fingerprints despite the contracts |
| `data-12` | low | Needs verification | NULL for a nullable List/Set<Relation> has no working encode or decode path |
| `data-5` | low | Needs verification | Rows larger than 4 MiB are rejected with an Internal/Serialize error instead of a limit error |
| `data-6` | low | Needs verification | By-kind decode accepts non-canonical or out-of-contract persisted bytes without failing |
| `data-7` | low | Needs verification | Historical-fill validation uses a decoder that cannot read canonical-wire enum/composite payloads |
| `data-8` | low | Needs verification | Fresh-boot convergence writes store memory before later fallible steps; a failure leaves memory that can never be admitted |
| `data-9` | low | Needs verification | Database-format observation and admission disagree on what counts as uninitialized control memory |
| `executor-aggregate-10` | low | Needs verification | Grouped limit failures are surfaced as generic execution-budget errors that are not distinguishable from scalar DISTINCT exhaustion |
| `executor-aggregate-9` | low | Needs verification | Ordered DISTINCT group seek hard-codes MissingRowPolicy::Error regardless of the query's policy |
| `executor-core-1` | low | Needs verification | Instruction ceilings count from each tracker's own start, so every mutation-job advance or maintenance allowance gets a fresh 30B regardless of work already done in the message |
| `executor-core-2` | low | Needs verification | Read executions never set an instruction baseline at start, so work before the 64th charge (or the first charge of 1MiB or more) is counted by neither the execution nor the request scope |
| `executor-core-3` | low | Needs verification | Checks inside the commit window that can fail after marker publication return Err to the caller while the marker is kept, so the reported-failed write is later committed by recovery |
| `executor-core-4` | low | Needs verification | The commit path marks every touched store's index Ready after each write, which is a latent visibility hazard and an Err that can follow a durable commit |
| `executor-core-5` | low | Needs verification | Instruction-budget and production commit-apply behavior are structurally untestable in the native test suite |
| `executor-stream-10` | low | Needs verification | Branch-ordered prefix family skips construction charging and child-count validation that the merged path enforces |
| `executor-stream-6` | low | Needs verification | Lookahead row is scanned and then discarded; an exactly-full final page still returns a non-null continuation (contract mismatch) |
| `executor-stream-7` | low | Needs verification | Residual-retry stop condition `post_access_rows > keep_count` is unreachable, so full pages trigger redundant widened re-scans |
| `executor-stream-8` | low | Needs verification | PrimaryRangeKeyStream physical seek drops buffered and remaining keys when the target lies inside the loaded chunk (latent) |
| `executor-stream-9` | low | Needs verification | Effective offset uses logical-boundary presence while keep caps use has_progress (latent offset skip/over-skip) |
| `facade-1` | low | Needs verification | Participant lifecycle mode silently compiles when the app forgets the participant call; database stays Recovering indefinitely |
| `facade-3` | low | Verified fixed | Grant and admission misconfiguration surfaces as an opaque E23, and the re-exported ic_memory_range! defaults to Reserved. **Current evidence:** A34 reproduces six typed failures before classification changes, then preserves distinct E276–E281 leaves through public errors, startup diagnostics and Candid. Generated startup/db access agrees for Reserved-default, missing-grant, invalid-declaration and incomplete-role rejection; explicit Allowed succeeds. Reserved remains the upstream default and cannot supply fresh logical slots. See [A34 qualification](#a34-memory-admission-diagnostics--2026-10-03). |
| `facade-4` | low | Needs verification | Production assert! in RequestExecutionFuture::poll can trap when a started future is polled under another request root |
| `facade-5` | low | Needs verification | SQL reply-size guard uses the 3 MiB non-replicated limit even when icydb_query runs in replicated mode |
| `facade-6` | low | Needs verification | crates/icydb/README.md contradicts the facade's model re-export and the workspace README |
| `facade-7` | low | Needs verification | Typed mixed batch repeats binding issuance per item and deep-clones every binding at execute (unmeasured instruction cost) |
| `index-access-10` | low | Needs verification | Raw index-key comparator falls back to raw byte order for undecodable keys, so the stable B-tree order is not transitive |
| `index-access-11` | low | Needs verification | Index-integrity audit doc still describes raw-byte lexicographic ordering; the implementation uses a decode-then-compare comparator |
| `index-access-8` | low | Needs verification | exact_child_prefixes_for_parent_set walks every multi-component prefix of the index on each call, without a budget |
| `index-access-9` | low | Needs verification | visit_raw_entries_in_merged_ranges calls BTreeMap::range without the empty-envelope guard |
| `integrity-relations-2` | low | Needs verification | Abandoned or expired Deep integrity jobs are never retired and permanently use per-owner and global progress capacity, including the slots that gate SQL mutation and resumable jobs |
| `integrity-relations-7` | low | Needs verification | Reverse keys from a heap (LiveSource) source into a journaled target exist only in the target's unjournaled live overlay; an online recovery pass resets them while the heap source rows survive |
| `integrity-relations-8` | low | Needs verification | A retention-page failure turns an already-persisted integrity result into an error; a key/payload job-id mismatch makes every integrity request fail permanently |
| `integrity-relations-9` | low | Needs verification | Storage report per-entity memory_bytes uses the maximum key size while store-level memory_bytes uses actual key bytes, so the two totals disagree |
| `journal-jobs-5` | low | Needs verification | append_batch_bytes has fallible steps after its first stable write; any Err leaves the tail in a state that identical replay cannot fix |
| `journal-jobs-6` | low | Needs verification | Prefix-repair branch in append is unreachable in production and loosens the sequence and commit-order checks |
| `journal-jobs-7` | low | Needs verification | Retirement preflight reads and reassembles the entire next batch (up to 16 MiB) just to get its header |
| `model-macros-10` | low | Needs verification | Derived index names can collide (slug normalization, predicate excluded), and the macro does not detect it |
| `model-macros-11` | low | Needs verification | `#[icydb::test]` puts the whole test under one request root, so per-message `with_request_execution` calls inside it share one aggregate budget |
| `model-macros-3` | low | Needs verification | Redundant-prefix index rejection is backwards for unique indexes and ignores filter predicates |
| `model-macros-4` | low | Needs verification | Runtime-adapter detection treats dev-dependencies and all target-specific dependencies as a usable `icydb` dependency |
| `model-macros-5` | low | Needs verification | Generated code uses unqualified prelude names and fixed derived item names, so common user aliases and names break compilation |
| `model-macros-6` | low | Needs verification | Validation and lowering disagree on predicate literals, and default parsing can panic, so proc-macro panics replace spanned errors |
| `model-macros-7` | low | Needs verification | Crate-path rewriting replaces every bare `icydb` / `icydb_model` identifier in the output, including user-authored names and path segments |
| `model-macros-8` | low | Needs verification | User attributes, doc comments, struct fields and generics on the annotated item are silently discarded |
| `model-macros-9` | low | Needs verification | Any string argument containing "::" is reinterpreted as a Rust path, so text defaults and args containing "::" cannot be written |
| `model-schema-crates-10` | low | Needs verification | NatBig/IntBig operators panic (subtraction underflow, division by zero); Decimal instead saturates and returns zero |
| `model-schema-crates-11` | low | Needs verification | Blob literal text means raw UTF-8 bytes for defaults but hex for migration fill literals |
| `model-schema-crates-12` | low | Needs verification | The persisted generated submission-key identity uses a 'v2' domain tag, contrary to the version-1 policy |
| `model-schema-crates-8` | low | Needs verification | Migration closure validation recurses without a depth bound and has superlinear cost on a 'bounded' public proposal |
| `model-schema-crates-9` | low | Needs verification | EntityFragment does not validate primary-key field shape (nullable/list/named/float) |
| `query-expr-10` | low | Needs verification | COALESCE and NULLIF evaluate every argument eagerly, unlike SQL's defined CASE equivalence |
| `query-expr-8` | low | Needs verification | The affine rewrite in grouped/UPDATE WHERE turns an integer literal into a Decimal, which switches Eq/Ne coercion to Strict and breaks non-Int64 field kinds |
| `query-intent-5` | low | Needs verification | The continuation signature and scalar token identify ORDER BY by display label, so order expressions differing only in literal type accept each other's cursors |
| `query-intent-6` | low | Needs verification | The READ_ADMISSION surface inventory omits public read surfaces that bypass QueryAdmissionPolicy |
| `query-plan-4` | low | Verified fixed | Primary-key predicate strip drops the entire filter expression without checking predicate coverage **Current evidence:** A23; existing intent coverage preserves partial expressions through exact-key reads, windows, COUNT and mutation selection; complete-coverage controls retain stripping. A24 separately fixes mixed-filter cache identity; A25 separately fixes simultaneous residual execution. |
| `query-plan-5` | low | Verified fixed | index_covering_existing_rows_terminal_eligible returns true when predicate is None without checking for a residual filter expression **Current evidence:** A23; existing residual compatibility owns absent-predicate eligibility and COUNT/EXISTS aggregate EXPLAIN controls; runtime reductions are qualified separately. |
| `r2-candid-stability-2` | low | Needs verification | Only the migration ABI is gated: other generated endpoint responses and public DTOs have no Candid golden or subtype check, and the CLI decodes strictly with no version handshake |
| `r2-candid-stability-3` | low | Needs verification | Recursive public input DTOs (FilterExpr, PublicValue/InputValue) have no decode-time depth bound on the IC, and FilterExpr's error wrapper re-formats the whole message at every level |
| `r2-persisted-sql-text-2` | low | Verified fixed | Stored filtered-index semantics depend on current parser code, not on the stored text, so parser fixes silently invalidate already-built index contents **Current evidence:** A32 removes persisted SQL and runtime accepted-SQL parsing. Current bounded version-1 trees retain operators, coercions, FieldIds and typed payloads; encoded catalog reload and interrupted-write checkpoint replay preserve membership. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `r2-persisted-sql-text-3` | low | Verified fixed | Migration planner rejects metadata-only enum-variant renames whenever a generated filtered-index predicate names the variant **Current evidence:** A32 retains canonical enum type/variant IDs instead of variant spelling. Variant rename changes display only; the migration fixture combines enum type/variant and field renames, reloads catalog bytes and reconciles generated declarations successfully. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `r2-query-call-caches-3` | low | Needs verification | Heap-store schema publications never wake the watchdog, so the database-wide runtime root stays cold for all query traffic until an unrelated update call |
| `r2-recursive-bounds-3` | low | Needs verification | Source check expressions reach bind_expression with nesting up to ~1020 levels; the binder recurses (and clones) before the 32-level check-tree bound is applied |
| `r2-recursive-bounds-4` | low | Needs verification | Generated-predicate depth bound in the model macros is looser than the accepted check-tree bound, so macro-accepted predicates are rejected at runtime schema acceptance |
| `r2-recursive-bounds-5` | low | Needs verification | Rendered partial-index predicate SQL can exceed the SQL predicate parser's source-depth budget even though the bound check tree is within its own limits |
| `schema-catalogs-4` | low | Needs verification | Catalog decoders accept states that construction rejects (zero-variant enums; invalid kind shapes inside enum payloads, tuples and newtypes) |
| `schema-catalogs-5` | low | Needs verification | DESCRIBE misreports nested structure: leaf names shown without their parent path, nested relation cardinality always 'single', partial unique shown as UNI |
| `schema-catalogs-6` | low | Needs verification | Store registry accepts registrations that share some, but not all, of their data/index/schema stores, or a journal store |
| `schema-catalogs-7` | low | Needs verification | IndexName derivation can give different field lists the same name; db::identity module docs are stale |
| `schema-migration-6` | low | Needs verification | Non-transform-slot ValueContract failures produce a finding with target FieldId(0), which try_new_transform rejects as store_invariant |
| `schema-migration-7` | low | Needs verification | Checked cast into a nullable target rejects every NULL source (NullSource); no transform can change a nullable column's type while preserving NULLs |
| `schema-migration-8` | low | Needs verification | Public migration findings drop the persisted source field, target field and transform reason |
| `schema-migration-9` | low | Needs verification | Pre-rewrite abort scans every index entry of each affected store (including unrelated entities) at 512 entries per call while the database stays gated |
| `schema-mutation-6` | low | Needs verification | A stale receipt acknowledgement (retried VALIDATE ... AFTER n) is reported as store corruption |
| `schema-mutation-7` | low | Needs verification | User-authored constraint names can take engine-reserved `__icydb_` names and block later NOT NULL additions |
| `schema-mutation-8` | low | Needs verification | Plain ADD CHECK is capped at one 256-row page but reports the SourceRows budget; heap-store VALIDATE over 256 rows is permanently unsupported |
| `schema-mutation-9` | low | Needs verification | RENAME COLUMN and SET DEFAULT derivations drop candidate owners but keep live activations, so they fail with misleading internal errors while an activation is pending |
| `schema-store-5` | low | Needs verification | Every accepted-bundle borrow re-scans and CRC-decodes the full identity-state inventory (retired records never shrink) |
| `schema-store-6` | low | Needs verification | Persisted-format inventory omits cardinality records and the ICYDBCAT frame, and misattributes identity-state storage |
| `schema-store-7` | low | Needs verification | Per-entity namespace-0 snapshot copies and their ICYDBCAT fingerprint header are written on every publication but only serve an orphan journal record kind |
| `schema-store-8` | low | Needs verification | Snapshot decode silently canonicalizes relation order instead of failing closed on noncanonical bytes |
| `session-sql-10` | low | Needs verification | Trusted mutation surface applies an undocumented 100-row / 1 MiB cap to INSERT but leaves DELETE unbounded; INSERT…SELECT materializes the whole source before the cap check |
| `session-sql-11` | low | Needs verification | "Public" UPDATE/DELETE policies only constrain statement shape; target selection runs as TrustedRead with no read admission or scan budget |
| `session-sql-4` | low | Needs verification | Hidden public SQL DELETE entry points skip the shared prepare/normalize phase, so the policy proof and the executed query can disagree |
| `session-sql-5` | low | Needs verification | SQL_SUBSET still says placeholders are unsupported, but the trusted query surface implements typed WHERE bindings |
| `session-sql-6` | low | Needs verification | Targetless `DROP INDEX name` is documented as supported but always rejected by the only DDL entry point |
| `session-sql-7` | low | Needs verification | Unknown column in UPDATE SET or INSERT column list is reported as an internal executor invariant |
| `session-sql-8` | low | Needs verification | Resumable Verify cannot finish on multi-page entities that keep receiving writes, and each drift restarts a full Forward sweep |
| `session-sql-9` | low | Needs verification | Positional INSERT without a column list uses an undocumented width heuristic that can map values to unexpected columns |
| `session-write-1` | low | Needs verification | Resumable update stamps UpdatedAt with a fresh clock reading on every forward advance, not the frozen continuation timestamp |
| `session-write-2` | low | Needs verification | Structural lane rejects explicit Default on database-owned insert fields (Identity PK, generated, CreatedAt/UpdatedAt) that SQL and the contract admit |
| `session-write-4` | low | Needs verification | Replace on a missing key lets callers author a database-generated Ulid/Timestamp primary key |
| `sql-parser-10` | low | Needs verification | INSERT table alias is parsed and discarded; alias-qualified column and RETURNING references are rejected, unlike UPDATE/DELETE |
| `sql-parser-11` | low | Needs verification | The token cursor's move-out helpers still clone every string, blob and identifier payload for error reporting |
| `sql-parser-6` | low | Needs verification | Flat AND/OR chains of more than ~127 terms are rejected as ExpressionDepthLimit, contradicting READ_ADMISSION; parentheses count double toward the depth limit |
| `sql-parser-7` | low | Needs verification | SQL_SUBSET.md has drifted from the implemented grammar (required clauses undocumented, extra shapes accepted) |
| `sql-parser-8` | low | Needs verification | Globally reserved keywords with no quoting escape make same-named fields and entities unaddressable |
| `sql-parser-9` | low | Needs verification | Integer literals above u64 are typed as Decimal (i128 mantissa); wide-int values are unrepresentable or rejected, and render_scalar_sql_value does not reliably round-trip |
| `value-types-error-7` | low | Needs verification | Identity-projection docs claim 'non-reversible' and 'correlation avoidance', but the hash is unkeyed over enumerable keys and ignores the entity |
| `value-types-error-8` | low | Needs verification | canonical_value_compare is not a total order across mixed numeric variants, which can break sort_by in ORDER BY |
| `value-types-error-9` | low | Needs verification | PublicValue::try_into_runtime_non_enum builds Value::Map without normalization or validation |
| `xc-architecture-10` | low | Needs verification | Oversized modules are mostly inline tests; the mutation coordinator sits in the session layer despite that module's own boundary header |
| `xc-architecture-2` | low | Verified fixed | Migration planner relabels filtered-index predicates one rename at a time, so chained or swapped field renames compute a wrong expected predicate **Current evidence:** A32 removes sequential predicate-name rewriting. Chained and swapped name projections retain both distinct FieldIds and canonical bytes; the real rename planner and encoded/reloaded reconciliation fixture pass. See [cluster closure](filtered-index-cluster.md#closure-receipts--2026-10-03). |
| `xc-architecture-3` | low | Needs verification | Resource-budget vocabulary is owned by the query/executor/session layers, inverting the module graph for index, access, schema and codec |
| `xc-architecture-4` | low | Needs verification | The catalog-native schema mutation layer depends on SQL-frontend DTOs, and some mutation semantics are SQL-owned |
| `xc-architecture-5` | low | Needs verification | cfg(test) forks production semantics: test-only AST/runtime variants and production-only recovery fast paths that unit tests never exercise |
| `xc-architecture-7` | low | Needs verification | IndexStore::clear is a fully public, uncalled, non-durable mutator exposed to generated/user code |
| `xc-architecture-9` | low | Needs verification | Index authoring limits are duplicated in the model macros and have drifted from the real bounds |
| `xc-contracts-2` | low | Needs verification | 1.0-TODO marks CLI SQL INSERT/DELETE complete, but the CLI routes them to the read-only `icydb_query` query method, which always rejects them |
| `xc-contracts-5` | low | Needs verification | Recovery traps on preflight/apply contradictions; a deterministic contradiction wedges startup with no failure receipt |
| `xc-contracts-7` | low | Needs verification | PERSISTED_FORMAT_POLICY says the current line adds no checksum bytes, while many persisted envelopes carry checksums |
| `xc-contracts-8` | low | Needs verification | The inventory says stale secondary-index entries are repaired at startup; no such repair exists, and the persisted `Missing` witness value is never written |
| `xc-contracts-9` | low | Needs verification | 1.0-FEATURES says NULL sorts before non-null values, but DESC reverses the comparator so NULL sorts last |
| `xc-docs-api-10` | low | Needs verification | The canonical INSTALLING example publishes metrics to anonymous callers without a caveat, exposing entity paths and per-entity instruction usage |
| `xc-docs-api-11` | low | Needs verification | The durability operator guide still uses pre-0.258 'memory ID' guidance that contradicts the logical-key model |
| `xc-docs-api-12` | low | Needs verification | Documented public surfaces are #[doc(hidden)]: the `icydb::types` module (Id, Ulid, ...) and `TypedEntityBinding::take_row_value` |
| `xc-docs-api-13` | low | Needs verification | icydb-model Store allocation builders panic for heap stores without `# Panics` docs |
| `xc-docs-api-2` | low | Needs verification | `.limit()` means a total-traversal cap for scalar reads but a page size (with a continuing cursor) for grouped reads; rustdoc describes neither |
| `xc-docs-api-5` | low | Needs verification | schema-authoring guide's 'complete starting declaration' link points to a README anchor that does not exist |
| `xc-docs-api-7` | low | Needs verification | SECURITY.md misdescribes the pre-commit hook and `make test` PocketIC download behavior, and has no vulnerability-reporting channel |
| `xc-docs-api-8` | low | Needs verification | The endpoints! declaration surface has constraints that user docs never state: SQL declaration name differs from the export, guarded syntax is order-sensitive, and fixtures need a hard-wired feature name |
| `xc-docs-api-9` | low | Needs verification | Typed-operation errors cannot be returned as the Candid `icydb::Error`, and the maintained endpoint template drops diagnostic facts that the diagnostics guide says must be preserved |
| `xc-performance-10` | low | Needs verification | Filtered raw-row lanes open and parse the same row twice |
| `xc-performance-11` | low | Needs verification | Opening a row reader has fixed O(field_count) setup: layout slot count recomputed, per-slot contract lookups, two allocations |
| `xc-performance-12` | low | Needs verification | Initial schema application checks store emptiness with a full O(N) count |
| `xc-performance-13` | low | Needs verification | Exact-key batch Candid-encodes every result row once just to measure its size |
| `xc-performance-4` | low | Needs verification | Mutation batch loop rebuilds and deep-clones the accepted row decode contract 4-5 times per item |
| `xc-performance-6` | low | Needs verification | Primary-range scans for full-row strategies walk keys, then do a separate point B-tree lookup per row; the single-pass path is rarely reachable |
| `xc-performance-7` | low | Needs verification | Journal publish reads and clones the full previous row only to learn whether it existed |
| `xc-performance-8` | low | Needs verification | Storage report scans each journaled data and index store three times |
| `xc-performance-9` | low | Needs verification | PrimaryRangeKeyStream allocates the raw key three times per scanned key |
| `xc-security-4` | low | Needs verification | Generated ULID primary keys on IC are fully predictable |
| `canisters-testing-ci-12` | info | Needs verification | CI invariant scanners have blind spots: rg errors are swallowed, and the no-panic scan misses several panic macros |
| `commit-7` | info | Needs verification | Production Gate-2 measurement and the marker-presence fast path have test-only replacements, so core unit tests never exercise them |
| `data-11` | info | Needs verification | Dead retired-slot machinery: writer and decoder would disagree on slot count if slot gaps ever appeared |
| `executor-aggregate-11` | info | Needs verification | Dedicated grouped COUNT(*) window selection sorts and heap-selects without charging sort budgets |
| `executor-core-6` | info | Needs verification | Verbose EXPLAIN always reports diag.p.order_pushdown=missing_model_context, even though the executor has accepted authority |
| `facade-8` | info | Needs verification | No compile-fail test for declaring a metrics endpoint without the icydb/metrics feature |
| `journal-jobs-10` | info | Needs verification | Application side effects inside compare_proof_and_advance are not atomic with progress persistence |
| `journal-jobs-8` | info | Needs verification | Batch validation repeats work: Identity-range check is O(ranges × records), records are validated twice, schema bundles are decoded several times |
| `journal-jobs-9` | info | Needs verification | Persisted-format inventory text for sequence-zero controls does not match the store implementation |
| `query-intent-7` | info | Needs verification | Explain gives no reliable admission or pushdown signal (typed order_pushdown is hard-coded) |
| `query-intent-8` | info | Needs verification | Production `.expect` in planner label rendering |
| `query-plan-6` | info | Needs verification | Secondary-index planning rejects NumericWiden equality even after normalization has canonicalized the literal to the field kind |
| `query-plan-7` | info | Needs verification | Branch-set constructor claims a proven PK-ascending suffix that the planner does not establish when ORDER BY is absent |
| `r2-candid-stability-4` | info | Needs verification | SQL RETURNING response-size check measures a hand-written one-variant copy of the response, not the delivered Result<SqlQueryResult, Error> envelope |
| `r2-query-call-caches-4` | info | Needs verification | Query calls pay cache-insertion work (retained-size walk, key and SQL clones, FIFO bookkeeping) whose result is always discarded |
| `schema-catalogs-8` | info | Needs verification | Per-entity commit and cache fingerprint does not cover the enum/composite catalogs |
| `schema-mutation-10` | info | Needs verification | Canonical check SQL rendering emits CARDINALITY(...) and LENGTH on nominal fields, which the SQL check binder cannot re-parse |
| `schema-store-9` | info | Needs verification | Production index `primary_key_slot_indices[0]` in the row-layout runtime contract |
| `session-write-6` | info | Needs verification | Normal structural writes run with no execution budget and are not charged to the request root |
| `session-write-7` | info | Needs verification | The insert-only dynamic batch has no result-size bound, so it can succeed natively but always fail on-chain |
| `session-write-8` | info | Needs verification | The request budget-bundle preflight does not aggregate a resource listed twice in one bundle |
| `value-types-error-10` | info | Needs verification | Value serde Deserialize recurses without a depth bound (no current untrusted decode path) |
| `value-types-error-11` | info | Needs verification | Fact-schema mismatch in with_diagnostic_facts turns the error into InvariantViolation instead of keeping its class |
| `xc-architecture-11` | info | Needs verification | The production panic CI gate scans only db/executor, not the recovery, commit and journal paths |
| `xc-architecture-8` | info | Needs verification | Persisted-format decoders still keep at least 7 private cursor readers alongside the shared ByteReader |
| `xc-contracts-10` | info | Needs verification | REF_INTEGRITY says relation diagnostics retain the constraint name; the code emits only numeric facts |
| `xc-performance-14` | info | Needs verification | Generic store visitors duplicate the full traversal and overlay-merge code for each visitor closure (wasm size, unmeasured) |
| `xc-performance-15` | info | Needs verification | Per-row budget and page-unit accounting repeats constant work |
| `xc-security-8` | info | Needs verification | Read guard gets no entity or statement context, so SQL authorization is all-entity |

### A32 — Accepted filtered predicate authority

Completed 2026-10-03. Six related findings are verified fixed through one
accepted predicate owner; counts are 30 Verified fixed, zero Open/In progress,
one Partial and 276 Needs verification. The recursive-bounds report remains
unchecked; current codec depth/size controls do not establish its original claim.

Generated and DDL predicates retain direct FieldIds, existing coercions and
canonical typed literal payloads. Runtime, identity, build, integrity and rename
consumers use that tree. Accepted literal kinds retain admission bounds and enum
identity; generated field comparisons use accepted type capabilities. Nested
predicate paths remain rejected, while native nested index keys retain their
separate support. Runtime SQL reparsing and predicate-name rewriting are removed.

All 62 distinct focused tests pass on the current locked dependency graph.
Controls cover numeric/enum/ULID membership, actual unique collisions, maintained
DDL identity, chained/swapped name projection, real rename planning and generated
reconciliation from encoded catalog bytes, malformed IDs/literal bounds, codec
size/depth, LIKE/ILIKE/coercions, planner implication, and interrupted-write replay.
Typed live reads select the filtered index only when its guard is satisfied and
match complete primary-scan results across continuations and repeated calls.
Maintainer/focused lint, core feature checks and invariant guards pass.

The current version-1 representation replaces text in place. Affected pre-1.0
metadata and index artifacts require recreation or regeneration. Full suites
remain user-owned; raw Wasm size, IC cycles and instructions are unmeasured.
The A32 footprint is approximately 55 files and +1,600 net lines, including
mechanical propagation, bounded binding/codec logic and direct qualification.
Ownership/execution flow is simpler; local representation code grows, with zero
additional behavior axes. Previous A28–A31 work and concurrent external dependency
updates are preserved; no Cargo package-version edit, commit, push or network
lifecycle action was performed. The disk block and initial pagination-test
assumption were resolved before final validation.

### Row-value boundary audit after A32 — 2026-10-03

The completed queue requires a scoped read-only audit before further corrections.
Six maintained historical-scalar regressions verify the existing `data-1` fix.
Two disposable observation probes reproduce `data-2` and the decode-depth seam
of `r2-recursive-bounds-1`; their passing defect assertions are not corrected
product behavior. Counts are 31 Verified fixed, two Open, one Partial and 273
Needs verification. `data-7` retains its separate verification requirement.

Both open findings discard current canonical wire/depth facts at the boundary
into borrowed or materializing traversal. Proposed A33 is one end-to-end current
value traversal correction: reuse canonical enum framing, fixed scalar widths
and the accepted recursive limit, then qualify nested projection/grouped reads
against general materialization. No mode, fallback, new format or persisted
state is proposed. Public execution, malformed/truncated bytes, exact depth,
null/missing paths and historical-fill controls remain implementation gates.

Four audit/status docs change; runtime source, existing dirty fixes and release
entries are preserved. Source comparison covers all 1,108 core Rust files, with
only the disposable probe file differing. Documentation and whitespace checks
pass. Code complexity stays unchanged; raw Wasm, cycles and instructions are
unmeasured. Full suites and publication remain user-owned.

### A33 current value traversal authority — 2026-10-03

The user selected the two open row-boundary findings. Borrowed traversal and
runtime materialization now use the canonical current persisted-value owner.
Its enum frame decoder and accepted depth limit remain authoritative; scalar
payload codecs retain their existing fixed widths. The duplicate cursor,
collection walkers, scalar cursor helpers, scalar-to-canonical copy and private
recursive limit are removed. No execution route, mode, cache, fallback, format
or persisted state is added. The current version-1 wire bytes are unchanged.

Five new regressions qualify unit/payload/nested enums, scalar siblings in both
wire orders, exact/over-limit list/map/enum nesting, malformed IDs/body/length,
truncation and trailing bytes, null/missing paths and non-map ancestor rejection.
Published catalog qualification compares public/trusted full-row reads against
nested SQL scalar/enum/deep-list projection, filters and single-path grouping,
including repeated calls. Historical scalar fills and maintained structured
ordering/grouping controls remain covered. All 117 distinct focused tests,
strict core lint, no-feature build, formatting and direct invariant guards pass.
Initial fixture compile/layout/header/public-admission assumptions were corrected
before final qualification. Full suites remain user-owned.

Both saved findings close: 33 Verified fixed, zero Open/In progress, one Partial
and 273 Needs verification. This is no verdict on unchecked findings; `data-7`
retains its separate qualification. A33 touches 19 files, removes approximately
540 production lines and has a small negative net line delta after tests/docs.
Traversal ownership and implementation are simpler; behavior-axis delta is zero.
Raw Wasm size, IC cycles and instructions are unmeasured. Earlier dirty changes
and external dependency updates are preserved; no package-version edit, commit,
push or network lifecycle action was performed.

### A34 memory admission diagnostics — 2026-10-03

The user explicitly selects `facade-3`, including real failure reproduction,
public startup/access parity, bounded facts, current grant docs and qualification.
Allocation, admission and recovery remain with their existing owners. Typed
upstream causes already reach `Error::from(DatabaseBootstrapError)`; all but the
bucket-size mismatch currently become runtime-internal errors. Generated
`startup_state()` uses this conversion, and ordinary `db!()` returns the startup
failure's exact error. A34 changes only that diagnostic projection and its docs.

Before extending public diagnostic variants: the demonstrated need is that
operators cannot distinguish rejected grants, incomplete roles, namespace
removal and declaration drift from internal faults. A blanket conflict code or
raw upstream strings would lose cause identity or abandon bounded diagnostics.
The existing facade conversion and diagnostic registry are the canonical owners.
Six diagnostic-only leaves are planned for resolution, removed namespaces,
incomplete roles, invalid declarations, declaration mismatch and unavailable
historical journals. Existing numeric memory-ID/count tags carry evidence where
available. Diagnostic leaf-space delta is six; allocation policy, execution
routes, persisted states and format-axis deltas are zero. Unknown/internal
upstream failures keep the existing internal classification.


A34 verified result: real local runtime bootstraps reproduced Reserved exhaustion,
incomplete controls, invalid namespace authority, omitted historical namespaces,
revoked historical-journal grants and changed sealed declarations. Before the
conversion change, all six retained typed upstream causes but became E23 at the
public boundary. Generated facade children separately reproduced Reserved-default,
missing-grant, incomplete-role and invalid-declaration rejection as E23; an
explicit Allowed grant succeeded. The historical range rejection is returned by
upstream's outer `Admission` before the policy wrapper, so both maintained
historical wrappers are qualified. Baseline receipts are local
`/tmp/icydb-facade3-before-typed.log` and
`/tmp/icydb-facade3-generated-before.log`.

One private facade projection now maps known causes into the existing diagnostic
registry: E276 resolution/grant (Unsupported), E277 removed namespace (Conflict),
E278 incomplete roles (Unsupported), E279 invalid declarations (Unsupported),
E280 declaration/adoption mismatch (Conflict), and E281 unavailable historical
journal (Conflict). Runtime origin is preserved. Existing count and memory-ID
facts are bounded to maintained role counts 3/4 and memory IDs 0–254; absent
numeric evidence is omitted. Namespace/authority/key strings do not cross the
public boundary. Existing E274 bucket evidence and unrelated internal E23 remain.
No allocation/admission/recovery owner, grant, mode default or format changes.

Qualification covers real incomplete store roles and rejected fixed declarations,
real unknown-host adoption, adoption ID evidence and metadata/key conflicts,
registry/validation range wrappers and internal state/registry controls. Rejected
recovered admission preserves backing bytes; cold rejection publishes no committed
allocation authority. Generated `startup_state()` and ordinary `db!()` preserve
identical payloads on repeated calls, including Candid and diagnostic/fact access.
Nine new regressions extend the maintained coverage. All 108 focused tests pass;
strict repository/focused lint, the facade no-feature build, formatting and six
direct invariants pass. The initial needless-borrow lint warning and an overly
strict CLI-note assertion were corrected; no validation failure remains.

Macro audit: all 25 original maintained canister/testing range examples already
use explicit Allowed. The added fixture exercises one intentional omitted-mode
Reserved declaration and one explicit Allowed success. Re-export Rustdoc, README,
schema/startup/diagnostic guides and durability guidance now explain fresh grants.
The CLI renders all six codes without accepted-schema artifacts. Root and shared
0.264.5 notes record the changed error taxonomy; callers matching old E23 startup
configuration failures must use the current codes/classes. No data migration.

The inventory now has 34 Verified fixed, zero Open/In progress, one Partial and
272 Needs verification. Full suites remain user-owned. Raw Wasm, cycles and
instructions are unmeasured. The slice changes 22 files, approximately +1,090 net lines. Production
adds about 180 net lines; most growth is regression coverage and documentation.
One diagnostic projection is more expressive without adding execution complexity;
six diagnostic leaves add no execution route, policy or format axis.
Existing dependency edits and unrelated report artifacts are preserved; edits
remain unstaged and uncommitted.

### Validation follow-up — no-default literal binding — 2026-10-03

The user's latest combined validation receipt reports one distinct compiler
failure, repeated by parent targets: the no-default-feature core test build
cannot resolve `input_value_from_strict_sql_literal_for_persisted_kind` through
`db::schema`. The helper and filtered-index test binder both use
`cfg(any(test, feature = "sql"))`, but the schema re-export used SQL alone.
Aligning that one re-export with its existing owner/consumer gate restores the
maintained test surface. Production feature gates and semantics are unchanged;
no helper, fallback, format or execution route is added. This is direct A32
qualification fallout, not another saved finding or a changed review count.

Validation passes: the full no-default core unit-test target compiles and all
eight selected filtered-index regressions pass, including strict numeric/enum
literal admission and maintained predicate codec/identity behavior. Strict
no-default all-target core lint, formatting, local documentation references and
whitespace checks pass. The separate literal-helper test module is SQL-only,
so its no-default selection contains zero tests and is not counted as evidence.
The supplied user's workspace/canister validations passed; full suites are not
rerun. No known validation failure remains. Four files change for this follow-up:
one Rust line is replaced and three status/changelog docs record the correction.
Production net lines and runtime complexity are unchanged; raw Wasm, cycles and
instructions are unmeasured. Existing dirty changes are preserved, and all edits
remain unstaged/uncommitted.
