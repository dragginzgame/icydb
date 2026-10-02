# Saved-review closeout audit

Audited 2026-10-02 against the current `0.264` worktree, including completed
A16/A17 changes. The original audit after the five-item repair queue was read-only;
its handoff changed documentation only. It is a scoped audit,
not a closeout verdict for all 307 distinct saved findings. Subsequent authorized
corrections refresh the admission, expression, key-stream and unordered projection
verdicts below.

Authority: [review status](status.md) and
[0.264 tracker](../design/0.264-signed-index-admission/0.264-status.md).

## Current evidence

| Finding | Current result | Canonical owner and evidence |
| --- | --- | --- |
| `xc-security-1` | Verified fixed by existing code | Database-format entropy admission obtains IC `raw_rand`, requires a ready seed, binds it to database identity, replaces the persisted cursor key each boot and blocks cursor use before admission. Five maintained entropy/session tests pass. |
| `query-intent-1` | Verified fixed in subsequent A18 | The audit reproduced PublicRead admission of `rare = 'group-a' ORDER BY wide_branch LIMIT 5` despite the required sort. A18 now projects the canonical scalar sort rule and exact-key candidate bounds; public secondary sorts reject and text/JSON EXPLAIN report materialization truthfully. All 44 focused tests pass. |
| `query-intent-2` | Verified fixed in subsequent A19 | The audit reproduced a whole `a_common_idx` range admitted by `ORDER BY common LIMIT 1`. A19 classifies empty-prefix, fully open ranges as logical FullScan while retaining the physical index. Public live/exhaustive/grouped calls and authenticated scalar resume reject; selective controls and text/JSON EXPLAIN pass within 54 focused tests. Alias `xc-security-3` shares this closure. |
| `query-expr-4` | Verified fixed in subsequent A20 | The audit reproduced a missing-path error vetoing both orders of a true OR sibling. A20 converts only that signal to UNKNOWN at shared compiled AND/OR boundaries. The 60-case truth matrix, typed errors, leaf/projection contracts, admitted structural/SQL reads, grouped continuation and UPDATE/DELETE scopes pass. Dynamic dotted-field frontend gaps remain separate observations. |
| `executor-stream-2` | Verified fixed in subsequent A21 | The audit reproduced a monotonic key-DISTINCT adapter rejecting decreasing primary keys across multi-lookup branches. A21 reuses their disjoint canonical equality prefixes and removes the redundant strategy. All 104 focused tests pass, including projected duplicates, repeated literals, direction, windows, public/trusted live/exhaustive continuation, typed budget failures and numeric/composite controls. |

The initial branch-DISTINCT fixture omitted ORDER BY fields from its projection
and correctly met the maintained typed SQL restriction. That result is not
evidence of the saved executor defect; the corrected fixture includes those
fields. That corrected composite branch-set query succeeds with and without
DISTINCT, including decreasing primary keys between branches. The single-field
multi-lookup fixture reproduces the saved defect instead. The plain queries
supply controls for accepted order and rows; an IN-list alone does not identify
the faulty access family.

The separate A16 observation is also reproduced: with accepted Nat64 components,
`SELECT DISTINCT operand, category FROM PlannerRow WHERE category = 3` returns
`Execute(InvariantViolation(19))`. The plain SELECT returns both input rows and
adding `ORDER BY category, operand` returns the single distinct output row.
This is a covering-eligible projection, but the current projection facade sends
DISTINCT through scalar execution and requires a resolved order when group seek
is absent. It is not evidence that unsupported hybrid decoding remains broken.
This observation is outside the saved 307-item inventory and does not change
its counters.

Subsequent A22 verifies this observation fixed: ordinary unordered DISTINCT
uses the existing global accumulator without synthesizing SQL order. Cursor
emission retains its resolved-order requirement and accepted primary-key default.
All 56 focused tests pass, including six new projection/expression/NULL/collection,
cold/warm, window-policy, public/trusted continuation and typed budget/scan
regressions. SQL LIMIT/OFFSET retain their typed ORDER BY requirement. This
qualifies DISTINCT's shared scalar projection flow rather than claiming that
covering-eligible selection executes a specialized covering DISTINCT route.

A20 qualification also finds separate dynamic frontend gaps:
`profile.rank = InputValue::nat64(5)` rejects with typed `LiteralTypeMismatch`
for an accepted nullable record with a Nat64 terminal; the signed dynamic spelling
also rejects. Numeric dynamic field equality rejects `InvalidCoercion(Strict)`.
An admitted ordering comparison instead lowers the dotted label as a root field,
losing descendant traversal and returning only the other disjunct's matches.
The maintained SQL lowerer produces structural FieldPath expressions, which A20
qualifies through accepted-schema execution. These frontend observations are
distinct from the shared missing-path boolean veto and remain outside the
inventory until mapped to existing saved findings; they do not change counters.

A21 qualification also observes that DESC multi-lookup DISTINCT EXPLAIN reports
`materialized_sort=false` in the shared admission summary but emits an
`OrderByMaterializedSort` descriptor node. The corrected query returns the
accepted secondary order. Descriptor construction appears to conflate the
projected DISTINCT materialization boundary with an ORDER BY sort. This is a
separate diagnostics observation, outside the inventory until mapped to a saved
finding; A21 does not claim complete EXPLAIN consistency. Current SQL `MOD`
outputs Decimal, and schema fixtures must not replace accepted catalog content
under a retained identity: the initial probes' Nat64 expectation and same-thread
unique/non-unique identity reuse were fixture mistakes, not product defects.

## Overlap and follow-up order

The saved review already deduplicates reports. Closing `xc-security-1` also
closes its source aliases `value-types-error-1` and `data-10`; these are not three
inventory entries. `query-intent-2` similarly owns alias `xc-security-3`.

The two admission findings share the planner-to-admission projection owner and
can reuse fixtures, but require separate proofs: truthful materialization facts
do not establish selective access bounds. Likewise, the two DISTINCT examples
must not be grouped merely because they return executor errors. A resolved-order
requirement and key-stream monotonicity are different boundaries. Every finding
needs its own semantic regression even when tests share setup.

Proposed next queue, within `0.264`, one bounded outcome per handoff:

1. `query-intent-1`: completed as A18; canonical scalar sort facts, exact-key
   bounded exceptions and truthful EXPLAIN/rejections are qualified.
2. `query-intent-2`: completed as A19; shared whole-index classification,
   public consuming boundaries, selective controls and diagnostics are qualified.
3. `query-expr-4`: completed as A20; preserve missing-path leaf semantics while
   allowing a true sibling disjunct to admit the row, qualified through shared
   expressions, accepted read/grouped boundaries and mutation consumers.
4. `executor-stream-2`: completed as A21; disjoint multi-lookup identity preserves
   secondary branch order, projected duplicates, continuation and accounting.
5. The unordered DISTINCT observation: completed as A22; preserve absent SQL
   order, accepted cursor-order authority, canonical projected results and
   maintained window/resource boundaries through existing global accumulation.

Items 1–5 are complete in subsequent user-authorized A18–A22 handoffs. The next
generic continuation performs a scoped read-only closeout audit within 0.264
and reports findings before further implementation. Dynamic dotted-field and
DESC EXPLAIN observations remain separate. The
original five authorized outcomes remain complete, and the audit was reported
before this implementation.
The historical nested-slot part of `data-4` remains Partial, and other unchecked
findings retain their individual status.

The subsequent [quick overlap scan](overlap-triage.md) screens the 291 unchecked
reports and distinguishes candidate common-owner corrections from related
families that need separate proofs. It proposes residual preservation as a small
two-item correction and typed filtered-index authority as the largest seven-item
structural cluster; it performs no production correction or inventory closure.

## A23 qualification: separate mixed-filter cache observation

The user authorized the residual-preservation pair after the overlap scan.
Qualification uncovered another defect outside that pair: the ordinary
structural cache key omitted the predicate fingerprint whenever an expression was
present, although a separately appended predicate-only filter contributes
semantics absent from that expression. Partial coverage correctly prevented a
parameter template, but did not prevent this ordinary-key collision.

Accepted-schema reproducer: append `operand + 1 > 5` and a normalized exact-key
predicate separately. In one request, prepare `id = 1`, then `id = 2` with the
same projection and lane. Before A24, the second query returned row 1 instead
of no rows. Likewise, COUNT over keys `[1,2,3,4]` returned 2 correctly after the
A23 fix, but the next COUNT over `[2,3]` incorrectly reused that plan and returned 2 instead
of 0. The rows are `(id,operand) = (1,7),(2,2),(3,NULL),(4,9)`.

The A23 evidence was in `query/intent/cache_key.rs`: its expression-present
branch discarded the supplied normalized predicate fingerprint. The
`session/query/cache.rs` coverage guard already excludes these queries from
parameterized templates. The shared cache spans request sessions, so a fresh
request alone does not isolate the scopes. A23 did not change cache identity or
close any cache finding by inference. Its accepted tests originally cleared the
shared test cache between predicate-only scopes, then repeated that scope to
qualify cold/warm reuse. That was the A23 handoff's stated limitation.

The proposed next independent outcome within 0.264 was to preserve the existing mixed
filter authorities in cache identity and qualify changed literals/scopes,
both append orders and repeated reads/counts against fresh planning. Prefer
reusing the current predicate fingerprint plus expression identity over adding
a mode or cache route. This observation is outside the saved inventory until
mapped and separately verified; it does not change the 307-item denominator.
The user subsequently authorized this outcome as A24. Its correction retains
both existing filter identities and removes the between-scope test workaround.

A24 is verified fixed outside the saved inventory. All 81 distinct focused
tests pass, including three new regressions and two strengthened A23 cases.
Both append orders, changed/revisited scopes, fresh requests/syntax, disabled
and retained cache policies, NULL/nonmatching/missing rows, reads, COUNT and
mutation selections are qualified. Existing template, budget and diagnostics
controls pass. Strict core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Nine incremental files add
approximately 300 net lines; production removes four including comments.
Implementation gets simpler without a state-space axis. Full suites remain
user-owned; raw Wasm, IC cycles and instructions are unmeasured. Cargo edits,
published notes and unrelated dirty work are preserved; no network lifecycle
action occurred.

## A24 qualification: simultaneous residual authorities

At the A24 handoff, an additional singleton-IN control failed independently of
cache identity, even with shared cache retention disabled. With the same accepted rows above, append
`operand + 1 > 5` and the normalized predicate `id IN (2)` separately, then
convert DELETE intent to its load selection and execute in the mutation lane.
It returned row 2 instead of no rows. The plan's residual expression was present;
the result was still wrong after the A24 key correction. The disabled-cache
control was the first policy tested, and this failure occurred on its second scope.

The A24 source evidence was `query/plan/semantics/logical.rs`:
`compile_effective_runtime_filter_program` chose a remaining predicate before
the residual expression, without proving that the predicate covers that
expression. This is a separate runtime projection of filter authority. Simply
selecting the expression instead is insufficient for mixed appends, because a
predicate-only append can constrain rows absent from the semantic expression.
The complete residual semantics must survive together through the maintained
runtime preparation boundary.

A24 qualified changed equality and multi-key scopes through mutation selection;
singleton-IN simultaneous-residual execution remained unfixed at that boundary.
The failed control was recorded here before separately implementing the next
outcome. The user subsequently authorized that correction as A25. It is outside
the saved inventory and does not add another closure to the 307-item denominator.

A25 is verified fixed. The existing expression-backed effective program retains
an optional native predicate when intent coverage cannot discharge the
expression. Shared structural/cow row evaluators execute the conjunction; slot
requirements and retained ownership include both parts. Predicate-only capability
projection declines composite programs, and full coverage retains native execution.
The representation need and alternatives were recorded before implementation:
valid compiled forms grow from two to three in the same flow, without a new
user mode, route, configuration, persisted state or format.

All 116 distinct focused tests pass, including three new regressions, two
strengthened singleton-IN cases and 111 surrounding controls. Four functional
scan/COUNT/read/mutation cases fail before correction. Both append orders,
disabled/retained caches, fresh requests, repeated operands, NULL/nonmatching/
missing rows, windows, counts and mutation selection are qualified. Scan controls
also reject an expression-only replacement. Cow-reader controls preserve
TRUE-only admission, short-circuiting, required-reader errors and both slot sets.
Existing template, retention/budget, storage, NULL, continuation and diagnostics
controls pass. Strict core library/test lint, formatting, schema/format/admission
guards and documentation/inventory/diff checks pass. Twelve incremental files
add approximately 375 net lines; production adds twenty-one including comments.
Implementation grows modestly within one filter authority. Prior dirty work,
Cargo edits and published notes are preserved. Full suites remain user-owned;
raw Wasm, IC cycles and instructions are unmeasured. No network lifecycle action
occurred. This follow-up is complete; other review findings remain individually
unchecked or partial as recorded in the status inventory.

## Post-A25 scoped audit: CLI SQL failure status

The user requests the next saved-review correction and emphasizes the remaining
289 unchecked findings. A scoped source audit confirms `cli-3` in current code:
both query and mutation response handlers convert decoded `Err(icydb::Error)`
into `Ok(rendered_error)`. One-shot execution then writes it to stdout and the
process entrypoint returns success. This finding is reported before correction;
A26 extends the same 0.264 line with this one outcome. The DESC EXPLAIN observation
remains separate and uncorrected.

The simplest correction preserves the endpoint error through the existing
`Result` path. The process entrypoint owns stderr and exit status; the interactive
loop already owns per-statement error reporting and continuation. No new mode,
configuration, execution route, persisted state, format or enum variant is needed.
Qualification must exercise actual child-process status and streams for query,
DDL and UPDATE, both one-shot argument forms, successes, malformed responses,
transport failures and interactive continuation. Migration outcome `cli-4` has a
different response owner and is not included.

A26 subsequently verifies `cli-3` fixed. The unchanged-handler process regression
first fails with exit 0 on a decoded endpoint rejection. All 32 focused tests then
pass after correction, including 27 process cases across both argument forms,
all three SQL call lanes, two typed remote errors, successful responses, malformed
Candid, transport failure and interactive continuation. Strict CLI lint,
formatting, documentation references and inventory/diff checks pass. Existing
`Result` and output owners remain canonical; production removes one net line
including comments and adds no behavior axis. No network lifecycle action occurs.
The saved inventory now has eighteen verified fixes, one partial and 288 unchecked
findings. Migration receipt status (`cli-4`) was the next planned outcome at that
handoff; A27 independently qualifies it below. Dynamic and diagnostic observations
stay open.

## A27 qualification: migration operation status

The user selects the planned `cli-4` correction after A26. Three process
regressions reproduce exit 0 on rejected run/advance and already-applied abort
before production edits. The original review calls bounded advance status
arguable; the maintained CLI contract now explicitly treats rejection/abortion
as failure, while successful bounded progress need not mean final application.
Rejected abort pages can represent continuing staging cleanup rather than a
terminal failed abort; identical public pages must not stop that cleanup loop.

A27 centralizes status output and operation-success checks in the existing
migration dispatcher. Run requires `Applied`, abort requires `Aborted`, and
advance rejects `Rejected`/`Aborted`. Status inspection, adoption, confirmation,
loop identity and endpoint error handling retain their contracts. Failed decoded
outcomes still expose their phase and findings, with a nonzero process result.

All fourteen focused tests pass. Seven new migration process regressions cover
73 cases; four maintained SQL regressions cover another 27 through their reused
fixture. Three existing migration CLI argument/wire tests also pass. The matrix
qualifies every displayed phase, bounded/new/existing terminal results, paged
rejected cleanup, already-applied abort, database/plan changes, no-progress run,
missing plans, confirmation/adoption, and remote/invalid/transport replies at
read and update boundaries. Strict CLI lint, formatting, documentation references,
inventory and diff checks pass. Production adds nineteen net lines; one owner
keeps implementation shape neutral without a new behavior axis. No network
lifecycle action occurs. Raw Wasm, cycles and instructions are unmeasured; full
suites remain user-owned. The inventory now contains nineteen verified fixes,
one partial and 287 unchecked findings. The planned queue is complete; generic
continuation stays in 0.264 for a scoped read-only audit before further correction.

## Validation and limits

At the read-only audit handoff, five maintained native tests and five disposable
observation probes pass: three
entropy state/key tests, two session startup/cursor tests, admission, missing-path
OR and three DISTINCT access/order probes. These confirm four saved defects and
the additional unordered DISTINCT observation. The wasm entropy source is
inspected in current source; no new on-chain entropy qualification was run.

Disposable functional probes use a copy of the current working files at
`/tmp/icydb-closeout-20261002`; only that copy receives probe code. Snapshot
production sources match the repository. The admission, unordered DISTINCT and
missing-path probes confirm defects; a passing observation probe does not mean
the product behavior is correct. No error-string matching or native timing
measurements are used as evidence.

The original audit handoff made no production correction, Cargo package-version
edit, commit, push or network lifecycle action. Its three documentation files
add approximately 120 net lines relative to A17; code complexity stays neutral.
Subsequent A18–A22 implementation and validation are recorded in the review status
and 0.264 tracker. Full suites remain user-owned; raw Wasm bytes, IC cycles and
instructions are unmeasured.
