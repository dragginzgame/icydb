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

## Numeric audit after published 0.264.4

The user reports 0.264.4 pushed and requests continuation. With the queue complete,
this handoff audits the related decimal findings before any new correction.
All 26 schema decimal and 17 core numeric tests pass. Current checked division uses
checked quotient/remainder; the maintained core consumer maps failed division to
typed overflow. A disposable probe verifies every scale 0–28, both original signed
MIN text inputs, division/assignment, zero divisors and identity at the maintained
18-digit division precision. The original panic finding `value-types-error-3` is
verified fixed. The original review did not execute SQL end to end; this audit also
distinguishes primitive/core evidence from a canister execution claim.

The shared multiplication boundary still rejects a product scale above 28 unless
trailing zeros can be removed. Primitive operators interpret this as magnitude
overflow and saturate. Both `0.000000000000001` squared and
`1.123456789012345678` squared produce
`17014118346.0469231731687303715884105727` through multiplication, assignment,
iterator product and `powu`. Signed multiplication reaches the negative extreme;
checked multiplication/powers return `None`. `1.1^30` reproduces the same defect.
Zero, ordinary `20 × 20`, and true mantissa-overflow controls pass.
`model-schema-crates-1` is therefore Open, not another closure from A13.

Proposed A28 is one shared precision correction plus direct primitive and checked
runtime qualification. The schema arithmetic owner should distinguish excess
fractional precision from true magnitude overflow, reusing the maintained rounding
policy without new modes, formats or alternative arithmetic routes. Qualify signs,
ties/underflow, scale 28 boundaries, mantissa limits and powers. Addition/alignment,
remainder and mixed numeric ordering remain independent findings.

The probe source and receipt are `/tmp/icydb-decimal-audit-probe.rs` and
`/tmp/icydb-decimal-audit-probe.log`; it links the current Cargo-built schema library
and changes no repository code. Only audit/status documentation changes, with no
new release note, Cargo edit or network lifecycle action. The inventory becomes
twenty verified fixes, one Open, one Partial and 285 Needs verification. Raw Wasm,
IC cycles and instructions are unmeasured; full suites remain user-owned.

## Decimal alignment and remainder audit after A28

Generic continuation after completed A28 audits the next arithmetic boundaries.
Both `model-schema-crates-3` and `model-schema-crates-4` remain Open in the current
working source. This handoff makes no runtime correction and preserves earlier
A28 changes.

The original addition cases still reproduce: `1e21 + 1e-18` returns about
`1.7e20`, and `1e30 + 1e-28` returns about `1.7e10`. Same-sign negative controls,
subtraction, assignments and iterator sums share the defect. Negative large-minus-
small also flips sign because the saturation branch uses signed ordering to
choose the dominant magnitude. Checked addition/subtraction return `None` after
alignment overflow. True quotient overflow `1e37 / 1e-28` returns about `1.7e20`
through primitive division/assignment. A28 changes neither of these boundaries.

The exact remainder of `5.0000000000000000000000000001` divided by
`10000000000000` is the dividend itself. Aligning the divisor currently exceeds
i128: `checked_rem` returns `None`, `%` and `%=` return zero, and the maintained
application `MultipleOf` validator accepts the non-multiple without an issue.
A disposable probe linked to the current schema/model libraries exercises 112
scale/sign cases at scales 1–28. Twelve at scales 26–28 reproduce both false zero
and false acceptance; the other hundred reject the non-multiple correctly.
Ordinary multiples, non-multiples, zero dividends and zero-divisor controls pass.

The application validator still uses `%`. Accepted runtime rules use
`checked_rem` through `exact_numeric_is_multiple`, mapping `None` to a typed
runtime-value mismatch; checked SQL numeric arithmetic maps it to overflow.
Those checked consumers avoid false acceptance but still share the spurious
failure. This audit executes the application validator and primitive boundary;
the core-consumer observations are current source evidence, without a new SQL
or canister execution claim.

Proposed A29 corrects the remainder boundary once in the Decimal owner. Reuse
the existing temporary wide arithmetic for exact alignment and remainder, then
narrow the exact result; a consumer-specific guard would leave other users
incorrect. One operand remains unscaled, and the remainder magnitude cannot
exceed either aligned operand; admitted nonzero divisors therefore have
representable exact remainders. Qualify all scales/signs, signed MIN, exact
multiples and zero divisors across primitive, application, checked numeric and
accepted-rule consumers.
No new mode, format, persisted state or fallback is needed. The alignment and
saturation finding remains a separate future correction.

All 54 focused maintained tests pass: 30 schema decimal, four application numeric
validators, 18 core numeric and two accepted-rule multiple-of controls.
Documentation/inventory checks pass. The disposable probe confirms defects
rather than correctness. Its source/receipt are
`/tmp/icydb-decimal-alignment-probe.rs` and
`/tmp/icydb-decimal-alignment-probe.log`. The inventory now has 21 Verified fixed,
two Open, one Partial and 283 Needs verification. Only three audit/status docs
change in this handoff, with no release entry or network lifecycle action.
Runtime complexity stays unchanged; raw Wasm, IC cycles and instructions are
unmeasured. Full suites remain user-owned.

## Arithmetic result qualification audit after A29

Continuation after the completed queue audits the open arithmetic saturation
finding. The original examples still reproduce. `MIN - MIN` returns a -1 mantissa
and fails checked subtraction at all 29 scales; a representable cancellation
also fails after intermediate addition alignment. For signed MAX divided by
the same signed magnitude at scales 1–28, all 112 sign/scale cases reject exact
signed powers of ten. Four genuine multiplication-overflow sign cases clamp at
scale 28, near 17 billion, rather than the global bound. This is an additional
fallback-scale observation, distinct from A28's corrected excess precision.

Proposed A30 qualifies arithmetic results before declaring magnitude overflow.
Reuse existing wide temporaries and the current rounding owner for fitting
addition/subtraction and division; primitive true-overflow bounds use scale zero.
Addition/subtraction use the maintained half-away rounding and 28-digit ceiling;
division retains an 18-digit ceiling and retries rounded-result narrowing.
Qualify operator/assignment/iterator and checked consumers, running SUM/AVG,
stored-field SQL, cancellation, signed limits, rounding, underflow and typed
overflow/zero-divisor controls. One existing arithmetic authority owns this
bounded outcome; no new mode, format, state or second arithmetic flow is needed.

All 55 maintained decimal/numeric/SQL parity tests pass; the additional defects
need new coverage. The disposable source/receipt are
`/tmp/icydb-a30-saturation-audit.rs` and `/tmp/icydb-a30-saturation-audit.log`.
The probe executes primitives and checked Decimal APIs; new core/SQL behavior
is inferred from their current calls into this owner, without a new query receipt.
Only three audit/status docs change, preserving dirty A28/A29 work. Inventory
counters stay unchanged, with `model-schema-crates-3` Open. Runtime complexity
is unchanged; raw Wasm, cycles and instructions are unmeasured. No network action
or release entry is made; full suites remain user-owned.

## Grouped-order audit after A30

The user requests the next correction after the completed repair queue. The
repository rule requires a scoped read-only audit and a reported finding first.
Filtered-index predicate authority remains split between typed generated checks,
persisted SQL and reparsed runtime predicates; the seven-report family needs a
separate design before any representation replacement. This audit selects a
smaller existing-owner boundary for the next proposed outcome.

`executor-aggregate-3` is Open in current source. Generic no-LIMIT finalization
in `grouped_fold/generic/page_finalize.rs` passes `sorted=true`, then
`into_finalize_groups` calls `GroupedAggregateBundle::into_sorted_groups` without
direction. Its comparison is always ascending. Candidate ranking receives route
direction afterwards but the unbounded path never sorts those candidates again.
Finite grouped limits admit this public no-LIMIT shape. The dedicated COUNT path
and bounded generic heap already use direction-aware ordering.

Proposed A31 qualifies uniform ASC/DESC generic grouped output through the existing
`compare_grouped_boundary_values` authority at bundle sorting. This addresses one
bounded outcome without an ordering mode, execution route, format, persisted state
or enum variant. Direct tests should cover generic/dedicated and bounded/unbounded
parity, cold/warm public reads, same-direction compound keys, offset/HAVING and
maintained budget/cursor boundaries. Mixed-direction admission/order reports need
separate qualification and are not included in this closure proposal.

The disposable workspace `/tmp/icydb-a31-order-audit` copies current tracked source;
only its `owned_group_keys.rs` receives the observation probe. The query uses an
unrelated selective category index, finite grouped limits and public execution,
so full-scan admission does not mask the issue. The probe compares COUNT-only,
bounded COUNT+SUM and unbounded COUNT+SUM for both cold and warm preparation.
Both cold and warm unbounded COUNT+SUM queries return `[eng, ops, sales]`
for DESC; COUNT-only and bounded COUNT+SUM return `[sales, ops, eng]`.
The observation probe passes by asserting this defect, not correct product behavior.
Nine maintained grouping tests also pass; the filtered run additionally repeats
the probe, for ten distinct passing tests across both selections. Both executions
use the disposable snapshot test artifact in the shared target directory. All
1,105 other core Rust files match the repository byte-for-byte, and the fixture
before the appended probe is unchanged. These tests do not close the DESC finding.
Receipts: `/tmp/icydb-a31-order-audit.log` and
`/tmp/icydb-a31-maintained-groups.log`. Documentation links, inventory counts and
whitespace checks pass.

No runtime source is changed in the repository. Three audit/status docs change,
prior dirty arithmetic work and versions are preserved, and no changelog entry,
commit, push or network lifecycle action occurs. Complexity stays neutral;
raw Wasm, cycles and instructions are unmeasured. Full suites remain user-owned.
Counters become 23 Verified fixed, one Open, one Partial and 282 unchecked.
A31 is proposed for the next selection within 0.264.

## Filtered-index cluster audit after A31

The user requests deeper cluster reasoning alongside further bug work. Current
source and three disposable probes confirm four symptoms of persisted SQL
predicate authority: typed literal loss, spelling-dependent index identity,
unrelated rename changing generated operand spelling and chained rename capture.
The [cluster design](filtered-index-cluster.md) records seven per-finding verdicts,
source boundaries, the narrower CHECK-tree mismatch and the proposed accepted
bound predicate owner. It supersedes the initial triage's assumption that the
existing CHECK tree can be reused unchanged for every maintained filtered form.

All six maintained tests pass on the disposable current-source snapshot: four
SQL index-binding tests, one accepted CHECK renderer test and one accepted index
normalization test. Three observation probes pass by asserting the defects.
All 1,105 other core Rust files match current source byte-for-byte, with the
original predicate module unchanged before appended probes. Receipts are
`/tmp/icydb-filtered-cluster-audit.log` and
`/tmp/icydb-filtered-maintained-{ddl,render,accepted}.log`. Full membership,
uniqueness, planner, startup and migration receipts are future correction gates;
no such end-to-end reproduction is claimed here.

Five audit/design/status docs change, with no repository runtime, version,
changelog, commit, push or network action. Documentation/inventory and whitespace
checks pass. Counts are 24 Verified fixed, four Open, one Partial and 278 unchecked.
Proposed A32 converges accepted semantics through bind/persist/execute/identity/
rename consumers in one outcome, preserving supported forms and requiring the
pre-1.0 current-version-1 hard cut and explicit metadata/index regeneration.
Runtime complexity is unchanged; raw Wasm, cycles and instructions are unmeasured.

## Validation and limits

Subsequent user-selected A31 closes the reproduced uniform-direction generic
grouped-sort finding at the shared comparison boundary. Its
[validation record](status.md#a31--grouped-sort-direction-authority) records 37
distinct focused tests, public cold/warm and SQL window matrices, maintained
implicit owned-key ordering, lint recovery and direct guards. The audit above
retains its original reproduction and proposed scope; mixed-direction planning
is a separate follow-up. Production adds four net lines with no new ordering
mode or extra sort. Raw Wasm, cycles and instructions remain unmeasured.


Subsequent user-selected A28 closes the reproduced multiplication finding.
The current [validation record](status.md#a28--decimal-multiplication-precision)
qualifies its shared wide-product/rounding correction; the numeric audit above
retains the original reproduction and handoff scope.

Subsequent user-selected A29 closes the reproduced remainder/MultipleOf finding
through the shared Decimal owner. Its
[validation record](status.md#a29--exact-decimal-remainder) qualifies primitive,
application, accepted-rule numeric and stored-field SQL consumers. The separate
addition/subtraction/division saturation finding remains Open.

Subsequent user-selected A30 closes that saturation finding and the directly
related subtraction, division and multiplication fallback cases through shared
result qualification. Its [validation record](status.md#a30--arithmetic-result-qualification)
records the independent arithmetic oracle, checked aggregate helpers, stored-field
SQL, lint recovery and remaining measurement limits. Earlier audit reproductions
retain their original handoff scope.

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

### A32 cluster correction — 2026-10-03

The user-selected accepted predicate owner closes six filtered-index findings.
Current [closure receipts](filtered-index-cluster.md#closure-receipts--2026-10-03)
qualify typed membership/uniqueness, DDL identity, rename/reconciliation from
encoded catalog bytes, planner-versus-scan reads and crash replay. All 62
distinct focused tests, lint, core feature checks and invariant guards pass.
The recursive-bounds report remains unchecked. Current version-1 metadata/index
artifacts require recreation/regeneration; cost deltas remain unmeasured and
full suites remain user-owned. Earlier audit statements retain their handoff scope.

## Row-value boundary audit after A32

Audited 2026-10-03 after the user requests more bugs before pushing. The planned
queue is complete, so this is the required scoped read-only audit within 0.264.
Earlier A28–A32 work, concurrent dependency changes and release entries remain
intact. No runtime correction is implemented in this handoff.

| Finding | Current evidence | Verdict |
| --- | --- | --- |
| `data-1` | Absent historical scalar slots use accepted cached materialization. Six maintained tests cover five scalar kinds, both leaf codecs, null/payload fills, repeated projection/scalar reads, SQL filtering/byte length and typed corrupt/rejected controls | Verified fixed; existing correction |
| `data-2` | Canonical unit/payload enums before/after a sibling scalar round-trip through the canonical codec, but the borrowed value walker rejects all four containing maps | Open; borrowed-frame seam reproduced |
| `r2-recursive-bounds-1` | A 48-level list beneath a record leaf encodes/decodes through the accepted canonical depth boundary; borrowed leaf selection succeeds and generic materialization rejects it | Open; depth seam reproduced |
| `data-7` | Historical enum/composite default validation needs separate accepted-catalog qualification | Needs verification; no closure claim |

Canonical enum tag 0x84 carries the maintained current version-1 ID-backed
header. Generic skip treats it as a nested Structural Binary frame. Map views
validate every sibling, so even a scalar path can fail when an enum appears
elsewhere in the record. Projection path resolution and the single-path grouped
reader use that borrowed view. Their selected-leaf materializer also uses the
generic recursive decoder, whose cursor and collection helper both advance
nesting depth. Canonical full decode already owns correct enum framing and the
accepted recursive limit; these facts drift at traversal boundaries.

Proposed A33 converges current persisted-value traversal on that authority,
reusing the existing scalar codecs and canonical enum wire decoder. The simplest
alternative, adding only an enum tag exception or increasing a private depth
constant, preserves competing semantic owners and leaves adjacent cases exposed.
One maintained grammar and one depth owner replace those divergent interpretations;
behavior-axis delta is zero. The wire stays in its current version-1 form; no
predecessor decoder, mode, cache, fallback or persisted state is proposed.

Qualification must cover unit/payload/nested enums, scalar siblings on either
side, list/map recursion at and beyond the accepted limit, exact trailing and
truncated-byte rejection, null/missing paths, scalar versus enum leaf selection,
projection/grouped execution and repeated calls against general materialization.
Keep historical-fill regression controls. Full public-query/lifecycle receipts
remain implementation work; source/probe evidence alone does not establish them.

Maintained receipt: `/tmp/icydb-row-boundary-historical.log` (six passing tests).
Disposable receipts: `/tmp/icydb-row-boundary-probes.log` (two passing defect
observation tests). Probe workspace: `/tmp/icydb-row-boundary-audit`. All 1,108
core Rust files match the repository except that copy's canonical codec file,
which contains the observation tests. No repository runtime source is edited.
Four documentation files change; runtime complexity stays unchanged. Raw Wasm,
IC cycles and instruction deltas are unmeasured; full suites and pushes remain
user-owned. Current counts are 31 Verified fixed, two Open, one Partial and 273
Needs verification.
