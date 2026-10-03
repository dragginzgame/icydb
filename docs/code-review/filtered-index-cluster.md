# Filtered-index predicate cluster

Audited 2026-10-02 after A31 in the current 0.264 worktree. The audit/design
sections record that read-only handoff under the user's instruction to rethink
bug clusters. The implementation and closure sections record the subsequent A32
correction. Earlier dirty fixes remain intact. The [review status](status.md)
owns finding counts; the [0.264 tracker](../design/0.264-signed-index-admission/0.264-status.md)
owns selection.

## Shared cause

Generated declarations bind an accepted, typed check expression, then discard
that authority by rendering SQL. SQL DDL validates a parsed predicate but retains
its original text. The index snapshot persists only text. Writes, planner access
contracts, integrity checking, unique-activation dependencies and domain builds
parse it again. Contract comparison uses text equality, and migration planning
rewrites names sequentially. A frontend/display representation therefore owns
runtime semantics and durable identity.

This is stronger evidence for an ownership change than for isolated corrections.
Canonicalizing SQL alone would address spelling differences but retain literal
type loss, parser-dependent durable interpretation and editable-name identity.
Adding rename maps alone would retain those same problems in other consumers.

## Evidence and limits

| Finding | Current evidence | Verdict |
| --- | --- | --- |
| `xc-architecture-1` | Nat64, Int128 and integral Decimal render to `3`, parse as Int64, and cease matching their original value under the runtime strict equality policy; same-typed controls match | Open; literal/comparison seam reproduced, full query/uniqueness receipt still needed |
| `sql-parser-5` | Equivalent predicate spellings normalize identically but retain different strings; unrelated rename canonicalizes the text; addition identity compares those strings exactly | Open; text/normalization seam reproduced, DDL script receipt still needed |
| `r2-persisted-sql-text-1` | Unrelated rename rewrites generated `billing_country = shipping_country` into the reversed operand spelling; generated reconciliation compares snapshots exactly | Open; rewrite seam reproduced, startup receipt still needed |
| `xc-architecture-2` | Sequential nickname→name→display_name rewriting captures both operands; the resulting predicate differs semantically from the simultaneous rename result | Open; rewrite seam reproduced, full migration receipt still needed |
| `r2-persisted-sql-text-2` | Runtime still reparses text while durable identity retains text; code-dependent interpretation is a structural durability risk | Needs verification; no parser-change/rebuild experiment |
| `r2-persisted-sql-text-3` | Enum literal rendering uses current variant names; migration expected-predicate rewriting only covers field names | Needs verification; enum-rename planner fixture required |
| `r2-recursive-bounds-5` | Check-tree bounds and SQL source-depth bounds differ | Needs verification; test current canonical simplification before accepting the old reproduction |

Three disposable observation tests exercise these seams in
`/tmp/icydb-filtered-cluster-audit`; their passing assertions demonstrate defects,
not corrected product behavior. The receipt is
`/tmp/icydb-filtered-cluster-audit.log`. No repository runtime source is edited.
Six maintained focused tests pass: four SQL index-binding, one accepted CHECK
renderer and one accepted-index normalization test. The original predicate source
before the appended probes and all 1,105 other core Rust files match the repository.
Full query/lifecycle regressions remain required; the [audit](closeout-audit.md#filtered-index-cluster-audit-after-a31) records receipts and limits.

## Replacement boundary

The canonical owner should be the accepted index snapshot's bound predicate.
Frontends bind once against accepted catalogs; runtime consumers use those bound
semantics. SQL remains input and display, never durable semantic authority.

Do not adopt `AcceptedCheckExprV1` unchanged. It supports root FieldId operands,
comparisons and bounded length operations, but not the complete maintained DDL
predicate vocabulary. Filtered DDL already resolves nested paths and its parser
admits prefix LIKE/ILIKE and explicit comparison coercions. Replacing it with the
CHECK subset would silently narrow supported behavior.

The simplest candidate is a bounded accepted representation of the maintained
predicate operators, using resolved field/path identity and canonical typed
literal payloads. Reuse accepted literal admission/codec and path authorities;
reuse existing execution and normalization semantics. Preserve explicit coercion
policy and WHERE null behavior. CHECK's UNKNOWN acceptance rule must not become
index membership. Do not widen CHECK syntax merely to house unrelated predicates.
Before choosing concrete types, enumerate accepted generated and DDL forms and
show how each binds, persists, executes and renders without a SQL round trip.

| Consumer | Required authority |
| --- | --- |
| Generated proposals and SQL DDL | Same accepted binder; exact field-kind/literal and coercion qualification |
| Snapshot acceptance, codec and fingerprint | Bounded canonical tree with catalog-valid IDs and typed payloads |
| Write membership, uniqueness, activation/build and integrity | Same compiled semantics; no independent reparsing or coercion repair |
| Planner implication and residual proofs | Bound identities and the same comparisons used by membership |
| Duplicate detection and IF NOT EXISTS | Canonical accepted predicate identity, independent of spelling |
| Rename/reconciliation | Retained IDs; explicit transition mapping where IDs change, with names projected only for display |
| Inspection and diagnostics | Render the accepted tree through current names without making output reparseable authority |

Remove persisted predicate text, runtime accepted-text parsing and text-based
rename/identity paths as their consumers converge. Retain ordinary SQL/query
parsing and any display rendering still serving maintained frontends. Do not
add a second predicate cache, query mode, dispatch route or repair fallback.
The demonstrated need is four current symptoms of discarded semantic identity;
the rejected simplest alternative is text canonicalization alone. The canonical
owner is the accepted index predicate. Behavioral-axis delta is zero: replace
one representation and its consumers rather than retain parallel authorities.

## Proposed A32 and qualification

A32 is one end-to-end accepted filtered-predicate authority correction within
0.264: bind, persist, validate, fingerprint and execute current semantics, then
propagate that authority through identity and rename consumers. Include the
direct tests, diagnostics, docs and fixture regeneration in the same handoff.
If implementation discovers an independent outcome, report and split it rather
than add another behavior axis to this correction.

Required controls include generated/DDL parity, signed/unsigned widths, Decimal,
enum/ULID literals, nullable and missing nested paths, field-to-field comparisons,
LIKE/ILIKE and admitted coercions; actual filtered membership and uniqueness;
planner-versus-scan result parity; duplicate/IF NOT EXISTS spelling variants;
unrelated, chained and swapped field renames; enum variant renames; canonical
codec corruption/size/depth/catalog bounds; generated reconciliation and restart.
Prove each finding separately before closing it. Do not promise seven closures
from a common source hypothesis alone.

This is a pre-1.0 hard cut. Replace the current version-1 encoding in place and
require recreation/reinstall or explicit regeneration of accepted metadata and
index contents. Decode only the current bounded representation or return a typed
error. No predecessor decoder, format-version increment, translator or automatic
repair path is permitted. Cost deltas require raw Wasm, IC cycles or instructions;
this audit makes no performance claim and leaves them unmeasured.

## Implementation qualification — 2026-10-03

A32 is complete. The accepted tree uses direct FieldIds and existing accepted
canonical literal payloads; runtime projects current names without parsing SQL.
Inspection corrected the audit's nested-path assumption: query/frontend type
metadata can resolve paths, but the maintained filtered predicate program
executes direct row slots and prior accepted-index validation rejected nested
predicate names. The bound intake preserves that rejection. Native nested
index keys remain separate; no nested predicate execution route is introduced.
The stored grammar covers constants, boolean operators, comparisons, field
comparisons, membership and null tests, including maintained LIKE/ILIKE prefix
coercions. Unsupported query-only predicates are not persisted. Accepted literal kinds retain admission bounds and nominal enum identity;
generated field comparisons use accepted type capabilities. Focused qualification
and closure receipts follow.

## Closure receipts — 2026-10-03

All 62 distinct focused tests pass on the current locked dependency graph;
maintainer/focused lint, core feature checks and invariant guards pass.
The original evidence table records the audit boundary. Current verdicts are:

| Finding | Current closure evidence | Verdict |
| --- | --- | --- |
| `xc-architecture-1` | A32 retains canonical typed literals through encoding and shared execution. Nat64/Int128/Decimal membership and actual unique collisions pass; typed Nat64 reads use the eligible filtered index and match a primary scan across continuations and repeated calls. | Verified fixed |
| `sql-parser-5` | A32 compares canonical bound trees. Maintained DDL tests cover parentheses, spacing, case and repeated guards through IF NOT EXISTS and active/candidate duplicate contracts, with differing predicates remaining conflicts. | Verified fixed |
| `r2-persisted-sql-text-1` | A32 preserves FieldIds and canonical field-comparison identity through rename. The complete migration fixture reloads current encoded catalog bytes and generated reconciliation returns no changes. | Verified fixed |
| `xc-architecture-2` | A32 removes sequential predicate-name rewriting. Chained and swapped name projections retain both distinct FieldIds and canonical bytes; the real rename planner and encoded/reloaded reconciliation fixture pass. | Verified fixed |
| `r2-persisted-sql-text-2` | A32 removes persisted SQL and runtime accepted-SQL parsing. Current bounded version-1 trees retain operators, coercions, FieldIds and typed payloads; encoded catalog reload and interrupted-write checkpoint replay preserve membership. | Verified fixed |
| `r2-persisted-sql-text-3` | A32 retains canonical enum type/variant IDs instead of variant spelling. Variant rename changes display only; the migration fixture combines enum type/variant and field renames, reloads catalog bytes and reconciles generated declarations successfully. | Verified fixed |
| `r2-recursive-bounds-5` | Current bounded codec/depth controls pass; the original claimed bypass has not been separately established | Needs verification |

Qualification also preserves existing null/missing and coercion policies,
LIKE/ILIKE prefix behavior, unique-activation budget frames, codec depth/size
bounds, and generated Date/U256 field comparisons. Nested predicate paths remain
rejected by the maintained direct-slot boundary; nested key admission is separate.

One current version-1 representation replaces SQL text in place. Affected
pre-1.0 metadata/index artifacts require recreation/regeneration. Approximately
55 files/+1,600 net lines include codec/binding, propagation and tests. The shared
ownership/execution flow is simpler; local representation code grows with zero
additional behavior axes. Raw Wasm, cycles and instructions remain unmeasured;
full suites remain user-owned. Concurrent dependency updates were preserved and
the final checks use locked `ic-memory 0.15.4`.
