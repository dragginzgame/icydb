# Ordered Scalar DISTINCT Prefix-Group Seek

Status: PROMOTED — THE NUMBERED 0.238 DESIGN IS AUTHORITATIVE

Promoted into [IcyDB 0.238 Ordered Scalar DISTINCT Prefix-Group
Seek](../0.238-ordered-scalar-distinct-prefix-group-seek/0.238-design.md). The
numbered design incorporates the accepted physical-entry definition, singular
planner contract, small-window cap, NULL/normalization/direction proof and hard
5M release gate. This note remains the historical intake and is no longer the
implementation authority.

This note evaluates the simple-query hotspot isolated by the completed 0.237
measurement line. It does not reopen 0.237 or alter runtime behavior. The
accepted implementation boundary and release gates now belong exclusively to
the numbered 0.238 design.

## Decision

The measured hotspot is real and narrowly removable. The preferred first
design is a cursorless, finite, pure-covering scalar `DISTINCT` lane that uses
canonical accepted-index prefix bounds to advance once per projected value
group. The prepared-plan owner must use the existing
`projection_distinct_strategy` proof exactly once while constructing the
immutable contract, and the existing adjacent accumulator remains the final
DISTINCT/window owner. The executor cannot rerun or reinterpret admission.

A sequential adjacent stop is rejected as the primary fix. It would avoid much
of the work in favourable distributions, but the measured descending case
would still consume 1,793 of 2,048 candidates. A prefix-group seek instead
makes physical work depend on `OFFSET + LIMIT + lookahead`, not on duplicate
population or traversal direction.

Verdict: **DESIGN VIABLE — ACCEPT PREFIX-GROUP SEEK FOR A SEPARATELY NAMED
IMPLEMENTATION SLICE**.

## Evidence and removable work

The 0.237 Patch 12 matrix held the 2,048-row fixture, query results, selected
index family and lifecycle constant. Direct projections whose complete
DISTINCT tuple is a leading source-order equivalence used the existing
`OrderedAdjacent` strategy. Nevertheless, the scalar handoff removed the
authored page, materialized all source rows, and only then allowed the adjacent
accumulator to stop.

| Measured query | Instructions | DISTINCT candidates | Row gets | Index entries | Range scans |
| --- | ---: | ---: | ---: | ---: | ---: |
| `age ASC LIMIT 1` | 92,605,670 | 129 | 2,048 | 2,048 | 33 |
| `age ASC LIMIT 3` | 93,144,415 | 385 | 2,048 | 2,048 | 33 |
| `age DESC LIMIT 3` | 92,261,973 | 1,793 | 2,048 | 2,048 | 33 |
| `name ASC LIMIT 3` | 90,861,504 | 64 | 2,048 | 2,048 | 33 |

The expression and nullable controls used `GlobalReplay`, consumed all 2,048
candidates, and are not candidates for this design. Their measured totals were
109,092,794 and 118,571,774 instructions respectively.

For `age ASC LIMIT 3`, the corresponding non-`DISTINCT` covering query cost
1,366,834 instructions, read three index entries, and fetched no rows. The
absolute best-case instruction ceiling is therefore 91,777,581 instructions.
That difference is not a candidate forecast, but it proves that DISTINCT
hash/key comparison is not the dominant owner: complete upstream row and index
materialization is.

### Distribution sensitivity

| Measured shape | Sequential candidates through lookahead | Entries avoidable by sequential stop | Conservative prefix-seek entries | Entries avoidable by prefix seek |
| --- | ---: | ---: | ---: | ---: |
| `age ASC LIMIT 1` | 129 | 1,919 (93.70%) | at most 2 | at least 2,046 (99.90%) |
| `age ASC LIMIT 3` | 385 | 1,663 (81.20%) | at most 4 | at least 2,044 (99.80%) |
| `age DESC LIMIT 3` | 1,793 | 255 (12.45%) | at most 4 | at least 2,044 (99.80%) |
| `name ASC LIMIT 3` | 64 | 1,984 (96.88%) | at most 4 | at least 2,044 (99.80%) |

The prefix-seek bounds include one lookahead group so the current adjacent
accumulator can preserve `has_more` and bounded-stop accounting. A cursorless
result could omit that lookahead, but the first implementation should preserve
the established finalization contract rather than optimize one more entry.

The realistic acceptance target is a maximum of 5,000,000 instructions and a
minimum 75,000,000-instruction saving for each of the four maximum-shape
queries. These are proposed build gates, not measured candidate results. A
prototype that cannot put every admitted direction and duplicate distribution
at or below 5,000,000 instructions must be rejected rather than widening the
cohort or raising a ceiling.

## Current owner map

| Component | Current responsibility | Design responsibility |
| --- | --- | --- |
| Accepted schema and `EntityAuthority` | Own index identity, fields, codecs and store paths | Unchanged; no generated-model reconstruction or fallback |
| Access planner and lowered index range | Select the index route, direction and raw scan envelope | Unchanged; the new lane consumes one already selected range |
| Covering-read plan | Prove direct fields can be obtained from index components and state row-presence policy | Supplies the existing pure-covering proof; does not decide DISTINCT semantics |
| Prepared-plan DISTINCT admission using `projection_distinct_strategy` | Decide `OrderedAdjacent` versus `GlobalReplay` from prepared projection and resolved order | Remains the sole classifier and creates the narrow execution contract only after returning `OrderedAdjacent` |
| Covering component stream | Pull decoded index components in selected order and retain a raw continuation anchor | Adds a local exact-prefix advance operation for the admitted lane |
| Row store / missing-row policy | Prove an accessed secondary entry still names an existing row, or fail/skip | Unchanged and applied before a group representative is admitted |
| Adjacent DISTINCT accumulator | Own canonical projected equality, offset/limit, output rows, lookahead and diagnostics | Unchanged; consumes bounded group representatives instead of a complete materialized source |

This ownership split is important. The covering lane does not infer that a
query is DISTINCT-safe, and raw key equality does not become a second SQL
semantic authority. The projection owner supplies an immutable execution
contract after its current leading-equivalence proof. The index codec supplies
only the physical boundary for the current already-proven group. Final
projected `Value` equality and output shaping remain with the existing
accumulator.

## Proposed admission contract

The first implementation should require all of the following:

1. The request is a scalar load with `DISTINCT`, a finite `LIMIT`, no live or
   exhaustive cursor, and no continuation progress.
2. `projection_distinct_strategy` returns `OrderedAdjacent`.
3. The projection contains exactly one direct, non-nullable field.
4. The covering plan sources that field from component zero of one accepted
   user secondary index.
5. The selected access is exactly one lowered index range whose physical order
   satisfies the resolved projection order in either direction.
6. There is no residual expression, residual predicate, prefix family,
   intersection, hybrid row field, post-access sort or primary-store route.
7. `LIMIT + OFFSET` is at most the single prepared-plan constant `3`; one
   lookahead makes the physical maximum exactly four representatives.
8. The accepted row-presence policy is `MissingRowPolicy::Error`; `Ignore`
   remains on the predecessor route.

These restrictions are deliberately narrower than every shape for which
adjacency could eventually be proven. Equality prefixes, composite DISTINCT
tuples, nullable direct fields, expression-index projections, unique indexes,
cursor continuation and index-only predicates should remain on existing paths
until separately measured. A unique index would gain little because every row
already forms one group.

The admission contract is an immutable derivative of the prepared projection,
resolved order, covering plan and lowered access range. It introduces no mode,
configuration, persisted representation, public enum, cache, authority,
invalidation edge or compatibility path.

## Proposed execution

The executor should perform these steps:

1. During prepared-plan construction, ask the existing projection DISTINCT
   classifier for its strategy. A `GlobalReplay` result ends admission before
   store access.
2. The same prepared-plan owner validates the narrow covering, page and
   single-range facts above and freezes an `OrderedCoveringDistinctWindow`
   contract.
3. The executor pins that prepared plan and accepted entity authority exactly
   as it does today, then consumes the contract without reconstructing its
   eligibility.
4. Open the existing covering component stream in the selected direction with
   a one-entry refill. A one-entry buffer is intentional: reading and charging
   duplicate entries only to discard the buffer would defeat physical seek.
5. Pull one entry, validate its raw key and existence witness, apply the
   existing row-presence policy, and decode the projected component through the
   covering codec.
6. If the admitted `MissingRowPolicy::Error` observes a missing row, return the
   same typed store-corruption error. `Ignore` is never admitted because
   searching duplicate entries for a present representative would break the
   four-entry maximum.
7. Pass the representative projected row to the existing adjacent accumulator.
8. Build the exact raw prefix envelope for the representative's encoded
   component through `raw_keys_for_component_prefix_with_kind`. For
   ascending traversal, advance after the group's high sentinel; for
   descending traversal, advance before its low sentinel.
9. Clear the one-entry buffer, validate the synthetic anchor against the
   original lowered range, and perform the next ordinary range refill. If the
   group boundary reaches the outer envelope, mark the stream exhausted.
10. Stop after the adjacent accumulator closes the requested window and its
    lookahead group, then return through the unchanged projection result path.

The raw key codec already owns exact-prefix low/high sentinels for arbitrary
encoded component bytes. `IndexScanContinuationInput` already owns directional
exclusive resume and envelope validation. The new behavior should compose
those owners locally; it must not duplicate segment framing, calculate a
lexical successor, consult generated index models, or expose a general seek on
every `OrderedKeyStream`.

One implementation detail needs an explicit invariant. The component stream
currently retains the last raw key as its refill anchor while returning a row
without that raw key. With a one-entry refill, that anchor is the current
representative. The group-advance helper may decode that anchor to obtain its
index id, key kind, component count and component bytes, then ask the canonical
index codec for bounds. It should verify that the decoded component matches the
covering payload before repositioning. This avoids widening
`IndexComponentRow` across unrelated consumers or adding index identity back to
`LoweredIndexScanContract`.

## Why not sequential stop first?

A sequential covering stop is semantically attractive: pull candidates,
verify row presence, compare adjacent projected values, and stop after the
lookahead group. It would remove all data-row decoding and would need no
physical seek target. It remains a valid fallback if prefix-bound composition
cannot be proven.

It is not the preferred build because its cost is proportional to the number
of duplicates before the window closes. The existing fixture deliberately
demonstrates the problem: reversing the same accepted index changes the
candidate count from 385 to 1,793. That is not a rare planner edge; it follows
directly from a common value occupying most rows. Shipping only sequential
stop would make the optimization look successful in ascending tests while
leaving the reverse query exposed to data distribution. It also creates a
likely second optimization later, increasing review and regression surface.

Prefix-group seek has a larger local proof but a smaller operational state
space: one representative per distinct value, independent of duplicates and
direction. The repository already supplies canonical prefix-bound encoding,
directional resumption, range-envelope checking, covering component decode and
row-presence checks. The design therefore adds one composition of existing
owners, not a general random-access executor.

## Corruption and fallback semantics

The current query scans all 2,048 candidates only because the page is
suppressed before projection DISTINCT. Complete duplicate scanning is not a
schema-validation API. Existing finite index windows already stop once their
requested rows are obtained, so corruption in an unvisited suffix is not
reported by that request.

The proposed rule is the same: every physically accessed entry must validate
its raw key, witness, component payload and authoritative row presence before
it can affect output. Entries skipped by a proven exact-prefix bound are not
accessed. A malformed or missing representative fails closed; execution must
not catch that error and fall back to a path that hides it.

Fallback is allowed only before the first index entry is read. An unsupported
plan shape returns “not admitted” and uses the current scalar/global path. Once
the bounded lane begins, an invalid raw key, invalid witness, impossible prefix
bound, envelope escape, borrow conflict or row-presence failure is a typed
error. There is no partial-result fallback and no default/generated-model
reconstruction.

The one semantic proof not supplied by Patch 12 alone is codec equivalence:
for every admitted non-null scalar field kind, equal projected SQL `Value`s
must have equal canonical index components, and unequal values must not share a
component. Equality lookup and unique-index correctness already rely on that
property, but the implementation slice must add direct cross-kind tests at
this boundary. If the proof fails for any kind, that kind stays unadmitted; the
executor must not normalize it locally.

## Alternatives

| Option | Expected gain | Complexity | Verdict |
| --- | --- | --- | --- |
| A. Keep complete scalar materialization | None; retains roughly 90M–93M for measured direct cases | No new code | Reject for the isolated hotspot |
| B. Scalar sequential adjacent stop | Saves work after 129/385/1,793/64 candidates but still fetches and decodes every consumed row | Small stop contract in scalar scan; distribution-sensitive | Reject as primary design |
| C. Covering sequential adjacent stop | Avoids row decode and complete suffix work, but still reads up to 1,793 index entries | Small local covering fold; distribution-sensitive | Retain only as fallback if D is disproved before implementation |
| D. Narrow covering prefix-group seek | At most two entries for `LIMIT 1` and four for `LIMIT 3` including lookahead; no data-row fetch | Bounded group-anchor composition and codec proof | Preferred |
| E. General seekable key-stream or arbitrary DISTINCT pushdown | Could serve more shapes | New stream capability, more routes, cursor and merge semantics | Reject for first build |
| F. Persisted distinct-value catalogue/cardinality surface | Could make group enumeration independent of index rows | New persisted authority, publication/recovery work and format state | Reject |

## Complexity budget

The accepted implementation should remain one substantive landing patch in a
separately named minor. The expected production footprint is three or four
existing files, approximately 120–220 net runtime lines, one small immutable
execution contract, and no new module. Focused unit and integration evidence
is expected to add approximately 250–450 lines.

| Dimension | Maximum accepted first-build delta |
| --- | --- |
| New public API / Candid / exports | None |
| New persisted or stable state | None |
| New schema or generated authority | None |
| New mutable owner / cache / invalidation edge | None |
| New runtime modules | None |
| New internal contracts | One immutable admission/window contract; one local group-advance result if needed |
| New failure states | No durable state; only typed existing execution/corruption failures |
| Production files | Prefer 3; no more than 4 without redesign review |
| Runtime line delta | Target 120–220 net lines |
| Raw Wasm | Target no more than +3,072 bytes; raw size is the decision metric |
| Gzip Wasm | Report as secondary context only |

### State-space delta

The proposal adds one internal execution-route choice with two values:
`prefix-group covering` for the admitted cohort and `existing execution` for
everything else. It can combine only with cursorless scalar loads already
classified as ordered-adjacent DISTINCT, one direct non-null projection, one
accepted index range and a finite page. The projection facade rejects every
other combination before store access by declining to construct the immutable
contract.

The canonical decision owner is prepared-plan construction, which invokes the
existing projection DISTINCT classifier as part of one admission decision;
the covering executor consumes but cannot recreate that decision. For the
admitted cohort, the new route replaces the complete scalar fetch rather than
running beside it. The complete path remains the fallback for all non-admitted
queries, so no product-visible mode or user choice is added. The only retained
execution specialization is justified by measured hot-path cost; output
equality, windowing and diagnostics converge immediately on the existing
adjacent accumulator.

No technical-debt ledger item, compatibility path or independent authority is
created. Maintenance burden increases by one narrow physical specialization
and its proof matrix; it is proportionate only if the proposed physical and
instruction gates pass.

If implementation requires a general stream trait, a planner mode, new
persisted cardinality data, cursor encoding, generated fallback, or more than
one DISTINCT classifier, this design has failed its simplicity boundary and
must return for review.

## Required proof matrix

### Semantic and ordering proofs

- `age ASC LIMIT 1`, `age ASC LIMIT 3`, `age DESC LIMIT 3`, and `name ASC LIMIT
  3` equal SQLite exactly.
- `OFFSET 0`, a non-zero distinct offset, `LIMIT 0`, fewer groups than the
  window, exactly full window, all rows equal, empty input and one-row input.
- Duplicate groups split across physical stable-map chunks in both directions.
- Every admitted scalar index codec kind proves projected equality iff encoded
  component equality; unsupported/nullable kinds remain on the old path.
- Publication changing the target index or field produces new-query results
  from the new immutable authority while a pinned old request remains
  internally consistent.

### Conservative fallbacks

- Nullable direct field, `CASE`, arithmetic order expression, expression
  projection, hidden order prefix and incompatible projection order.
- Composite projection, multiple indexes/prefixes, equality prefix, residual
  filter, intersection, hybrid covering, primary-store access and no limit.
- Live cursor, exhaustive cursor, continuation progress and page-work envelope.
- Each fallback proves the current `GlobalReplay` or scalar route and exact
  physical-work behavior rather than merely checking output.

### Corruption and lifecycle

- Missing first representative in ascending and descending traversal fails
  with the existing typed store-corruption taxonomy.
- `MissingRowPolicy::Ignore` is rejected before store access and retains the
  predecessor route and physical work.
- Malformed representative raw key, invalid existence witness, component
  mismatch and a synthetic group anchor outside the original envelope fail
  closed.
- Corruption errors after lane admission never trigger a fallback.
- Heap-only, stable-only and journal-overlay-visible index states return the
  same result and physical bound.
- Journal replay/fold, projection reset, same-Wasm upgrade and reinstall retain
  current behavior; the design adds no recovery state.
- A borrow conflict remains a typed error and retains no partial cursor or
  result state.

### Performance and anti-regression

- At 2,048 rows, every admitted `LIMIT 3` case reads at most four index entries,
  performs at most four row-presence probes, performs no data-row gets, and
  uses at most four range refills.
- Candidate count and instructions remain bounded when the dominant duplicate
  group moves from the low edge to the high edge and when traversal reverses.
- Maximum totals are at most 5,000,000 instructions and save at least
  75,000,000 versus each frozen predecessor measurement.
- True-warm execution preserves the compiled-command hit and the same physical
  bound.
- The existing nullable/expression global controls do not regress, and all six
  shipped 0.237 optimization families retain their exact physical assertions
  and approximately one-percent CI ceilings. This is the cross-query
  no-whack-a-mole gate.
- No-default, SQL-only and all-feature focused builds pass; stable extent,
  Candid and exports are equal.
- Raw and gzip Wasm deltas are measured in an isolated candidate snapshot, with
  raw bytes controlling acceptance.

## Landing decision

The hotspot justifies one separately accepted implementation slice because the
current complete fetch costs about 90M–93M instructions for four simple finite
queries, the removable phase is observed, and the narrow design can cap
physical reads independently of duplicate distribution. The gain cannot be
obtained by ordinary page pushdown, and sequential-only stop does not solve the
measured reverse-distribution case.

The build must stay inside the existing accepted-schema, planner, covering
codec and adjacent-DISTINCT ownership chain. It may prepare this design as one
landing patch only after the maintainer names the target minor and accepts the
accessed-corruption semantics and complexity budget above. It must not be
added to 0.237 or begin from a generic continuation request.
