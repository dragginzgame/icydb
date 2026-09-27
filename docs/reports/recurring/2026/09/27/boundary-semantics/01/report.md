# Boundary / Envelope Semantics Audit

## Metadata And Selection

- Requested work: locate the least recently run active audit, review and improve
  its definition, then execute it. This authorizes the method edit and this new
  report, not implementation of findings.
- Definition: `docs/audits/recurring/range/boundary-envelope-semantics.md`.
- Method: `BOUNDARY-5` + `DOMAIN-1`; definition SHA-256:
  `2bb0a5c398ecd02b10f8fcd62c503d059eaac52d6c981079f8b20dd6a7ef10ca`.
- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace version
  `0.261.11`, plus pre-existing dirty `Cargo.toml` and `Cargo.lock`.
  The manifest changes `ic-testkit` from `0.10.0` to `0.10.1`; the lockfile
  includes dependency updates. Neither was changed by this audit.
- Input SHA-256: `Cargo.toml`
  `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c`;
  `Cargo.lock`
  `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716`.
- Compared baseline: **N/A**; no prior `BOUNDARY-5` / `DOMAIN-1` run.
- Historical reference:
  [2026-05-11 run 01](../../../../05/11/boundary-semantics/01/report.md),
  Method V4. This is the latest execution of this active scope.
- Comparability: **non-comparable (method and coverage change)**. Affected
  deltas are `N/A (method change)`. Stable qualitative anchor: ASC excludes the
  anchor at the lower edge; DESC excludes it at the upper edge. No historical
  test execution is reused or counted as new evidence.

Selection used canonical report dates and each active definition's latest
run, not file modification times or the oldest individual historical report:

| Active scope | Latest run before this audit |
| --- | --- |
| boundary-semantics | 2026-05-11 / 01 |
| state-machine-integrity | 2026-05-13 / 01 |
| error-taxonomy, recovery-consistency, resource-model-compliance | 2026-05-14 / 01 each |
| completeness, cursor-ordering, index-integrity, security-boundary | 2026-06-25 / 01 each |
| complexity-and-technical-debt, flow-convergence-and-duplication | 2026-08-26 / 01 each |
| wasm-footprint | 2026-08-26 / 02 |
| invariant-preservation, perf-audit | 2026-08-27 / 01 each |

Archived structural methods, summary reports, and superseded performance scope
names are not separate active definitions eligible for this selection.

### Review Of The Audit And Method Changes

The prior definition contradicted the DESC behavior already recorded in its
last report: it required rewriting only the lower bound and preserving every
upper bound. It also required raw lexicographic byte order everywhere, treated
component bounds as complete raw keys, assumed a particular historical raw
cursor representation, and repeated proof/output requirements across 340
lines. These issues could produce false findings and overstate coverage.

The revised definition uses directional opposite-edge preservation, complete
suffix-aware bounds, the actual `Ord` implementation, route-specific logical
ordering, and current cursor admission owners. It requires independent
semantic encoding evidence and a storage-level empty-envelope proof, links the
shared executed-test contract, corrects the canonical baseline location, and
consolidates the repeated tables. This is a governance-only change; runtime
states, APIs, formats, and release metadata are unchanged.

### Frozen Scope

Requested baseline: maintained range lowering and envelope consumers, traced
through scalar continuation and current storage adapters. Selected obligations:
directional containment/progress, strictness and suffix lowering, comparator
alignment, empty traversal suppression, and scalar query/authority/route binding.

Owners: planner range bounds, access lowering, index range/codec/envelope,
raw key comparison, index scan adapters, internal chunk continuation, covering
prefix merge, scalar token/session admission, and logical page filtering.

Excluded: grouped/HAVING cursor semantics (separate ordering domain), mutation
job continuation, exhaustive-page revision-proof lifecycle, whole recovery
protocol, all-schema-type encoding certification, hostile persisted-key
ordering, performance and Wasm measurement. The journaled scanner and one
fold/reopen test remain included because they consume the selected bounds;
this is not a recovery audit. Tests sample maintained scalar and numeric value
domains and do not certify every accepted enum/collection encoding. Public
scalar token checks are inspected through the shared session path; public
admission-policy breadth is outside this range audit.

## Invariant Registry And Transformation Proof

Source paths below are relative to `crates/icydb-core/src/db/` and refer to the
recorded snapshot. Symbols identify the evidence more precisely than directory
names alone.

| Invariant / transformation | Owner and source evidence | Enforcement and conclusion |
| --- | --- | --- |
| Logical strictness and equal ties | `query/plan/planner/range/bounds.rs::merge_range_constraint`, `merge_lower_bound`, `merge_upper_bound`, `range_bounds_are_compatible` | Runtime logic maps four operators; equal `Excluded` replaces `Included`, never the reverse. Both equal bounds must be included for a singleton. Incomparable bounds decline range extraction. |
| Accepted index identity to raw envelope | `access/lowering.rs::lower_index_range_bounds_for_scope`, `LoweredIndexRangeSpec` | Uses schema-informed prefix encoding and semantic index ordinal, generation and arity; runtime receives materialized bounds and scan contract. No generated-model reconstruction on this path. |
| Value-to-component strictness | `index/range.rs::encode_semantic_component_bound`, `build_index_component_range_with_encoded_prefix` | Preserves `Bound` variant; bounded fallible encoding and construction admission precede full-key creation. |
| Full-key suffix lowering | `index/key/codec/mod.rs::raw_bounds_for_prefix_component_range_with_kind`, `RangeBoundSide` | Bound side and exclusivity choose low/high suffix and primary-key sentinels. Prefix, namespace and physical index identity stay embedded. |
| Anchor containment and direction | `index/envelope/mod.rs::resume_bounds_for_continuation`, `KeyEnvelope::contains` | Production containment guard runs before the private rewrite; debug assertions are supplementary. ASC changes lower only, DESC upper only, no anchor preserves both. |
| Candidate progress | `index/envelope/mod.rs::validate_index_scan_continuation_advancement`; `cursor/runtime.rs::ContinuationRuntime`; `executor/stream/access/scan.rs::accept_scan_key` | Strict `>` for ASC and `<` for DESC; equality rejected. Uses the same raw-key `Ord` as containment and traversal. |
| Empty interval before traversal | `index/envelope/mod.rs::envelope_is_empty`; `index/scan/raw.rs::visit_raw_entries_in_range` and `visit_canonical_raw_entries_in_range` | Inverted or equal-with-an-exclusive-edge returns before backend range construction. Source supports the invariant; required behavioral evidence is missing (BND-01). |
| Chunk resume preserves envelope | `executor/stream/access/physical.rs::load_next_chunk`; `executor/stream/access/scan.rs::resolve_component_chunk`, `chunk_structural` | Chunk carries last physically visited raw key and retains original bounds; next scan revalidates containment and uses an exclusive edge. |
| Covering scan and merge | `executor/covering.rs::direct_covering_prefix_merge_bounds`, `CoveringPrefixComponentStream::load_next_chunk`; `executor/stream/access/scan.rs::merged_components_without_index_values` | Single/chunk routes use the shared continuation guard. Direct merge receives exact-prefix intervals rather than resumed arbitrary ranges; per-child order merges on decoded primary-key values. |
| External continuation binding | `session/query/dynamic.rs::scalar_cursor_contract`, `validate_scalar_page_token`; `cursor/token/codec.rs::decode_authenticated_payload` | MAC checked before using payload; signature, mode, accepted root/entity identity, route pin, window and order terms checked before row execution. |
| Logical cursor filtering | `executor/terminal/page/post_access.rs::apply_post_access_to_kernel_rows_dyn`, `apply_load_cursor_and_pagination_window` | Orders using the resolved query contract when route order is insufficient, then applies strict logical boundary and page window. |

### Logical To Complete Raw Mapping

For equality prefix `p`, range component `v`, and remaining components/PK,
`low(v)` uses low sentinels and `high(v)` high sentinels. The codec's
`RangeBoundSide::{suffix_sentinel,primary_key_sentinel}` implements:

| Operator | Semantic interval edge | Complete raw edge |
| --- | --- | --- |
| `>` | lower `Excluded(v)` | lower `Excluded(high(v))` |
| `>=` | lower `Included(v)` | lower `Included(low(v))` |
| `<` | upper `Excluded(v)` | upper `Excluded(low(v))` |
| `<=` | upper `Included(v)` | upper `Included(high(v))` |

The low sentinel is `[0]`; high uses `0xFF` at the admitted maximum segment
length. Maintained encoded scalar components lie inside these limits. A
component-level unbounded side still receives physical prefix/identity bounds.
This is different from a fully `Bound::Unbounded` raw envelope. The BigInt
store test exercises both endpoint strictnesses, a fixed equality prefix,
remaining component and two PK suffixes, in ASC and DESC after fold/reopen.
Expression and text-prefix session tests supply neighboring logical values.

## Adversarial Matrix

`E` denotes the envelope test family; `R` the planner merge tests; `S` the
store scan family; `P` the selected session tests; `T` the token tests. The
verification section records executions. “Source only” is not test evidence.

| Scenario | Direction and expected result | Enforcement / evidence | Remaining risk |
| --- | --- | --- | --- |
| Anchor at included lower | ASC excludes it and retains upper; DESC collapses empty | Containment + directional rewrite; E has DESC endpoint collapse | No dedicated ASC endpoint result assertion found |
| Anchor at excluded lower | Both reject before rewrite | `KeyEnvelope::contains`; E exercises exclusion generally | This precise lower-anchor combination is source-only |
| Anchor at included upper | ASC collapses empty; DESC excludes it and retains lower | Directional rewrite; E has ASC endpoint collapse | Storage no-traversal observation missing, BND-01 |
| Anchor at excluded upper | Both reject | E containment and public resume helper reject excluded upper | DESC uses same direction-independent admission |
| Below lower / above upper | Both reject | Two-sided comparisons in `contains`; E tests adjacent out-of-range containment | Full directional anchor matrix is not separately executed |
| Inverted / equal endpoints | Inverted empty; equal empty unless both included | Raw emptiness source; R tests all semantic equal endpoint inclusion combinations | Raw traversal observation missing, BND-01 |
| Singleton and resume | Only included/included contains key; either resume direction empties it | Containment and emptiness composition; R singleton | Exact raw singleton-to-store case source-only |
| Either/both unbounded | Existing bounded edge enforced; anchored edge replaced in scan direction | E lower-unbounded and upper-unbounded containment; source covers both unbounded | Both-unbounded resume not separately tested |
| Empty after resume | Return before range construction on either backend | E endpoint collapse plus `index/scan/raw.rs:410` guard | Required store-level proof absent, BND-01 |
| Equal value, different suffix/PK | Keep all qualifying keys, strict resume at complete key | S BigInt two-page equality comparison in both directions; full-key comparison test | Tested domain is bounded; no all-type claim |
| Equal-bound tightening / prefix neighbors | Excluded tie remains strongest; adjacent nonmatching prefix rows removed | R strict ties; P expression operators and starts-with with residual filtering | No demonstrated escape |
| Mutated authority / index / query / order / route | Reject external mismatches before row execution | Session contract source; P foreign root and index pin; T tamper/wrong key and route identity | Exact query/order/window mismatch execution not claimed; equality guard inspected |

### Opposite Edge And Empty-Envelope Proof

For a contained anchor `a` in `(l,u)`, ASC becomes `(Excluded(a),u)` and DESC
becomes `(l,Excluded(a))`. Since `a` is already contained, the rewrite cannot
widen the interval. The opposite bound is cloned without modification. A
missing anchor clones both bounds. Exclusive resumption removes precisely the
anchor and previously traversed side under the selected direction.

`envelope_is_empty` treats `l > u` as empty and `l == u` as empty unless both
edges are included. An unbounded side cannot prove structural emptiness, even
if there are no matching stored keys. The shared single-range scanner returns
before selecting heap/journaled backends. The canonical-only scanner has the
same pre-range guard. Therefore its suppression mechanism is source-supported,
but the two endpoint tests stop at the helper and do not call storage.

The merged scanner does not have this per-envelope guard. Its inspected
production caller constructs exact-prefix `[low,high]` intervals; these are
structurally ordered even when no stored key matches. It does not accept the
raw resumed envelope from the chunk path. No reachable empty-resume bypass
was demonstrated. Accepting arbitrary resumed intervals in this API would
require revisiting that caller precondition.

## Ordering And Continuation Analysis

### Actual Comparator And Independent Evidence

`key_taxonomy.rs::RawIndexStoreKey::cmp` delegates to
`compare_raw_index_store_key_bytes` (line 1418). For valid complete frames it
compares kind, index identity, component count, component payloads, and PK.
Length-prefix framing is not part of component value order. Plain serialized
byte comparison is therefore not the maintained valid-key order.

The E `cross_layer_canonical_ordering_is_consistent` property compares encoded
keys with envelope/advancement helpers using the same comparator. Its finite
Int64/Text/Nat64 domain supports comparator agreement only; it does not itself
compare semantic values or perform the decode/re-encode claimed in its comment.
Independent ordered-semantic tests compare value/numeric order with encoded
bytes, including primitive samples, Decimal, IntBig, NatBig and U256 property
families. The full-key test compares decoded `IndexKey` order against raw
order across all admitted component counts and differing PKs. S verifies raw
store direction and a separate decoded-order merge across multiple backings.

Malformed-frame fallback behavior exists in the raw comparator but is outside
this valid-key range baseline; no malformed-key total-order verdict is made.

### Current Public Tokens Versus Internal Raw Anchors

Scalar tokens retain logical last-emitted progress and optional physical
primary-key progress. They do not expose the historical raw index anchor.
`ScalarContinuationContext::access_scan_input` supplies no external raw index
anchor and only carries primary-key seek progress for PK-ordered plans.
Internal physical/covering chunk streams create raw anchors from visited keys.
`resolve_component_chunk` updates `last_raw_key` before residual acceptance,
so even a chunk emitting no rows can make physical progress without losing an
unvisited candidate. The logical boundary remains separate.

The session decodes an authenticated current version-1 token, resolves any
pinned eligible route under current accepted authority, compares the complete
cursor contract, then constructs continuation execution state. The physical
primary-key boundary decoder checks key validity and entity identity; token
boundary decoding bounds slot count and values. MAC protection prevents the
caller from independently replacing those slots. This inspection does not
claim a newly executed test for every individual contract equality branch.

No-duplication/no-omission evidence is bounded: the stable-data session tests
compare complete actual row sequences in ASC/DESC one-sided ranges over
multiple pages; the authored-limit test compares the exact eligible output;
the store test compares both pages against expected raw keys with repeated
indexed values and distinct PK suffixes. Logical filtering executes after the
route's resolved ordering. These do not imply snapshot pagination under live
between-page mutations or certify the separate exhaustive-page proof mode.

## Findings, Drift Triggers, And Verdict

### BND-01 — MEDIUM — Missing Empty-Envelope Store Verification

- **Owner:** `db::index::scan` tests at the envelope-to-store boundary.
- **Evidence:** `index/envelope/tests.rs::{anchor_equal_to_upper_resumes_to_empty_envelope,desc_anchor_equal_to_lower_resumes_to_empty_envelope}`
  assert only `envelope_is_empty`. The selected `index/scan/tests.rs` tests
  traverse nonempty envelopes. Current-source searches found no replacement
  for the historical inverted-range/no-scan test recorded in the May report.
- **Present consequence:** the required no-store-traversal guarantee has a
  source proof, but cannot be reported as behaviorally verified. A regression
  that moves/removes the pre-range guard could reach an invalid range before
  these helper tests fail. This is a verification gap, not a demonstrated
  production correctness defect.
- **Disposition:** leave runtime and tests unchanged in this audit. Follow-up
  should add a focused populated-store check for ASC upper-end and DESC
  lower-end collapse, plus inverted/equal-exclusive raw bounds, observing
  zero callbacks with the guard verified ahead of range construction. Exercise
  heap and journaled backings at this one owner rather than duplicating tests
  across every query frontend.
- **Action trigger:** close this gap before claiming a complete boundary audit
  PASS or changing the envelope-to-store traversal boundary.

**Verdict: BLOCKED.** The inspected mechanisms support the selected contracts,
and no runtime violation was demonstrated. The mandatory storage proof in
BND-01 is unavailable, so passing selected tests cannot justify an overall
PASS. Other matrix rows explicitly distinguish source-only reasoning from
executed cases; out-of-scope domains receive no verdict.

Drift triggers: new admitted component encodings require independent ordering
proof; new callers of merged traversal must preserve its ordered exact-prefix
precondition; changes to internal chunk progress or public token contracts
require corresponding directional and binding evidence. No debt ledger or
implementation plan was changed.

## Verification Readout

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`; native core unit target,
`sql` feature, default feature set empty, repository Cargo home/target paths
from Makefile, `RUST_TEST_THREADS=8`. No ignored-test override or property-case
reduction was used. No IC/PocketIC network lifecycle action occurred.

The following are the exact contents of the temporary command wrappers used
for listing and execution. The wrappers themselves are disposable; their
commands and outcomes are retained here. Both calls to each wrapper used the
same selectors, features, target, toolchain and snapshot.

Selection A (`/tmp/icydb-boundary-audit-selection.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail
cd /home/adam/projects/icydb
CARGO_HOME="$(make --no-print-directory -s print-cargo-home)" \
CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)" \
RUST_TEST_THREADS=8 \
cargo test --locked -p icydb-core --lib --features sql -- \
  db::index::envelope::tests:: \
  db::index::range::tests:: \
  db::index::key::tests::ordered_semantics:: \
  db::index::scan::tests:: \
  db::session::tests::cardinality_tiebreak::expression_ordered_ranges_preserve_bounds_and_warm_results \
  db::session::tests::cardinality_tiebreak::starts_with_ranges_preserve_longer_matches_and_warm_results \
  db::session::tests::cardinality_tiebreak::pinned_route_requires_one_current_eligible_index_identity \
  db::session::tests::cardinality_tiebreak::pinned_cursor_rejects_a_foreign_accepted_root \
  db::session::tests::cardinality_tiebreak::scalar_page_limits::authored_total_limit_stays_stable_across_filtered_pages \
  db::session::tests::cardinality_tiebreak::scalar_page_limits::unfiltered_small_pages_resume_until_physical_exhaustion \
  db::session::tests::cardinality_tiebreak::scalar_page_limits::one_sided_primary_ranges_resume_across_small_pages \
  "$@"
```

Selection B (`/tmp/icydb-boundary-audit-admission.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail
cd /home/adam/projects/icydb
CARGO_HOME="$(make --no-print-directory -s print-cargo-home)" \
CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)" \
RUST_TEST_THREADS=8 \
cargo test --locked -p icydb-core --lib --features sql -- \
  db::query::plan::planner::range::tests::range_merges_move_text_bounds_and_keep_strict_ties \
  db::query::plan::planner::range::tests::range_merges_preserve_singleton_empty_and_incomparable_intervals \
  db::cursor::token::scalar::tests::authenticated_scalar_token_round_trips_complete_mixed_order_contract \
  db::cursor::token::scalar::tests::current_version_one_scalar_token_round_trips_exact_route_pin \
  db::cursor::token::scalar::tests::current_scalar_token_rejects_route_pin_for_another_entity \
  db::cursor::token::scalar::tests::authenticated_scalar_token_rejects_tampering_and_wrong_database_key \
  db::key_taxonomy::tests::raw_index_ordering_matches_decoded_keys_at_every_component_count \
  db::executor::terminal::page::tests::load_cursor_and_pagination_window_compacts_in_one_pass \
  "$@"
```

| Executed command / check | Outcome | Selected / passed / failed / ignored | Obligation and limit |
| --- | --- | --- | --- |
| Initial A with `--offline` inserted after `--locked`, invoked with `--list` | BLOCKED | 0 / 0 / 0 / 0 | Cache lacked locked `candid 0.10.37`; dependency resolution stopped before compilation/listing. |
| `bash /tmp/icydb-boundary-audit-selection.sh --list` | PASS | 61 / N/A / N/A / 0 | Network-enabled retry fetched locked dependencies; selectors confirmed before behavioral execution. |
| `bash /tmp/icydb-boundary-audit-selection.sh` | PASS | 61 / 61 / 0 / 0 | Envelope 14; semantic encoding 29; range lowering 5; store scan 6; selected session cases 7. |
| `bash /tmp/icydb-boundary-audit-admission.sh --list` | PASS | 8 / N/A / N/A / 0 | All eight exact named cases discovered; no zero-match family. |
| `bash /tmp/icydb-boundary-audit-admission.sh` | PASS | 8 / 8 / 0 / 0 | Planner ties/singletons 2; token authentication and route identity 4; full-key order 1; logical cursor filtering 1. |
| Required empty-envelope storage-observation check | BLOCKED | 0 / 0 / 0 / 0 | No maintained case found; BND-01. Helper tests are not substituted for it. |
| Definition/report consistency, local link resolution, input identity and diff whitespace checks | PASS | N/A | New method/report only; historical report and pre-existing Cargo changes preserved. |

**Newly executed behavioral tests: 69 passed, 0 failed, 0 ignored.** Property
iterations are not counted as additional tests. The 65 compiler warnings from
the `sql`-only core test build concern unused/dead-code surfaces; compilation
and both selections succeeded. Clippy was not run and no clippy failure was
reported. No full repository/workspace suite was run; that remains user-owned.
No production code was edited, so Rust formatting and runtime release notes
were not applicable. Raw search/build output remains disposable under `/tmp`;
this report preserves the commands, selected families, counts and first blocked
attempt instead of committing duplicate logs.

Performance, IC cycles/instructions and raw Wasm deltas: **unmeasured**;
correctness/documentation work only. Complexity: two audit-owned Markdown files
changed; the definition shrank from 340 to 166 lines, and this report adds the
execution evidence. Runtime implementation shape is unchanged; no independent
behavior axes, duplicated execution flows or runtime debt were added. The
method is simpler and its supported verdict is narrower and explicit.

Follow-up: address BND-01 with the focused storage proof, then create a new run
rather than changing this report. No other implementation follow-up is opened.
