# IcyDB 0.202 Closeout Audit

## Verdict

**READY WITH DOCUMENTED CAVEATS.**

The released `v0.202.0` explicit-Rust-default architecture is implemented as
designed, has one accepted-schema runtime authority, and passes focused plus
workspace-wide validation. The audit found and fixed one high-risk integration
gap in the pending dependency hard cut: IcyDB accepted stable-memory names that
`ic-memory 0.11.1` rejects during generated initialization. It also closed two
important rejection-test gaps around indexed database-default changes and
malformed accepted default payloads.

There are no remaining 0.202 implementation blockers. Two caveats must remain
explicit:

1. the uncommitted `ic-memory 0.11.1` follow-up changes the durable allocation
   ledger and therefore is not covered by 0.202.0's “no persisted format
   changes” statement; it needs a named patch entry and supplemental status
   during release preparation;
2. generated memory bootstrap still converts the dependency's typed bootstrap
   error to `String` and traps through the historically infallible generated
   `db()` surface. It is fail-closed and deterministic, but public typed-cause
   propagation requires a separate generated-API decision and is outside the
   Rust-default scope.

## Executive Assessment

- Branch: `main`
- Audited commit: `14a610cd1` (`v0.202.0`, `Release 0.202.0`)
- Worktree: contains the user-authorized `ic-memory 0.11.1` and `ulid 2.0.1`
  follow-up, this audit's focused fixes, and the separate untracked 0.203 design
- 0.202 architectural sediment: low in the implemented default/persistence
  slice
- Runtime authority conflict: none found between Rust `Default` and accepted
  database defaults
- Highest-risk issue found: generated stable-key grammar lagged the new
  dependency and could defer rejection to runtime initialization; fixed
- Safe to extend the 0.202 implementation: yes, within its established owners
- Safe to publish the pending dependency follow-up immediately: only after its
  target patch and persisted-format documentation are finalized
- Confidence: high

## Authority Sources Reviewed

- `docs/design/0.202-explicit-rust-default-generation/0.202-design.md`
- `docs/changelog/0.202.md`
- root `CHANGELOG.md`
- current public/generated schema examples and macro fixtures
- macro behavior and UI tests
- accepted-schema, SQL DDL, structural-write, persistence, and recovery tests
- generated stable-memory wiring and `ic-memory 0.11.1` source contract
- Git history from `v0.201.4` through `v0.202.0`
- the dated 0.203 audit/design only as a separate historical baseline, not as
  0.202 implementation authority

No separate 0.202 status or implementation-tracker document exists. The
design's “Implementation Closeout” and thirteen-item “Closeout Standard” are
the implementation tracker.

## Current Architecture Map

| Concern | Canonical owner | Primary symbols | Competing owner found? |
| --- | --- | --- | --- |
| Domain Rust `Default` selection | Schema derive trait configuration | `TraitBuilder`, `TYPE_TRAITS`, node `validate()` implementations | No |
| Generated `Default` implementation | Owning derive strategy | `imp::default::DefaultTrait` | No |
| Intrinsic collection defaults | Collection nodes | `node::{list,set,map}::traits` | No |
| Create-input empty construction | Generated entity create DTO | generated `<Entity>_Create: Default` | No |
| Rust-only enum default marker | Derive parser/validator | derive `EnumVariant::default`; omitted from runtime schema node | No |
| Accepted field absence/default | Accepted schema snapshot/runtime contract | `AcceptedFieldAbsencePolicy`, `SchemaFieldDefault`, `StructuralRowContract` | No |
| Insert omission | Accepted-schema policy | `accepted_insert_field_is_omittable` | No; SQL has only a field-shaped adapter |
| Accepted missing-slot bytes | Structural row contract | `StructuralRowContract::missing_slot_payload` | No |
| Structural write after-image | Save executor | `SaveExecutor::prepare_structural_mutation_row_op` | No |
| Default-change index safety | Schema mutation admission | `validate_sql_ddl_field_default_change_candidate`; `PersistedIndexSnapshot::references_field` | No |
| Accepted bundle/recovery validation | Schema publication decoder | `AcceptedSchemaRevisionBundle::validate`; `decode_accepted_schema_revision_bundle` | No |
| Stable-memory allocation authority | `ic-memory` runtime, fed by generated declarations | generated `ic_memory_range!`, `ic_memory_declaration!`, `ic_memory_key!` | No |
| Stable-key syntax admission | IcyDB schema/build boundary, matching dependency contract | `stable_key_segment_is_canonical`, `stable_key_is_canonical` | No after fix |
| ULID persisted representation | IcyDB `Ulid` wrapper | canonical `[u8; 16]`; `Ulid::increment` | No |

## Representative End-to-End Traces

### Typed create

The generated create DTO records authored slots, then
`missing_create_authored_fields` evaluates omissions through
`accepted_insert_field_is_omittable`. Save preflight and canonical row emission
consume the selected accepted row contract. Generated Rust construction values
do not authorize runtime omission.

### SQL insert

SQL binding resolves fields against `AcceptedRowLayoutRuntimeContract`, rejects
explicit `NULL` when the accepted field is non-nullable, applies the same
accepted omission helper, and lowers into the private structural mutation
boundary. Omission and explicit `NULL` remain distinct.

### Structural insert, replace, and update

Public structural admission checks the accepted descriptor. Insert and replace
materialize a complete accepted image; update overlays the accepted baseline.
The executor validates one canonical structural after-image, re-emits normalized
generated fields, and merges accepted non-generated/DDL-owned slots from that
same image. No accepted patch-backed runtime entity materializer remains.

### SQL DDL default change

Binding resolves the accepted field, encodes the candidate default, preserves
exact no-ops, then rejects a real change when either an index key or a parsed
filtered-index predicate references the field. Malformed predicate metadata is
fail-closed. The typed binder error retains entity, column, and index identity
before the public admission projection intentionally reduces it to a stable
public code.

### Accepted recovery

Accepted bundle decode checks envelope/version/bounds, reconstructs the bundle,
and invokes the same bundle validator used for publication. Every persisted
default payload is validated against its accepted field and enum-catalog
contract before the bundle becomes runtime authority.

## Closeout Gate Summary

| Gate | Status | Evidence |
| --- | --- | --- |
| Shared trait set excludes `Default` | Pass | `DEFAULT_TRAITS` is Clone/Debug/Path; `TYPE_TRAITS` contains no `Default`. |
| Domain defaults are explicit | Pass | Entity, record, newtype, enum, and tuple nodes consult explicit `traits(add(Default))`. |
| Intrinsic defaults are local | Pass | List/set/map nodes add `Default`; create DTO and internal markers own their derives locally. |
| Impossible requests fail at macro boundary | Pass | Node validation plus compile-error invariant; full macro UI suite passes. |
| No arbitrary enum default | Pass | An enum without an explicit Rust default needs no marked variant. |
| Field/newtype metadata does not grant trait | Pass | Database/default constructor metadata and trait selection remain separate. |
| Accepted DB defaults own runtime behavior | Pass | One omission helper, direct accepted payload reuse, one structural after-image. |
| Fixtures/examples express intent | Pass | Domain opt-ins are confined to intentional examples/tests and color value types. |
| No compatibility path | Pass | No alias, retired directive spelling, sentinel reconstruction, or dual trait policy found. |
| `variant(unspecified)` removed | Pass | No active directive or generated sentinel remains; unrelated `ReadIntentKind::Unspecified` is a distinct concept. |
| Rust enum marker absent from schema metadata | Pass | Runtime `EnumVariant` stores only `ident` and optional `value`. |
| Focused validation | Pass | Positive, rejection, boundary, recovery, SQL, and UI cases pass. |
| Cost/wasm reported | Pass | 0.202.0 and refreshed dependency-follow-up artifacts recorded below. |

## Findings

| ID | Severity | Status | Area | Finding | Risk | Action |
| --- | --- | --- | --- | --- | --- | --- |
| 202-CO-001 | High | Fixed | Stable-memory admission | IcyDB allowed leading digit/underscore segments and keys over 128 bytes although `ic-memory 0.11.1` rejects them | Generated code could compile and later panic during static declaration/bootstrap | Match the dependency grammar at schema and derive validation boundaries |
| 202-CO-002 | Medium | Fixed | DDL tests | Index-predicate default changes had end-to-end coverage, but direct index-key changes and typed index cause did not | A regression could stale physical index entries or lose the authoritative rejection cause | Add exact no-op, SET, DROP, typed binder, and public rejection assertions |
| 202-CO-003 | Medium | Fixed | Recovery tests | Bundle code validated default payloads, but no focused publication/bundle rejection test proved the path | Malformed accepted defaults are a high-value recovery rejection boundary | Add a malformed default payload bundle test |
| 202-CO-004 | Medium | Caveat | Release documentation | Pending `ic-memory 0.11.1` work changes the allocation-ledger format while the 0.202.0 design says 0.202 has no persisted-format change | Publishing it as an undocumented continuation of 0.202.0 would misstate upgrade requirements | Give the follow-up a named patch and supplemental/detailed release record |
| 202-CO-005 | Medium | Deferred | Error boundary | Generated `ensure_memory_bootstrap` stores `Result<(), String>` and traps; the dependency's typed cause does not cross generated `db()` | Operators receive deterministic text, but callers cannot branch on the typed bootstrap cause | Decide the fallible generated session-construction API in 0.203; do not add an alias/shim |
| 202-CO-006 | Low | Caveat | Scope/lockfile | `socket2 0.6.4 -> 0.6.5` moved alongside the requested dependency updates without being part of either direct dependency | Small unreviewed transitive churn can obscure release scope | Classify or remove the incidental lock drift during release prep |

## Detailed Findings

### 202-CO-001 — Stable-key admission lagged `ic-memory 0.11.1`

**Files and symbols:**

- `crates/icydb-schema/src/node/mod.rs` —
  `stable_key_segment_is_canonical`, `stable_key_is_canonical`,
  `validate_stable_key`
- `crates/icydb-schema-derive/src/validate/memory.rs` — derive/schema parity
- `crates/icydb-schema-derive/src/node/{canister,store}.rs` — diagnostics
- `crates/icydb-build/src/db/store.rs` — generated declarations and bootstrap

**Before the fix:** IcyDB accepted `1db`, `_db`, and unbounded full keys. The
dependency requires every dot-separated segment to start with a lowercase ASCII
letter and caps the full key at 128 bytes.

**Reachability:** production-reachable through authored canister memory
namespaces/store names and generated stable-memory declarations.

**Compatibility:** no compatibility obligation. This is an admission hard cut;
invalid names now fail before generated runtime initialization.

**End state:** IcyDB validates exactly the dependency's bounded current grammar,
with 128-byte boundary tests and derive/schema parity tests.

**Semantic impact:** previously accepted invalid schemas now reject during
schema/macro validation instead of panicking later. Valid schemas are unchanged.

### 202-CO-002 — Indexed-key default-change rejection lacked complete proof

**Files and symbols:**

- `crates/icydb-core/src/db/schema/mutation/field.rs` —
  `validate_sql_ddl_field_default_change_candidate`
- `crates/icydb-core/src/db/schema/snapshot.rs` —
  `PersistedIndexSnapshot::references_field`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs` — new field-key test

**Current behavior:** exact SET DEFAULT no-ops remain legal. Real SET/DROP
changes reject when a field is in an accepted index key or its filtered
predicate. The binder preserves `IndexedFieldDefaultChangeRejected` with exact
entity, column, and index names; the public boundary emits the stable
`UnsupportedTransitionClass` detail.

**Reachability:** production SQL DDL path.

**End state:** implementation unchanged; focused coverage now proves direct key
and predicate dependencies, no-op behavior, typed internal cause, and public
projection.

### 202-CO-003 — Accepted bundle default validation lacked a direct test

**Files and symbols:**

- `crates/icydb-core/src/db/schema/enum_catalog/publication.rs` —
  `AcceptedSchemaRevisionBundle::validate`,
  `decode_accepted_schema_revision_bundle`
- `crates/icydb-core/src/db/schema/enum_catalog/publication/tests.rs` — new
  malformed-payload rejection

**Current behavior:** publication and recovery share bundle validation. A
malformed accepted default payload fails before a bundle can become authority.

**Reachability:** schema publication and startup/recovery bundle decode.

**Persisted compatibility:** current-format corrupt data is rejected. No old
decoder or repair fallback is added.

### 202-CO-004 — The pending dependency hard cut needs separate release truth

**Evidence:**

- `docs/design/0.202-explicit-rust-default-generation/0.202-design.md` says the
  0.202 implementation does not change persisted format.
- `docs/changelog/0.202.md` correctly says no recreation is needed for
  `0.202.0`.
- root `CHANGELOG.md` Unreleased correctly says `ic-memory 0.11.1` drops
  pre-0.11 ledgers and requires store recreation.
- `Cargo.lock` moves `ic-memory 0.7.5 -> 0.11.1` and
  `serde_cbor -> ciborium` within that dependency.

These statements can coexist only when the no-format-change claim is explicitly
scoped to `0.202.0` and the dependency hard cut is recorded as a later patch.
No patch number was invented during this audit.

### 202-CO-005 — Generated bootstrap does not expose the typed dependency cause

**File and symbol:** `crates/icydb-build/src/db/store.rs`, generated
`ensure_memory_bootstrap`.

The dependency returns `RuntimeBootstrapError<Infallible>`. Generated code maps
that error to `String`, caches it in a `OnceLock`, and panics from the infallible
`core_db()`/`db()` construction path. This predates the 0.202.0 default slice and
remains fail-closed and repeatable. Correcting it requires choosing whether the
generated public session constructor becomes fallible; that is a public API
decision, not a safe local conversion tweak. The hard-cut answer must replace
the infallible surface directly if chosen—no `try_` alias plus legacy wrapper.

### 202-CO-006 — Incidental `socket2` lock movement

`Cargo.lock` also moves `socket2 0.6.4 -> 0.6.5`. It passed all validation, but
it is not a direct consequence exposed by the `ic-memory` or ULID API changes.
Release prep should deliberately keep or remove it rather than leave it
unclassified.

## Fixes Made During Audit

- Enforced the `ic-memory 0.11.1` stable-key first-character and 128-byte
  limits at IcyDB's schema/build boundary.
- Updated derive diagnostics and parity/boundary tests for the current key
  grammar.
- Added end-to-end direct index-key default-change rejection coverage,
  including exact no-op and typed binder cause.
- Added focused accepted-bundle rejection coverage for malformed persisted
  default payloads.
- Corrected the root Unreleased dependency note to name `ic-memory 0.11.1`
  exactly.

No production query, write, persistence, recovery, or response semantics were
otherwise redesigned.

## False Positives And Deliberate Retention

- `materialize_entity_from_serialized_structural_patch_for_model_proposal_for_test`
  is `#[cfg(test)]`, explicitly named as generated-model proposal comparison,
  and has no runtime caller. It is not the removed accepted-runtime fallback.
- `ReadIntentKind::Unspecified` is query diagnostic metadata, not the removed
  enum `variant(unspecified)` directive.
- Historical design/changelog mentions of the removed enum sentinel remain
  valid release archaeology and are not active compatibility promises.
- Current accepted bundle, cursor, row, and ledger version checks are fail-closed
  durable format boundaries, not source/API compatibility shims.
- List/set/map and generated create-input `Default` implementations remain
  deliberate intrinsic construction contracts.

## Change And Footprint Readout

Before adding this audit artifact, the pending tracked slice spans 45 files with
approximately 317 insertions and 67 deletions. Most files are one-line imports
moving from the removed `ic_memory::stable_structures` re-export to the direct
`ic-stable-structures` dependency. The implementation shape is neutral: one
explicit memory authority is threaded through generated declarations, one key
grammar is enforced before runtime, and no compatibility branch is added.

The refreshed standard `ten_entity_fluent_rows` (`wasm-release`, SQL-on)
artifact measures:

| Artifact | `v0.202.0` | Current follow-up | Delta |
| --- | ---: | ---: | ---: |
| Raw non-gzipped wasm | 2,366,601 | 2,096,898 | -269,703 (-11.40%) |
| Canonically shrunk wasm | 2,204,554 | 1,950,484 | -254,070 (-11.52%) |
| Deterministic raw gzip | 768,620 | 737,533 | -31,087 (-4.04%) |
| Deterministic shrunk gzip | 733,155 | 706,247 | -26,908 (-3.67%) |

This whole-canister delta includes the `ic-memory 0.11.1`, ULID 2.0.1, and
associated integration changes; it must not be attributed solely to Ciborium.

A matched isolated encode/decode probe attributes the codec component more
narrowly: Ciborium 0.2.2 is 78,940 raw bytes versus 134,228 for
`serde_cbor 0.11.2` (-55,288, -41.19%), and 69,549 versus 117,648 after the
same size-optimized LTO profile (-48,099, -40.88%). `crunchy` appears in the
lockfile only as a development/SPIR-V dependency of `half`; it is absent from
the wasm target dependency graph.

No runtime performance claim is made. The dependency and validation changes do
not alter query or mutation algorithms.

## Validation

| Area | Result | Notes |
| --- | --- | --- |
| Focused derive/default tests | Pass | 31 derive strategy/default tests selected by filter. |
| Macro behavior | Pass | Four explicit/non-default/intrinsic behavior tests. |
| Macro UI/compile-fail | Pass | Full trybuild suite, including every 0.202 rejection boundary. |
| ICRC-3 typed decode | Pass | Protocol `Unit` rejection test. |
| Accepted omission/default paths | Pass | Typed create, structural insert, accepted enum payload reuse, SQL NULL/omission, malformed missing payload. |
| DDL index dependencies | Pass | Direct key and predicate paths, no-op and rejection cases. |
| Accepted bundle/recovery | Pass | Malformed default payload rejects. |
| Stable key/build generation | Pass | Schema, derive parity, generated store wiring. |
| ULID 2.0.1 | Pass | Eight wrapper/generator tests, including overflow and canonical bytes. |
| Repository architecture checks | Pass | All ten invariant scripts. |
| Feature matrix | Pass | No-default, SQL-only, diagnostics-only, and workspace no-default combinations. Initial sandboxed run could not resolve crates.io; approved network retry passed. |
| `cargo fmt --check` | Pass | Exact requested command. |
| `cargo check --workspace` | Pass | Exact requested command. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Pass | Exact requested command; also compiles benchmark targets. |
| `cargo test --workspace` | Pass | Exact requested command; unit, integration, PocketIC SQL, UI, generated-code, and doc tests passed. |
| Fresh standard wasm report | Pass | Raw and shrunk measurements recorded above. |
| `git diff --check` | Pass | Run after the final audit artifacts were written. |

Default-ignored tests are manual performance capture, saved-artifact comparison,
and optional SQLite differential cases. They compiled where applicable but were
not executed by `cargo test --workspace`. The repository rule prohibiting full
`make test` was respected; the exact workspace test requested by this audit was
run directly and passed.

## Intentionally Deferred

- Generated bootstrap typed-cause propagation: requires the public fallible
  session-construction decision described in 202-CO-005; target 0.203.
- The broader legacy/public-query excavation described by the separate proposed
  0.203 design: outside the explicit-Rust-default release contract.
- Any persisted-data migration: explicitly forbidden by the requested hard cut;
  the pending dependency release must require recreation.
- Actual benchmark execution: 0.202 makes no performance claim; all benchmark
  targets compile under warnings-denied Clippy, and a fresh wasm measurement was
  taken.

## Exact Documentation And Status Updates Required To Close The Pending Release

1. Name the target 0.202 patch through the release workflow; do not invent it
   in development.
2. Collapse root `CHANGELOG.md` Unreleased into exactly one concise bullet for
   that patch and add its detailed entry to `docs/changelog/0.202.md`.
3. Add `docs/design/0.202-explicit-rust-default-generation/0.202-supplemental-status.md`
   (or an equivalent linked closeout section) that explicitly separates:
   - the `0.202.0` no-persisted-format-change default slice;
   - the later `ic-memory 0.11.1` allocation-ledger hard cut and recreation
     requirement;
   - explicit generated authority and stable-key grammar;
   - ULID 2.0.1's unchanged canonical 16-byte representation;
   - the validation and refreshed raw wasm numbers in this report.
4. Scope both “No persisted format changes in 0.202” sentences in the normative
   design to `0.202.0`, and link the supplemental status so the minor line does
   not present contradictory upgrade guidance.
5. Record 202-CO-005 as an explicit 0.203 generated-API/error-boundary decision
   if 0.203 owns the fix; do not add a compatibility constructor.
6. Classify the incidental `socket2` lockfile movement before release.

## Final Recommendation

Close the 0.202 implementation as **READY WITH DOCUMENTED CAVEATS**. The
explicit Rust-default architecture is coherent, accepted schema remains the
sole database-default authority, important rejection paths are now directly
proved, and every requested validation gate passes. Release preparation needs
documentation/version assignment for the post-0.202.0 dependency hard cut, not
another 0.202 runtime redesign.
