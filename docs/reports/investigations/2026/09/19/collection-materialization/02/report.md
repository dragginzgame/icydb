# Collection materialization — nullable boundary correction

## Result and disposition

Retain the smaller null check as a correctness/simplification correction, not
as a measured collection-query speedup. Raw actor Wasm falls from **4,255,817
to 4,255,565 bytes (-252)**. All **54** measured messages have exactly the same
typed results, query instructions and explicit-call cycles as the
[frozen baseline](../01/report.md). Defined functions remain **10,133**.

The measurement fixture uses a named collection type and therefore the
canonical CatalogValue decoder, not the changed by-kind boundary. This run is
a matched actor-size comparison and a query non-regression control. It does
**not** measure the removed traversal's cost. The initial candidate selection
overestimated its relevance to this fixture; the result resolves that rather
than supporting an allocation-only or universal performance claim.

C1 concludes with this bounded correction and a **no-build decision for a
streaming predicate path**. The remaining collection cost is real, but no
measured allocation attribution or simpler accepted-admission replacement was
established. Existing ownership is retained. Reopening streaming needs new
evidence at the actual canonical consumer, not another query/decoder route
justified by these unchanged numbers. W1/ICYDB-033 remains awaiting Canic.

## Runtime change and semantic evidence

`persisted_row/contract.rs` checks nullable by-kind structural fields before
decoding or validating them. Previously `value_storage_bytes_are_null` parsed
the entire generic value-storage grammar merely to inspect its root tag.
The accepted by-kind decoder then validated the payload again. Besides
duplicate work, the grammars disagree on Float32/Float64 wire tags: by-kind
uses `0x14`/`0x15`, whereas value-storage uses `0x85`/`0x86`.

The helper now recognizes only `[TAG_NULL]`. Every other payload, including
empty, truncated or trailing bytes, still reaches the selected accepted codec.
The helper's sole consumer is the nullable guard shared by the decode and
validation entrypoints. Primary-key-compatible null encodings remain excluded.
No wire version, schema authority, public API, error category or ownership
contract changes. Canonical admission remains unchanged.

Before the runtime change, the two new field-contract tests yielded one pass
and one failure: the Nat64/Float32/Float64 roundtrip test failed at its decode
assertion. After the change, both pass. They cover exact null, non-null lists,
empty bytes, trailing nulls, truncation, invalid tags, the maximum accepted
kind nesting and a payload one list deeper than that kind. Existing accepted
codec and predicate tests additionally cover nested/relation collections,
wrong-kind/oversized tails after an early match, and whole-output parity.
These are maintained-surface tests, not legacy/anti-resurrection checks.

Relevant source owners, relative to `crates/icydb-core/src/db/`:

- `data/persisted_row/contract.rs`: accepted dispatch and nullable guard.
- `data/structural_field/value_storage/decode/value.rs`: exact sentinel.
- `data/structural_field/accepted.rs`: complete accepted by-kind decoding.
- `schema/application_lowering.rs::field_storage_decode`: named field types
  select CatalogValue. The fixture's `items` names `SqlTestCollectionItems`.
- `schema/runtime.rs::AcceptedFieldDecodeContract::uses_canonical_value_wire`
  and `data/persisted_row/contract.rs::decode_runtime_value_from_row_contract`:
  canonical dispatch bypasses the changed guard for this fixture.

## Comparable inputs and exact artifact

HEAD remains `4698cecb8dbba6786079b759b43494b020559997`; Cargo remains 0.259.6.
The subject is that HEAD plus the dirty fixture inputs recorded in baseline
01 and the two runtime files below, not HEAD alone. All five fixture/host
source hashes and the lock hash were rechecked and exactly match baseline 01.
Other dirty core files contain test-only additions; documentation is not an
actor input. No dependency, toolchain, feature or profile changes.

| Changed actor source | SHA-256 |
| --- | --- |
| `crates/icydb-core/src/db/data/persisted_row/contract.rs` | `180825efa8e5e3acfade027d8f4e635ba7e7e3327c7b742048f83ad2730297aa` |
| `crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs` | `ba24cb2249a6fa57afb02ace251b8fd1141f76f8399af80252817ac20589b77a` |

Lock SHA-256:
`8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658`.
Rust 1.98.1 (`48a229cea`), Binaryen 132, PocketIC 16.0.0; same existing
retained `canister_test_sql` build, defaults off, features
`candid-export,local-sql-query,test-admin-api`, LocalTest, SQL/Candid enabled,
`wasm-release` and existing -Oz post-link configuration as baseline 01.

Final raw Wasm SHA-256:
`925259957cda0d5324b4a169b38d8e5fd137b9d9b66744087506da7b17f480ff`.
Retained namespace:
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`;
entry `2a9e66cef7a776b610251986b824da2cc65301797e99d9fb6fd2f31606d22c09`,
`outputs/0000.artifact`. Retention covers the byte read; all three instances
install clones of those exact bytes. Hash was rechecked before section counts.

| Metric | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Raw Wasm bytes | 4,255,817 | 4,255,565 | -252 |
| Defined functions | 10,133 | 10,133 | 0 |
| Code-section payload bytes | 4,021,895 | 4,021,655 | -240 |
| Data-section payload bytes | 218,386 | 218,374 | -12 |
| Query instructions, each of 54 messages | See baseline messages | Identical | 0 |
| Explicit-call cycles, each of 54 messages | See baseline messages | Identical | 0 |

Sections include their internal encoding overhead. Gzip, allocation bytes,
peak/live heap, direct by-kind IC costs and production ceilings are unmeasured.
No native timing or stable-page inference is used.

## Workload, validation and exclusions

The host fixture, seeds, query order, 16/256/1,024 lengths, two observations
per scenario, 64 pre-call ticks and all independent typed assertions are
unchanged. Full per-message evidence is retained in [messages.txt](messages.txt).
The query counter covers `execute_trusted_live_page`, not request setup or
serialization; balance-delta cycles cover the broader explicit update. Seed,
install, startup and preceding ticks are excluded. Co-scheduled work is not
separately attributed. Observation 1 is a repeated call, not an isolated
warm-cache experiment. This is not an application-lifecycle measurement.

Fresh final-source checks:

- Core selection: **18 passed, 0 failed, 0 ignored**, 2,806 filtered out;
  `nullable_by_kind_collections`, `canonical_materialization`,
  `collection_emptiness`, and `db::data::structural_field::accepted::tests`
  under the locked/offline SQL unit-test target.
- Ordinary focused integration target: **1 passed, 0 failed, 0 ignored**,
  99 filtered out; all 54 typed-result measurements passed.
- Core Clippy with SQL/migration library and tests, plus the maintained
  SQL/no-default library lane: passed with warnings denied.
- Formatting and whitespace checks passed. Full suites were not run.

The SQL-without-migration unit selection emits the same 65 pre-existing
dead-code warnings as baseline; this is distinct from the warning-free
Clippy selections. The pre-fix regression failure was intentionally reproduced
and is resolved. No final focused test failure remains.

Local lifecycle: started a local PocketIC server and created/dropped three
disposable instances. No deployed actor, downstream repository, Cargo version,
commit or publication changed. Full repository validation remains user-owned.

Complexity: this candidate changes **two source files**, approximately **-10
production lines +83 test lines (net +73)**, plus evidence/current notes. The
existing two qualification tests and five-file measurement fixture are prior
C1 work and remain unchanged. No runtime mode, visitor, cache, public report,
configuration or execution route was added; runtime structure is simpler.
