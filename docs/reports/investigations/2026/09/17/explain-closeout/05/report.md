# Single-pass accepted-root validation

2026-09-17 · active 0.257.22 candidate; user-approved follow-up to
[selected-authority reuse](../04/report.md). This slice does not change the
checksum algorithm, add a cache or start another optimization project.

## Change and correctness

Root-slot classification checked the checksum and then called a strict decoder
that checked it again. Both callers now share field decoding after their own
envelope checks. The strict candidate decoder still rejects an unsupported
version before checking its checksum; slot selection still checks the checksum
first so torn bytes cannot masquerade as a supported or unsupported format.
A checksummed unsupported version still fails instead of falling back to the
other slot. Field invariants, equal-revision conflict rejection and selection
of the highest valid revision remain unchanged.

Production delta: one file, six net lines and one private field-decoding helper.
The execution flow is simpler: one checksum per valid slot. No new state, API,
budget, format, authority lifetime or compatibility path. Regression tests
exercise one-bit corruption at every byte, truncation, checksummed invalid
revision/bundle-key fields and unsupported version zero, in both slot orders.

## Matched IC instructions

The baseline is receipt 04's final typed actor. All nine before/after reports
match, and rendering instructions are unchanged. These are local IC instruction
intervals for cold/warm/warm explains in one request over one empty entity, not
populated-read, whole-endpoint or charged-cycle measurements.

| Query | Cold planning before → after | First warm planning before → after | Warm planning + rendering before → after |
| --- | ---: | ---: | ---: |
| Primary-key equality | 2,283,159 → 2,190,754 | 541,771 → 504,809 | 772,285 → 735,323 |
| Scan/sort | 2,273,155 → 2,180,750 | 499,186 → 462,224 | 769,409 → 732,447 |
| Grouped COUNT | 2,248,342 → 2,155,937 | 511,315 → 474,353 | 832,752 → 795,790 |

Warm planning saves 36,962 instructions (6.82–7.40%), or 4.44–4.80% including
rendering. Cold planning saves 92,405 instructions. Complete [samples](samples.txt) and the
temporary paired [probe](probe.rs.txt) accompany this receipt; the probe wiring
was removed from maintained integration tests after measurement.

Typed raw Wasm grows **2,510,810 → 2,510,841 bytes (+31)**. Defined functions
grow **6,411 → 6,412**. This is a measured cost trade-off, not a binary-size win.
The shared checksum algorithm and all live-authority checks remain intact.

Final SQL and mixed actors are 3,383,869 / 3,407,662 raw bytes with
8,512 / 8,596 defined functions. Each grows 31 bytes and one function versus
receipt 04. SQL also benefits: warm calls use 18,481 fewer instructions than
receipt 04, so this shared optimization does not remove the typed/SQL gap.

The maintained SQL-relative gate ran once and still **fails four of nine
samples**: warm equality and warm sort. All report comparisons and the mixed
actor's retained SQL read pass. The unchanged gate compares typed planning
against SQL's render-inclusive total, not equal intervals. Warm equality
including rendering is 735,323 typed versus 429,543 SQL instructions; no cost
parity, threshold adjustment or cost acceptance is claimed.

## Reproduction and qualification

Rust 1.98.1, Binaryen 132, locked/offline dependencies, package
`canister_audit_one_entity_typed_query`, `wasm-release`, target
`wasm32-unknown-unknown`, no default features. The matched features are
`typed-explain-measurement`, `sql-explain-measurement`, and
`sql,typed-explain-measurement`. Optimization flags remain
`-Oz --enable-bulk-memory --enable-sign-ext --enable-nontrapping-float-to-int
--one-caller-inline-max-function-size=0`.

Baseline typed SHA-256:
`93a108cc3ea477a7f3d67bd5b667b4073ba65914742547f08a71a3e74598081b`.
Final typed SHA-256:
`e0ad9589761f9acc692f05b089604f6b32dce78b89536f47e1c8a208e22f46f4`.
Cargo.lock SHA-256:
`2861ffbeb525446769bda64557a70e263e750ebfb547b2a6d4e3323f9d3d9562`.
Logs and artifacts: `target/root-validation-25722/`.
Final SQL / mixed SHA-256:
`569a1ef04c283abf03488ca7769dc3dd26965df2117bd8d101cc8831855ceae9` /
`78e676aa2faf73e9754d18ba2823722f42226ef7ee7224408a9e63d59c020b6d`.

For the paired probe, temporarily wire `probe.rs.txt` as `root_validation_probe`
beside the maintained `typed_explain_measurement` target and point
`ICYDB_ROOT_VALIDATION_DIR` at `before.wasm` and `after.wasm`.

Native qualification passes: 12 publication, 52 schema-store, 13 persisted-format
and 44 SQL-feature planner tests; the two new root tests pass again after lint
qualification (123 successful executions, including repeats). The paired IC
probe passes. Repository Clippy, invariants, formatting and whitespace checks
pass. No full suite or downstream application tests ran.

The first temporary probe build used an incorrect module location; correcting
it fixed that build. An ad hoc SQL-only **test** Clippy configuration reports
65 unused migration-helper errors; no unrelated migration code or lint settings
were changed. The repository's complete `make clippy` gate subsequently passes,
including its supported SQL-only **library** configuration. These limitations
are recorded rather than presenting every attempted command as passing.

Five disposable PocketIC fixtures were created and released for paired and
matched tests; shared local networks were untouched. Temporary test wiring was
removed; the archived probe is evidence only. No cost acceptance or 0.257
closeout is inferred. The checksum lookup-table candidate remains separate and
unimplemented.

Handoff footprint: nine files, approximately 300 net lines including regression
tests, release/status notes and evidence; only six net lines are production code.
State-space is unchanged and repeated validation is reduced.
