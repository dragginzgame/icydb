> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Recovery Consistency And Replay Equivalence Audit

## Metadata And Declared Scope

- Request: implement the approved RCV-01 correction and rerun recovery.

- Definition: `docs/audits/recurring/storage/storage-recovery-consistency.md`.

- Method: **RECOVERY-5 + DOMAIN-1**, unchanged and frozen before execution.

- Compared baseline: [2026-09-27 run 01](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/recovery-consistency/01/report.md).

- Comparability: **comparable**; same obligations, capabilities, feature
  selection and classification, with the required missing proof added.

- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  plus four recovery test/instrumentation files identified below. The earlier
  Cargo dependency edits, boundary scan tests, audit/governance/report edits
  and active release notes remain in the worktree.

Scope remains direct durable row replay, journaled live/fold behavior,
mixed-entity and nested relation mutations, unique-value handoffs, retained
marker and stage-loss idempotence, accepted schema/index publication, physical
migration, compound controls, and startup admission. All 48 baseline tests
execute again; the two new publication tests add eight fixture scenarios.
Historical results are not counted as fresh proof.

### Source And Verification Identity

Paths in this table are repository-relative; identities are SHA-256.

| Input | Identity |
| --- | --- |
| Recovery definition | `e3598124eb3fea57096367d7df31ca5f2179bc265881af2b35cd5288ebffa961` |
| Cargo.toml | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| Cargo.lock | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| `crates/icydb-core/src/db/index/scan/tests.rs` (pre-existing) | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |
| `crates/icydb-core/src/db/commit/mod.rs` | `07a7376243c170244e5a53b9ec6f74cb664b629732f826229c40862f4c9ecb63` |
| `crates/icydb-core/src/db/commit/schema_publication.rs` | `a48f295472b17add2dc1cac589889631429b5ec240ec6064ac1bb46de26e6d70` |
| `crates/icydb-core/src/db/session/write.rs` | `c5cec6e199c57e0b97c5519ab79e19a55c68a2e45d5aa48fcea1112b3ae15db2` |
| `crates/icydb-core/src/db/session/write/identity_pre_key_tests/schema_publication_tests.rs` | `8cb6e597f849b22d26489030bbcc3f230fa0e8cd8bf30c6cd14f52592344b097` |

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`. Core native library tests use
`sql,migration`, locked dependencies, Makefile Cargo home `.cache/cargo/icydb`
and target `target/icydb`, with eight test threads. Other Rust source is
unchanged from HEAD except the pre-existing scan test. Cargo versions and
dependency edits were not changed. Run 01 remains immutable.

## Findings, Verdict And Comparable Delta

**PASS** for the declared baseline. **RCV-01 resolved**: the previously missing
populated producer/recovery proof now executes for field and expression
indexes at marker-only and two partial-derived-state cuts. All 50 selected
tests pass. No executed case demonstrates replay divergence; no production
behavior correction was needed and no new finding is opened.

Compared with run 01: verdict BLOCKED → PASS; open verification gaps 1 → 0;
selected/passed tests 48 → 50 (+2); behavioral failures remain 0. Method and
scope are unchanged. The two tests contain eight fixture scenarios; those
scenarios are not counted as eight separate tests.

## Verification Readout

| Check | Outcome | Selected | Passed | Failed | Ignored |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial new-fixture listing compile | FAIL | 0 | 0 | 0 | 0 |
| Corrected new-fixture listing | PASS | 2 | 0 | 0 | 0 |
| New-fixture focused execution | PASS | 2 | 2 | 0 | 0 |
| Audit listing and required-family count assertions | PASS | 50 | 0 | 0 | 0 |
| Audit focused execution | PASS | 50 | 50 | 0 | 0 |
| Non-test core library check | PASS | 0 | 0 | 0 | 0 |
| Formatting and diff whitespace checks | PASS | 0 | 0 | 0 | 0 |

The first compile rejected returning an Rc-backed schema bundle across test
threads (E0277); no tests executed. The fixture now returns the selected
accepted root, whose identity binds the complete bundle, and performs bundle
assertions within each thread. No production ownership was widened. Final
compilation and execution have no warnings or failures.

All 33 selectors match: mixed-entity 5, direct mixed 3, reader-state 6,
nested-relation 5, rename 2, new publication 2; each other selector matches 1.
The audit filters out 2835 unrelated tests. The new-fixture execution is a
subset of the final 50, not an additional unique-test count.

Exact command configuration follows. The two test commands were each listed
first by adding `--list` to their test arguments, then executed without it.

## Follow-Up And Complexity

No correction remains for RCV-01. Keep platform qualification and broader
mutation/feature matrices separate from this bounded native baseline.

Seven files change in this correction: four Rust files (about 366 net lines,
including the 317-line fixture), two existing release-note files and this new
immutable report, about 600 net lines overall. Production behavior axes,
formats and execution routes stay
unchanged. Test instrumentation grows; production implementation shape stays
neutral. Raw Wasm, IC cycles and instruction deltas are unmeasured; no
performance claim is made. The earlier audit definition and reports are
unchanged by this correction.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/recovery-consistency/02/report.md).
