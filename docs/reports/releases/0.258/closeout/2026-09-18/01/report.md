# 0.258 closeout audit

Verdict: **PASS WITH FINDINGS** for IcyDB's logical-memory implementation and
retained build-artifact handoff. No new data-loss, allocation-authority or
fail-open defect was demonstrated. Three bounded follow-ups are listed below;
real Canic adoption remains a separate prerequisite, not qualified by this audit.

## Scope and method

- Snapshot: clean `main`, `ece74446595f3ef948bde6973942eab1c7e38f7e`, published
  `v0.258.0`. Only this new report is added by the audit.
- Method: `LM-CLOSEOUT-1 / DOMAIN-1`, bounded release-closeout investigation under
  `docs/audits/README.md`; not a whole-system or full recurring-domain audit.
- Baseline report: N/A. Source comparison: published `v0.257.22`
  (`f3bd969be9dd7087a0c00e2655d703c397756616`). There is no local `v0.257.23` tag;
  the initial attempted comparison against it failed, then the verified release
  baseline was selected. No missing baseline was treated as evidence.
- Comparability: non-comparable as an audit series; this is the first run of
  this scoped method. Historical performance observations retain their original
  measurement scopes and are not fresh measurements.
- Trigger: closeout of the numeric-ID to logical-key hard cut, shared admission,
  dependency codec adoption and build-result ownership repair.
- Obligations: permanent-key validation; committed allocation/open authority;
  original-request role/namespace admission; omitted-journal selection; rejection
  and retry; database retirement/commit barriers; error propagation; generated
  and host composition; retained artifact lifetimes; truthful delivery evidence.
- Excluded: query planning, index codecs, SQL, cursor execution and broader
  batch behaviour, whose implementation is not changed by this line; arbitrary
  interruption fuzzing and whole-dependency audits. ICYDB-036 application
  qualification is not claimed. Full suites remain user-owned.

## Findings

### F1 — MEDIUM: generated upgrade regression coverage is manual-only

Owner: `testing/integration/tests/logical_memory.rs:81`, its fixture build and
release-test wiring. All three tests are explicitly ignored and consume
`ICYDB_LOGICAL_MEMORY_{FULL,OMITTED}_WASM`. `testing/logical-memory/README.md`
documents manual builds; neither Makefile, scripts nor workflow wiring selects
the ignored family. Ordinary workspace validation therefore skips the tests.

These assertions cover meaningful boundaries: empty-store retirement preserves
surviving rows/slots, journal debt blocks removal, and a positively validated
pending marker blocks registry changes. They are not replaced by the native
allocation tests or the single-shape lifecycle actor. Historical execution is
recorded, so this is an ongoing regression-detection gap, not proof of a broken
release. The final 0.14.3 evidence records native admission and lifecycle checks,
not a fresh execution of this generated full/omitted actor matrix.

Disposition: non-blocking for the already published release. Recommended next
bounded IcyDB correction is to build the two variants through existing retained
artifact helpers and execute this exact family from a maintained validation
entry point. Keep one fixture and the existing three cases; no new framework,
fault model, runtime mode or compatibility fixture is needed. Trigger: the next
memory/recovery/dependency change, or an explicitly approved closeout correction.

### F2 — LOW: recovery lookup error origin differs between entry points

Owner: `crates/icydb-core/src/db/commit/recovery.rs:130`.
`ensure_recovery_admitted` maps a failed `C::commit_memory_id()` through
`InternalError::commit_memory_id_registration_failed`, which returns
`Internal/Store`. The old admission path mapped its allocation failure to
`Recovery`, and `continue_recovery_with_failure_authority` still performs that
mapping. The remaining errors in ordinary recovery admission also use Recovery.

This is source-proven diagnostic inconsistency on a failed lookup, not a
demonstrated successful-query or data-integrity defect. Normal generated entry
points bootstrap before reaching it; direct core/error paths are the relevant
boundary. Rejection still happens before allocation selection or row work.

Disposition: restore Recovery origin at this boundary and add one focused typed
failure assertion when authorised. Do not change the general Store-origin helper
or refactor unrelated errors. No new failing-path execution was added in this
audit; evidence is the direct constructor/caller chain and release diff.

### F3 — LOW: active status still reads as pre-publication

Owner: `docs/design/0.258-logical-memory/0.258-status.md:3` and its closeout
summary. The active tracker still describes release preparation and the next
user-owned gate, without a top-level published/closed disposition. The tag and
user handoff establish that 0.258.0 is live. This can send the next continuation
back into completed work.

Disposition: update only the active summary to record publication, this audit's
findings and the external Canic qualification boundary. Preserve historical
measurements and published changelog text; do not claim this audit ran the full
release gate. No tracker or release metadata was changed here.

## Authority and safety assessment

| Boundary | Inspected owner and conclusion |
| --- | --- |
| Authored identity | Macro/model validation rejects invalid key segments, overlong generated role keys and duplicate store keys. Rust paths are not the allocation identity. |
| Allocation authority | `ic-memory` remains the allocator/ledger. Generated stores resolve committed keys; schema physical-ID fields and the duplicate commit allocation registry are removed. |
| Admission | `db/memory_admission.rs` checks original sealed declarations and complete role groups, rejects removed historical namespaces and selects only omitted IcyDB journals. Foreign history and host grants remain protected. |
| Bounded construction | The dependency caps sealed declarations/requests and recovered allocation records; admission uses bounded maps/vectors, not a new per-step accounting framework. |
| Publication/recovery | Allocation commitment precedes database convergence. Journal debt, pending markers and retirement stay with the existing database owner. A later database rejection does not promise allocation-ledger rollback. |
| Warm reuse | Committed opens retain authority. Warm adoption cannot replace host policy or retroactively run historical admission; the host must invoke the hook before commitment. |
| Build results | Single and batch flows retain Cargo records through post-link and retain output records through reads/staging. Mutable output destinations are not read authority; retention remains owned by ic-testkit. |

The known Canic 0.110.22 preparation-hook gap is owned by downstream ICYDB-029.
The user has assigned Canic's upgrade separately. IcyDB's public hook and
schema-authoring guide state the host obligation; adding a second bootstrap or
an application bypass would weaken this contract. This audit does not certify
an unexamined future Canic release, application upgrade or deployed data.

## Fresh verification

Rust `1.98.1 (48a229cea 2026-09-01)`, repository Cargo environment:
`CARGO_HOME=/home/adam/projects/icydb/.cache/cargo/icydb`,
`CARGO_TARGET_DIR=/home/adam/projects/icydb/target/icydb`. Default package
features; explicit targets below. Assertions were inspected and each selection
listed with the same arguments plus `-- --list` before execution.

| Outcome | Selection | Executed evidence |
| --- | --- | --- |
| PASS | `cargo test --locked -p icydb --test memory_admission --test default_memory_manager` | 13 passed, 0 failed, 0 ignored: roles, namespace rejection/retry, foreign claims, journal retention, revoked grants, exhaustion/retry, profile mismatch, warm-policy boundary and committed opens. |
| PASS | `cargo test --locked -p icydb-testing-integration --lib canister_build_cache::tests` | 5 passed, 0 failed, 0 ignored, 14 filtered out: cache configuration and retained partial/warm results under destination replacement/removal and pruning. |
| PASS | `bash scripts/ci/check-memory-id-invariants.sh` | Current source-wiring invariants; not behavioural proof. |
| PASS | `bash scripts/ci/check-wasm-post-link-invariants.sh` | Post-link wiring and 32 audit-capture cases. |
| PASS | `git diff --check` | Whitespace check. |

No full suite, PocketIC run, deployment, network lifecycle action, dependency
update, formatter or production-source edit was performed. Generated upgrade
and lifecycle reports were reviewed as historical evidence only, not counted
as fresh passes or as bit-identical final-release qualification.

## Cost and complexity

The retained 0.14.3 investigation reports worst lifecycle cost falling from
14,985,588 to 5,131,230 instructions under the unchanged 12,750,000 ceiling.
Allocated stable extent is unchanged. Its consumer raw-Wasm/cycle deltas are
unmeasured. The earlier matched actor's +30,133 raw bytes and +1.168% warm-open
instructions belong to 0.14.2, not a final 0.14.3 Wasm measurement. See
`docs/reports/investigations/2026/09/17/logical-memory/{02,03}/report.md`.
No native timing is used as a performance metric.

The release diff against 0.257.22 is 140 files, +4,428/-2,247 lines including
tests, fixtures, reports and release metadata. The 59 changed files under
`crates/` total +1,556/-1,747 (net -191); this includes tests and is not a
production-only count. The implementation removes manual placement and a
duplicate registry, adds one stateless admission owner, and adds no second
allocator or recovery flow. Complexity is better structured rather than
universally smaller.

Audit footprint: one report, no runtime/state-space delta. Closeout should not
expand into unrelated optimisation. F1 is the substantive validation follow-up;
F2 and F3 are narrow correctness/documentation corrections, subject to approval.
