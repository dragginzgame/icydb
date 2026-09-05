# Audit Framework Review

## Preamble

- Scope: bounded investigation of the audit-framework changes in the current
  worktree: shared execution/scope/evidence rules, domain-definition propagation,
  recovery failure criteria, and Wasm report capture/baseline behavior.
- Date: 2026-09-05 UTC.
- Method: `AUDIT-FRAMEWORK-1.0`; source inspection plus focused shell fixtures.
  This is an investigation, not a full domain audit or release closeout.
- Baseline: `N/A` (first investigation of this bounded scope).
- Code snapshot: `6c17ea4a14c0b7a41b37bdf2cc80ee5ce7d22806` plus dirty worktree.
- HEAD tree: `81a600575c8040e29df30c9fe07588f65e39300d`.
- Relevant file identity: [source manifest](artifacts/source-snapshot.json),
  including the untracked regression script. The manifest hashes the inspected
  file bytes, not just HEAD; its sorted compact `files` map SHA-256 is
  `b0854a2ec35077ef4ff625be854a351664b450d5482555d1e47e9ce7b4747202`.
- Comparability: non-comparable; first bounded investigation, no earlier
  like-for-like baseline or numerical deltas claimed.
- Authorization: write a new report and focused evidence only. No fixes,
  definition edits, release changes, real canister builds, or network actions.

## Verdict

`FAIL`: two demonstrated audit-contract defects remain, plus one documentation
drift finding. Existing capture regressions pass, but do not cover the failing
baseline-selection cases. This verdict concerns the audit framework only;
it does not establish a database runtime defect or a repository release verdict.

## Findings

### AF-01 — MEDIUM: baseline lookup can violate chronology and the daily baseline

- Owner: `scripts/ci/wasm-audit-report.sh`, `write_summary_report`, lines 192-205.
- Contract: same-day reruns compare with run `01`; first runs compare with a
  prior comparable report, not a future report.
- Cause: the daily branch depends on `report_run`, which is populated only for
  automatic output paths. An explicit `--report-dir` falls through to a global
  sorted scan. That scan excludes the current path but not future dates.
- Reproduced observations in an isolated fixture repository:

  | Requested output | Expected baseline | Observed baseline |
  | --- | --- | --- |
  | Automatic 2026-09-04 run 02 (control) | 2026-09-04 run 01 | 2026-09-04 run 01 |
  | Explicit canonical 2026-09-04 run 03 | 2026-09-04 run 01 | 2026-09-04 run 02 |
  | Backdated 2026-09-03 run 01 | Earlier compatible 2026-01-01 run 01 | Future 2026-09-04 run 03 |

- Evidence: [reproducer](artifacts/reproduce-baseline-selection.sh), executed
  successfully. Exit zero means both defects were reproduced, not that the
  audited contract passed. Fixture Twiggy is stubbed; no attribution-quality
  or real build claim follows from these runs.
- Consequence: reported previous sizes can refer to the wrong run or even a
  future snapshot, distorting review of footprint changes.
- Disposition: fix requested separately; do not use affected comparisons as
  regression evidence. Trigger: before relying on explicit-path or backdated
  Wasm audit comparisons.
- Smallest correction: make baseline resolution obey the same day/run rules
  for explicit and automatic canonical paths, constrain candidates to earlier
  runs, and test the two reproduced cases alongside the positive control.

### AF-02 — MEDIUM: the state-machine checklist still excludes journal-only recovery

- Owner: `docs/audits/recurring/executor/executor-state-machine-integrity.md`,
  Commit Marker Authority Check, lines 344-358.
- Evidence: the checklist requires marker absence to prevent replay activation
  and describes the marker as the sole durable handoff authority. In
  `crates/icydb-core/src/db/commit/recovery.rs:250`, the no-recovery branch
  requires both an absent marker and empty journal tails. Otherwise line 264
  calls `perform_recovery_page` with the optional marker. Lines 350-368 fold a
  journal batch and clear a marker only when one existed.
- Consequence: the audit can classify legitimate marker-free journal recovery
  as a violation, or omit the journal-tail authority needed to assess startup
  safety. The preceding rollback correction is valid but does not resolve this
  separate stale criterion.
- Disposition: correct the definition, not the runtime. Trigger: before the
  next state-machine audit includes startup or journal recovery.
- Smallest correction: distinguish incomplete marker-owned application from
  committed journal-tail folding, model both maintained recovery authorities,
  and retain the startup admission/visibility gates for each.
- Validation basis: source inspection of explicit branches; no Rust behavioral
  test was run and no runtime failure is alleged.

### AF-03 — LOW: documented default Wasm matrix omits four actual targets

- Owner: `docs/audits/recurring/crosscutting/crosscutting-wasm-footprint.md:24`
  and `scripts/ci/wasm-report-common.sh:3`.
- Evidence: the definition says its exact default is 11 canisters; the shared
  helper emits 15, additionally including `nested_relation_none`,
  `nested_relation_direct`, `nested_relation_shallow`, and
  `nested_relation_repeated`.
- Consequence: an operator following the definition cannot accurately predict
  the default build scope or evidence matrix.
- Disposition: align the definition with the maintained helper or reference
  that helper as the canonical list. Trigger: next Wasm definition correction.
  No runtime or default-target change is needed to resolve the drift.

## Coverage And Positive Evidence

- All nine domain definitions link to the shared change-trigger/scope contract.
  Applicable consumer/recovery obligations remain required; exclusions are not
  passing evidence. Finding ownership and evidence reuse are centrally defined.
- The revised failure rule distinguishes zero-write preflight from retained
  marker-owned application failures. `finish_commit` in
  `crates/icydb-core/src/db/commit/guard.rs:294` supports that distinction.
- Capture fixtures exercised source identity without checkout HEAD, compatible
  clean baselines, completed/reserved report preservation, Wasm/gzip corruption
  and byte-count mismatches, missing/malformed identity, wrong subject/profile/
  SQL variant, mixed actor provenance, and dirty-source non-comparability.
- Twenty unique maintained shell cases passed through the post-link invariant
  gate and passed again as reproducer setup. Repetition adds no new coverage.
- Local-link inspection covered 19 modified audit/report-governance documents:
  53 file/anchor references resolved, with no missing targets.

## Verification Readout

All shell commands ran from `/home/adam/projects/icydb` against the manifest
snapshot. Shell fixtures have no Cargo target, feature, or ignored-test selector;
their cases and assertions were inspected before execution.

| Check | Outcome | Evidence / limits |
| --- | --- | --- |
| `bash -n scripts/ci/wasm-audit-report.sh scripts/ci/test-wasm-audit-report.sh scripts/ci/check-wasm-post-link-invariants.sh` | PASS | Shell syntax only |
| `shellcheck --exclude=SC2001,SC2016 scripts/ci/wasm-audit-report.sh scripts/ci/wasm-report-common.sh scripts/ci/test-wasm-audit-report.sh scripts/ci/check-wasm-post-link-invariants.sh` | PASS | Repository lint exclusions; no diagnostics |
| `bash scripts/ci/check-wasm-post-link-invariants.sh` | PASS | Static owner gates plus 20 fixture cases passed, zero failed or ignored |
| `bash docs/reports/investigations/2026/09/05/audit-framework-review/01/artifacts/reproduce-baseline-selection.sh /home/adam/projects/icydb` | PASS | Reproduction harness: 20 existing cases plus one positive control and two confirmed defects; audited baseline contract FAIL |
| One-off local Markdown file/heading scan | PASS | 19 documents, 53 local references; source inspection, not behavioral tests |
| Source comparison of marker recovery and default target lists | FAIL | AF-02 and AF-03 demonstrate definition/implementation disagreement |
| `git diff --check` | PASS | Existing tracked worktree whitespace |

An exploratory search under `db/commit/tests/journal*.rs` found no such path.
File discovery resolved the maintained journal tests under `db/journal/`; the
failed search was not treated as executable proof or a runtime finding.

Excluded: full repository suites (user-owned), Rust runtime behavior, real
Wasm builds/Twiggy attribution, performance and footprint measurements, and
unrelated correctness/architecture domains. No proof from these excluded
surfaces is claimed and no environment blocker was encountered.

## Handoff

Only this new investigation report and its supporting evidence were added.
Existing scripts, definitions, release metadata, and historical reports were
not changed. No implementation was performed; follow-up is AF-01 through AF-03.
