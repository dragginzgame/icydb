> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

- Relevant file identity: [source manifest](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/05/audit-framework-review/01/artifacts/source-snapshot.json),
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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/05/audit-framework-review/01/report.md).
