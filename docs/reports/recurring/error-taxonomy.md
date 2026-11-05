> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Error Taxonomy Audit

## Metadata And Scope

- Request: improve and execute the next least recently run audit.

- Definition: `docs/audits/recurring/contracts/error-taxonomy.md`.

- Method: **ERROR-5 + DOMAIN-1**, frozen before test discovery/execution.

- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  with pre-existing Cargo dependency edits and the boundary-audit storage test.
  Earlier audit definitions, reports, and changelog edits are retained.

- Compared baseline: **N/A**, no comparable ERROR-5 run exists.

- Historical reference: [2026-05-14 run 01](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/05/14/error-taxonomy/01/report.md),
  Method V4. Historical tests are not counted as current execution.

- Comparability: **non-comparable (method change)**; affected deltas are
  `N/A (method change)`. Seven runtime classes and corruption containment
  remain stable qualitative anchors.

Error taxonomy, recovery consistency, and resource-model compliance were tied
at 2026-05-14 as the least recently run active scopes after the preceding
boundary and state-transition runs. Alphabetical scope order selects error
taxonomy. Archived audits and targeted cleanup playbooks are not recurring runs.

### Source And Configuration Identity

| Input | Identity |
| --- | --- |
| Definition SHA-256 | `bd02f058f581e8e155cf8dac61b941cd37600db1c707210ed956d4867f3968ac` |
| Cargo.toml SHA-256 | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| Cargo.lock SHA-256 | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| Pre-existing index scan test SHA-256 | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |
| Rust | `rustc 1.98.1 (48a229cea 2026-09-01)` |
| Cargo home / target | `.cache/cargo/icydb` / `target/icydb`, resolved by Makefile |
| Core / facade features | `sql,migration`; native library tests, eight test threads |
| Diagnostic-code features | defaults; native library tests, eight test threads |

All other Rust source is unchanged from HEAD. This run changes no production
code, tests, Cargo input, or release metadata. No network lifecycle actions.

## Method Review

The 313-line prior definition missed the numeric diagnostic registry, bounded
fact schemas, optional query-field context, and the actual five-field Candid
record. It mentioned a removed message-rewriting helper and response-error
surface. Its unconditional origin rules conflicted with deliberate recovery
relabeling. Its well-formed index/row mismatch rule could incorrectly demand an
invariant error where accepted storage inconsistency is corruption.

ERROR-5 reduces the method to 159 lines, includes the diagnostic-code crate,
separates runtime and public axes, requires classification by trust boundary,
and replaces repetitive tables with one propagation matrix. It no longer
requires live proposals and corrupt accepted state to have identical errors.
Adjacent audits own behavior; this audit owns meaning and projection.

## Verification Readout

All final selectors were listed with the same configuration before execution.
The `error::tests::` core filter selects 45 central taxonomy tests plus one index
unique-diagnostic test and one grouped resource-diagnostic test. Their assertions
were inspected and their additional coverage is included, not mistaken for
central-module coverage. Cursor selects seven tests; the other fourteen core
selectors each select one test.

The initial core listing used `commit_marker_rejects_future_version`, which
matched zero tests. Its process exited successfully, but discovery failed the
required format-version obligation. Before execution it was replaced with
`commit_marker_future_version`, selecting the inspected
`commit_marker_future_version_fails_closed` test. The other selectors were
unchanged at that correction. Two inspected startup samples were subsequently
added to A, and the startup projection family to B, then both were relisted.
No behavioral test failed. Final total: **124 passed, 0 failed, 0 ignored**.

## Verdict And Follow-Up

**PASS** for the declared classification/projection scope. No actionable
misclassification or unresolved required verification gap was found. The stale
selector was resolved before behavioral execution. No runtime fix is proposed.

The separate requested holistic audit-definition review is inspection-only;
its consolidation recommendations are returned in the conversation, not turned
into runtime findings or automatic definition retirements here.

Complexity: two documentation files changed/added. The definition removes 154
net lines (313 to 159); this report records the run evidence. Runtime shape,
behavior axes, and production debt are unchanged. The method is simpler while
covering current diagnostic representation. Wasm bytes, cycles, and instruction
deltas are unmeasured; no performance claim is made. Governance-only changes
need no release note. Full-suite and deployed endpoint qualification remain
outside this audit's scope.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/error-taxonomy/01/report.md).
