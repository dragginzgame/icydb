# Cursor Ordering Audit - 2026-03-12

## Report Preamble

- scope: cursor ordering correctness, anchor progression, and resume determinism
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/cursor-ordering-2.md`
- code snapshot identifier: `f12b0b74`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Anchor containment guard rejects out-of-envelope anchors | `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` | PASS | Low |
| Equal-to-upper anchors resume to empty envelope deterministically | `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` | PASS | Low |

## Overall Cursor/Ordering Risk Index

**3/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture` -> PASS
