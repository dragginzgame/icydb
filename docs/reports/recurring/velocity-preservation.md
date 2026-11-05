> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Velocity Preservation Audit - 2026-06-06 (Forward-Looking Rerun 4)

Preamble:

- scope: `docs/audits/recurring/crosscutting/crosscutting-velocity-preservation.md`

- prior non-scoring context report:
  `docs/audits/reports/2026-06/2026-06-06/velocity-preservation-4.md`

- code snapshot identifier: `2b97a0d33` with dirty working tree

- method tag/version: `VP-FEF-1.0`

- comparability status: method-compatible with the earlier same-day
  forward-looking velocity reports; scored sections below use only the current
  code shape.

## 1. Run Metadata + Method

Scoring uses only current code shape. Accepted persisted-kind semantics now sit
under `db::schema::field_kind_semantics` and are consumed by SQL capability
checks, persisted schema predicate/literal/type helpers, relation key
validation, executor queryability, and executor accepted value-shape dispatch.
Executor write policy still owns exact runtime `Value` matching, scalar bounds,
collection recursion, and map/list admission.

The prior partial result was caused by current-code compile failures around
feature-gated projection/facade re-exports. The current snapshot now passes the
focused no-feature and SQL-feature core checks, so this rerun is scored as a
verified current-code pass.

## 7. Decision Shock Radius

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |
| `Value` | 24 variants | high | 360+ runtime files mention `Value` | 6+ | high | High |
| `FieldKind` / `PersistedFieldKind` | 32 variants each | medium-high | many runtime files mention either kind; selected accepted-schema decisions now flow through the classifier | 5+ | medium | Medium |
| `PersistedFieldKindSemantics` | 21 scalar classes plus collection/relation/structured categories | moderate | 5 production consumer families plus tests | 2 | medium-low | Low-Medium |
| projection materialization contracts | several row/layout/result shapes | moderate | projection, terminal, pipeline, delete/session facade | 2-3 | medium | Medium |
| `AggregateKind` | 8 variants | moderate | 70+ runtime files mention it | 4 | medium-high | Medium |
| `AccessPath` / `AccessPathKind` | 7 path families | moderate | 30+ runtime files mention it | 2 | medium | Medium |
| `ContinuationMode` | small route continuation taxonomy | low | route/planning-local | 1 | low | Low |

Persisted-kind shock radius is now materially lower for classification
questions: queryability, comparison eligibility, relation key eligibility, SQL
capability, and write value-shape dispatch share one schema-owned view. Runtime
`Value`, storage codec, index key, and projection/materialization decisions are
still the higher-cost extension surfaces.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/06/velocity-preservation/05/report.md).
