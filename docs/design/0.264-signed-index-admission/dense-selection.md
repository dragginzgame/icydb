# Dense index-versus-scan selection investigation

Authorized 2026-10-01 after the P1/P2 handoff. This is the next bounded
investigation in the [0.264 tracker](0.264-status.md); it does not reopen signed
normalization or the completed fallback change.

The subsequent [P4 crossover study](p4-crossover.md) completes the follow-up
below and records the current decision: retain indexed selection in 0.264.1.
The remaining proposal is a future contract, not an unfinished runtime feature.

## Question and smallest alternative

P1 removes discarded intersection stream construction. The older S2 experiment
found dense signed queries slower after index admission, while selective queries
benefited. Determine the remaining scan-versus-index gap under the same current
strict predicate, then assess whether a planner change is proportionate.

The smallest alternative is to retain existing indexed selection after P1.
Do not add an executor-local scan heuristic, new statistics, a public tuning
option or a density constant fitted to one 160-row population.

## Current owners and boundaries

- Accepted schema normalization establishes strict Eq/In semantics. Cost policy
  must consume that predicate rather than reverting to widening comparisons.
- `query/plan/access_choice` enumerates structurally and residually tied index
  candidates. The current exact-cardinality families are Prefix, MultiLookup
  and BranchSet; full scans and intersections are outside that tie set.
- `session/query/cardinality_tiebreak` binds exact prefix counts and applies
  selection through the canonical plan finalizer. The registry owns recovered,
  generation-bound counts and journal deltas; new statistics are unnecessary.
- `query/admission/policy` requires indexed access and rejects full scans for
  ordinary public reads. Dynamic public page admission evaluates the prepared
  plan before execution. A returned-row limit does not authorize a full scan.
- `session/query/cache/identity` includes accepted schema, index visibility and
  structural query identity, but no public/trusted admission distinction.
  Selecting a scan only on a trusted cache miss could retain that choice for a
  structurally identical public request. Lane-specific accounting alone does
  not isolate cached plans.
- `session/query/cache` refreshes unavailable evidence on lifecycle changes;
  exact selection remains cached after ordinary writes. A cached scan chosen
  when a prefix covers most rows could become expensive after unrelated rows
  are inserted. Fresh-request reselection and cursor replay need distinct
  freshness rules; the existing unavailable-evidence stamp is not a general
  entity/prefix density freshness contract.
- `CardinalityTiebreakRoutePin` identifies a real accepted index, its prefix
  family and nonzero prefix arity. Scalar tokens authenticate that pin and
  resumed preparation validates it against the eligible index tie set. A scan
  cannot be represented by a fake index ID or zero-arity sentinel.
- Intersection traversal may retain the first index as a candidate superset;
  residual evaluation completes the predicate. Replacing that stream with an
  entity scan without changing the prepared plan would hide physical access
  from public admission and EXPLAIN.

## Matched measurement

Reuse the P1 frozen source and indexed candidate artifact, keeping dependencies,
compiler, accepted schema and populations constant. Build one experimental
artifact that reselects intersections to FullScan through the existing plan
finalizer on trusted cache misses. Preserve the normalized strict predicate,
projection and order. This unconditional counterfactual is a cost study, not a
density policy or release candidate; its source stays outside the repository.

Reuse the maintained IC matrix, exact answer table and every issued cursor
suffix. Repeat the experimental run independently. Compare dense queries and
retain selective, disjoint and wide-row controls to expose the cost of a bad
choice. Record instructions, charged cycles and raw Wasm bytes. Do not infer
performance from elapsed time or entry counters. The artifact excludes later
parallel A4/A5 edits, which remain intact in the workspace.

The indexed artifact is the P1 candidate; reuse its `candidate` observations
in [p1-costs.csv](p1-costs.csv). The [scan receipt](p3-scan-costs.csv) adds only
the 116 experimental observations, avoiding another copy of indexed evidence.
Both scan runs pass and all 116 costs repeat exactly. Each validates seven
160-row populations, 28 dynamic shapes, 20 interleaved SQL shapes, 38 main page
steps and all ten issued cursor suffixes. Rows and page boundaries match the
indexed run. Residual and authored-limit correctness controls also pass.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| P1 indexed candidate | 4,499,444 | `e26fd4ae2503004d918c716e6253dd8957caceb35c5cf427a6d3677e8cfb9ac6` |
| Experimental scan | 4,499,896 | `9d485c4b148184a0b7bf510f63c6b832e54b037059c9e1c479c98bba72c4c8cc` |

Raw experimental delta: **+452 bytes (+0.010%)**. This includes experimental
reselection wiring; it does not estimate a maintained policy's Wasm footprint.
Rust 1.98.1, Binaryen 132, Cargo 0.264.0 and the same dependency lockfile,
SQL/Candid features and canonical local wasm-release pipeline are held fixed.
Candid matches. Only two isolated files change: the cardinality handoff receives
the current accounting lane and the selection owner forces FullScan for trusted
intersections on preparation/rebinding. The source was restored afterward and
every captured file matches the frozen indexed context.

Compare second identical samples; sum wide page costs over the complete main
traversal, excluding suffix-verification calls. Wide queries project 1 MiB
payloads inside the instruction window, discard them, and return compact
ID/work/cursor replies. Whole-call cycles include that compact reply. Entry
counters confirm traversal behavior and are not performance proxies.

| Dynamic workload | Shapes | Scan instruction change | Scan charged-cycle change |
| --- | ---: | ---: | ---: |
| Dense, 128/160 match | 4 | −10.37% to −8.04% | −7.34% to −6.38% |
| Selective/disjoint small rows | 16 | +88.14% to +297.18% | +50.47% to +108.62% |
| Complete wide traversal | 8 | +19.27% to +42.52% | +17.29% to +38.39% |

For dense two-predicate DESC, instructions fall from 25,959,288 to 23,266,342
and cycles from 34,535,426 to 31,998,967. These are additional savings relative
to P1, with the same strict predicate. Twenty interleaved SQL controls are
retained as context; diagnostic cache population can preserve their indexed
plans, so they do not establish a pure scan-versus-index SQL comparison.

This measures the alternative execution without the costs of a maintained
eligibility/density policy or refreshed evidence. It does not justify an 80%
threshold, predict mainnet costs or establish costs for authored limits 1/5.
Only one dense population and one small-row width are sampled; wide controls
are selective rather than dense.

## Decision gate

Before implementing scan selection, establish one planner-owned contract that
all admission, execution, diagnostics, caching and continuation consumers share.
Preserve public indexed-read requirements. A trusted-only optimization requires
an explicit admission capability in plan selection/cache identity and rejection
of incompatible cursor replay; it cannot depend on which caller fills a shared
cache first. Broadening public admission is a separate product decision.

A maintained route identity must represent index versus primary scan directly,
with current version-1 encoding and typed rejection of invalid forms. Do not add
another independent pin beside the existing selected-route identity or retain
predecessor token encodings. Initial selection may read current counts; resumed
selection must honor its authenticated route rather than rerank after writes.
Fresh initial requests must invalidate a cost-selected scan when the relevant
entity/prefix population changes, while cursors retain their chosen route.
Reuse current publication identities rather than adding persisted statistics;
measure refresh and preparation costs alongside execution savings.

Compare exact eligible prefix counts with exact visible entity count under one
accepted-root/lifecycle proof. Do not mix root-bound prefix evidence with an
unqualified entity total or scan data to manufacture unavailable statistics.
Unavailable, overflowed or incomplete evidence retains the normal indexed plan.

These extensions add three interacting distinctions: scan eligibility at the
admission/cache boundary, primary versus indexed selected-route identity, and
freshness for cost-selected initial plans versus pinned continuations.
They interact with accepted schema/index lifecycle, cursor mode, ordering and
mutation between pages. No existing execution primitive, public mode, stored
statistics or configuration is needed. Any implementation must cover adjacent
exact prefix/membership/branch-set queries using the same contract, rather than
adding a signed-Int32 intersection-specific rule.

## Recommendation and handoff

Keep the current 0.264.1 runtime unchanged by this investigation. There is a
repeatable dense saving, but an executor-local heuristic would bypass public
admission, and trusted-only cached selection needs eligibility, freshness and
continuation contracts. The current evidence does not identify a cost crossover
across densities, dense wide rows, larger populations, covering projections or
short authored limits. Implementing a threshold now would fit one fixture.

The follow-up is a bounded density/row-width/limit study and a single
planner-owned admission-aware selection design. Preserve public index
requirements; reuse existing full-scan/index primitives and exact counters.
Start implementation only once that design identifies an affordable automatic
rule and settles the three boundaries above. This investigation adds no runtime
mode, state, configuration, cursor format or duplicated execution flow.

Validation passes: both IC runs, all repeated costs and 34 focused native
admission/token/pinned-route tests. Documentation references and diff checks
pass. Two disposable PocketIC servers were started and stopped. Full suites
were not run; no Cargo versions change. Four workspace files change, roughly
300 added lines of design, receipts and status/release notes; production code
and implementation complexity stay unchanged.
