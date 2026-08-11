# Flow Convergence And Duplication

## Preamble And Comparability

- scope: `flow-convergence-and-duplication`
- definition:
  `docs/audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md`
- method: `FCD-1.0`
- run: `2026-08-10/01`
- auditor: `Codex`
- released snapshot: `d8d6a33c0e238fb90eb3b3f0c189b4e5332aa57d`
  (`0.223.4`), tree `f0e51668fa938d1fd6b4c2bbf8afe0f5f59a580d`
- worktree relevance: clean at audit start; this run then applied the one
  authorized same-owner correction described as `FCD-001` and added only the
  audit, status, and changelog follow-through
- compared baseline: `N/A`
- comparability: non-comparable first `FCD-1.0` baseline; the 2026-07-28
  current-architecture investigation is contextual evidence only
- run mode: broad baseline plus bounded same-owner remediation; no external
  service or full repository suite

The audit enumerated evidence once across memory bootstrap, query planning and
execution, structural mutation, catalog mutation, durable mutation jobs, and
commit recovery. Mention and line counts were used only to locate owners.

## Verdict

`PASS`

The released tree already removes the obsolete caller-custodied resumable SQL
update API and its historical patch-number comments. Read and write frontends
converge on one maintained planner/executor and mutation/commit flow. The run
found one local Forward/Verify eligibility-preparation duplication; it is
consolidated in this worktree, leaving no active `DELETE`, `CONSOLIDATE`, or
`LOCALIZE` finding.

## Behavior And Owner Map

| Behavior | Canonical owner | Inputs | Carried contract | Consumers |
| --- | --- | --- | --- | --- |
| Default stable-memory bootstrap | `ic-memory` committed runtime plus IcyDB declaration validation | generated static declarations and authority | `CommittedAllocations` | generated database wiring and store openers |
| Scalar and grouped reads | structural query planner and accepted authority | SQL, dynamic, typed, or prepared request | `SharedPreparedExecutionPlan` | scalar/grouped executors, diagnostics, and `EXPLAIN` |
| Row mutation | accepted structural mutation boundary | SQL, dynamic, typed, or mutation-job patch | `AcceptedStructuralMutation` and canonical after-images | commit preparation, index/relation effects, result projection |
| Accepted schema mutation | schema application/DDL lowering and schema publication | generated proposal or administrative SQL DDL | catalog-native accepted candidate | schema commit/publication and runtime-root invalidation |
| Durable fixed SQL update | resumable-update engine plus mutation-job custody | accepted SQL at start; retained intent thereafter | canonical intent, private continuation, mutation-job transition | bounded Forward/Verify traversal and public receipts |
| Crash recovery | commit marker and recovery | exact marker, journal batches, control effects | marker-carried row/schema/progress effects | live projection restore and durable progress replacement |

## Flow Trace

| Entry surface | Frontend-only work | Convergence point | Runtime path | Result projection |
| --- | --- | --- | --- | --- |
| Generated `db!()` wiring | register IcyDB declarations and cache the result | `ensure_default_memory_manager` | adopt committed allocations or bootstrap once | typed bootstrap error |
| SQL/dynamic/typed read | parse or bind request vocabulary | `StructuralQuery` and `SharedPreparedExecutionPlan` | shared scalar/grouped executor | SQL rows or typed/dynamic projection |
| SQL/dynamic/typed write | parse or lower authored patch | `AcceptedStructuralMutation` | one structural materialization and commit window | frontend-specific affected-row/value result |
| Generated proposal / SQL DDL | source or SQL-specific binding | catalog-native accepted candidate | schema-owned publication through commit | proposal receipt or DDL report |
| Mutation-job start | parse and admit fixed SQL once | canonical intent plus private engine continuation | excluded progress store | public sequence-zero state |
| Mutation-job advance | validate request identity and retained authority | shared traversal eligibility/scope preparation | bounded Forward mutation or read-only Verify, then marker-owned progress | replayable typed receipt |
| Recovery | none | exact persisted marker | shared mechanical marker application | recovered state or typed recovery failure |

## Findings

| ID | Class | Risk | Owner | Evidence | Friction | Disposition | Action trigger |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FCD-001 | `DuplicateFlow` | `MEDIUM` | `db::session::sql::resumable_update` | released Forward and Verify runtime preparation each repeated the same fixed-eligibility proof, row-contract selection, scope compilation, and `IntentIneligible` mapping | a future eligibility or compilation rule could drift by phase despite the status record claiming shared preparation | `CONSOLIDATE` | Resolved during this run by one `PreparedMutationJobTraversalRuntime` owner used by both phases |

No active finding remains after the correction.

## Resolved Baseline Concerns

- The public `prepare_trusted_sql_resumable_update` and
  `resume_trusted_sql_resumable_update` custody methods, continuation DTOs,
  compile fixture, and performance-canister path are absent.
- Mutation-job start, state, advance, replay, completion, and acknowledgement
  are the sole maintained trusted fixed-update lifecycle.
- Mutation target rows and exact progress succession share the existing commit
  marker and recovery flow; no second WAL, progress allocation, or transaction
  coordinator exists.
- Production source no longer contains the historical `Patch 4` reason text
  previously found in mutation-progress and structural-write code.
- IcyDB manifests contain no Canic dependency. Generated database wiring calls
  one automatic IcyDB ensure boundary over `ic-memory` committed allocations.

## Retained Separations

- SQL, dynamic, and typed construction remain distinct frontends because they
  bind different public contracts. They converge before planning or mutation.
- Forward and Verify traversal remain distinct execution phases. Forward
  collects accepted loaded rows and commits mutations; Verify is read-only and
  proves stable exhaustion. They now share the identical eligibility and scope
  preparation without forcing their different effects into one branch-heavy
  executor.
- Generic revision-strict resumable jobs, integrity jobs, mutation jobs, schema
  applications, and migrations retain separate state machines because their
  authorization, completion, and recovery semantics differ. A speculative
  generic job framework would increase vocabulary without removing an owner.
- Generated proposals and SQL DDL retain separate lowering because source
  reconciliation and operator DDL have different admission contracts. Accepted
  candidates and publication remain singular.
- Executor fast paths remain separate only after planner-owned route selection;
  the published 0.223 evidence measures their cost and does not show runtime
  policy rediscovery.

## Complexity And State-Space Delta

`FCD-1.0` has no comparable prior baseline.

The worktree correction removes one duplicated semantic branch and 12 net
runtime lines. It adds no public mode, configuration, persisted state, format,
cursor, error, or execution route. Forward alone retains the row-layout
descriptor required for mutation; both phases consume one traversal contract.
Implementation structure is simpler. Maintained raw final Wasm moves only
+13 bytes for the dynamic-query subject and +142 bytes for the typed-query
subject from released 0.223.4, far below the 64 KiB review gate. Candid remains
byte-identical.

## Focused Verification Readout

| Verification | Status | Result |
| --- | --- | --- |
| Released snapshot and clean-start check | `PASS` | `0.223.4` at `d8d6a33c0`; no pre-existing worktree changes |
| Old public custody API search | `PASS` | no maintained Rust/canister occurrence |
| Historical production patch-label search | `PASS` | no maintained production occurrence |
| Canic manifest dependency search | `PASS` | no dependency occurrence |
| Memory bootstrap owner trace | `PASS` | committed allocations are adopted and IcyDB declarations validated without a caller policy |
| Query/write/schema convergence trace | `PASS` | one carried plan, structural mutation boundary, and accepted publication flow |
| Mutation-job focused native tests | `PASS` | 11 passed; 0 failed; 1,639 filtered out |
| Maintained raw Wasm | `PASS` | dynamic 2,632,176 bytes (+13); typed 1,819,128 bytes (+142) |
| Candid | `PASS` | byte-identical at 4,670/64 bytes with released hashes |
| Full repository suite | `BLOCKED` | push-owned and prohibited for this focused correction |
| Live canister performance replay | `BLOCKED` | not rerun; released 0.223.4 evidence remains contextual, not substituted by this audit |
