# IcyDB report retention

Git retains compact topic conclusions and selected absolute results. Routine
checks belong in the owning changelog or GitHub issue and need no new report.
Apply the [Shared retention contract](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/rules/engineering-details.md#changelogs-and-artifact-preservation).

- Keep one current summary per topic, normally at most 150 lines, with source/tool
  identities, reproduction commands, limitations and selected results.
- A topic may keep at most two small data files: 100 KiB combined per topic and
  5 MiB of experimental evidence across the repository are ceilings, not targets.
- Raw logs, run tables, source extracts, Twiggy output and downloads belong under
  `target/evidence/`; an explicit workspace clean may remove them. No automatic
  archive or promotion into Git is required.
- Release receipts, active deployment/recovery records and maintained regression
  inputs keep their separate contracts. This cleanup does not remove them.
- Any necessary durable raw-evidence exception must name the claim, minimal
  payload, measured size, owner, destination and retention/review period.

## Historical cleanup

The 2026-11-05 cleanup retained one selected summary per historical investigation
or recurring topic, now at stable `investigations/<topic>.md` and
`recurring/<topic>.md` paths. Update the same topic file rather than adding a run
directory. Raw captures use unique directories under `target/evidence/`.
Superseded raw output and reports were removed from the working tree. Original
content remains in ordinary Git history; no history rewrite or replacement bulk
archive was made. Each retained summary links its original revision. Historical
results do not qualify current code, and hashes/summaries do not establish that
old binaries or live environments remain available. GitHub issues own follow-up.

`releases/` retains its existing closeout records. Original saved code-review
inputs remain under `docs/code-review/` while open findings refer to them; they
are review source material rather than experimental run output.

## Selected topic summaries

### Investigations

- [0.40-planner-executor-semantic-ownership](investigations/0.40-planner-executor-semantic-ownership.md)
- [access-stream-anchor-interpretation-followup](investigations/access-stream-anchor-interpretation-followup.md)
- [aggregate-contracts](investigations/aggregate-contracts.md)
- [application-batch-workload](investigations/application-batch-workload.md)
- [audit-framework-review](investigations/audit-framework-review.md)
- [batch-preparation](investigations/batch-preparation.md)
- [bucket-sizing](investigations/bucket-sizing.md)
- [cache-traversal](investigations/cache-traversal.md)
- [canic-dependencies](investigations/canic-dependencies.md)
- [catalog-selection](investigations/catalog-selection.md)
- [chatgpt-db-audit](investigations/chatgpt-db-audit.md)
- [collection-materialization](investigations/collection-materialization.md)
- [complexity-surface-hygiene-icydb](investigations/complexity-surface-hygiene-icydb.md)
- [complexity-surface-hygiene-icydb-build](investigations/complexity-surface-hygiene-icydb-build.md)
- [complexity-surface-hygiene-icydb-config-build](investigations/complexity-surface-hygiene-icydb-config-build.md)
- [complexity-surface-hygiene-icydb-derive](investigations/complexity-surface-hygiene-icydb-derive.md)
- [complexity-surface-hygiene-icydb-primitives](investigations/complexity-surface-hygiene-icydb-primitives.md)
- [complexity-surface-hygiene-icydb-schema](investigations/complexity-surface-hygiene-icydb-schema.md)
- [complexity-surface-hygiene-icydb-schema-derive](investigations/complexity-surface-hygiene-icydb-schema-derive.md)
- [complexity-surface-hygiene-icydb-utils-csh-1.2](investigations/complexity-surface-hygiene-icydb-utils-csh-1.2.md)
- [composed-wasm-preflight](investigations/composed-wasm-preflight.md)
- [config-hygiene](investigations/config-hygiene.md)
- [current-architecture-state](investigations/current-architecture-state.md)
- [db-redundancy-shim-compatibility](investigations/db-redundancy-shim-compatibility.md)
- [decode-cursor-authority-followup](investigations/decode-cursor-authority-followup.md)
- [dependency-audit](investigations/dependency-audit.md)
- [execution-routing-refactor-followup](investigations/execution-routing-refactor-followup.md)
- [executor-test-triage](investigations/executor-test-triage.md)
- [explain-closeout](investigations/explain-closeout.md)
- [facet-adoption](investigations/facet-adoption.md)
- [fallible-canonical-sort](investigations/fallible-canonical-sort.md)
- [forensic-complexity-accretion](investigations/forensic-complexity-accretion.md)
- [grouped-heap-replacement](investigations/grouped-heap-replacement.md)
- [hard-cut-legacy-architecture](investigations/hard-cut-legacy-architecture.md)
- [journal-batch-ownership](investigations/journal-batch-ownership.md)
- [load-entrypoints-continuation-contract-followup](investigations/load-entrypoints-continuation-contract-followup.md)
- [load-entrypoints-cursor-resolution-followup](investigations/load-entrypoints-cursor-resolution-followup.md)
- [load-entrypoints-final-surface-followup](investigations/load-entrypoints-final-surface-followup.md)
- [load-entrypoints-module-split-followup](investigations/load-entrypoints-module-split-followup.md)
- [load-entrypoints-shape-dispatch-followup](investigations/load-entrypoints-shape-dispatch-followup.md)
- [load-entrypoints-wrapper-typed-surface-followup](investigations/load-entrypoints-wrapper-typed-surface-followup.md)
- [load-runtime-hub-split-followup](investigations/load-runtime-hub-split-followup.md)
- [logical-memory](investigations/logical-memory.md)
- [module-surface-hardening-subaccount](investigations/module-surface-hardening-subaccount.md)
- [module-surface-hardening-ulid](investigations/module-surface-hardening-ulid.md)
- [nested-relation-integrity-recovery](investigations/nested-relation-integrity-recovery.md)
- [preflight-overlay-lifetime](investigations/preflight-overlay-lifetime.md)
- [resource-model-followup-0.37.2](investigations/resource-model-followup-0.37.2.md)
- [returning-counted-output](investigations/returning-counted-output.md)
- [returning-selected-cells](investigations/returning-selected-cells.md)
- [row-journal-ownership](investigations/row-journal-ownership.md)
- [schema-preparation](investigations/schema-preparation.md)
- [selected-field-ergonomics](investigations/selected-field-ergonomics.md)
- [sql-harness-delta](investigations/sql-harness-delta.md)
- [sql-harness-expanded-delta](investigations/sql-harness-expanded-delta.md)
- [sql-performance-remediation](investigations/sql-performance-remediation.md)
- [structural-architecture-continuation-audit](investigations/structural-architecture-continuation-audit.md)
- [technical-debt](investigations/technical-debt.md)
- [toko-cost7](investigations/toko-cost7.md)
- [toko-miner-catalogue-reads](investigations/toko-miner-catalogue-reads.md)
- [toko-miner-shared-shard-convergence](investigations/toko-miner-shared-shard-convergence.md)
- [toko-miner-upstream](investigations/toko-miner-upstream.md)
- [typed-catalogue-reads](investigations/typed-catalogue-reads.md)
- [unified-commit-completion](investigations/unified-commit-completion.md)
- [wasm-query-audit](investigations/wasm-query-audit.md)
- [write-cost-reduction](investigations/write-cost-reduction.md)

### Recurring

- [boundary-semantics](recurring/boundary-semantics.md)
- [canonical-semantic-authority](recurring/canonical-semantic-authority.md)
- [completeness](recurring/completeness.md)
- [complexity-accretion](recurring/complexity-accretion.md)
- [complexity-and-technical-debt](recurring/complexity-and-technical-debt.md)
- [cursor-ordering](recurring/cursor-ordering.md)
- [dry-consolidation](recurring/dry-consolidation.md)
- [error-taxonomy](recurring/error-taxonomy.md)
- [flow-convergence](recurring/flow-convergence.md)
- [flow-convergence-and-duplication](recurring/flow-convergence-and-duplication.md)
- [index-integrity](recurring/index-integrity.md)
- [invariant-preservation](recurring/invariant-preservation.md)
- [layer-violation](recurring/layer-violation.md)
- [layer-violation-results](recurring/layer-violation-results.md)
- [meta-audit](recurring/meta-audit.md)
- [module-structure](recurring/module-structure.md)
- [month-summary](recurring/month-summary.md)
- [perf-audit](recurring/perf-audit.md)
- [perf-matrix](recurring/perf-matrix.md)
- [recovery-consistency](recurring/recovery-consistency.md)
- [resource-model-compliance](recurring/resource-model-compliance.md)
- [security-boundary](recurring/security-boundary.md)
- [sql-perf-audit](recurring/sql-perf-audit.md)
- [state-machine-integrity](recurring/state-machine-integrity.md)
- [summary](recurring/summary.md)
- [velocity-preservation](recurring/velocity-preservation.md)
- [wasm-footprint](recurring/wasm-footprint.md)
