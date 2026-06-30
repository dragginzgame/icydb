# 0.183 Status

Status: closed.

## Landed

- First narrow branch-aware composite-prefix route for fixed prefix plus small
  `IN` ordered by primary key.
- Branch-set route validation, diagnostics, bounded execution tests, residual
  stripping coverage, and covered/hybrid projection checks.
- Shared SQL/fluent count, covering, prefix-cardinality, and large-`IN`
  optimization cleanup.
- Perf attribution and feature-matrix guardrails for the query optimization
  line.

## Deferred

- 0.184: mega-audit cleanup, correctness findings, SQL/fluent flow
  simplification, and test hardening.
- 0.185: Branch-Aware Query Revisited, including broader branch-tree routing,
  continuation/cursor design, adaptive route choice, and branch-heavy perf
  matrix expansion.
