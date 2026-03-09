# 0.40 Planner vs Executor Semantic Ownership Audit

Date: 2026-03-02  
Scope: `crates/icydb-core/src/db/executor/**` semantic-vs-invariant ownership

## Summary

This slice audits executor-side semantic branching for grouped load paths and
records the first ownership-consolidation changes landed in `0.40.2`.

Primary result:

- grouped projection-layout structural validation is now planner-owned.
- executor keeps only debug-time trust assertions for planner handoff shape.
- grouped cursor-policy semantic helper is now continuation-facade scoped in
  production and removed from grouped global-distinct fold runtime.

## Findings

1. Closed: grouped projection layout validation moved to planner boundary.
   - Added planner validator: `db::query::plan::grouped_layout::validate_grouped_projection_layout`.
   - Enforced at handoff construction in `db::query::plan::group::grouped_executor_handoff`.
   - Removed executor runtime validation hook from `executor::load::grouped_route`.
   - Removed fold-stage runtime call in `executor::load::grouped_fold::execute_group_fold`.

2. Closed (scope-adjusted): grouped cursor-policy helper no longer appears in
   executor grouped-fold runtime.
   - `executor::load::grouped_fold::global_distinct` no longer calls planner
     grouped cursor-policy helper.
   - grouped cursor-policy helper access now goes through
     `query::plan::grouped_cursor_policy_violation_for_continuation(...)`.
   - direct `query::plan::grouped_cursor_policy_violation(...)` exports were
     removed from production re-exports; a test-only shim remains for planner
     semantic contract tests.

3. Open: route feasibility still derives grouped strategy eligibility at executor route layer.
   - This appears intentional for physical feasibility, but should remain
     invariant/mechanical and must not drift into semantic policy re-validation.

## Recommendations (Next Slice)

1. Decide final authority framing for grouped cursor policy
   (`planner policy` vs `continuation protocol rule`) and document it explicitly.
2. Add one structural guard that locks continuation-facade-only access to
   grouped cursor-policy helper surfaces in production modules.
3. Continue inventory of planner-policy helper calls in executor route
   feasibility paths to ensure they remain mechanical feasibility checks.

## Evidence (Key Paths)

- `crates/icydb-core/src/db/query/plan/grouped_layout.rs`
- `crates/icydb-core/src/db/query/plan/group.rs`
- `crates/icydb-core/src/db/executor/load/grouped_route.rs`
- `crates/icydb-core/src/db/executor/load/grouped_fold/mod.rs`
- `crates/icydb-core/src/db/executor/continuation/mod.rs`
- `crates/icydb-core/src/db/executor/load/grouped_fold/global_distinct.rs`
- `crates/icydb-core/src/db/query/plan/mod.rs`
- `crates/icydb-core/src/db/query/plan/semantics.rs`
