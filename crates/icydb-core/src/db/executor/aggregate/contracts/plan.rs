//! Module: executor::aggregate::contracts::plan
//! Responsibility: aggregate-facing query-plan contracts.
//! Does not own: logical aggregate planning or query-plan construction.
//! Boundary: centralizes query-plan DTOs consumed by aggregate execution.

#[cfg(feature = "sql")]
pub(in crate::db::executor) use crate::db::query::plan::expr::{
    FieldId, ProjectionField, admit_true_only_boolean_value,
    compile_scalar_projection_expr_from_schema,
};
pub(in crate::db::executor) use crate::db::query::plan::{
    AccessPlannedQuery, AggregateKind, CoveringProjectionFacts, CoveringProjectionOrder,
    EffectiveRuntimeFilterProgram, FieldSlot, GlobalDistinctAggregateKind,
    GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedExecutionConfig,
    OrderDirection, OrderSpec, PageSpec, PlannedProjectionLayout,
    constant_covering_projection_value_from_access,
    covering_index_adjacent_distinct_eligible as plan_covering_index_adjacent_distinct_eligible,
    covering_index_projection_facts_with_primary_key_names as plan_covering_index_projection_facts,
    expr::{
        CompiledExpr, Expr, ProjectionSpec, collapse_true_only_boolean_admission,
        eval_builder_expr_for_value_preview,
    },
    global_distinct_group_spec_for_aggregate_identity,
};

#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor) use crate::db::query::plan::expr::BinaryOp;
