//! Executor-shaped budget attribution and result-row charging.
//! Limits, counters and construction accounting belong to `db::resource`.

use crate::{
    db::{
        executor::{EntityAuthority, RuntimeGroupedRow, SharedPreparedExecutionPlan},
        resource::budget::{
            HardExecutionContext, charge_current_execution_budget, runtime_value_work,
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

/// Build literal-free attribution for one prepared read shape.
pub(in crate::db::executor) fn prepared_read_execution_context(
    plan: &SharedPreparedExecutionPlan,
    lane: DiagnosticExecutionLane,
) -> HardExecutionContext {
    HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        lane,
        plan.execution_shape_fingerprint_prefix(),
    )
}

/// Derive one literal-free shape prefix while immutable prepared residents are built.
pub(in crate::db::executor) fn read_shape_fingerprint_prefix(
    authority: &EntityAuthority,
    logical: &crate::db::query::plan::AccessPlannedQuery,
) -> Result<u64, crate::error::InternalError> {
    let fingerprint = authority.accepted_schema_fingerprint();
    let mut prefix = u64::from_be_bytes([
        fingerprint[0],
        fingerprint[1],
        fingerprint[2],
        fingerprint[3],
        fingerprint[4],
        fingerprint[5],
        fingerprint[6],
        fingerprint[7],
    ]) ^ authority.entity_tag().value().rotate_left(17);
    let scalar = logical.scalar_plan();
    prefix ^= u64::from(logical.has_residual_filter_predicate()?).rotate_left(7);
    prefix ^= u64::from(scalar.distinct).rotate_left(11);
    prefix ^=
        usize_as_u64(scalar.order.as_ref().map_or(0, |order| order.fields.len())).rotate_left(23);
    prefix ^= usize_as_u64(
        logical
            .scalar_projection_plan()
            .map_or(0, <[crate::db::query::plan::expr::CompiledExpr]>::len),
    )
    .rotate_left(31);
    prefix ^= usize_as_u64(logical.grouped_aggregate_execution_specs().map_or(
        0,
        <[crate::db::query::plan::GroupedAggregateExecutionSpec]>::len,
    ))
    .rotate_left(41);

    Ok(prefix)
}

/// Build bounded attribution for a direct read terminal without a full plan.
pub(in crate::db) const fn direct_read_execution_context(
    authority: &EntityAuthority,
    lane: DiagnosticExecutionLane,
    shape_domain: u64,
) -> HardExecutionContext {
    let fingerprint = authority.accepted_schema_fingerprint();
    let prefix = u64::from_be_bytes([
        fingerprint[0],
        fingerprint[1],
        fingerprint[2],
        fingerprint[3],
        fingerprint[4],
        fingerprint[5],
        fingerprint[6],
        fingerprint[7],
    ]) ^ authority.entity_tag().value().rotate_left(17)
        ^ shape_domain;

    HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, prefix)
}

/// Charge grouped runtime rows before cursor and public DTO finalization.
pub(in crate::db::executor) fn charge_runtime_grouped_rows(
    rows: &[RuntimeGroupedRow],
) -> Result<(), InternalError> {
    let (bytes, nested_steps) = rows
        .iter()
        .flat_map(|row| row.group_key().iter().chain(row.aggregate_values()))
        .fold((0_u64, 0_u64), |total, value| {
            let value_work = runtime_value_work(value);
            (
                total.0.saturating_add(value_work.0),
                total.1.saturating_add(value_work.1),
            )
        });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::ResultRows,
        usize_as_u64(rows.len()),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::MaterializedBytes, bytes)?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, bytes)
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
