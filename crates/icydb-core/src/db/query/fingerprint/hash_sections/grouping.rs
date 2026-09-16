mod having;

#[cfg(test)]
mod tests;

use crate::db::{
    codec::write_hash_u64,
    query::{
        construction::ConstructionBudget,
        fingerprint::{
            aggregate_hash::hash_group_aggregate_structural_fingerprint,
            hash_sections::{
                GROUP_FIELD_DIRECT_TAG, GROUP_FIELD_SCALAR_PATH_TAG, GROUPING_NONE_TAG,
                GROUPING_PRESENT_TAG, GROUPING_STRATEGY_HASH_TAG, GROUPING_STRATEGY_ORDERED_TAG,
                grouping::having::{GroupHavingFingerprintSource, hash_group_having_projection},
                write_str, write_tag, write_u32,
            },
            projection_hash::{
                admission::admit_projection_hash, hash_projection_structural_fingerprint,
            },
        },
        plan::{
            AccessPlannedQuery, GroupAggregateSpec, GroupFieldSet, GroupedPlanStrategy,
            expr::{PathSpec, ProjectionSpec},
            grouped_plan_strategy,
        },
    },
};
use crate::error::InternalError;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use sha2::Sha256;

// Grouping is hashed both as a continuation section and alongside projection.
// Preserve both occurrences: projection alone does not encode grouped HAVING.
pub(super) fn hash_grouping_shape(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    let Some(grouped) = plan.grouped_plan() else {
        write_tag(hasher, GROUPING_NONE_TAG);
        return Ok(());
    };
    let strategy = grouped_plan_strategy(plan, || plan.prepare_residual_filter_shape(budget))?
        .ok_or_else(InternalError::planner_executor_invariant)?;
    write_tag(hasher, GROUPING_PRESENT_TAG);
    hash_grouped_strategy_projection(hasher, strategy);
    hash_group_field_slots(hasher, &grouped.group.group_fields, budget)?;
    hash_group_aggregate_shapes(hasher, &grouped.group.aggregates, budget)?;
    let having = grouped
        .having_expr()
        .map(|expr| GroupHavingFingerprintSource {
            expr,
            group_fields: &grouped.group.group_fields,
            aggregates: &grouped.group.aggregates,
        });
    hash_group_having_projection(hasher, having.as_ref(), budget)?;
    write_hash_u64(hasher, grouped.group.execution.max_groups);
    write_hash_u64(hasher, grouped.group.execution.max_group_bytes);
    Ok(())
}

pub(super) fn hash_projection_spec(
    hasher: &mut Sha256,
    projection: &ProjectionSpec,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    admit_projection_hash(projection, budget)?;
    hash_projection_structural_fingerprint(hasher, projection)?;
    if plan.grouped_plan().is_some() {
        hash_grouping_shape(hasher, plan, budget)?;
    }
    Ok(())
}

fn hash_group_field_slots(
    hasher: &mut Sha256,
    fields: &GroupFieldSet,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    write_u32(hasher, fields.len() as u32);
    for field in fields.iter() {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        if let Some(path) = field.as_scalar_path() {
            write_tag(hasher, GROUP_FIELD_SCALAR_PATH_TAG);
            write_u32(hasher, field.root_slot() as u32);
            hash_field_path(hasher, path.path(), budget)?;
        } else {
            write_tag(hasher, GROUP_FIELD_DIRECT_TAG);
            write_u32(hasher, field.root_slot() as u32);
            budget.charge(
                Resource::PredicateExpressionSteps,
                field.field().len() as u64,
            )?;
            write_str(hasher, field.field());
        }
    }
    Ok(())
}

// Group keys and HAVING paths share their component framing and byte admission.
// Callers retain their distinct section tags and slot encoding.
fn hash_field_path(
    hasher: &mut Sha256,
    path: &PathSpec,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        path.root().as_str().len() as u64,
    )?;
    write_str(hasher, path.root().as_str());
    write_u32(hasher, path.segments().len() as u32);
    budget.charge(
        Resource::PredicateExpressionSteps,
        path.segments().len() as u64,
    )?;
    for segment in path.segments() {
        budget.charge(Resource::PredicateExpressionSteps, segment.len() as u64)?;
        write_str(hasher, segment);
    }
    Ok(())
}

fn hash_group_aggregate_shapes(
    hasher: &mut Sha256,
    aggregates: &[GroupAggregateSpec],
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    write_u32(hasher, aggregates.len() as u32);
    for aggregate in aggregates {
        hash_group_aggregate_structural_fingerprint(hasher, aggregate, budget)?;
    }
    Ok(())
}

fn hash_grouped_strategy_projection(hasher: &mut Sha256, strategy: GroupedPlanStrategy) {
    if strategy.is_ordered_group() {
        write_tag(hasher, GROUPING_STRATEGY_ORDERED_TAG);
    } else {
        write_tag(hasher, GROUPING_STRATEGY_HASH_TAG);
    }

    write_str(hasher, strategy.aggregate_family().code());
}
