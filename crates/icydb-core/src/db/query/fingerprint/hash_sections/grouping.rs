mod having;

use crate::db::{
    codec::write_hash_u64,
    query::{
        builder::scalar_projection::render_scalar_projection_expr_plan_label,
        fingerprint::{
            aggregate_hash::{AggregateHashShape, hash_group_aggregate_structural_fingerprint},
            hash_sections::{
                GROUP_FIELD_DIRECT_TAG, GROUP_FIELD_SCALAR_PATH_TAG, GROUPING_NONE_TAG,
                GROUPING_PRESENT_TAG, GROUPING_STRATEGY_HASH_TAG, GROUPING_STRATEGY_ORDERED_TAG,
                grouping::having::{GroupHavingFingerprintSource, hash_group_having_projection},
                write_str, write_tag, write_u32,
            },
            projection_hash::hash_projection_structural_fingerprint,
        },
        plan::{
            AccessPlannedQuery, GroupAggregateSpec, GroupFieldSet, GroupedPlanAggregateFamily,
            GroupedPlanFallbackReason, GroupedPlanStrategy, ScalarGroupPath,
            expr::{PathSpec, ProjectionSpec},
            grouped_plan_strategy,
        },
    },
};
use crate::error::InternalError;
use sha2::Sha256;

// Grouping is hashed both as a continuation section and alongside projection.
// Preserve both occurrences: projection alone does not encode grouped HAVING.
pub(super) fn hash_grouping_shape(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    let Some(grouped) = plan.grouped_plan() else {
        write_tag(hasher, GROUPING_NONE_TAG);
        return Ok(());
    };
    let strategy = grouped_plan_strategy(plan).unwrap_or_else(|| {
        debug_assert!(
            grouped_plan_strategy(plan).is_some(),
            "grouped fingerprint projection requires planner-owned grouped strategy"
        );
        GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
            GroupedPlanAggregateFamily::from_grouped_aggregates(&grouped.group.aggregates),
        )
    });
    write_tag(hasher, GROUPING_PRESENT_TAG);
    hash_grouped_strategy_projection(hasher, strategy);
    hash_group_field_slots(hasher, &grouped.group.group_fields);
    hash_group_aggregate_shapes(hasher, &grouped.group.aggregates);
    let having = grouped
        .having_expr()
        .map(|expr| GroupHavingFingerprintSource {
            expr,
            group_fields: &grouped.group.group_fields,
            aggregates: &grouped.group.aggregates,
        });
    hash_group_having_projection(hasher, having.as_ref())?;
    write_hash_u64(hasher, grouped.group.execution.max_groups);
    write_hash_u64(hasher, grouped.group.execution.max_group_bytes);
    Ok(())
}

pub(super) fn hash_projection_spec(
    hasher: &mut Sha256,
    projection: &ProjectionSpec,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    hash_projection_structural_fingerprint(hasher, projection)?;
    if plan.grouped_plan().is_some() {
        hash_grouping_shape(hasher, plan)?;
    }
    Ok(())
}

fn hash_group_field_slots(hasher: &mut Sha256, fields: &GroupFieldSet) {
    write_u32(hasher, fields.len() as u32);
    for field in fields.iter() {
        hash_group_field(
            hasher,
            field.root_slot() as u32,
            field.field(),
            field.as_scalar_path().map(ScalarGroupPath::path),
        );
    }
}

fn hash_group_field(hasher: &mut Sha256, root_slot: u32, field: &str, path: Option<&PathSpec>) {
    if let Some(path) = path {
        write_tag(hasher, GROUP_FIELD_SCALAR_PATH_TAG);
        write_u32(hasher, root_slot);
        write_str(hasher, path.root().as_str());
        write_u32(hasher, path.segments().len() as u32);
        for segment in path.segments() {
            write_str(hasher, segment);
        }
    } else {
        write_tag(hasher, GROUP_FIELD_DIRECT_TAG);
        write_u32(hasher, root_slot);
        write_str(hasher, field);
    }
}

fn hash_group_aggregate_shapes(hasher: &mut Sha256, aggregates: &[GroupAggregateSpec]) {
    write_u32(hasher, aggregates.len() as u32);
    for aggregate in aggregates {
        let input = aggregate
            .input_expr()
            .map(render_scalar_projection_expr_plan_label);
        let filter = aggregate
            .filter_expr()
            .map(render_scalar_projection_expr_plan_label);
        hash_group_aggregate_structural_fingerprint(
            hasher,
            &AggregateHashShape::semantic(
                aggregate.kind(),
                aggregate.target_field(),
                input.as_deref(),
                filter.as_deref(),
                aggregate.semantic_distinct(),
            ),
        );
    }
}

fn hash_grouped_strategy_projection(hasher: &mut Sha256, strategy: GroupedPlanStrategy) {
    if strategy.is_ordered_group() {
        write_tag(hasher, GROUPING_STRATEGY_ORDERED_TAG);
    } else {
        write_tag(hasher, GROUPING_STRATEGY_HASH_TAG);
    }

    write_str(hasher, strategy.aggregate_family().code());
}
