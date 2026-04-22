mod having;

use crate::db::{
    codec::write_hash_u64,
    query::{
        builder::scalar_projection::render_scalar_projection_expr_sql_label,
        explain::ExplainGrouping,
        fingerprint::{
            aggregate_hash::{AggregateHashShape, hash_group_aggregate_structural_fingerprint},
            hash_parts::{
                GROUPING_NONE_TAG, GROUPING_PRESENT_TAG, GROUPING_STRATEGY_HASH_TAG,
                GROUPING_STRATEGY_ORDERED_TAG, write_str, write_tag, write_u32,
            },
            projection_hash::hash_projection_structural_fingerprint,
        },
        plan::{
            AccessPlannedQuery, GroupAggregateSpec, GroupedPlanAggregateFamily,
            expr::ProjectionSpec, grouped_plan_strategy,
        },
    },
};
use sha2::Sha256;

use crate::db::query::fingerprint::hash_parts::grouping::having::{
    GroupHavingFingerprintSource, hash_group_having_projection,
};

///
/// GroupedFingerprintShape
///
/// Canonical grouped fingerprint projection shared by logical-plan and explain
/// hashing callsites. Both surfaces project into this neutral grouped shape so
/// hashing does not keep parallel semantic projection seams.
///

struct GroupedFingerprintShape<'a> {
    ordered_group: bool,
    aggregate_family_code: Option<&'a str>,
    group_fields: Vec<(u32, &'a str)>,
    aggregates: Vec<AggregateHashShape<'a>>,
    having: Option<GroupHavingFingerprintSource<'a>>,
    max_groups: u64,
    max_group_bytes: u64,
}

/// Canonical grouped fingerprint projection state shared by plan and explain hashing.
enum ProjectedGroupingShape<'a> {
    None,
    Grouped(GroupedFingerprintShape<'a>),
}

///
/// GroupingFingerprintSource
///
/// Canonical grouped fingerprint source shared by logical-plan and explain
/// hashing callsites. This keeps the grouped-shape and grouped-projection
/// fallback wrappers on one source-neutral seam before hashing.
///

pub(super) enum GroupingFingerprintSource<'a> {
    Explain(&'a ExplainGrouping),
    Plan(&'a AccessPlannedQuery),
}

// Grouped shape semantics that remain part of continuation identity independent
// from projection expression hashing.
pub(super) fn hash_grouping_shape_v1(
    hasher: &mut Sha256,
    source: GroupingFingerprintSource<'_>,
    include_group_strategy: bool,
) {
    let grouping = ProjectedGroupingShape::from_source(source);

    hash_projected_grouping_shape_v1(hasher, &grouping, include_group_strategy);
}

pub(super) fn hash_projection_spec_v1(
    hasher: &mut Sha256,
    projection: Option<&ProjectionSpec>,
    grouping: GroupingFingerprintSource<'_>,
    include_group_strategy: bool,
) {
    let projected_grouping = ProjectedGroupingShape::from_source(grouping);

    // Projection identity does not subsume grouped semantic identity: grouped
    // `HAVING` remains outside projection lowering, so grouped plan hashes
    // must include both the projected output shape and the grouped shape.
    if let Some(projection) = projection {
        hash_projection_structural_fingerprint(hasher, projection);
        if matches!(projected_grouping, ProjectedGroupingShape::None) {
            return;
        }
    }

    hash_projected_grouping_shape_v1(hasher, &projected_grouping, include_group_strategy);
}

impl<'a> ProjectedGroupingShape<'a> {
    fn from_source(source: GroupingFingerprintSource<'a>) -> Self {
        match source {
            GroupingFingerprintSource::Explain(grouping) => Self::from_explain(grouping),
            GroupingFingerprintSource::Plan(plan) => Self::from_plan(plan),
        }
    }

    fn from_explain(grouping: &'a ExplainGrouping) -> Self {
        match grouping {
            ExplainGrouping::None => Self::None,
            ExplainGrouping::Grouped {
                strategy,
                fallback_reason: _,
                group_fields,
                aggregates,
                having,
                max_groups,
                max_group_bytes,
            } => {
                let aggregate_family = GroupedPlanAggregateFamily::from_grouped_aggregates(
                    &aggregates
                        .iter()
                        .map(|aggregate| GroupAggregateSpec {
                            kind: aggregate.kind(),
                            #[cfg(test)]
                            target_field: aggregate.target_field().map(str::to_string),
                            input_expr: aggregate.target_field().map(|field| {
                                Box::new(crate::db::query::plan::expr::Expr::Field(
                                    crate::db::query::plan::expr::FieldId::new(field),
                                ))
                            }),
                            filter_expr: None,
                            distinct: aggregate.distinct(),
                        })
                        .collect::<Vec<_>>(),
                );

                Self::Grouped(GroupedFingerprintShape {
                    ordered_group: *strategy == "ordered_group",
                    aggregate_family_code: Some(aggregate_family.code()),
                    group_fields: group_fields
                        .iter()
                        .map(|field| (field.slot_index() as u32, field.field()))
                        .collect(),
                    aggregates: aggregates
                        .iter()
                        .map(|aggregate| {
                            AggregateHashShape::semantic(
                                aggregate.kind(),
                                aggregate.target_field(),
                                aggregate.input_expr().map(str::to_string),
                                aggregate.filter_expr().map(str::to_string),
                                aggregate.distinct(),
                            )
                        })
                        .collect(),
                    having: having
                        .as_ref()
                        .map(|having| GroupHavingFingerprintSource::Explain {
                            expr: having.expr(),
                            group_fields,
                            aggregates,
                        }),
                    max_groups: *max_groups,
                    max_group_bytes: *max_group_bytes,
                })
            }
        }
    }

    fn from_plan(plan: &'a AccessPlannedQuery) -> Self {
        let Some(grouped) = plan.grouped_plan() else {
            return Self::None;
        };
        let strategy = grouped_plan_strategy(plan)
            .expect("grouped grouping-shape hashing requires planner-owned grouped strategy");

        Self::Grouped(GroupedFingerprintShape {
            ordered_group: strategy.is_ordered_group(),
            aggregate_family_code: Some(strategy.aggregate_family().code()),
            group_fields: grouped
                .group
                .group_fields
                .iter()
                .map(|field| (field.index as u32, field.field.as_str()))
                .collect(),
            aggregates: grouped
                .group
                .aggregates
                .iter()
                .map(|aggregate| {
                    AggregateHashShape::semantic(
                        aggregate.kind,
                        aggregate.target_field(),
                        aggregate
                            .input_expr()
                            .map(render_scalar_projection_expr_sql_label),
                        aggregate
                            .filter_expr()
                            .map(render_scalar_projection_expr_sql_label),
                        aggregate.distinct,
                    )
                })
                .collect(),
            having: grouped.effective_having_expr().map(|expr| match expr {
                std::borrow::Cow::Borrowed(expr) => GroupHavingFingerprintSource::PlanBorrowed {
                    expr,
                    group_fields: grouped.group.group_fields.as_slice(),
                    aggregates: grouped.group.aggregates.as_slice(),
                },
                std::borrow::Cow::Owned(expr) => GroupHavingFingerprintSource::PlanOwned {
                    expr,
                    group_fields: grouped.group.group_fields.as_slice(),
                    aggregates: grouped.group.aggregates.as_slice(),
                },
            }),
            max_groups: grouped.group.execution.max_groups,
            max_group_bytes: grouped.group.execution.max_group_bytes,
        })
    }
}

// Hash the canonical grouped identity payload after plan/explain have already
// projected onto the shared grouped fingerprint shape.
// This is one grouped semantic identity surface, so it intentionally consumes
// canonical grouped form. Prepared/template identity remains outside this seam
// and stays syntax-bound in the SQL-front-end caches.
fn hash_projected_grouping_shape_v1(
    hasher: &mut Sha256,
    grouping: &ProjectedGroupingShape<'_>,
    include_group_strategy: bool,
) {
    match grouping {
        ProjectedGroupingShape::None => write_tag(hasher, GROUPING_NONE_TAG),
        ProjectedGroupingShape::Grouped(grouped) => {
            write_tag(hasher, GROUPING_PRESENT_TAG);
            if include_group_strategy {
                hash_grouped_strategy_projection(
                    hasher,
                    grouped.ordered_group,
                    grouped.aggregate_family_code,
                );
            }

            hash_group_field_slots(
                hasher,
                grouped.group_fields.len(),
                grouped
                    .group_fields
                    .iter()
                    .map(|(slot_index, field)| (*slot_index, *field)),
            );
            hash_group_aggregate_shapes(
                hasher,
                grouped.aggregates.len(),
                grouped.aggregates.iter().cloned(),
            );
            hash_group_having_projection(hasher, grouped.having.as_ref());

            write_hash_u64(hasher, grouped.max_groups);
            write_hash_u64(hasher, grouped.max_group_bytes);
        }
    }
}

// Hash grouped key order using stable slot identity first, then the canonical
// field label as a guardrail against grouped projection drift.
fn hash_group_field_slots<'a, I>(hasher: &mut Sha256, field_count: usize, fields: I)
where
    I: IntoIterator<Item = (u32, &'a str)>,
{
    write_u32(hasher, field_count as u32);
    for (slot_index, field) in fields {
        write_u32(hasher, slot_index);
        write_str(hasher, field);
    }
}

// Hash grouped aggregate semantics from one already-lowered aggregate shape stream.
fn hash_group_aggregate_shapes<'a, I>(hasher: &mut Sha256, aggregate_count: usize, aggregates: I)
where
    I: IntoIterator<Item = AggregateHashShape<'a>>,
{
    write_u32(hasher, aggregate_count as u32);
    for aggregate in aggregates {
        hash_group_aggregate_structural_fingerprint(hasher, &aggregate);
    }
}

fn hash_grouped_strategy_projection(
    hasher: &mut Sha256,
    ordered_group: bool,
    aggregate_family_code: Option<&str>,
) {
    if ordered_group {
        write_tag(hasher, GROUPING_STRATEGY_ORDERED_TAG);
    } else {
        write_tag(hasher, GROUPING_STRATEGY_HASH_TAG);
    }

    if let Some(aggregate_family_code) = aggregate_family_code {
        write_str(hasher, aggregate_family_code);
    }
}
