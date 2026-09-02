mod having;

use crate::db::{
    codec::write_hash_u64,
    query::{
        builder::scalar_projection::render_scalar_projection_expr_plan_label,
        explain::{ExplainGroupField, ExplainGrouping},
        fingerprint::{
            aggregate_hash::{AggregateHashShape, hash_group_aggregate_structural_fingerprint},
            hash_sections::{
                GROUP_FIELD_DIRECT_TAG, GROUP_FIELD_SCALAR_PATH_TAG, GROUPING_NONE_TAG,
                GROUPING_PRESENT_TAG, GROUPING_STRATEGY_HASH_TAG, GROUPING_STRATEGY_ORDERED_TAG,
                write_str, write_tag, write_u32,
            },
            projection_hash::hash_projection_structural_fingerprint,
        },
        plan::{
            AccessPlannedQuery, GroupAggregateSpec, GroupFieldSet, GroupedPlanAggregateFamily,
            GroupedPlanFallbackReason, GroupedPlanStrategy, ScalarGroupPath, expr::PathSpec,
            expr::ProjectionSpec, grouped_plan_strategy,
        },
    },
};
use sha2::Sha256;

use crate::db::query::fingerprint::hash_sections::grouping::having::{
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
    group_fields: GroupedFingerprintFields<'a>,
    aggregates: Vec<AggregateHashShape<'a>>,
    having: Option<GroupHavingFingerprintSource<'a>>,
    max_groups: u64,
    max_group_bytes: u64,
}

enum GroupedFingerprintFields<'a> {
    Explain(&'a [ExplainGroupField]),
    Plan(&'a GroupFieldSet),
}

impl GroupedFingerprintFields<'_> {
    const fn len(&self) -> usize {
        match self {
            Self::Explain(fields) => fields.len(),
            Self::Plan(fields) => fields.len(),
        }
    }
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
pub(super) fn hash_grouping_shape(
    hasher: &mut Sha256,
    source: GroupingFingerprintSource<'_>,
    include_group_strategy: bool,
) {
    let grouping = ProjectedGroupingShape::from_source(source);

    hash_projected_grouping_shape(hasher, &grouping, include_group_strategy);
}

pub(super) fn hash_projection_spec(
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

    hash_projected_grouping_shape(hasher, &projected_grouping, include_group_strategy);
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
                        .map(|aggregate| {
                            GroupAggregateSpec::from_optional_field_input(
                                aggregate.kind(),
                                aggregate.target_field().map(str::to_string),
                                aggregate.distinct(),
                            )
                        })
                        .collect::<Vec<_>>(),
                );

                Self::Grouped(GroupedFingerprintShape {
                    ordered_group: *strategy == "ordered_group",
                    aggregate_family_code: Some(aggregate_family.code()),
                    group_fields: GroupedFingerprintFields::Explain(group_fields),
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
        let strategy = grouped_plan_strategy(plan).unwrap_or_else(|| {
            debug_assert!(
                grouped_plan_strategy(plan).is_some(),
                "grouped fingerprint projection requires planner-owned grouped strategy",
            );
            GroupedPlanStrategy::hash_group_with_aggregate_family(
                GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
                GroupedPlanAggregateFamily::from_grouped_aggregates(
                    grouped.group.aggregates.as_slice(),
                ),
            )
        });

        Self::Grouped(GroupedFingerprintShape {
            ordered_group: strategy.is_ordered_group(),
            aggregate_family_code: Some(strategy.aggregate_family().code()),
            group_fields: GroupedFingerprintFields::Plan(&grouped.group.group_fields),
            aggregates: grouped
                .group
                .aggregates
                .iter()
                .map(|aggregate| {
                    AggregateHashShape::semantic(
                        aggregate.kind(),
                        aggregate.target_field(),
                        aggregate
                            .input_expr()
                            .map(render_scalar_projection_expr_plan_label),
                        aggregate
                            .filter_expr()
                            .map(render_scalar_projection_expr_plan_label),
                        aggregate.semantic_distinct(),
                    )
                })
                .collect(),
            having: grouped
                .having_expr()
                .map(|expr| GroupHavingFingerprintSource::Plan {
                    expr,
                    group_fields: &grouped.group.group_fields,
                    aggregates: grouped.group.aggregates.as_slice(),
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
fn hash_projected_grouping_shape(
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

            hash_group_field_slots(hasher, &grouped.group_fields);
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
fn hash_group_field_slots(hasher: &mut Sha256, fields: &GroupedFingerprintFields<'_>) {
    write_u32(hasher, fields.len() as u32);
    match fields {
        GroupedFingerprintFields::Explain(fields) => {
            for field in *fields {
                hash_group_field(
                    hasher,
                    field.slot_index() as u32,
                    field.field(),
                    field.path.as_ref(),
                );
            }
        }
        GroupedFingerprintFields::Plan(fields) => {
            for field in fields.iter() {
                hash_group_field(
                    hasher,
                    field.root_slot() as u32,
                    field.field(),
                    field.as_scalar_path().map(ScalarGroupPath::path),
                );
            }
        }
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

// Hash grouped aggregate identity from one already-lowered aggregate shape stream.
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
