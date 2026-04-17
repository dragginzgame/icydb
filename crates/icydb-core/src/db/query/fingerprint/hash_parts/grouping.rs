use crate::{
    db::{
        codec::write_hash_u64,
        query::{
            explain::{ExplainGroupHavingExpr, ExplainGroupHavingValueExpr, ExplainGrouping},
            fingerprint::{
                aggregate_hash::{AggregateHashShape, hash_group_aggregate_structural_fingerprint},
                hash_parts::{
                    GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
                    GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
                    GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
                    GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG,
                    GROUPING_NONE_TAG, GROUPING_PRESENT_TAG, GROUPING_STRATEGY_HASH_TAG,
                    GROUPING_STRATEGY_ORDERED_TAG, write_str, write_tag, write_u32, write_value,
                },
                projection_hash::hash_projection_structural_fingerprint,
            },
            plan::{
                AccessPlannedQuery, GroupAggregateSpec, GroupHavingExpr, GroupHavingValueExpr,
                expr::{BinaryOp, ProjectionSpec},
                grouped_plan_aggregate_family, grouped_plan_strategy,
            },
        },
    },
    value::Value,
};
use sha2::Sha256;

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

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
enum GroupHavingFingerprintSource<'a> {
    Explain(&'a ExplainGroupHavingExpr),
    Plan(&'a GroupHavingExpr),
}

/// Canonical grouped HAVING value projection shared by plan and explain hashing.
enum ProjectedGroupHavingValueExpr<'a> {
    GroupField {
        slot_index: u32,
        field: &'a str,
    },
    AggregateIndex {
        index: u32,
    },
    Literal(&'a Value),
    FunctionCall {
        function: &'a str,
        args: Vec<Self>,
    },
    Binary {
        op_tag: u8,
        left: Box<Self>,
        right: Box<Self>,
    },
}

/// Canonical grouped HAVING expression projection shared by plan and explain hashing.
enum ProjectedGroupHavingExpr<'a> {
    Compare {
        left: ProjectedGroupHavingValueExpr<'a>,
        op_tag: u8,
        right: ProjectedGroupHavingValueExpr<'a>,
    },
    And(Vec<Self>),
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
    // Explain-only hashing callsites may not have planner projection semantics.
    // In that case, preserve grouped-shape identity semantics.
    if let Some(projection) = projection {
        hash_projection_structural_fingerprint(hasher, projection);
        return;
    }

    hash_grouping_shape_v1(hasher, grouping, include_group_strategy);
}

impl<'a> ProjectedGroupHavingValueExpr<'a> {
    fn from_explain(expr: &'a ExplainGroupHavingValueExpr) -> Self {
        match expr {
            ExplainGroupHavingValueExpr::GroupField { slot_index, field } => Self::GroupField {
                slot_index: *slot_index as u32,
                field,
            },
            ExplainGroupHavingValueExpr::AggregateIndex { index } => Self::AggregateIndex {
                index: *index as u32,
            },
            ExplainGroupHavingValueExpr::Literal(value) => Self::Literal(value),
            ExplainGroupHavingValueExpr::FunctionCall { function, args } => Self::FunctionCall {
                function,
                args: args.iter().map(Self::from_explain).collect(),
            },
            ExplainGroupHavingValueExpr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag_from_explain(op),
                left: Box::new(Self::from_explain(left)),
                right: Box::new(Self::from_explain(right)),
            },
        }
    }

    fn from_plan(expr: &'a GroupHavingValueExpr) -> Self {
        match expr {
            GroupHavingValueExpr::GroupField(field_slot) => Self::GroupField {
                slot_index: field_slot.index() as u32,
                field: field_slot.field(),
            },
            GroupHavingValueExpr::AggregateIndex(index) => Self::AggregateIndex {
                index: *index as u32,
            },
            GroupHavingValueExpr::Literal(value) => Self::Literal(value),
            GroupHavingValueExpr::FunctionCall { function, args } => Self::FunctionCall {
                function: function.sql_label(),
                args: args.iter().map(Self::from_plan).collect(),
            },
            GroupHavingValueExpr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag(*op),
                left: Box::new(Self::from_plan(left)),
                right: Box::new(Self::from_plan(right)),
            },
        }
    }
}

impl<'a> ProjectedGroupHavingExpr<'a> {
    fn from_source(source: &'a GroupHavingFingerprintSource<'a>) -> Self {
        match source {
            GroupHavingFingerprintSource::Explain(expr) => Self::from_explain(expr),
            GroupHavingFingerprintSource::Plan(expr) => Self::from_plan(expr),
        }
    }

    fn from_explain(expr: &'a ExplainGroupHavingExpr) -> Self {
        match expr {
            ExplainGroupHavingExpr::Compare { left, op, right } => Self::Compare {
                left: ProjectedGroupHavingValueExpr::from_explain(left),
                op_tag: op.tag(),
                right: ProjectedGroupHavingValueExpr::from_explain(right),
            },
            ExplainGroupHavingExpr::And(children) => {
                Self::And(children.iter().map(Self::from_explain).collect())
            }
        }
    }

    fn from_plan(expr: &'a GroupHavingExpr) -> Self {
        match expr {
            GroupHavingExpr::Compare { left, op, right } => Self::Compare {
                left: ProjectedGroupHavingValueExpr::from_plan(left),
                op_tag: op.tag(),
                right: ProjectedGroupHavingValueExpr::from_plan(right),
            },
            GroupHavingExpr::And(children) => {
                Self::And(children.iter().map(Self::from_plan).collect())
            }
        }
    }
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
                let aggregate_family = grouped_plan_aggregate_family(
                    &aggregates
                        .iter()
                        .map(|aggregate| GroupAggregateSpec {
                            kind: aggregate.kind(),
                            target_field: aggregate.target_field().map(str::to_string),
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
                                aggregate.distinct(),
                            )
                        })
                        .collect(),
                    having: having
                        .as_ref()
                        .map(|having| GroupHavingFingerprintSource::Explain(having.expr())),
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
                        aggregate.target_field.as_deref(),
                        aggregate.distinct,
                    )
                })
                .collect(),
            having: grouped.effective_having_expr().map(|expr| match expr {
                std::borrow::Cow::Borrowed(expr) => GroupHavingFingerprintSource::Plan(expr),
                std::borrow::Cow::Owned(expr) => {
                    GroupHavingFingerprintSource::Plan(Box::leak(Box::new(expr)))
                }
            }),
            max_groups: grouped.group.execution.max_groups,
            max_group_bytes: grouped.group.execution.max_group_bytes,
        })
    }
}

// Hash the canonical grouped identity payload after plan/explain have already
// projected onto the shared grouped fingerprint shape.
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
                grouped.aggregates.iter().copied(),
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

fn hash_projected_group_having_value_expr(
    hasher: &mut Sha256,
    expr: &ProjectedGroupHavingValueExpr<'_>,
) {
    match expr {
        ProjectedGroupHavingValueExpr::GroupField { slot_index, field } => {
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            write_u32(hasher, *slot_index);
            write_str(hasher, field);
        }
        ProjectedGroupHavingValueExpr::AggregateIndex { index } => {
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            write_u32(hasher, *index);
        }
        ProjectedGroupHavingValueExpr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value);
        }
        ProjectedGroupHavingValueExpr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function);
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_projected_group_having_value_expr(hasher, arg);
            }
        }
        ProjectedGroupHavingValueExpr::Binary {
            op_tag,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, *op_tag);
            hash_projected_group_having_value_expr(hasher, left);
            hash_projected_group_having_value_expr(hasher, right);
        }
    }
}

fn hash_projected_group_having_expr(hasher: &mut Sha256, expr: &ProjectedGroupHavingExpr<'_>) {
    match expr {
        ProjectedGroupHavingExpr::Compare {
            left,
            op_tag,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_projected_group_having_value_expr(hasher, left);
            write_tag(hasher, *op_tag);
            hash_projected_group_having_value_expr(hasher, right);
        }
        ProjectedGroupHavingExpr::And(children) => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_projected_group_having_expr(hasher, child);
            }
        }
    }
}

const fn grouped_having_binary_op_tag(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Add => 0x01,
        BinaryOp::Sub => 0x02,
        BinaryOp::Mul => 0x03,
        BinaryOp::Div => 0x04,
        #[cfg(test)]
        BinaryOp::And => 0x05,
        #[cfg(test)]
        BinaryOp::Eq => 0x06,
    }
}

fn grouped_having_binary_op_tag_from_explain(op: &str) -> u8 {
    match op {
        "+" => 0x01,
        "-" => 0x02,
        "*" => 0x03,
        "/" => 0x04,
        "and" => 0x05,
        "=" => 0x06,
        other => panic!("unsupported explain grouped HAVING binary op: {other}"),
    }
}

fn hash_group_having_projection(
    hasher: &mut Sha256,
    expr: Option<&GroupHavingFingerprintSource<'_>>,
) {
    let Some(expr) = expr else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return;
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    let projected = ProjectedGroupHavingExpr::from_source(expr);

    hash_projected_group_having_expr(hasher, &projected);
}
