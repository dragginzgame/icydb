use crate::{
    db::{
        predicate::SchemaInfo,
        query::plan::{
            FieldSlot, GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
            GroupHavingSpec, GroupHavingSymbol, GroupSpec, OrderSpec, ScalarPlan,
            expr::{ProjectionField, ProjectionSpec, expr_references_only_fields, infer_expr_type},
            grouped_distinct_admissibility, grouped_having_compare_op_supported,
            resolve_global_distinct_field_aggregate,
            validate::{ExprPlanError, GroupPlanError, PlanError},
        },
    },
    model::entity::EntityModel,
};
use std::collections::{BTreeSet, HashSet};

// Validate grouped structural invariants before policy/cursor gates.
pub(super) fn validate_group_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
    projection: &ProjectionSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() && having.is_some() {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
        ));
    }

    validate_group_spec_structure(schema, model, group)?;
    validate_group_projection_expr_compatibility(group, projection)?;
    validate_grouped_having_structure(group, having)?;

    Ok(())
}

// Validate grouped policy gates independent from structural shape checks.
pub(super) fn validate_group_policy(
    schema: &SchemaInfo,
    logical: &ScalarPlan,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    validate_grouped_distinct_policy(logical, having.is_some())?;
    validate_grouped_having_policy(having)?;
    validate_group_spec_policy(schema, group, having)?;

    Ok(())
}

// Validate grouped cursor-order constraints in one dedicated gate.
pub(super) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy:
    // grouped ORDER BY requires LIMIT and must align with grouped-key prefix.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };
    if logical.page.as_ref().and_then(|page| page.limit).is_none() {
        return Err(PlanError::from(GroupPlanError::OrderRequiresLimit));
    }
    if order_prefix_aligned_with_group_fields(order, group.group_fields.as_slice()) {
        return Ok(());
    }

    Err(PlanError::from(
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys,
    ))
}

// Validate grouped DISTINCT policy gates for grouped v1 hardening.
fn validate_grouped_distinct_policy(
    logical: &ScalarPlan,
    has_having: bool,
) -> Result<(), PlanError> {
    match grouped_distinct_admissibility(logical.distinct, has_having) {
        GroupDistinctAdmissibility::Allowed => Ok(()),
        GroupDistinctAdmissibility::Disallowed(reason) => Err(PlanError::from(
            group_plan_error_from_distinct_policy_reason(reason, None),
        )),
    }
}

// Validate grouped HAVING structural symbol/reference compatibility.
fn validate_grouped_having_structure(
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    let Some(having) = having else {
        return Ok(());
    };

    for (index, clause) in having.clauses().iter().enumerate() {
        match clause.symbol() {
            GroupHavingSymbol::GroupField(field_slot) => {
                if !group
                    .group_fields
                    .iter()
                    .any(|group_field| group_field.index() == field_slot.index())
                {
                    return Err(PlanError::from(
                        GroupPlanError::HavingNonGroupFieldReference {
                            index,
                            field: field_slot.field().to_string(),
                        },
                    ));
                }
            }
            GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                if *aggregate_index >= group.aggregates.len() {
                    return Err(PlanError::from(
                        GroupPlanError::HavingAggregateIndexOutOfBounds {
                            index,
                            aggregate_index: *aggregate_index,
                            aggregate_count: group.aggregates.len(),
                        },
                    ));
                }
            }
        }
    }

    Ok(())
}

// Validate grouped HAVING policy gates and operator support.
fn validate_grouped_having_policy(having: Option<&GroupHavingSpec>) -> Result<(), PlanError> {
    let Some(having) = having else {
        return Ok(());
    };

    for (index, clause) in having.clauses().iter().enumerate() {
        if !grouped_having_compare_op_supported(clause.op()) {
            return Err(PlanError::from(
                GroupPlanError::HavingUnsupportedCompareOp {
                    index,
                    op: format!("{:?}", clause.op()),
                },
            ));
        }
    }

    Ok(())
}

// Return true when ORDER BY starts with GROUP BY key fields in declaration order.
fn order_prefix_aligned_with_group_fields(order: &OrderSpec, group_fields: &[FieldSlot]) -> bool {
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

// Validate grouped structural declarations against model/schema shape.
fn validate_group_spec_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        if group.aggregates.iter().any(GroupAggregateSpec::distinct) {
            return Ok(());
        }

        return Err(PlanError::from(GroupPlanError::EmptyGroupFields));
    }
    if group.aggregates.is_empty() {
        return Err(PlanError::from(GroupPlanError::EmptyAggregates));
    }

    let mut seen_group_slots = BTreeSet::<usize>::new();
    for field_slot in &group.group_fields {
        if model.fields.get(field_slot.index()).is_none() {
            return Err(PlanError::from(GroupPlanError::UnknownGroupField {
                field: field_slot.field().to_string(),
            }));
        }
        if !seen_group_slots.insert(field_slot.index()) {
            return Err(PlanError::from(GroupPlanError::DuplicateGroupField {
                field: field_slot.field().to_string(),
            }));
        }
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        if schema.field(target_field).is_none() {
            return Err(PlanError::from(
                GroupPlanError::UnknownAggregateTargetField {
                    index,
                    field: target_field.clone(),
                },
            ));
        }
    }

    Ok(())
}

// Validate grouped execution policy over a structurally valid grouped spec.
fn validate_group_spec_policy(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        validate_global_distinct_aggregate_without_group_keys(schema, group, having)?;
        return Ok(());
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        if aggregate.distinct() && !aggregate.kind().supports_grouped_distinct_v1() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateKindUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                },
            ));
        }

        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        if aggregate.distinct() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateFieldTargetUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                    field: target_field.clone(),
                },
            ));
        }
        return Err(PlanError::from(
            GroupPlanError::FieldTargetAggregatesUnsupported {
                index,
                kind: format!("{:?}", aggregate.kind()),
                field: target_field.clone(),
            },
        ));
    }

    Ok(())
}

// Validate GROUP BY expression compatibility over canonical projection semantics.
fn validate_group_projection_expr_compatibility(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        return Ok(());
    }

    let grouped_fields = group
        .group_fields
        .iter()
        .map(FieldSlot::field)
        .collect::<HashSet<_>>();

    for (index, field) in projection.fields().enumerate() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                if !expr_references_only_fields(expr, &grouped_fields) {
                    return Err(PlanError::from(
                        ExprPlanError::GroupedProjectionReferencesNonGroupField { index },
                    ));
                }
            }
        }
    }

    Ok(())
}

// Validate deterministic planner expression typing over one canonical projection shape.
pub(super) fn validate_projection_expr_types(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                infer_expr_type(expr, schema)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub(in crate::db::query) fn validate_group_projection_expr_compatibility_for_test(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    validate_group_projection_expr_compatibility(group, projection)
}

// Validate the restricted global DISTINCT aggregate shape (`GROUP BY` omitted).
fn validate_global_distinct_aggregate_without_group_keys(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    let aggregate = match resolve_global_distinct_field_aggregate(
        group.group_fields.as_slice(),
        group.aggregates.as_slice(),
        having,
    ) {
        Ok(Some(aggregate)) => aggregate,
        Ok(None) => {
            return Err(PlanError::from(
                GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
            ));
        }
        Err(reason) => {
            let aggregate = group.aggregates.first();
            return Err(PlanError::from(
                group_plan_error_from_distinct_policy_reason(reason, aggregate),
            ));
        }
    };

    let target_field = aggregate.target_field();
    let Some(field_type) = schema.field(target_field) else {
        return Err(PlanError::from(
            GroupPlanError::UnknownAggregateTargetField {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    };
    if aggregate.kind().is_sum() && !field_type.supports_numeric_coercion() {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctSumTargetNotNumeric {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    }

    Ok(())
}

// Map one grouped DISTINCT policy reason to planner-visible grouped plan errors.
fn group_plan_error_from_distinct_policy_reason(
    reason: GroupDistinctPolicyReason,
    aggregate: Option<&GroupAggregateSpec>,
) -> GroupPlanError {
    match reason {
        GroupDistinctPolicyReason::DistinctHavingUnsupported => {
            GroupPlanError::DistinctHavingUnsupported
        }
        GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired => {
            GroupPlanError::DistinctAdjacencyEligibilityRequired
        }
        GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        | GroupDistinctPolicyReason::GlobalDistinctRequiresSingleAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresDistinctAggregateTerminal => {
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        }
        GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind => {
            let kind = aggregate.map_or_else(
                || "Unknown".to_string(),
                |aggregate| format!("{:?}", aggregate.kind()),
            );
            GroupPlanError::DistinctAggregateKindUnsupported { index: 0, kind }
        }
    }
}
