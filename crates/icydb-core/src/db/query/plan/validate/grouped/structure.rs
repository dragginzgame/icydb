use crate::{
    db::{
        predicate::SchemaInfo,
        query::plan::{
            GroupAggregateSpec, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
            expr::ProjectionSpec,
            validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
            validate::{GroupPlanError, PlanError},
        },
    },
    model::entity::EntityModel,
};
use std::collections::BTreeSet;

// Validate grouped structural invariants before policy/cursor gates.
pub(in crate::db::query::plan::validate) fn validate_group_structure(
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
