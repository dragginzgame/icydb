//! Module: query::plan::validate::grouped::structure
//! Responsibility: grouped structural validation before grouped policy gates.
//! Does not own: grouped policy admissibility rules or runtime grouped execution checks.
//! Boundary: validates grouped spec and HAVING symbol structure at planner boundary.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        query::plan::{
            GroupAggregateSpec, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
            expr::ProjectionSpec,
            validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
            validate::{GroupPlanError, PlanError},
        },
        schema::SchemaInfo,
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
                validate_having_group_field_reference(group, field_slot, index)?;
            }
            GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                validate_having_aggregate_index(group, *aggregate_index, index)?;
            }
        }
    }

    Ok(())
}

// Validate that HAVING group-field references are a subset of declared GROUP BY keys.
fn validate_having_group_field_reference(
    group: &GroupSpec,
    field_slot: &crate::db::query::plan::FieldSlot,
    index: usize,
) -> Result<(), PlanError> {
    group
        .group_fields
        .iter()
        .any(|group_field| group_field.index() == field_slot.index())
        .then_some(())
        .ok_or_else(|| {
            PlanError::from(GroupPlanError::HavingNonGroupFieldReference {
                index,
                field: field_slot.field().to_string(),
            })
        })
}

// Validate that HAVING aggregate symbols point at declared aggregate slots.
fn validate_having_aggregate_index(
    group: &GroupSpec,
    aggregate_index: usize,
    index: usize,
) -> Result<(), PlanError> {
    (aggregate_index < group.aggregates.len())
        .then_some(())
        .ok_or_else(|| {
            PlanError::from(GroupPlanError::HavingAggregateIndexOutOfBounds {
                index,
                aggregate_index,
                aggregate_count: group.aggregates.len(),
            })
        })
}

// Validate grouped structural declarations against model/schema shape.
fn validate_group_spec_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    match (
        group.group_fields.is_empty(),
        group.aggregates.iter().any(GroupAggregateSpec::distinct),
    ) {
        (true, true) => return Ok(()),
        (true, false) => return Err(PlanError::from(GroupPlanError::EmptyGroupFields)),
        (false, _) => {}
    }
    (!group.aggregates.is_empty())
        .then_some(())
        .ok_or_else(|| PlanError::from(GroupPlanError::EmptyAggregates))?;

    let mut seen_group_slots = BTreeSet::<usize>::new();
    for field_slot in &group.group_fields {
        model.fields.get(field_slot.index()).ok_or_else(|| {
            PlanError::from(GroupPlanError::UnknownGroupField {
                field: field_slot.field().to_string(),
            })
        })?;
        seen_group_slots
            .insert(field_slot.index())
            .then_some(())
            .ok_or_else(|| {
                PlanError::from(GroupPlanError::DuplicateGroupField {
                    field: field_slot.field().to_string(),
                })
            })?;
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        schema.field(target_field).ok_or_else(|| {
            PlanError::from(GroupPlanError::UnknownAggregateTargetField {
                index,
                field: target_field.clone(),
            })
        })?;
    }

    Ok(())
}
