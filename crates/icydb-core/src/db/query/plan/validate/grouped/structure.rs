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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            predicate::{CompareOp, SchemaInfo},
            query::plan::{
                AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
                GroupHavingSymbol, GroupSpec, GroupedExecutionConfig,
                expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
                validate::{ExprPlanError, PlanUserError},
            },
        },
        model::{entity::EntityModel, field::FieldKind, index::IndexModel},
        traits::EntitySchema,
        types::Ulid,
        value::Value,
    };

    const EMPTY_INDEX_FIELDS: [&str; 0] = [];
    const EMPTY_INDEX: IndexModel = IndexModel::new(
        "query::plan::validate::grouped::structure::idx_empty",
        "query::plan::validate::grouped::structure::Store",
        &EMPTY_INDEX_FIELDS,
        false,
    );

    crate::test_entity! {
        ident = GroupStructureValidateEntity,
        id = Ulid,
        entity_name = "GroupStructureValidateEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("team", FieldKind::Text),
            ("region", FieldKind::Text),
            ("score", FieldKind::Uint),
        ],
        indexes = [&EMPTY_INDEX],
    }

    fn model() -> &'static EntityModel {
        <GroupStructureValidateEntity as EntitySchema>::MODEL
    }

    fn schema() -> SchemaInfo {
        SchemaInfo::from_entity_model(model()).expect("schema should validate")
    }

    fn grouped_spec() -> GroupSpec {
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(model(), "team").expect("field slot should resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
        }
    }

    #[test]
    fn grouped_structure_rejects_projection_expr_referencing_non_group_field() {
        let group = grouped_spec();
        let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("score")),
            alias: None,
        }]);

        let err = validate_group_structure(&schema(), model(), &group, &projection, None)
            .expect_err("projection references outside GROUP BY keys must fail in planner");

        assert!(matches!(
            err,
            PlanError::User(inner)
                if matches!(
                    inner.as_ref(),
                    PlanUserError::Expr(expr)
                        if matches!(
                            expr.as_ref(),
                            ExprPlanError::GroupedProjectionReferencesNonGroupField { index: 0 }
                        )
                )
        ));
    }

    #[test]
    fn grouped_structure_rejects_having_group_field_symbol_outside_group_keys() {
        let group = grouped_spec();
        let projection = ProjectionSpec::default();
        let having = GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::GroupField(
                    FieldSlot::resolve(model(), "region").expect("field slot should resolve"),
                ),
                op: CompareOp::Eq,
                value: Value::Text("eu".to_string()),
            }],
        };

        let err = validate_group_structure(&schema(), model(), &group, &projection, Some(&having))
            .expect_err("HAVING group-field symbols outside GROUP BY keys must fail in planner");

        assert!(matches!(
            err,
            PlanError::User(inner)
                if matches!(
                    inner.as_ref(),
                    PlanUserError::Group(group)
                        if matches!(
                            group.as_ref(),
                            GroupPlanError::HavingNonGroupFieldReference { index: 0, .. }
                        )
                )
        ));
    }

    #[test]
    fn grouped_structure_rejects_having_aggregate_index_out_of_bounds() {
        let group = grouped_spec();
        let projection = ProjectionSpec::default();
        let having = GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(1),
                op: CompareOp::Gt,
                value: Value::Uint(5),
            }],
        };

        let err = validate_group_structure(&schema(), model(), &group, &projection, Some(&having))
            .expect_err("HAVING aggregate symbols outside declared aggregate range must fail");

        assert!(matches!(
            err,
            PlanError::User(inner)
                if matches!(
                    inner.as_ref(),
                    PlanUserError::Group(group)
                        if matches!(
                            group.as_ref(),
                            GroupPlanError::HavingAggregateIndexOutOfBounds {
                                index: 0,
                                aggregate_index: 1,
                                aggregate_count: 1,
                            }
                        )
                )
        ));
    }
}
