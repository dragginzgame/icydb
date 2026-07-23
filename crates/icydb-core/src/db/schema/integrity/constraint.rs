//! Structural constraint registry closure validation.

use crate::db::schema::{
    AcceptedConstraintCatalog, AcceptedConstraintKind, ConstraintActivationKind, ConstraintOrigin,
    FieldId, PersistedFieldSnapshot, PersistedIndexSnapshot, PersistedRelationEdgeSnapshot,
    constraint::accepted_constraint_name_is_valid,
};

// Prove exact one-to-one closure without repeating the structural owners'
// execution semantics inside the constraint registry.
pub(in crate::db::schema) fn schema_snapshot_constraint_integrity_detail(
    primary_key_field_ids: &[FieldId],
    fields: &[PersistedFieldSnapshot],
    indexes: &[PersistedIndexSnapshot],
    relations: &[PersistedRelationEdgeSnapshot],
    candidate_indexes: &[PersistedIndexSnapshot],
    candidate_relations: &[PersistedRelationEdgeSnapshot],
    catalog: &AcceptedConstraintCatalog,
) -> Option<()> {
    let constraints = catalog.constraints();
    if constraint_headers_are_invalid(catalog)
        || primary_key_constraint_is_invalid(primary_key_field_ids, constraints)
        || fields
            .iter()
            .any(|field| field_constraint_is_invalid(field, constraints))
        || indexes
            .iter()
            .any(|index| index_constraint_is_invalid(index, constraints))
        || relations
            .iter()
            .any(|relation| relation_constraint_is_invalid(relation, constraints))
        || checks_are_invalid(fields, constraints)
        || activations_are_invalid(
            fields,
            indexes,
            relations,
            candidate_indexes,
            candidate_relations,
            catalog,
        )
        || candidate_indexes.iter().any(|candidate| {
            !catalog.activations().iter().any(|activation| {
                matches!(
                    activation.kind(),
                    ConstraintActivationKind::Unique { index_id }
                        if *index_id == candidate.schema_id()
                )
            })
        })
        || candidate_relations.iter().any(|candidate| {
            !catalog.activations().iter().any(|activation| {
                matches!(
                    activation.kind(),
                    ConstraintActivationKind::Relation { relation_id }
                        if *relation_id == candidate.id()
                )
            })
        })
    {
        return Some(());
    }

    let expected_count = 1usize
        .checked_add(fields.iter().filter(|field| !field.nullable()).count())?
        .checked_add(indexes.iter().filter(|index| index.unique()).count())?
        .checked_add(relations.len())?
        .checked_add(
            constraints
                .iter()
                .filter(|constraint| {
                    matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                })
                .count(),
        )?;
    (constraints.len() != expected_count).then_some(())
}

fn constraint_headers_are_invalid(catalog: &AcceptedConstraintCatalog) -> bool {
    let constraints = catalog.constraints();
    let mut prior_id = 0;
    for (position, constraint) in constraints.iter().enumerate() {
        let id = constraint.id().get();
        if id <= prior_id
            || id > catalog.allocator().high_water()
            || !accepted_constraint_name_is_valid(constraint.name())
            || constraints[..position]
                .iter()
                .any(|other| other.name() == constraint.name())
        {
            return true;
        }
        prior_id = id;
    }
    let activations = catalog.activations();
    let mut prior_activation_id = 0;
    for (position, activation) in activations.iter().enumerate() {
        let id = activation.id().get();
        if id <= prior_activation_id
            || id > catalog.allocator().high_water()
            || !accepted_constraint_name_is_valid(activation.name())
            || constraints.iter().any(|constraint| {
                constraint.id() == activation.id() || constraint.name() == activation.name()
            })
            || activations[..position]
                .iter()
                .any(|other| other.id() == activation.id() || other.name() == activation.name())
            || !activation.has_valid_fingerprint()
        {
            return true;
        }
        prior_activation_id = id;
    }
    false
}

fn primary_key_constraint_is_invalid(
    primary_key_field_ids: &[FieldId],
    constraints: &[crate::db::schema::AcceptedConstraintSnapshot],
) -> bool {
    let mut primary_keys = constraints
        .iter()
        .filter(|constraint| matches!(constraint.kind(), AcceptedConstraintKind::PrimaryKey));
    let first = primary_keys.next();
    primary_key_field_ids.is_empty()
        || first.is_none_or(|constraint| constraint.origin() != ConstraintOrigin::Generated)
        || primary_keys.next().is_some()
}

fn field_constraint_is_invalid(
    field: &PersistedFieldSnapshot,
    constraints: &[crate::db::schema::AcceptedConstraintSnapshot],
) -> bool {
    let mut matches = constraints.iter().filter(|constraint| {
        matches!(
            constraint.kind(),
            AcceptedConstraintKind::NotNull { field_id } if *field_id == field.id()
        )
    });
    let first = matches.next();
    if field.nullable() {
        return first.is_some();
    }
    first.is_none_or(|constraint| {
        constraint.origin() != ConstraintOrigin::from_field_origin(field.origin())
    }) || matches.next().is_some()
}

fn index_constraint_is_invalid(
    index: &PersistedIndexSnapshot,
    constraints: &[crate::db::schema::AcceptedConstraintSnapshot],
) -> bool {
    let mut matches = constraints.iter().filter(|constraint| {
        matches!(
            constraint.kind(),
            AcceptedConstraintKind::Unique { index_id } if *index_id == index.schema_id()
        )
    });
    let first = matches.next();
    if !index.unique() {
        return first.is_some();
    }
    first.is_none_or(|constraint| {
        constraint.origin() != ConstraintOrigin::from_index_origin(index.origin())
    }) || matches.next().is_some()
}

fn relation_constraint_is_invalid(
    relation: &PersistedRelationEdgeSnapshot,
    constraints: &[crate::db::schema::AcceptedConstraintSnapshot],
) -> bool {
    let mut matches = constraints.iter().filter(|constraint| {
        matches!(
            constraint.kind(),
            AcceptedConstraintKind::Relation { relation_id } if *relation_id == relation.id()
        )
    });
    let first = matches.next();
    first.is_none_or(|constraint| constraint.origin() != ConstraintOrigin::Generated)
        || matches.next().is_some()
}

fn checks_are_invalid(
    fields: &[PersistedFieldSnapshot],
    constraints: &[crate::db::schema::AcceptedConstraintSnapshot],
) -> bool {
    constraints.iter().any(|constraint| {
        let AcceptedConstraintKind::Check { expression } = constraint.kind() else {
            return false;
        };
        expression.validate_snapshot_local(fields).is_err()
    })
}

fn activations_are_invalid(
    fields: &[PersistedFieldSnapshot],
    indexes: &[PersistedIndexSnapshot],
    relations: &[PersistedRelationEdgeSnapshot],
    candidate_indexes: &[PersistedIndexSnapshot],
    candidate_relations: &[PersistedRelationEdgeSnapshot],
    catalog: &AcceptedConstraintCatalog,
) -> bool {
    let constraints = catalog.constraints();
    catalog
        .activations()
        .iter()
        .any(|activation| match activation.kind() {
            ConstraintActivationKind::NotNull { field_id } => {
                fields
                    .iter()
                    .find(|field| field.id() == *field_id)
                    .is_none_or(|field| !field.nullable())
                    || constraints.iter().any(|constraint| {
                        matches!(
                            constraint.kind(),
                            AcceptedConstraintKind::NotNull {
                                field_id: accepted
                            } if accepted == field_id
                        )
                    })
            }
            ConstraintActivationKind::Unique { index_id } => {
                indexes.iter().any(|index| index.schema_id() == *index_id)
                    || candidate_indexes
                        .iter()
                        .filter(|index| index.schema_id() == *index_id)
                        .count()
                        != 1
                    || candidate_indexes
                        .iter()
                        .find(|index| index.schema_id() == *index_id)
                        .is_none_or(|index| {
                            !index.unique()
                                || index.name() != activation.name()
                                || index.physical_generation() != activation.activation_epoch()
                                || ConstraintOrigin::from_index_origin(index.origin())
                                    != activation.origin()
                        })
                    || constraints.iter().any(|constraint| {
                        matches!(
                            constraint.kind(),
                            AcceptedConstraintKind::Unique {
                                index_id: accepted
                            } if accepted == index_id
                        )
                    })
                    || catalog.activations().iter().any(|other| {
                        other.id() != activation.id()
                            && matches!(
                                other.kind(),
                                ConstraintActivationKind::Unique { index_id: other_id }
                                    if other_id == index_id
                            )
                    })
            }
            ConstraintActivationKind::Relation { relation_id } => {
                let activation_generation = activation.activation_epoch();
                relations
                    .iter()
                    .any(|relation| relation.id() == *relation_id)
                    || candidate_relations
                        .iter()
                        .filter(|relation| relation.id() == *relation_id)
                        .count()
                        != 1
                    || candidate_relations
                        .iter()
                        .find(|relation| relation.id() == *relation_id)
                        .is_none_or(|relation| {
                            relation.name() != activation.name()
                                || relation.physical_generation() != activation_generation
                                || activation.origin() != ConstraintOrigin::Generated
                        })
                    || constraints.iter().any(|constraint| {
                        matches!(
                            constraint.kind(),
                            AcceptedConstraintKind::Relation {
                                relation_id: accepted
                            } if accepted == relation_id
                        )
                    })
                    || catalog.activations().iter().any(|other| {
                        other.id() != activation.id()
                            && matches!(
                                other.kind(),
                                ConstraintActivationKind::Relation {
                                    relation_id: other_id
                                } if other_id == relation_id
                            )
                    })
            }
            ConstraintActivationKind::Check { expression } => {
                expression.validate_snapshot_local(fields).is_err()
            }
        })
}
