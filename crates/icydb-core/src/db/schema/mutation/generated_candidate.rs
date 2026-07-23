//! Generated-proposal lowering into catalog-native accepted candidates.

use crate::db::schema::{
    AcceptedConstraintKind, AcceptedSchemaFingerprint, ConstraintActivationKind, ConstraintOrigin,
    FieldId, PersistedFieldSnapshot, PersistedIndexSnapshot, PersistedRelationEdgeSnapshot,
    PersistedSchemaSnapshot, RelationId, SchemaHistoricalFill, SchemaIndexId, SchemaRowLayout,
};

/// Accepted-root identity required to publish one generated activation safely.
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct GeneratedConstraintActivationContext {
    base_schema_fingerprint: AcceptedSchemaFingerprint,
    activation_epoch: u64,
}

impl GeneratedConstraintActivationContext {
    /// Bind generated activation semantics to the current root and next revision.
    #[must_use]
    pub(in crate::db::schema) const fn new(
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Self {
        Self {
            base_schema_fingerprint,
            activation_epoch,
        }
    }
}

/// Failure to lower a generated proposal into an accepted temporal candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum GeneratedAcceptedCandidateError {
    /// Structural constraint identity or name allocation failed.
    ConstraintCatalog,
    /// The next physical row-layout identity cannot be represented.
    RowLayoutVersionExhausted,
    /// A live generated activation no longer matches the generated proposal.
    StaleConstraintActivation,
}

/// Derive accepted temporal facts for one generated proposal.
///
/// Generated models propose current field intent, but never own persisted row
/// history. Existing-field default changes preserve the accepted row layout
/// and frozen historical fill. Exact append-only additions preserve the
/// accepted prefix and freeze one new physical layout for all appended fields.
/// Unsupported shapes remain untouched for transition classification.
pub(in crate::db::schema) fn derive_generated_accepted_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: Option<GeneratedConstraintActivationContext>,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if let Some(activation) = activation
        && let Some(candidate) =
            derive_generated_activation_candidate(accepted, generated, activation)?
    {
        return Ok(Some(candidate));
    }
    if !accepted.constraint_activations().is_empty() {
        return Ok(None);
    }
    if let Some(candidate) = derive_generated_default_candidate(accepted, generated) {
        return Ok(Some(candidate));
    }

    if generated.fields().len() <= accepted.fields().len()
        || generated.row_layout().field_to_slot().len()
            <= accepted.row_layout().field_to_slot().len()
        || accepted.fields().len() != accepted.row_layout().field_to_slot().len()
        || generated.fields().len() != generated.row_layout().field_to_slot().len()
        || !generated_structural_owners_match(accepted, generated)
    {
        return Ok(None);
    }

    if !accepted
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(generated.row_layout().field_to_slot())
        .all(|(accepted_entry, generated_entry)| accepted_entry == generated_entry)
    {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(generated.fields().len());
    for (accepted_field, generated_field) in accepted.fields().iter().zip(generated.fields()) {
        let candidate = field_with_temporal_contract(
            generated_field,
            accepted_field.introduced_in_layout(),
            accepted_field.historical_fill().clone(),
        );
        if &candidate != accepted_field {
            return Ok(None);
        }
        fields.push(candidate);
    }

    let current_layout = accepted
        .row_layout()
        .current_version()
        .checked_next()
        .ok_or(GeneratedAcceptedCandidateError::RowLayoutVersionExhausted)?;
    for generated_field in &generated.fields()[accepted.fields().len()..] {
        let historical_fill = match generated_field.insert_default().slot_payload() {
            Some(payload) => SchemaHistoricalFill::SlotPayload(payload.to_vec()),
            None if generated_field.nullable() => SchemaHistoricalFill::Null,
            None => return Ok(None),
        };
        fields.push(field_with_temporal_contract(
            generated_field,
            current_layout,
            historical_fill,
        ));
    }
    let constraint_catalog = fields[accepted.fields().len()..]
        .iter()
        .try_fold(accepted.constraint_catalog().clone(), |catalog, field| {
            catalog.with_added_not_null(field)
        })
        .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)?;

    Ok(Some(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            generated.version(),
            generated.entity_path().to_string(),
            generated.entity_name().to_string(),
            generated.primary_key_field_ids().to_vec(),
            SchemaRowLayout::new(
                current_layout,
                accepted.row_layout().history_floor(),
                generated.row_layout().field_to_slot().to_vec(),
            ),
            fields,
            accepted.indexes().to_vec(),
        )
        .with_constraint_catalog(constraint_catalog)
        .with_relations(accepted.relations().to_vec()),
    ))
}

fn derive_generated_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if let Some(candidate) =
        derive_generated_additive_relation_activation_candidate(accepted, generated, activation)?
    {
        return Ok(Some(candidate));
    }
    if let Some(candidate) =
        derive_generated_relation_activation_candidate(accepted, generated, activation)?
    {
        return Ok(Some(candidate));
    }
    if let Some(candidate) =
        derive_generated_not_null_activation_candidate(accepted, generated, activation)?
    {
        return Ok(Some(candidate));
    }
    if let Some(candidate) =
        derive_generated_unique_activation_candidate(accepted, generated, activation)?
    {
        return Ok(Some(candidate));
    }
    derive_generated_check_activation_candidate(accepted, generated, activation)
}

fn derive_generated_additive_relation_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if generated.fields().len() <= accepted.fields().len()
        || accepted.entity_path() != generated.entity_path()
        || accepted.entity_name() != generated.entity_name()
        || accepted.primary_key_field_ids() != generated.primary_key_field_ids()
        || accepted.fields().len() != accepted.row_layout().field_to_slot().len()
        || generated.fields().len() != generated.row_layout().field_to_slot().len()
        || !accepted
            .row_layout()
            .field_to_slot()
            .iter()
            .zip(generated.row_layout().field_to_slot())
            .all(|(accepted_entry, generated_entry)| accepted_entry == generated_entry)
        || !generated_index_contracts_match(accepted, generated)
        || !generated_check_contracts_match(accepted, generated)
        || !accepted.constraint_activations().is_empty()
    {
        return Ok(None);
    }

    let additions = added_generated_relations(accepted, generated)?;
    let [proposed] = additions.as_slice() else {
        return Ok(None);
    };
    let first_added_field = FieldId::from_initial_slot(accepted.fields().len());
    if !proposed
        .local_field_ids()
        .iter()
        .any(|field_id| *field_id >= first_added_field)
    {
        return Ok(None);
    }

    let current_layout = accepted
        .row_layout()
        .current_version()
        .checked_next()
        .ok_or(GeneratedAcceptedCandidateError::RowLayoutVersionExhausted)?;
    let mut fields = Vec::with_capacity(generated.fields().len());
    for (accepted_field, generated_field) in accepted.fields().iter().zip(generated.fields()) {
        let candidate = field_with_temporal_contract(
            generated_field,
            accepted_field.introduced_in_layout(),
            accepted_field.historical_fill().clone(),
        );
        if &candidate != accepted_field {
            return Ok(None);
        }
        fields.push(candidate);
    }
    for generated_field in &generated.fields()[accepted.fields().len()..] {
        let historical_fill = match generated_field.insert_default().slot_payload() {
            Some(payload) => SchemaHistoricalFill::SlotPayload(payload.to_vec()),
            None if generated_field.nullable() => SchemaHistoricalFill::Null,
            None => return Ok(None),
        };
        fields.push(field_with_temporal_contract(
            generated_field,
            current_layout,
            historical_fill,
        ));
    }
    let constraint_catalog = fields[accepted.fields().len()..]
        .iter()
        .try_fold(accepted.constraint_catalog().clone(), |catalog, field| {
            catalog.with_added_not_null(field)
        })
        .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)?;
    let base = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        generated.version(),
        accepted.entity_path().to_string(),
        accepted.entity_name().to_string(),
        accepted.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            current_layout,
            accepted.row_layout().history_floor(),
            generated.row_layout().field_to_slot().to_vec(),
        ),
        fields,
        accepted.indexes().to_vec(),
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relations(accepted.relations().to_vec());
    with_generated_relation_activation(base, accepted, proposed, activation).map(Some)
}

fn derive_generated_relation_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if accepted.entity_path() != generated.entity_path()
        || accepted.entity_name() != generated.entity_name()
        || accepted.primary_key_field_ids() != generated.primary_key_field_ids()
        || !generated_fields_match_accepted_temporal_contract(accepted, generated)
        || !generated_index_contracts_match(accepted, generated)
        || !generated_check_contracts_match(accepted, generated)
    {
        return Ok(None);
    }

    let additions = added_generated_relations(accepted, generated)?;
    let [proposed] = additions.as_slice() else {
        return Ok(None);
    };

    if let Some(existing) = accepted.candidate_relations().first() {
        let matching = accepted.constraint_activations().iter().any(|pending| {
            pending.origin() == ConstraintOrigin::Generated
                && matches!(
                    pending.kind(),
                    ConstraintActivationKind::Relation { relation_id }
                        if *relation_id == existing.id()
                )
                && existing.physical_generation() == pending.activation_epoch()
                && relation_semantics_match(existing, proposed)
        });
        return Ok(matching.then(|| accepted.clone()));
    }
    if !accepted.constraint_activations().is_empty() {
        return Ok(None);
    }

    let base = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        generated.version(),
        accepted.entity_path().to_string(),
        accepted.entity_name().to_string(),
        accepted.primary_key_field_ids().to_vec(),
        accepted.row_layout().clone(),
        accepted.fields().to_vec(),
        accepted.indexes().to_vec(),
    )
    .with_constraint_catalog(accepted.constraint_catalog().clone())
    .with_relations(accepted.relations().to_vec());
    with_generated_relation_activation(base, accepted, proposed, activation).map(Some)
}

fn with_generated_relation_activation(
    base: PersistedSchemaSnapshot,
    accepted: &PersistedSchemaSnapshot,
    proposed: &PersistedRelationEdgeSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<PersistedSchemaSnapshot, GeneratedAcceptedCandidateError> {
    let highest_relation_id = accepted
        .relations()
        .iter()
        .chain(accepted.candidate_relations())
        .map(|relation| relation.id().get())
        .max()
        .unwrap_or(0);
    let relation_id = highest_relation_id
        .checked_add(1)
        .and_then(RelationId::new)
        .ok_or(GeneratedAcceptedCandidateError::ConstraintCatalog)?;
    let candidate = PersistedRelationEdgeSnapshot::new(
        relation_id,
        proposed.name().to_string(),
        proposed.target_path().to_string(),
        proposed.local_field_ids().to_vec(),
    )
    .clone_with_physical_generation(activation.activation_epoch);
    base.with_added_relation_activation(
        candidate,
        activation.base_schema_fingerprint,
        activation.activation_epoch,
    )
    .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)
}

fn added_generated_relations<'a>(
    accepted: &PersistedSchemaSnapshot,
    generated: &'a PersistedSchemaSnapshot,
) -> Result<Vec<&'a PersistedRelationEdgeSnapshot>, GeneratedAcceptedCandidateError> {
    if accepted.relations().iter().any(|accepted_relation| {
        !generated.relations().iter().any(|generated_relation| {
            relation_semantics_match(accepted_relation, generated_relation)
        })
    }) {
        return Err(GeneratedAcceptedCandidateError::ConstraintCatalog);
    }
    Ok(generated
        .relations()
        .iter()
        .filter(|generated_relation| {
            !accepted.relations().iter().any(|accepted_relation| {
                relation_semantics_match(accepted_relation, generated_relation)
            })
        })
        .collect())
}

fn generated_index_contracts_match(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> bool {
    generated
        .indexes()
        .iter()
        .all(|index| accepted.indexes().contains(index))
        && accepted
            .indexes()
            .iter()
            .filter(|index| index.generated())
            .all(|index| generated.indexes().contains(index))
}

fn generated_fields_match_accepted_temporal_contract(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> bool {
    accepted.fields().len() == generated.fields().len()
        && accepted.row_layout().field_to_slot() == generated.row_layout().field_to_slot()
        && accepted.fields().iter().zip(generated.fields()).all(
            |(accepted_field, generated_field)| {
                field_with_temporal_contract(
                    generated_field,
                    accepted_field.introduced_in_layout(),
                    accepted_field.historical_fill().clone(),
                ) == *accepted_field
            },
        )
}

fn relation_semantics_match(
    left: &PersistedRelationEdgeSnapshot,
    right: &PersistedRelationEdgeSnapshot,
) -> bool {
    left.name() == right.name()
        && left.target_path() == right.target_path()
        && left.local_field_ids() == right.local_field_ids()
}

fn derive_generated_unique_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if accepted.entity_path() != generated.entity_path()
        || accepted.entity_name() != generated.entity_name()
        || accepted.primary_key_field_ids() != generated.primary_key_field_ids()
        || accepted.row_layout() != generated.row_layout()
        || accepted.fields() != generated.fields()
        || accepted.relations() != generated.relations()
        || !generated_check_contracts_match(accepted, generated)
    {
        return Ok(None);
    }

    let accepted_generated_indexes = accepted
        .indexes()
        .iter()
        .filter(|index| index.generated())
        .collect::<Vec<_>>();
    if accepted_generated_indexes
        .iter()
        .any(|accepted_index| !generated.indexes().contains(accepted_index))
    {
        return Ok(None);
    }
    let additions = generated
        .indexes()
        .iter()
        .filter(|generated_index| !accepted_generated_indexes.contains(generated_index))
        .collect::<Vec<_>>();
    let [proposed] = additions.as_slice() else {
        return Ok(None);
    };
    if !proposed.unique() || !proposed.generated() {
        return Ok(None);
    }

    if let Some(existing) = accepted.candidate_indexes().first() {
        let matching = accepted.constraint_activations().iter().any(|pending| {
            pending.origin() == ConstraintOrigin::Generated
                && matches!(
                    pending.kind(),
                    ConstraintActivationKind::Unique { index_id }
                        if *index_id == existing.schema_id()
                )
                && existing.physical_generation() == pending.activation_epoch()
                && candidate_index_semantics_match(existing, proposed)
        });
        return Ok(matching.then(|| accepted.clone()));
    }
    if !accepted.constraint_activations().is_empty() {
        return Ok(None);
    }

    let highest_schema_id = accepted
        .indexes()
        .iter()
        .map(|index| index.schema_id().get())
        .max()
        .unwrap_or(0);
    let schema_id = highest_schema_id
        .checked_add(1)
        .and_then(SchemaIndexId::new)
        .ok_or(GeneratedAcceptedCandidateError::ConstraintCatalog)?;
    let ordinal = u16::try_from(accepted.indexes().len())
        .ok()
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or(GeneratedAcceptedCandidateError::ConstraintCatalog)?;
    let candidate =
        proposed.clone_with_schema_identity(schema_id, ordinal, activation.activation_epoch);
    let base = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        generated.version(),
        accepted.entity_path().to_string(),
        accepted.entity_name().to_string(),
        accepted.primary_key_field_ids().to_vec(),
        accepted.row_layout().clone(),
        accepted.fields().to_vec(),
        accepted.indexes().to_vec(),
    )
    .with_constraint_catalog(accepted.constraint_catalog().clone())
    .with_relations(accepted.relations().to_vec());
    base.with_added_unique_activation(
        candidate,
        activation.base_schema_fingerprint,
        activation.activation_epoch,
    )
    .map(Some)
    .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)
}

fn candidate_index_semantics_match(
    candidate: &PersistedIndexSnapshot,
    proposed: &PersistedIndexSnapshot,
) -> bool {
    candidate.name() == proposed.name()
        && candidate.store() == proposed.store()
        && candidate.unique() == proposed.unique()
        && candidate.origin() == proposed.origin()
        && candidate.physical_generation() != 0
        && candidate.key() == proposed.key()
        && candidate.predicate_sql() == proposed.predicate_sql()
}

fn derive_generated_not_null_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if generated.fields().is_empty()
        || generated.fields().len() > accepted.fields().len()
        || accepted.entity_path() != generated.entity_path()
        || accepted.entity_name() != generated.entity_name()
        || accepted.primary_key_field_ids() != generated.primary_key_field_ids()
        || accepted.row_layout().field_to_slot() != generated.row_layout().field_to_slot()
        || !generated_structural_owners_match(accepted, generated)
    {
        return Ok(None);
    }

    let mut tightening = None;
    for (accepted_field, generated_field) in accepted.fields().iter().zip(generated.fields()) {
        let candidate = field_with_temporal_contract(
            generated_field,
            accepted_field.introduced_in_layout(),
            accepted_field.historical_fill().clone(),
        );
        if candidate == *accepted_field {
            continue;
        }
        if accepted_field.nullable()
            && !candidate.nullable()
            && field_with_nullable(&candidate, true) == *accepted_field
            && tightening.replace(accepted_field).is_none()
        {
            continue;
        }
        return Ok(None);
    }
    if accepted.fields()[generated.fields().len()..]
        .iter()
        .any(PersistedFieldSnapshot::generated)
    {
        return Ok(None);
    }
    let Some(target) = tightening else {
        return Ok(None);
    };

    let matching = accepted.constraint_activations().iter().find(|pending| {
        pending.origin() == ConstraintOrigin::from_field_origin(target.origin())
            && matches!(
                pending.kind(),
                ConstraintActivationKind::NotNull { field_id } if *field_id == target.id()
            )
    });
    let catalog = if matching.is_some() {
        accepted.constraint_catalog().clone()
    } else {
        if !accepted.constraint_activations().is_empty() {
            return Ok(None);
        }
        accepted
            .constraint_catalog()
            .clone()
            .with_added_not_null_activation(
                target,
                activation.base_schema_fingerprint,
                activation.activation_epoch,
            )
            .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)?
    };

    Ok(Some(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            generated.version(),
            accepted.entity_path().to_string(),
            accepted.entity_name().to_string(),
            accepted.primary_key_field_ids().to_vec(),
            accepted.row_layout().clone(),
            accepted.fields().to_vec(),
            accepted.indexes().to_vec(),
        )
        .with_constraint_catalog(catalog)
        .with_relations(accepted.relations().to_vec()),
    ))
}

fn derive_generated_check_activation_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
    activation: GeneratedConstraintActivationContext,
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if !generated_non_check_shape_matches(accepted, generated) {
        return Ok(None);
    }

    let generated_checks = generated_check_constraints(generated);
    let accepted_checks = generated_check_constraints(accepted);
    if accepted.constraint_activations().iter().any(|pending| {
        if pending.origin() != ConstraintOrigin::Generated {
            return false;
        }
        let ConstraintActivationKind::Check {
            expression: pending_expression,
        } = pending.kind()
        else {
            return false;
        };
        !generated_checks.iter().any(|generated_check| {
            generated_check.name() == pending.name()
                && matches!(
                    generated_check.kind(),
                    AcceptedConstraintKind::Check { expression }
                        if expression == pending_expression
                )
        })
    }) {
        return Err(GeneratedAcceptedCandidateError::StaleConstraintActivation);
    }
    if accepted_checks.iter().any(|accepted_check| {
        !generated_checks.iter().any(|generated_check| {
            generated_check.name() == accepted_check.name()
                && generated_check.kind() == accepted_check.kind()
        })
    }) {
        return Ok(None);
    }

    let mut catalog = accepted.constraint_catalog().clone();
    let mut added = false;
    for generated_check in generated_checks {
        if accepted_checks.iter().any(|accepted_check| {
            accepted_check.name() == generated_check.name()
                && accepted_check.kind() == generated_check.kind()
        }) {
            continue;
        }
        let matching_activation = accepted.constraint_activations().iter().find(|pending| {
            pending.origin() == ConstraintOrigin::Generated
                && pending.name() == generated_check.name()
        });
        let AcceptedConstraintKind::Check { expression } = generated_check.kind() else {
            return Ok(None);
        };
        if let Some(pending) = matching_activation {
            if !matches!(
                pending.kind(),
                ConstraintActivationKind::Check {
                    expression: pending_expression
                } if pending_expression == expression
            ) {
                return Ok(None);
            }
            continue;
        }
        catalog = catalog
            .with_added_check_activation(
                generated_check.name().to_string(),
                ConstraintOrigin::Generated,
                (**expression).clone(),
                activation.base_schema_fingerprint,
                activation.activation_epoch,
            )
            .map_err(|_| GeneratedAcceptedCandidateError::ConstraintCatalog)?;
        added = true;
    }

    let preserves_generated_check_activation =
        accepted.constraint_activations().iter().any(|pending| {
            pending.origin() == ConstraintOrigin::Generated
                && matches!(pending.kind(), ConstraintActivationKind::Check { .. })
        });
    if !added && !preserves_generated_check_activation {
        return Ok(None);
    }

    Ok(Some(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            generated.version(),
            accepted.entity_path().to_string(),
            accepted.entity_name().to_string(),
            accepted.primary_key_field_ids().to_vec(),
            accepted.row_layout().clone(),
            accepted.fields().to_vec(),
            accepted.indexes().to_vec(),
        )
        .with_constraint_catalog(catalog)
        .with_relations(accepted.relations().to_vec()),
    ))
}

fn generated_check_constraints(
    snapshot: &PersistedSchemaSnapshot,
) -> Vec<&crate::db::schema::AcceptedConstraintSnapshot> {
    snapshot
        .constraints()
        .iter()
        .filter(|constraint| {
            constraint.origin() == ConstraintOrigin::Generated
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        })
        .collect()
}

fn generated_non_check_shape_matches(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> bool {
    accepted.entity_path() == generated.entity_path()
        && accepted.entity_name() == generated.entity_name()
        && accepted.primary_key_field_ids() == generated.primary_key_field_ids()
        && accepted.row_layout() == generated.row_layout()
        && accepted.fields() == generated.fields()
        && accepted.indexes() == generated.indexes()
        && accepted.relations() == generated.relations()
        && accepted.constraints().iter().all(|constraint| {
            if constraint.origin() == ConstraintOrigin::Generated
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
            {
                return true;
            }
            generated.constraints().iter().any(|generated_constraint| {
                generated_constraint.name() == constraint.name()
                    && generated_constraint.kind() == constraint.kind()
                    && generated_constraint.origin() == constraint.origin()
            })
        })
}

// Lower a generated-owned default change without changing any accepted
// temporal or physical fact. Accepted-only trailing DDL fields and indexes are
// retained because the generated proposal is not their authority.
fn derive_generated_default_candidate(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> Option<PersistedSchemaSnapshot> {
    if generated.fields().is_empty()
        || generated.fields().len() > accepted.fields().len()
        || accepted.entity_path() != generated.entity_path()
        || accepted.entity_name() != generated.entity_name()
        || accepted.primary_key_field_ids() != generated.primary_key_field_ids()
        || generated.row_layout().field_to_slot().len() != generated.fields().len()
        || !accepted
            .row_layout()
            .field_to_slot()
            .iter()
            .zip(generated.row_layout().field_to_slot())
            .all(|(accepted_entry, generated_entry)| accepted_entry == generated_entry)
        || accepted.fields()[generated.fields().len()..]
            .iter()
            .any(PersistedFieldSnapshot::generated)
        || !generated_structural_owners_match(accepted, generated)
    {
        return None;
    }

    let mut fields = accepted.fields().to_vec();
    for (index, (accepted_field, generated_field)) in
        accepted.fields().iter().zip(generated.fields()).enumerate()
    {
        if !accepted_field.generated() {
            return None;
        }
        let candidate = field_with_temporal_contract(
            generated_field,
            accepted_field.introduced_in_layout(),
            accepted_field.historical_fill().clone(),
        );
        if candidate.clone_with_insert_default(accepted_field.insert_default().clone())
            != *accepted_field
        {
            return None;
        }
        if candidate.insert_default() != accepted_field.insert_default() {
            fields[index] = candidate;
        }
    }

    Some(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            generated.version(),
            accepted.entity_path().to_string(),
            accepted.entity_name().to_string(),
            accepted.primary_key_field_ids().to_vec(),
            accepted.row_layout().clone(),
            fields,
            accepted.indexes().to_vec(),
        )
        .with_constraint_catalog(accepted.constraint_catalog().clone())
        .with_relations(accepted.relations().to_vec()),
    )
}

// Generated field/default reconciliation may carry accepted-only SQL DDL
// indexes forward, but it may not add, remove, rename, or take ownership of a
// generated structural contract in the same candidate. Relations are
// generated-only today and therefore remain exact.
fn generated_structural_owners_match(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> bool {
    generated.relations() == accepted.relations()
        && generated_check_contracts_match(accepted, generated)
        && generated
            .indexes()
            .iter()
            .all(|index| accepted.indexes().contains(index))
        && accepted
            .indexes()
            .iter()
            .filter(|index| index.generated())
            .all(|index| generated.indexes().contains(index))
}

fn generated_check_contracts_match(
    accepted: &PersistedSchemaSnapshot,
    generated: &PersistedSchemaSnapshot,
) -> bool {
    let generated_checks = generated_check_constraints(generated);
    let accepted_checks = generated_check_constraints(accepted);

    generated_checks.len() == accepted_checks.len()
        && generated_checks.iter().all(|generated| {
            accepted_checks.iter().any(|accepted| {
                accepted.name() == generated.name() && accepted.kind() == generated.kind()
            })
        })
}

fn field_with_temporal_contract(
    field: &PersistedFieldSnapshot,
    introduced_in_layout: crate::db::schema::RowLayoutVersion,
    historical_fill: SchemaHistoricalFill,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_with_write_policy_and_origin(
        field.id(),
        field.name().to_string(),
        field.slot(),
        field.kind().clone(),
        field.nested_leaves().to_vec(),
        field.nullable(),
        introduced_in_layout,
        field.insert_default().clone(),
        historical_fill,
        field.write_policy(),
        field.origin(),
        field.storage_decode(),
        field.leaf_codec(),
    )
}

fn field_with_nullable(field: &PersistedFieldSnapshot, nullable: bool) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_with_write_policy_and_origin(
        field.id(),
        field.name().to_string(),
        field.slot(),
        field.kind().clone(),
        field.nested_leaves().to_vec(),
        nullable,
        field.introduced_in_layout(),
        field.insert_default().clone(),
        field.historical_fill().clone(),
        field.write_policy(),
        field.origin(),
        field.storage_decode(),
        field.leaf_codec(),
    )
}
