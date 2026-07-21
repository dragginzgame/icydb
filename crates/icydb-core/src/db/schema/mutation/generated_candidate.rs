//! Generated-proposal lowering into catalog-native accepted candidates.

use crate::db::schema::{
    PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaHistoricalFill, SchemaRowLayout,
};

/// Failure to lower a generated proposal into an accepted temporal candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum GeneratedAcceptedCandidateError {
    /// The next physical row-layout identity cannot be represented.
    RowLayoutVersionExhausted,
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
) -> Result<Option<PersistedSchemaSnapshot>, GeneratedAcceptedCandidateError> {
    if let Some(candidate) = derive_generated_default_candidate(accepted, generated) {
        return Ok(Some(candidate));
    }

    if generated.fields().len() <= accepted.fields().len()
        || generated.row_layout().field_to_slot().len()
            <= accepted.row_layout().field_to_slot().len()
        || accepted.fields().len() != accepted.row_layout().field_to_slot().len()
        || generated.fields().len() != generated.row_layout().field_to_slot().len()
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
            generated.indexes().to_vec(),
        )
        .with_relations(generated.relations().to_vec()),
    ))
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
        || !generated
            .indexes()
            .iter()
            .all(|index| accepted.indexes().contains(index))
        || !generated
            .relations()
            .iter()
            .all(|relation| accepted.relations().contains(relation))
    {
        return None;
    }

    let mut fields = accepted.fields().to_vec();
    let mut changed = false;
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
            changed = true;
        }
    }

    changed.then(|| {
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            generated.version(),
            accepted.entity_path().to_string(),
            accepted.entity_name().to_string(),
            accepted.primary_key_field_ids().to_vec(),
            accepted.row_layout().clone(),
            fields,
            accepted.indexes().to_vec(),
        )
        .with_relations(accepted.relations().to_vec())
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
