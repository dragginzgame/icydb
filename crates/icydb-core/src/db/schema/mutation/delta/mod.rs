//! Catalog-native admission of additive schema changes.

use super::SchemaMutationRequest;
use crate::{
    db::{
        data::decode_runtime_value_from_accepted_field_contract,
        schema::{
            AcceptedFieldDecodeContract, PersistedFieldSnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, SchemaHistoricalFill,
        },
    },
    value::Value,
};

/// Admit one supported addition, including its historical-row fill contract.
/// Publication consumes the resulting typed admission rather than reclassifying snapshots.
pub(in crate::db::schema) fn schema_mutation_request_for_snapshots<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<SchemaMutationRequest<'a>> {
    if let Some(fields) = append_only_additive_fields(actual, expected) {
        return fields
            .iter()
            .all(|field| {
                field_has_supported_historical_fill(field, expected.row_layout().history_floor())
            })
            .then_some(SchemaMutationRequest::AppendOnlyFields(fields));
    }
    let index = single_added_index(actual, expected)?;
    SchemaMutationRequest::from_accepted_field_path_index(index)
        .or_else(|_| SchemaMutationRequest::from_accepted_expression_index(index))
        .ok()
}

// Return added fields for the additive shape that can become an accepted
// mutation plan: stored fields and row-layout entries must be exact prefixes of
// the candidate snapshot. Admission also checks absence/default policy.
fn append_only_additive_fields<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    if actual.row_layout().history_floor() != expected.row_layout().history_floor() {
        return None;
    }

    append_only_additive_fields_with_admitted_floor(actual, expected)
}

/// Return whether one canonically derived required field addition is the sole
/// accepted change after an exact empty-entity proof advances the history floor.
#[cfg(feature = "sql")]
pub(in crate::db::schema) fn required_empty_entity_field_addition_matches(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
    field: &PersistedFieldSnapshot,
) -> bool {
    if !matches!(
        field.historical_fill(),
        crate::db::schema::SchemaHistoricalFill::Reject
    ) || expected.row_layout().history_floor() != expected.row_layout().current_version()
    {
        return false;
    }

    append_only_additive_fields_with_admitted_floor(actual, expected)
        .is_some_and(|added| added == std::slice::from_ref(field))
}

// Validate the shared append-only shape after the caller has established the
// operation-specific history-floor contract. Ordinary addition requires
// an unchanged floor; required SQL DDL may advance it only after proving the
// entity is exactly empty.
fn append_only_additive_fields_with_admitted_floor<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    let next_layout_version = actual.row_layout().current_version().checked_next()?;
    if actual.fields().len() >= expected.fields().len()
        || actual.row_layout().field_to_slot().len() >= expected.row_layout().field_to_slot().len()
        || expected.row_layout().current_version() != next_layout_version
        || actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || actual.indexes() != expected.indexes()
        || actual.relations() != expected.relations()
    {
        return None;
    }

    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return None;
    }

    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return None;
    }

    let added = &expected.fields()[actual.fields().len()..];
    let expected_catalog = added
        .iter()
        .try_fold(actual.constraint_catalog().clone(), |catalog, field| {
            catalog.with_added_not_null(field)
        })
        .ok()?;
    if &expected_catalog != expected.constraint_catalog() {
        return None;
    }

    Some(added)
}

// Return one appended index only when all non-index schema facts and prior
// accepted index contracts remain unchanged. The current path deliberately supports one
// index mutation at a time, so multiple additions stay incompatible.
fn single_added_index<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a PersistedIndexSnapshot> {
    if actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || actual.row_layout() != expected.row_layout()
        || actual.fields() != expected.fields()
        || actual.relations() != expected.relations()
        || expected.indexes().len() != actual.indexes().len().saturating_add(1)
    {
        return None;
    }

    if !actual
        .indexes()
        .iter()
        .zip(expected.indexes())
        .all(|(actual_index, expected_index)| actual_index == expected_index)
    {
        return None;
    }

    let added = expected.indexes().last()?;
    let expected_catalog = actual
        .constraint_catalog()
        .clone()
        .with_added_unique(added)
        .ok()?;
    (&expected_catalog == expected.constraint_catalog()).then_some(added)
}

// Decide whether one added field can be absent from older physical rows.
// Nullable no-default fields materialize as `NULL`; fields with explicit
// persisted default payloads materialize from that slot payload. A rejecting
// fill is valid only when the history floor proves no admitted row predates
// the field.
fn field_has_supported_historical_fill(
    field: &PersistedFieldSnapshot,
    history_floor: crate::db::schema::RowLayoutVersion,
) -> bool {
    match field.historical_fill() {
        SchemaHistoricalFill::Reject => field.introduced_in_layout() <= history_floor,
        SchemaHistoricalFill::Null => field.nullable(),
        SchemaHistoricalFill::SlotPayload(_) => field_historical_fill_payload_is_valid(field),
    }
}

// Validate one accepted default payload before a schema transition can rely on
// it for missing-slot materialization. Defaults are persisted bytes, so policy
// must ask the accepted field-codec boundary to prove the payload is decodable
// and non-null instead of trusting the schema metadata blindly.
fn field_historical_fill_payload_is_valid(field: &PersistedFieldSnapshot) -> bool {
    let Some(payload) = field.historical_fill().slot_payload() else {
        return false;
    };

    let contract = AcceptedFieldDecodeContract::new(
        field.name(),
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    );

    decode_runtime_value_from_accepted_field_contract(contract, payload)
        .is_ok_and(|value| !matches!(value, Value::Null))
}

#[cfg(test)]
mod tests;
