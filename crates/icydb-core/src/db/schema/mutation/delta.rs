//! Accepted schema mutation delta classification.

use super::SchemaMutationRequest;
use crate::db::schema::{PersistedFieldSnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot};

///
/// SchemaMutationDelta
///
/// Snapshot-delta classification between two accepted catalog snapshots. This
/// keeps structural mutation detection inside the mutation layer while the
/// transition layer remains responsible for validation and diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationDelta<'a> {
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    AddFieldPathIndex(&'a PersistedIndexSnapshot),
    AddExpressionIndex(&'a PersistedIndexSnapshot),
    ExactMatch,
    Incompatible,
}

/// Classify the structural mutation shape between an accepted snapshot and a
/// proposed replacement. This does not decide whether the mutation is safe; it
/// only names the catalog delta shape for policy code.
pub(in crate::db::schema) fn classify_schema_mutation_delta<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationDelta<'a> {
    if actual == expected {
        return SchemaMutationDelta::ExactMatch;
    }

    if let Some(fields) = append_only_additive_fields(actual, expected) {
        return SchemaMutationDelta::AppendOnlyFields(fields);
    }

    if let Some(index) = single_added_index(actual, expected)
        && SchemaMutationRequest::from_accepted_field_path_index(index).is_ok()
    {
        return SchemaMutationDelta::AddFieldPathIndex(index);
    }

    if let Some(index) = single_added_index(actual, expected)
        && SchemaMutationRequest::from_accepted_expression_index(index).is_ok()
    {
        return SchemaMutationDelta::AddExpressionIndex(index);
    }

    SchemaMutationDelta::Incompatible
}

/// Build one mutation request from the structural delta between two accepted
/// snapshots. Policy validation remains in transition; this function only
/// classifies the catalog operation to keep lowering centralized.
pub(in crate::db::schema) fn schema_mutation_request_for_snapshots<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationRequest<'a> {
    SchemaMutationRequest::from(classify_schema_mutation_delta(actual, expected))
}

impl<'a> From<SchemaMutationDelta<'a>> for SchemaMutationRequest<'a> {
    fn from(delta: SchemaMutationDelta<'a>) -> Self {
        match delta {
            SchemaMutationDelta::AppendOnlyFields(fields) => Self::AppendOnlyFields(fields),
            SchemaMutationDelta::AddFieldPathIndex(index) => {
                Self::from_accepted_field_path_index(index).unwrap_or(Self::Incompatible)
            }
            SchemaMutationDelta::AddExpressionIndex(index) => {
                Self::from_accepted_expression_index(index).unwrap_or(Self::Incompatible)
            }
            SchemaMutationDelta::ExactMatch => Self::ExactMatch,
            SchemaMutationDelta::Incompatible => Self::Incompatible,
        }
    }
}

// Return generated fields for the additive shape that can become an accepted
// mutation plan: stored fields and row-layout entries must be exact prefixes of
// the generated proposal. Absence/default policy is validated by transition.
fn append_only_additive_fields<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    if actual.fields().len() >= expected.fields().len()
        || actual.row_layout().field_to_slot().len() >= expected.row_layout().field_to_slot().len()
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

    Some(&expected.fields()[actual.fields().len()..])
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
        || actual.row_layout().field_to_slot() != expected.row_layout().field_to_slot()
        || actual.fields() != expected.fields()
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

    expected.indexes().last()
}
