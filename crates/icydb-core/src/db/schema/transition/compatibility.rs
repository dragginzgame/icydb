//! Schema transition compatibility predicates.

use crate::{
    db::{
        data::decode_runtime_value_from_accepted_field_contract,
        schema::{
            AcceptedFieldDecodeContract, PersistedFieldSnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, SchemaMutationRequest,
        },
    },
    value::Value,
};

// Generated index names are diagnostic/catalog metadata; physical index keys
// are addressed by stable ordinal. This admits hard-cut generated-name changes
// while preserving extra accepted DDL indexes, but only when every durable
// generated index contract other than `name` is unchanged.
pub(super) fn generated_index_names_only_changed(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    if actual == expected {
        return false;
    }
    if actual.version() != expected.version()
        || actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || !active_row_layout_matches(actual, expected)
        || actual.fields() != expected.fields()
    {
        return false;
    }

    let mut renamed = false;
    for expected_index in expected.indexes() {
        let Some(actual_index) = actual
            .indexes()
            .iter()
            .find(|index| index.ordinal() == expected_index.ordinal())
        else {
            return false;
        };
        if !index_contract_matches_ignoring_name(actual_index, expected_index) {
            return false;
        }
        renamed |= actual_index.name() != expected_index.name();
    }

    renamed
        && actual
            .indexes()
            .iter()
            .filter(|index| {
                !expected
                    .indexes()
                    .iter()
                    .any(|expected_index| expected_index.ordinal() == index.ordinal())
            })
            .all(is_supported_extra_accepted_index)
}

fn index_contract_matches_ignoring_name(
    actual: &PersistedIndexSnapshot,
    expected: &PersistedIndexSnapshot,
) -> bool {
    actual.ordinal() == expected.ordinal()
        && actual.store() == expected.store()
        && actual.unique() == expected.unique()
        && actual.key() == expected.key()
        && actual.predicate_sql() == expected.predicate_sql()
}

// Accepted schema remains the authority after SQL DDL publishes an index that
// generated metadata does not declare. Treat those snapshots as compatible
// when all generated facts are still present and every extra accepted index is
// a supported DDL-published secondary index.
pub(super) fn accepted_snapshot_extends_generated_indexes(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    if actual == expected {
        return false;
    }
    if actual.version() < expected.version()
        || actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || actual.row_layout().field_to_slot() != expected.row_layout().field_to_slot()
        || actual.fields() != expected.fields()
    {
        return false;
    }
    if !expected
        .indexes()
        .iter()
        .all(|index| actual.indexes().contains(index))
    {
        return false;
    }

    let has_ddl_index_extension = actual
        .indexes()
        .iter()
        .any(|index| !expected.indexes().contains(index));
    has_ddl_index_extension
        && actual
            .indexes()
            .iter()
            .filter(|index| !expected.indexes().contains(index))
            .all(is_supported_extra_accepted_index)
}

// SQL field DDL will publish DDL-owned accepted fields that generated Rust
// models do not mention. Treat those snapshots as compatible only when all
// generated field/layout/index facts are still exact prefixes or members, and
// every extra accepted field is explicitly DDL-owned.
pub(super) fn accepted_snapshot_extends_generated_with_ddl_fields(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    if actual == expected {
        return false;
    }
    if actual.version() < expected.version()
        || actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || actual.fields().len() < expected.fields().len()
        || actual.row_layout().field_to_slot().len() < expected.row_layout().field_to_slot().len()
    {
        return false;
    }
    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return false;
    }
    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return false;
    }
    if actual.fields()[expected.fields().len()..]
        .iter()
        .any(PersistedFieldSnapshot::generated)
    {
        return false;
    }
    if !expected
        .indexes()
        .iter()
        .all(|index| actual.indexes().contains(index))
    {
        return false;
    }

    let has_ddl_field_extension = actual.fields().len() > expected.fields().len()
        || actual.row_layout().field_to_slot().len() > expected.row_layout().field_to_slot().len();
    has_ddl_field_extension
        && actual
            .indexes()
            .iter()
            .filter(|index| !expected.indexes().contains(index))
            .all(is_supported_extra_accepted_index)
}

fn active_row_layout_matches(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    actual.row_layout().version() == expected.row_layout().version()
        && actual.row_layout().field_to_slot() == expected.row_layout().field_to_slot()
}

fn is_supported_extra_accepted_index(index: &PersistedIndexSnapshot) -> bool {
    SchemaMutationRequest::from_accepted_field_path_index(index).is_ok()
        || SchemaMutationRequest::from_accepted_expression_index(index).is_ok()
}

// Decide whether one added field can be absent from older physical rows.
// Nullable no-default fields materialize as `NULL`; fields with explicit
// persisted default payloads materialize from that slot payload.
pub(super) fn field_has_supported_missing_absence_policy(field: &PersistedFieldSnapshot) -> bool {
    (field.nullable() && field.default().is_none()) || field_default_payload_is_valid(field)
}

// Validate one accepted default payload before a schema transition can rely on
// it for missing-slot materialization. Defaults are persisted bytes, so policy
// must ask the accepted field-codec boundary to prove the payload is decodable
// and non-null instead of trusting the schema metadata blindly.
fn field_default_payload_is_valid(field: &PersistedFieldSnapshot) -> bool {
    let Some(payload) = field.default().slot_payload() else {
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
