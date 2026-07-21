//! Schema transition compatibility predicates.

use crate::{
    db::{
        data::decode_runtime_value_from_accepted_field_contract,
        schema::{
            AcceptedFieldDecodeContract, PersistedFieldSnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, SchemaHistoricalFill, SchemaMutationRequest,
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
        || !generated_row_shape_matches(actual, expected)
        || !generated_current_fields_match(actual.fields(), expected.fields())
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

// Generated-owned insertion defaults are future write policy. A candidate may
// change only those defaults while every accepted temporal, physical, index,
// and relation fact remains exact.
pub(super) fn generated_field_defaults_only_changed(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    if actual == expected
        || actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
        || actual.row_layout() != expected.row_layout()
        || actual.fields().len() != expected.fields().len()
        || actual.indexes() != expected.indexes()
        || actual.relations() != expected.relations()
    {
        return false;
    }

    let mut changed = false;
    for (actual_field, expected_field) in actual.fields().iter().zip(expected.fields()) {
        if actual_field.clone_with_insert_default(expected_field.insert_default().clone())
            != *expected_field
        {
            return false;
        }
        if actual_field.insert_default() != expected_field.insert_default() {
            if !actual_field.generated() || !expected_field.generated() {
                return false;
            }
            changed = true;
        }
    }

    changed
}

// Identify the hard-cut slot collision where accepted SQL DDL already owns a
// trailing field identity that a later generated declaration would claim.
pub(super) fn generated_field_follows_accepted_ddl_extension(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<usize> {
    if actual.entity_path() != expected.entity_path()
        || actual.entity_name() != expected.entity_name()
        || actual.primary_key_field_ids() != expected.primary_key_field_ids()
    {
        return None;
    }
    let ddl_index = actual
        .fields()
        .iter()
        .position(|field| !field.generated())?;
    let generated_field = expected.fields().get(ddl_index)?;
    if !generated_field.generated()
        || actual.fields()[..ddl_index]
            .iter()
            .zip(&expected.fields()[..ddl_index])
            .any(|(accepted, generated)| !generated_current_field_matches(accepted, generated))
        || actual
            .row_layout()
            .field_to_slot()
            .iter()
            .zip(expected.row_layout().field_to_slot())
            .take(ddl_index)
            .any(|(accepted, generated)| accepted != generated)
    {
        return None;
    }

    Some(ddl_index)
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
        || !generated_current_fields_match(actual.fields(), expected.fields())
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
        .all(|(actual_field, expected_field)| {
            generated_current_field_matches(actual_field, expected_field)
        })
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

// Accepted schema can return to the generated shape after a sequence of SQL
// DDL mutations while retaining its newer schema/layout version. Generated
// metadata is a compatibility proposal here; it must not roll accepted
// authority back merely because the surviving shape is identical again.
pub(super) fn accepted_snapshot_matches_generated_shape(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    actual != expected
        && actual.version() >= expected.version()
        && actual.entity_path() == expected.entity_path()
        && actual.entity_name() == expected.entity_name()
        && actual.primary_key_field_ids() == expected.primary_key_field_ids()
        && actual.row_layout().field_to_slot() == expected.row_layout().field_to_slot()
        && actual.row_layout().allocated_slot_count()
            == expected.row_layout().allocated_slot_count()
        && generated_current_fields_match(actual.fields(), expected.fields())
        && actual.indexes() == expected.indexes()
        && actual.relations() == expected.relations()
}

fn generated_row_shape_matches(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> bool {
    actual.row_layout().field_to_slot() == expected.row_layout().field_to_slot()
        && actual.row_layout().allocated_slot_count()
            == expected.row_layout().allocated_slot_count()
}

// Generated metadata proposes current field intent but cannot reproduce the
// accepted introduction layout or frozen historical fill. Every other durable
// field fact remains exact at this compatibility boundary.
fn generated_current_fields_match(
    accepted: &[PersistedFieldSnapshot],
    generated: &[PersistedFieldSnapshot],
) -> bool {
    accepted.len() == generated.len()
        && accepted
            .iter()
            .zip(generated)
            .all(|(accepted, generated)| generated_current_field_matches(accepted, generated))
}

fn generated_current_field_matches(
    accepted: &PersistedFieldSnapshot,
    generated: &PersistedFieldSnapshot,
) -> bool {
    accepted.id() == generated.id()
        && accepted.name() == generated.name()
        && accepted.slot() == generated.slot()
        && accepted.kind() == generated.kind()
        && accepted.nested_leaves() == generated.nested_leaves()
        && accepted.nullable() == generated.nullable()
        && accepted.insert_default() == generated.insert_default()
        && accepted.write_policy() == generated.write_policy()
        && accepted.origin() == generated.origin()
        && accepted.storage_decode() == generated.storage_decode()
        && accepted.leaf_codec() == generated.leaf_codec()
}

fn is_supported_extra_accepted_index(index: &PersistedIndexSnapshot) -> bool {
    SchemaMutationRequest::from_accepted_field_path_index(index).is_ok()
        || SchemaMutationRequest::from_accepted_expression_index(index).is_ok()
}

// Decide whether one added field can be absent from older physical rows.
// Nullable no-default fields materialize as `NULL`; fields with explicit
// persisted default payloads materialize from that slot payload. A rejecting
// fill is valid only when the history floor proves no admitted row predates
// the field.
pub(super) fn field_has_supported_historical_fill(
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
