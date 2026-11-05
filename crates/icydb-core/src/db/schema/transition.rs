//! Module: db::schema::transition
//! Responsibility: plan supported SQL DDL additions and enforce schema-version admission.
//! Does not own: generated proposal reconciliation, physical work or publication.
//! Boundary: accepted before/after snapshots -> one additive DDL plan or refusal.

#[cfg(feature = "sql")]
mod admission;

use crate::{
    db::{
        data::decode_runtime_value_from_accepted_field_contract,
        schema::{
            AcceptedFieldDecodeContract, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildTarget,
            SchemaHistoricalFill, SchemaMutationRequest, schema_mutation_request_for_snapshots,
        },
    },
    value::Value,
};

#[cfg(feature = "sql")]
pub(in crate::db::schema) use admission::{
    SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionClassification,
    SchemaAdmissionRejectionReason, classify_schema_admission_rejection,
};

/// One checked additive shape. Each index variant carries its physical-work
/// target; only field additions may publish without staging an index domain.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionPlan {
    AppendOnlyFields,
    AddFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
}

// Only additions reach this boundary. Metadata changes, generated activation
// and no-op reconciliation use their own owners; none yields an additive plan.
pub(in crate::db::schema) fn plan_sql_ddl_addition(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<SchemaTransitionPlan> {
    let request = schema_mutation_request_for_snapshots(actual, expected)?;
    let plan = match request {
        SchemaMutationRequest::AppendOnlyFields(added_fields) => {
            if !added_fields.iter().all(|field| {
                field_has_supported_historical_fill(field, expected.row_layout().history_floor())
            }) {
                return None;
            }
            SchemaTransitionPlan::AppendOnlyFields
        }
        SchemaMutationRequest::AddFieldPathIndex { target } => {
            SchemaTransitionPlan::AddFieldPathIndex { target }
        }
        SchemaMutationRequest::AddExpressionIndex { target } => {
            SchemaTransitionPlan::AddExpressionIndex { target }
        }
        SchemaMutationRequest::ExactMatch => return None,
    };

    Some(plan)
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
