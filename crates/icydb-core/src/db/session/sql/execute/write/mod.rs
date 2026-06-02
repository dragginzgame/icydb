mod insert;
mod update;

use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        commit::CommitSchemaFingerprint,
        data::{FieldSlot, StructuralPatch},
        schema::{
            AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            SchemaFieldWritePolicy, SchemaInfo, ValidateError, accepted_commit_schema_fingerprint,
            canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
            literal_matches_type,
        },
        session::sql::{
            SqlStatementResult,
            execute::write_returning::{
                projection_labels_from_accepted_write_descriptor,
                sql_returning_statement_projection,
            },
        },
        sql::parser::SqlReturningProjection,
    },
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    traits::{CanisterKind, EntityKind, EntityValue, KeyValueCodec},
    value::Value,
};

fn sql_write_key_from_literal<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    value: &Value,
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if descriptor.primary_key_names().len() > 1 {
        let Value::List(values) = value else {
            return Err(QueryError::unsupported_query(format!(
                "SQL write primary key literal for '{}' must contain all composite key components",
                primary_key_label(descriptor),
            )));
        };

        return sql_write_key_from_component_literals::<E>(descriptor, values.as_slice());
    }

    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let pk_name = descriptor.first_primary_key_name();
    let primary_key_kind = descriptor.first_primary_key_kind();
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(primary_key_kind, value)
        .unwrap_or_else(|| value.clone());

    <E::Key as KeyValueCodec>::from_key_value(&normalized).ok_or_else(|| {
        QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
        ))
    })
}

fn sql_write_key_from_component_literals<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    values: &[Value],
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    let primary_key_names = descriptor.primary_key_names();
    let primary_key_kinds = descriptor.primary_key_kinds();
    if values.len() != primary_key_names.len() {
        return Err(QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{}' must contain {} component(s)",
            primary_key_label(descriptor),
            primary_key_names.len(),
        )));
    }

    let mut normalized = Vec::with_capacity(values.len());
    for ((_field_name, kind), value) in primary_key_names
        .iter()
        .zip(primary_key_kinds.iter())
        .zip(values.iter())
    {
        let value = canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
            .unwrap_or_else(|| value.clone());

        normalized.push(value);
    }

    let key_value = if normalized.len() == 1 {
        normalized
            .into_iter()
            .next()
            .expect("primary key normalization preserves one scalar value")
    } else {
        Value::List(normalized)
    };

    <E::Key as KeyValueCodec>::from_key_value(&key_value).ok_or_else(|| {
        QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{}' is not compatible with entity key type",
            primary_key_label(descriptor),
        ))
    })
}

fn primary_key_label(descriptor: &AcceptedRowLayoutRuntimeContract<'_>) -> String {
    descriptor.primary_key_names().join(", ")
}

fn checked_accepted_write_descriptor<E>(
    schema: &AcceptedSchemaSnapshot,
) -> Result<AcceptedRowLayoutRuntimeContract<'_>, QueryError>
where
    E: EntityKind,
{
    let (descriptor, _) =
        AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(schema, E::MODEL)
            .map_err(QueryError::execute)?;

    Ok(descriptor)
}

fn accepted_sql_write_save_contract<E>(
    schema: &AcceptedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<
    (
        AcceptedRowDecodeContract,
        AcceptedRowDecodeContract,
        SchemaInfo,
        CommitSchemaFingerprint,
    ),
    QueryError,
>
where
    E: EntityKind,
{
    let row_decode_contract = descriptor.row_decode_contract();
    let mutation_row_decode_contract = row_decode_contract.clone();
    let schema_info = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, schema);
    let schema_fingerprint =
        accepted_commit_schema_fingerprint(schema).map_err(QueryError::execute)?;

    Ok((
        row_decode_contract,
        mutation_row_decode_contract,
        schema_info,
        schema_fingerprint,
    ))
}

fn accepted_write_field_slot(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<FieldSlot, QueryError> {
    let accepted_slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(|| {
            QueryError::invariant("SQL write field must resolve against accepted schema metadata")
        })?;

    Ok(FieldSlot::from_validated_index(accepted_slot))
}

fn sql_write_patch_set_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: StructuralPatch,
    field_name: &str,
    value: Value,
) -> Result<StructuralPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set(slot, value))
}

fn write_policy_for_accepted_name(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<SchemaFieldWritePolicy, QueryError> {
    let Some(field) = descriptor.field_by_name(field_name) else {
        return Err(QueryError::invariant(
            "SQL write field must resolve against accepted schema metadata",
        ));
    };

    Ok(field.write_policy())
}

fn sql_write_value_for_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    let accepted_kind = descriptor.field_kind_by_name(field_name).ok_or_else(|| {
        QueryError::invariant("SQL write field must resolve against accepted schema metadata")
    })?;
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(accepted_kind, value)
        .unwrap_or_else(|| value.clone());

    let field_type = field_type_from_persisted_kind(accepted_kind);
    if !literal_matches_type(&normalized, &field_type) {
        return Err(QueryError::unsupported_query(
            ValidateError::invalid_literal(field_name, "literal type does not match field type")
                .to_string(),
        ));
    }

    Ok(normalized)
}

fn reject_explicit_sql_write_to_managed_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    statement_kind: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.write_management().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL {statement_kind} does not allow explicit writes to managed field '{field_name}' in this release"
        )));
    }

    Ok(())
}

fn reject_explicit_sql_write_to_generated_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    statement_kind: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.insert_generation().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL {statement_kind} does not allow explicit writes to generated field '{field_name}' in this release"
        )));
    }

    Ok(())
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const fn sql_returning_rows(returning: Option<&SqlReturningProjection>, mutated_rows: u64) -> u64 {
    if returning.is_some() { mutated_rows } else { 0 }
}

fn record_sql_write_metrics(
    entity_path: &'static str,
    kind: SqlWriteKind,
    matched_rows: u64,
    mutated_rows: u64,
    returning_rows: u64,
) {
    record(MetricsEvent::SqlWrite {
        entity_path,
        kind,
        matched_rows,
        mutated_rows,
        returning_rows,
    });
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql::execute) fn execute_sql_delete_statement<E>(
        &self,
        query: &crate::db::query::intent::StructuralQuery,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let typed_query = Query::<E>::from_inner(query.clone());

        // Phase 1: keep pure count deletes on the direct terminal so the
        // delete lane does not hop through projection shaping it will discard.
        match returning {
            None => {
                let row_count = self.execute_delete_count(&typed_query)?;
                let rows = u64::from(row_count);
                record_sql_write_metrics(E::PATH, SqlWriteKind::Delete, rows, rows, 0);

                Ok(SqlStatementResult::Count { row_count })
            }
            Some(returning) => {
                let schema = self
                    .ensure_accepted_schema_snapshot::<E>()
                    .map_err(QueryError::execute)?;
                let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;

                // Phase 2: returning deletes reuse the structural projection
                // terminal once, then shape the requested outbound row contract
                // from executor-materialized rows at the SQL write boundary.
                let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(&typed_query)?;
                let deleted = self
                    .with_metrics(|| {
                        self.delete_executor::<E>()
                            .execute_structural_projection(plan)
                    })
                    .map_err(QueryError::execute)?;
                let (rows, row_count) = deleted.into_rows_and_count();
                let rows = rows.into_value_rows();
                let metric_rows = u64::from(row_count);
                record_sql_write_metrics(
                    E::PATH,
                    SqlWriteKind::Delete,
                    metric_rows,
                    metric_rows,
                    metric_rows,
                );

                sql_returning_statement_projection(
                    projection_labels_from_accepted_write_descriptor(&descriptor),
                    rows,
                    row_count,
                    returning,
                )
            }
        }
    }
}
