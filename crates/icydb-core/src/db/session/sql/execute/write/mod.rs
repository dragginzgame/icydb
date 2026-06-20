mod insert;
mod update;

use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        data::{FieldSlot, StructuralPatch},
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, SchemaFieldWritePolicy,
            SchemaInfo, accepted_commit_schema_fingerprint,
            canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
            literal_matches_type,
        },
        session::{
            AcceptedSaveContract, accepted_save_contract_for_descriptor,
            sql::{
                SqlStatementResult,
                execute::write_returning::{
                    projection_labels_from_accepted_write_descriptor,
                    sql_returning_statement_projection, validate_sql_returning_projection_fields,
                },
            },
        },
        sql::parser::SqlReturningProjection,
    },
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    traits::{CanisterKind, EntityKind, EntityValue, KeyValueCodec},
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

fn sql_write_key_from_literal<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    value: &Value,
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if descriptor.primary_key_names().len() > 1 {
        let Value::List(values) = value else {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
            ));
        };

        return sql_write_key_from_component_literals::<E>(descriptor, values.as_slice());
    }

    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let primary_key_kind = descriptor.first_primary_key_kind();
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(primary_key_kind, value)
        .unwrap_or_else(|| value.clone());

    <E::Key as KeyValueCodec>::from_key_value(&normalized).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
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
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
        ));
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
        normalized.into_iter().next().expect("sql write invariant")
    } else {
        Value::List(normalized)
    };

    <E::Key as KeyValueCodec>::from_key_value(&key_value).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
    })
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
    schema_info: Option<SchemaInfo>,
) -> Result<AcceptedSaveContract, QueryError>
where
    E: EntityKind,
{
    if let Some(schema_info) = schema_info {
        let row_decode_contract = descriptor.row_decode_contract();
        let mutation_row_decode_contract = row_decode_contract.clone();
        let schema_fingerprint =
            accepted_commit_schema_fingerprint(schema).map_err(QueryError::execute)?;

        return Ok((
            row_decode_contract,
            mutation_row_decode_contract,
            schema_info,
            schema_fingerprint,
        ));
    }

    accepted_save_contract_for_descriptor::<E>(schema, descriptor).map_err(QueryError::execute)
}

fn accepted_write_field_slot(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<FieldSlot, QueryError> {
    let accepted_slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;

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
        return Err(QueryError::invariant());
    };

    Ok(field.write_policy())
}

fn sql_write_value_for_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    let accepted_kind = descriptor
        .field_kind_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(accepted_kind, value)
        .unwrap_or_else(|| value.clone());

    let field_type = field_type_from_persisted_kind(accepted_kind);
    if !literal_matches_type(&normalized, &field_type) {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::InvalidFieldLiteral,
        ));
    }

    Ok(normalized)
}

fn reject_explicit_sql_write_to_managed_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.write_management().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitManagedField,
        ));
    }

    Ok(())
}

fn reject_explicit_sql_write_to_generated_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.insert_generation().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitGeneratedField,
        ));
    }

    Ok(())
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const fn sql_returning_rows(returning: Option<&SqlReturningProjection>, mutated_rows: u64) -> u64 {
    if returning.is_some() { mutated_rows } else { 0 }
}

#[derive(Clone, Copy)]
struct SqlWriteRowAttribution {
    matched: u64,
    mutated: u64,
    returning: u64,
}

#[derive(Clone, Copy)]
struct SqlWriteStagedRows(usize);

impl SqlWriteStagedRows {
    fn attribution_after_mutation(
        self,
        mutated_rows: usize,
        returning: Option<&SqlReturningProjection>,
    ) -> SqlWriteRowAttribution {
        SqlWriteRowAttribution::mutation_batch(self, mutated_rows, returning)
    }
}

impl SqlWriteRowAttribution {
    const fn delete_count(row_count: u32) -> Self {
        let rows = row_count as u64;

        Self {
            matched: rows,
            mutated: rows,
            returning: 0,
        }
    }

    const fn delete_returning(row_count: u32) -> Self {
        let rows = row_count as u64;

        Self {
            matched: rows,
            mutated: rows,
            returning: rows,
        }
    }

    fn mutation_batch(
        staged_rows: SqlWriteStagedRows,
        mutated_rows: usize,
        returning: Option<&SqlReturningProjection>,
    ) -> Self {
        let matched_rows = usize_to_u64_saturating(staged_rows.0);
        let mutated_rows = usize_to_u64_saturating(mutated_rows);

        Self {
            matched: matched_rows,
            mutated: mutated_rows,
            returning: sql_returning_rows(returning, mutated_rows),
        }
    }
}

struct SqlWriteMutationBatch<K> {
    rows: Vec<(K, StructuralPatch)>,
}

impl<K> SqlWriteMutationBatch<K> {
    const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    fn push(&mut self, key: K, patch: StructuralPatch) {
        self.rows.push((key, patch));
    }

    const fn staged_rows(&self) -> SqlWriteStagedRows {
        SqlWriteStagedRows(self.rows.len())
    }

    fn into_rows(self) -> Vec<(K, StructuralPatch)> {
        self.rows
    }
}

fn record_sql_write_metrics(
    entity_path: &'static str,
    kind: SqlWriteKind,
    rows: SqlWriteRowAttribution,
) {
    record(MetricsEvent::SqlWrite {
        entity_path,
        kind,
        matched_rows: rows.matched,
        mutated_rows: rows.mutated,
        returning_rows: rows.returning,
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
                record_sql_write_metrics(
                    E::PATH,
                    SqlWriteKind::Delete,
                    SqlWriteRowAttribution::delete_count(row_count),
                );

                Ok(SqlStatementResult::Count { row_count })
            }
            Some(returning) => {
                let schema = self
                    .ensure_accepted_schema_snapshot::<E>()
                    .map_err(QueryError::execute)?;
                let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;
                validate_sql_returning_projection_fields(&descriptor, Some(returning))?;

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
                record_sql_write_metrics(
                    E::PATH,
                    SqlWriteKind::Delete,
                    SqlWriteRowAttribution::delete_returning(row_count),
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
