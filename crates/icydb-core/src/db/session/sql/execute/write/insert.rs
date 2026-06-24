use super::{
    SqlWriteMutationBatch, SqlWriteMutationExecution, reject_explicit_sql_write_to_generated_field,
    reject_explicit_sql_write_to_managed_field, sql_write_key_from_component_literals,
    sql_write_patch_set_accepted_field, sql_write_value_for_accepted_field,
};
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        data::StructuralPatch,
        executor::MutationMode,
        query::intent::StructuralQuery,
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            AcceptedSchemaSnapshot, SchemaFieldWritePolicy,
        },
        session::sql::SqlStatementResult,
        sql::parser::{SqlInsertSource, SqlInsertStatement, SqlProjection},
        sql_shared::SqlSyntaxErrorKind,
    },
    metrics::sink::SqlWriteKind,
    model::field::FieldInsertGeneration,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityValue},
    types::{Timestamp, Ulid},
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

const fn write_policy_for_accepted_field(
    field: &AcceptedRowLayoutRuntimeField<'_>,
) -> SchemaFieldWritePolicy {
    field.write_policy()
}

fn sql_write_generated_field_value(generation: FieldInsertGeneration) -> Value {
    match generation {
        FieldInsertGeneration::Ulid => Value::Ulid(Ulid::generate()),
        FieldInsertGeneration::Timestamp => Value::Timestamp(Timestamp::now()),
    }
}

const fn sql_insert_field_is_omittable(policy: SchemaFieldWritePolicy) -> bool {
    if policy.insert_generation().is_some() {
        return true;
    }

    policy.write_management().is_some()
}

const fn sql_insert_accepted_field_is_omittable(field: &AcceptedRowLayoutRuntimeField<'_>) -> bool {
    let policy = write_policy_for_accepted_field(field);

    sql_insert_field_is_omittable(policy)
}

fn ensure_sql_insert_required_fields(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    columns: &[String],
) -> Result<(), QueryError> {
    let mut missing_required_fields = Vec::new();
    for field in descriptor.fields() {
        if columns.iter().any(|column| column == field.name()) {
            continue;
        }
        if sql_insert_accepted_field_is_omittable(field) {
            continue;
        }

        missing_required_fields.push(field.name());
    }

    if missing_required_fields.is_empty() {
        return Ok(());
    }

    let primary_key_names = descriptor.primary_key_names();
    let missing_only_primary_key_fields = missing_required_fields
        .iter()
        .all(|field| primary_key_names.contains(field));
    if missing_only_primary_key_fields {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::MissingPrimaryKey,
        ));
    }

    Err(QueryError::sql_write_boundary(
        SqlWriteBoundaryCode::MissingRequiredFields,
    ))
}

fn sql_insert_source_width_hint(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    source: &SqlInsertSource,
) -> Option<usize> {
    match source {
        SqlInsertSource::Values(values) => values.first().map(Vec::len),
        SqlInsertSource::Select(select) => match &select.projection {
            SqlProjection::All => {
                let mut count = 0usize;
                for field in descriptor.fields() {
                    let policy = write_policy_for_accepted_field(field);
                    if policy.write_management().is_none() {
                        count = count.saturating_add(1);
                    }
                }
                Some(count)
            }
            SqlProjection::Items(items) => Some(items.len()),
        },
    }
}

fn accepted_insert_columns(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    include_omittable_fields: bool,
) -> Vec<String> {
    let mut columns = Vec::new();
    for field in descriptor.fields() {
        if !include_omittable_fields && sql_insert_accepted_field_is_omittable(field) {
            continue;
        }
        if include_omittable_fields
            && write_policy_for_accepted_field(field)
                .write_management()
                .is_some()
        {
            continue;
        }

        columns.push(field.name().to_string());
    }

    columns
}

fn sql_insert_columns(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    statement: &SqlInsertStatement,
) -> Vec<String> {
    if !statement.columns.is_empty() {
        return statement.columns.clone();
    }

    let columns = accepted_insert_columns(descriptor, false);
    let full_columns = accepted_insert_columns(descriptor, true);
    let first_width = sql_insert_source_width_hint(descriptor, &statement.source);

    if first_width == Some(columns.len()) {
        return columns;
    }

    full_columns
}

fn sql_insert_primary_key_values(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    columns: &[String],
    values: &[Value],
    generated_fields: &[(&str, Value)],
) -> Result<Vec<Value>, QueryError> {
    let mut key_values = Vec::with_capacity(descriptor.primary_key_names().len());
    for primary_key_name in descriptor.primary_key_names() {
        if let Some(pk_index) = columns.iter().position(|field| field == primary_key_name) {
            let pk_value = values.get(pk_index).ok_or_else(QueryError::invariant)?;
            key_values.push(pk_value.clone());
            continue;
        }

        if let Some((_, pk_value)) = generated_fields
            .iter()
            .find(|(field_name, _)| *field_name == *primary_key_name)
        {
            key_values.push(pk_value.clone());
            continue;
        }

        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::MissingPrimaryKey,
        ));
    }

    Ok(key_values)
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_insert_patch_and_key<E>(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        columns: &[String],
        values: &[Value],
    ) -> Result<(E::Key, StructuralPatch), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let mut generated_fields = Vec::new();
        for accepted_field in descriptor.fields() {
            if columns.iter().any(|column| column == accepted_field.name()) {
                continue;
            }

            let policy = write_policy_for_accepted_field(accepted_field);
            if let Some(generation) = policy.insert_generation() {
                generated_fields.push((
                    accepted_field.name(),
                    sql_write_generated_field_value(generation),
                ));
            }
        }
        let key_values =
            sql_insert_primary_key_values(descriptor, columns, values, &generated_fields)?;
        let key = sql_write_key_from_component_literals::<E>(descriptor, key_values.as_slice())?;

        let mut patch = StructuralPatch::new();
        for (field_name, generated_value) in &generated_fields {
            patch = sql_write_patch_set_accepted_field(
                descriptor,
                patch,
                field_name,
                generated_value.clone(),
            )?;
        }
        for (field, value) in columns.iter().zip(values.iter()) {
            reject_explicit_sql_write_to_generated_field(descriptor, field)?;
            reject_explicit_sql_write_to_managed_field(descriptor, field)?;
            let normalized = sql_write_value_for_accepted_field(descriptor, field, value)?;
            patch = sql_write_patch_set_accepted_field(descriptor, patch, field, normalized)?;
        }

        Ok((key, patch))
    }

    // Execute the SELECT source for `INSERT ... SELECT` and consume the
    // projected rows directly into the structural mutation batch. SQL
    // projection still owns row materialization, but write execution no longer
    // exposes that materialized source as a separate helper result.
    fn execute_sql_insert_select_source_patches<E>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        source_query: &StructuralQuery,
        columns: &[String],
    ) -> Result<(crate::db::schema::SchemaInfo, SqlWriteMutationBatch<E::Key>), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (authority, save_schema_info) =
            Self::accepted_sql_write_authority_schema_info::<E>(schema)?;
        let rows = self.collect_sql_write_mutation_batch_from_structural_query(
            schema,
            authority,
            source_query,
            |row| {
                if row.len() != columns.len() {
                    return Err(QueryError::sql_write_boundary(
                        SqlWriteBoundaryCode::InsertSelectWidthMismatch,
                    ));
                }

                Self::sql_insert_patch_and_key::<E>(descriptor, columns, row)
            },
        )?;

        Ok((save_schema_info, rows))
    }

    // Convert one already-validated INSERT source row into the structural
    // mutation batch. Keeping this helper at the row boundary lets VALUES and
    // INSERT SELECT feed patches directly without first cloning/staging the
    // whole source row set behind a shared temporary vector.
    fn sql_insert_push_patch_row<E>(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        rows: &mut SqlWriteMutationBatch<E::Key>,
        columns: &[String],
        values: &[Value],
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (key, patch) = Self::sql_insert_patch_and_key::<E>(descriptor, columns, values)?;
        rows.push(key, patch);

        Ok(())
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_insert_statement<E>(
        &self,
        statement: &SqlInsertStatement,
        source_query: Option<&StructuralQuery>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            statement.returning.as_ref(),
            |schema, descriptor| {
                let columns = sql_insert_columns(&descriptor, statement);
                ensure_sql_insert_required_fields(&descriptor, columns.as_slice())?;
                let write_context =
                    SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
                let mut rows = SqlWriteMutationBatch::new();
                let mut save_schema_info = None;

                match &statement.source {
                    SqlInsertSource::Values(values) => {
                        rows.reserve(values.len());
                        for tuple in values {
                            if tuple.len() != columns.len() {
                                return Err(QueryError::from_sql_parse_error(
                                    crate::db::sql::parser::SqlParseError::invalid_syntax(
                                        SqlSyntaxErrorKind::InsertValuesTupleLengthMismatch,
                                    ),
                                ));
                            }

                            Self::sql_insert_push_patch_row::<E>(
                                &descriptor,
                                &mut rows,
                                columns.as_slice(),
                                tuple.as_slice(),
                            )?;
                        }
                    }
                    SqlInsertSource::Select(_) => {
                        let source_query = source_query.ok_or_else(QueryError::invariant)?;
                        let (schema_info, collected_rows) = self
                            .execute_sql_insert_select_source_patches::<E>(
                                schema,
                                &descriptor,
                                source_query,
                                columns.as_slice(),
                            )?;
                        save_schema_info = Some(schema_info);
                        rows = collected_rows;
                    }
                }
                let staged_rows = rows.staged_rows();
                let kind = match &statement.source {
                    SqlInsertSource::Values(_) => SqlWriteKind::Insert,
                    SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
                };
                self.execute_sql_write_mutation_batch::<E>(
                    schema,
                    &descriptor,
                    SqlWriteMutationExecution {
                        rows,
                        staged_rows,
                        kind,
                        mode: MutationMode::Insert,
                        context: write_context,
                        returning_bounds: None,
                        save_schema_info,
                    },
                    statement.returning.as_ref(),
                )
            },
        )
    }
}
