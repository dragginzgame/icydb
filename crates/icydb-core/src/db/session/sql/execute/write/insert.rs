use super::{
    accepted_sql_write_save_contract, checked_accepted_write_descriptor, record_sql_write_metrics,
    reject_explicit_sql_write_to_generated_field, reject_explicit_sql_write_to_managed_field,
    sql_returning_rows, sql_write_key_from_component_literals, sql_write_patch_set_accepted_field,
    sql_write_value_for_accepted_field, usize_to_u64_saturating,
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        data::StructuralPatch,
        executor::MutationMode,
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            AcceptedSchemaSnapshot, SchemaFieldWritePolicy,
        },
        session::sql::{SqlStatementResult, execute::write_returning::sql_write_statement_result},
        sql::{
            lowering::{
                bind_prepared_sql_select_statement_structural_with_schema,
                extract_prepared_sql_insert_select_source, prepare_sql_statement,
            },
            parser::{
                SqlExpr, SqlInsertSource, SqlInsertStatement, SqlOrderDirection, SqlOrderTerm,
                SqlProjection, SqlSelectStatement, SqlStatement,
            },
        },
    },
    metrics::sink::SqlWriteKind,
    model::field::FieldInsertGeneration,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityValue},
    types::{Timestamp, Ulid},
    value::Value,
};

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
        if primary_key_names.len() == 1 {
            let pk_name = primary_key_names[0];
            return Err(QueryError::unsupported_query(format!(
                "SQL INSERT requires primary key column '{pk_name}' in this release",
            )));
        }

        return Err(QueryError::unsupported_query(format!(
            "SQL INSERT requires primary key columns '{}' in this release",
            missing_required_fields.join(", "),
        )));
    }

    Err(QueryError::unsupported_query(format!(
        "SQL INSERT requires explicit values for non-generated fields {} in this release",
        missing_required_fields.join(", ")
    )))
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
            let pk_value = values.get(pk_index).ok_or_else(|| {
                QueryError::invariant(
                    "INSERT primary key column must align with one VALUES literal",
                )
            })?;
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

        return Err(QueryError::unsupported_query(format!(
            "SQL INSERT requires primary key column '{primary_key_name}' in this release"
        )));
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
            reject_explicit_sql_write_to_generated_field(descriptor, field, "INSERT")?;
            reject_explicit_sql_write_to_managed_field(descriptor, field, "INSERT")?;
            let normalized = sql_write_value_for_accepted_field(descriptor, field, value)?;
            patch = sql_write_patch_set_accepted_field(descriptor, patch, field, normalized)?;
        }

        Ok((key, patch))
    }

    fn sql_insert_select_source_statement(
        schema: &AcceptedSchemaSnapshot,
        primary_key_names: &[&str],
        statement: &SqlInsertStatement,
    ) -> Result<SqlSelectStatement, QueryError> {
        if primary_key_names.is_empty() {
            return Err(QueryError::invariant(
                "SQL INSERT SELECT must resolve the primary key from accepted schema metadata",
            ));
        }
        let statement = SqlStatement::Insert(statement.clone());
        let prepared = prepare_sql_statement(&statement, schema.entity_name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let mut select = extract_prepared_sql_insert_select_source(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        for primary_key_name in primary_key_names {
            if select.order_by.iter().any(
                |term| matches!(&term.field, SqlExpr::Field(field) if field == primary_key_name),
            ) {
                continue;
            }

            select.order_by.push(SqlOrderTerm {
                field: SqlExpr::Field((*primary_key_name).to_string()),
                direction: SqlOrderDirection::Asc,
            });
        }

        Ok(select)
    }

    // Execute the SELECT source for `INSERT ... SELECT` and consume the
    // projected rows directly into the structural mutation batch. SQL
    // projection still owns row materialization, but write execution no longer
    // exposes that materialized source as a separate helper result.
    fn execute_sql_insert_select_source_patches<E>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        source: &SqlSelectStatement,
        columns: &[String],
        rows: &mut Vec<(E::Key, StructuralPatch)>,
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = SqlStatement::Select(source.clone());
        let prepared = prepare_sql_statement(&statement, schema.entity_name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let authority =
            Self::accepted_entity_authority_for_schema::<E>(schema).map_err(QueryError::execute)?;
        let schema_info = authority.accepted_schema_info().ok_or_else(|| {
            QueryError::invariant("SQL INSERT SELECT authority must carry accepted schema info")
        })?;
        let query = bind_prepared_sql_select_statement_structural_with_schema(
            prepared,
            authority.model(),
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                query, authority, schema,
            )?;
        let (_, _, projected_rows, _) = payload.into_components();
        rows.reserve(projected_rows.len());
        for row in projected_rows {
            if row.len() != columns.len() {
                return Err(QueryError::unsupported_query(
                    "SQL INSERT SELECT projection width must match the target INSERT column list in this release",
                ));
            }

            Self::sql_insert_push_patch_row::<E>(descriptor, rows, columns, row.as_slice())?;
        }

        Ok(())
    }

    // Convert one already-validated INSERT source row into the structural
    // mutation batch. Keeping this helper at the row boundary lets VALUES and
    // INSERT SELECT feed patches directly without first cloning/staging the
    // whole source row set behind a shared temporary vector.
    fn sql_insert_push_patch_row<E>(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        rows: &mut Vec<(E::Key, StructuralPatch)>,
        columns: &[String],
        values: &[Value],
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (key, patch) = Self::sql_insert_patch_and_key::<E>(descriptor, columns, values)?;
        rows.push((key, patch));

        Ok(())
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_insert_statement<E>(
        &self,
        statement: &SqlInsertStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;
        let columns = sql_insert_columns(&descriptor, statement);
        ensure_sql_insert_required_fields(&descriptor, columns.as_slice())?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
        let mut rows = Vec::new();

        match &statement.source {
            SqlInsertSource::Values(values) => {
                rows.reserve(values.len());
                for tuple in values {
                    if tuple.len() != columns.len() {
                        return Err(QueryError::from_sql_parse_error(
                            crate::db::sql::parser::SqlParseError::invalid_syntax(
                                "INSERT column list and VALUES tuple length must match",
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
                let source = Self::sql_insert_select_source_statement(
                    &schema,
                    descriptor.primary_key_names(),
                    statement,
                )?;
                self.execute_sql_insert_select_source_patches::<E>(
                    &schema,
                    &descriptor,
                    &source,
                    columns.as_slice(),
                    &mut rows,
                )?;
            }
        }
        let source_rows = usize_to_u64_saturating(rows.len());
        let kind = match &statement.source {
            SqlInsertSource::Values(_) => SqlWriteKind::Insert,
            SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
        };
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        ) = accepted_sql_write_save_contract::<E>(&schema, &descriptor)?;
        let entities = self
            .execute_save_with_checked_accepted_row_contract::<E, _, _>(
                row_decode_contract,
                accepted_schema_info,
                accepted_schema_fingerprint,
                |save| {
                    save.apply_internal_lowered_structural_mutation_batch(
                        MutationMode::Insert,
                        rows,
                        write_context,
                        mutation_row_decode_contract,
                    )
                },
                std::convert::identity,
            )
            .map_err(QueryError::execute)?;
        let mutated_rows = usize_to_u64_saturating(entities.len());
        record_sql_write_metrics(
            E::PATH,
            kind,
            source_rows,
            mutated_rows,
            sql_returning_rows(statement.returning.as_ref(), mutated_rows),
        );

        sql_write_statement_result::<E>(entities, statement.returning.as_ref(), &descriptor)
    }
}
