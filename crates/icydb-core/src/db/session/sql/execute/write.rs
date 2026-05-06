use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        data::{FieldSlot, StructuralPatch},
        executor::{EntityAuthority, MutationMode},
        query::intent::StructuralQuery,
        query::plan::expr::{FieldId, ProjectionSelection},
        schema::{
            AcceptedRowLayoutRuntimeDescriptor, AcceptedRowLayoutRuntimeField,
            AcceptedSchemaSnapshot, SchemaFieldWritePolicy, SchemaInfo, ValidateError,
            canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
            literal_matches_type,
        },
        session::sql::{
            SqlStatementResult,
            execute::write_returning::{
                projection_labels_from_accepted_write_descriptor,
                sql_returning_statement_projection, sql_write_statement_result,
            },
        },
        sql::lowering::{
            bind_prepared_sql_select_statement_structural_with_schema,
            bind_sql_update_selector_query_structural_with_schema,
            extract_prepared_sql_insert_select_source, prepare_sql_statement,
        },
        sql::parser::{
            SqlExpr, SqlInsertSource, SqlInsertStatement, SqlOrderDirection, SqlOrderTerm,
            SqlProjection, SqlReturningProjection, SqlSelectStatement, SqlStatement,
            SqlUpdateStatement,
        },
    },
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    model::field::FieldInsertGeneration,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityKind, EntityValue, KeyValueCodec},
    types::{Timestamp, Ulid},
    value::Value,
};

fn sql_write_key_from_literal<E>(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    value: &Value,
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let pk_name = descriptor.primary_key_name();
    let primary_key_kind = descriptor.primary_key_kind();
    let Some(normalized) =
        canonicalize_strict_sql_literal_for_persisted_kind(primary_key_kind, value)
    else {
        return Err(QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
        )));
    };

    <E::Key as KeyValueCodec>::from_key_value(&normalized).ok_or_else(|| {
        QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
        ))
    })
}

fn checked_accepted_write_descriptor<E>(
    schema: &AcceptedSchemaSnapshot,
) -> Result<AcceptedRowLayoutRuntimeDescriptor<'_>, QueryError>
where
    E: EntityKind,
{
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(schema)
        .map_err(QueryError::execute)?;
    descriptor
        .generated_compatible_row_shape_for_model(E::MODEL)
        .map_err(QueryError::execute)?;

    Ok(descriptor)
}

fn accepted_write_field_slot(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    patch: StructuralPatch,
    field_name: &str,
    value: Value,
) -> Result<StructuralPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set(slot, value))
}

fn write_policy_for_accepted_name(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    field_name: &str,
) -> Result<SchemaFieldWritePolicy, QueryError> {
    let Some(field) = descriptor.field_by_name(field_name) else {
        return Err(QueryError::invariant(
            "SQL write field must resolve against accepted schema metadata",
        ));
    };

    Ok(field.write_policy())
}

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

fn sql_write_value_for_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    pk_name: &str,
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

    if missing_required_fields.len() == 1 && missing_required_fields[0] == pk_name {
        return Err(QueryError::unsupported_query(format!(
            "SQL INSERT requires primary key column '{pk_name}' in this release",
        )));
    }

    Err(QueryError::unsupported_query(format!(
        "SQL INSERT requires explicit values for non-generated fields {} in this release",
        missing_required_fields.join(", ")
    )))
}

fn sql_insert_source_width_hint(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
    fn sql_insert_patch_and_key<E>(
        descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
        let pk_name = descriptor.primary_key_name();
        let key = if let Some(pk_index) = columns.iter().position(|field| field == pk_name) {
            let pk_value = values.get(pk_index).ok_or_else(|| {
                QueryError::invariant(
                    "INSERT primary key column must align with one VALUES literal",
                )
            })?;
            sql_write_key_from_literal::<E>(descriptor, pk_value)?
        } else if let Some((_, pk_value)) = generated_fields
            .iter()
            .find(|(field_name, _)| *field_name == pk_name)
        {
            sql_write_key_from_literal::<E>(descriptor, pk_value)?
        } else {
            return Err(QueryError::unsupported_query(format!(
                "SQL INSERT requires primary key column '{pk_name}' in this release"
            )));
        };

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

    fn sql_structural_patch(
        descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralPatch, QueryError> {
        let pk_name = descriptor.primary_key_name();
        let mut patch = StructuralPatch::new();
        for assignment in &statement.assignments {
            if assignment.field == pk_name {
                return Err(QueryError::unsupported_query(format!(
                    "SQL UPDATE does not allow primary key mutation for '{pk_name}' in this release"
                )));
            }
            reject_explicit_sql_write_to_generated_field(
                descriptor,
                assignment.field.as_str(),
                "UPDATE",
            )?;
            reject_explicit_sql_write_to_managed_field(
                descriptor,
                assignment.field.as_str(),
                "UPDATE",
            )?;
            let normalized = sql_write_value_for_accepted_field(
                descriptor,
                assignment.field.as_str(),
                &assignment.value,
            )?;

            patch = sql_write_patch_set_accepted_field(
                descriptor,
                patch,
                assignment.field.as_str(),
                normalized,
            )?;
        }

        Ok(patch)
    }

    fn sql_update_selector_query<E>(
        schema: &AcceptedSchemaSnapshot,
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralQuery, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, schema);
        let pk_name = E::MODEL.primary_key.name;
        let selector = bind_sql_update_selector_query_structural_with_schema(
            E::MODEL,
            statement,
            MissingRowPolicy::Ignore,
            &schema,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(selector.projection_selection(ProjectionSelection::Fields(vec![FieldId::new(pk_name)])))
    }

    fn sql_insert_select_source_statement<E>(
        pk_name: &str,
        statement: &SqlInsertStatement,
    ) -> Result<SqlSelectStatement, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = SqlStatement::Insert(statement.clone());
        let prepared = prepare_sql_statement(&statement, E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let mut select = extract_prepared_sql_insert_select_source(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;
        if select.order_by.is_empty()
            || !select
                .order_by
                .iter()
                .any(|term| matches!(&term.field, SqlExpr::Field(field) if field == pk_name))
        {
            select.order_by.push(SqlOrderTerm {
                field: SqlExpr::Field(pk_name.to_string()),
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
        descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
        source: &SqlSelectStatement,
        columns: &[String],
        rows: &mut Vec<(E::Key, StructuralPatch)>,
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = SqlStatement::Select(source.clone());
        let prepared = prepare_sql_statement(&statement, E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let authority = EntityAuthority::for_type::<E>();
        let schema_info = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, schema);
        let query = bind_prepared_sql_select_statement_structural_with_schema(
            prepared,
            authority.model(),
            MissingRowPolicy::Ignore,
            &schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                query, authority,
            )?;
        let (_, _, projected_rows, _) = payload.into_parts();
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
        descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
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
        let pk_name = descriptor.primary_key_name();
        let columns = sql_insert_columns(&descriptor, statement);
        ensure_sql_insert_required_fields(&descriptor, pk_name, columns.as_slice())?;
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
                let source = Self::sql_insert_select_source_statement::<E>(pk_name, statement)?;
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
        let entities = self
            .execute_save_with_checked_accepted_schema::<E, _, _>(
                |save| {
                    save.apply_internal_lowered_structural_mutation_batch(
                        MutationMode::Insert,
                        rows,
                        write_context,
                        Some(descriptor.row_decode_contract()),
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

    pub(in crate::db::session::sql::execute) fn execute_sql_update_statement<E>(
        &self,
        statement: &SqlUpdateStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;
        let selector = Self::sql_update_selector_query::<E>(&schema, statement)?;
        let patch = Self::sql_structural_patch(&descriptor, statement)?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
        let authority = EntityAuthority::for_type::<E>();
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                selector, authority,
            )?;
        let (_, _, projected_rows, _) = payload.into_parts();
        let matched_rows = usize_to_u64_saturating(projected_rows.len());
        let mut rows = Vec::with_capacity(projected_rows.len());

        for row in projected_rows {
            let Some(value) = row.first() else {
                return Err(QueryError::invariant(
                    "SQL UPDATE target selector emitted an empty primary-key projection row",
                ));
            };
            let key = sql_write_key_from_literal::<E>(&descriptor, value)?;

            rows.push((key, patch.clone()));
        }
        let entities = self
            .execute_save_with_checked_accepted_schema::<E, _, _>(
                |save| {
                    save.apply_internal_lowered_structural_mutation_batch(
                        MutationMode::Update,
                        rows,
                        write_context,
                        Some(descriptor.row_decode_contract()),
                    )
                },
                std::convert::identity,
            )
            .map_err(QueryError::execute)?;
        let mutated_rows = usize_to_u64_saturating(entities.len());
        record_sql_write_metrics(
            E::PATH,
            SqlWriteKind::Update,
            matched_rows,
            mutated_rows,
            sql_returning_rows(statement.returning.as_ref(), mutated_rows),
        );

        sql_write_statement_result::<E>(entities, statement.returning.as_ref(), &descriptor)
    }

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
                let (rows, row_count) = deleted.into_parts();
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
