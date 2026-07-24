use super::{
    SqlWriteCandidateBoundCheck, SqlWriteCandidateBounds, SqlWriteCandidateCollection,
    SqlWriteCandidateRows, SqlWriteMutationExecution, reject_explicit_sql_write_to_generated_field,
    reject_explicit_sql_write_to_managed_field, sql_insert_candidate_bounds,
    sql_write_input_for_accepted_field, sql_write_patch_set_accepted_field,
    sql_write_patch_set_insert_default,
};
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        data::AcceptedMutationIntentPatch,
        executor::{MutationMode, StructuralMutationTargetKey},
        query::intent::StructuralQuery,
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            SchemaFieldWritePolicy, accepted_insert_field_is_omittable,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlStatementResult,
                write_policy::{
                    DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT,
                    DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES, SqlWriteExecutionBounds,
                    SqlWriteReturningBounds,
                },
            },
        },
        sql::parser::{SqlInsertSource, SqlInsertStatement, SqlProjection, SqlWriteValue},
        sql_shared::SqlSyntaxErrorKind,
    },
    metrics::sink::SqlWriteKind,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

const SQL_INSERT_VALUES_INITIAL_RESERVE_ROWS: usize = 64;

const fn sql_insert_update_surface_execution_bounds(
    returning: Option<&crate::db::sql::parser::SqlReturningProjection>,
) -> SqlWriteExecutionBounds {
    let returning_requested = returning.is_some();

    SqlWriteExecutionBounds {
        max_staged_rows: Some(DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT),
        returning: SqlWriteReturningBounds {
            max_rows: if returning_requested {
                Some(DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT)
            } else {
                None
            },
            max_response_bytes: Some(DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES),
        },
    }
}

const fn write_policy_for_accepted_field(
    field: &AcceptedRowLayoutRuntimeField<'_>,
) -> SchemaFieldWritePolicy {
    field.write_policy()
}

const fn sql_insert_accepted_field_is_omittable(field: &AcceptedRowLayoutRuntimeField<'_>) -> bool {
    accepted_insert_field_is_omittable(field.insert_omission_policy(), field.write_policy())
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
        SqlInsertSource::DefaultValues => None,
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
    if matches!(statement.source, SqlInsertSource::DefaultValues) {
        return Vec::new();
    }

    let columns = accepted_insert_columns(descriptor, false);
    let full_columns = accepted_insert_columns(descriptor, true);
    let first_width = sql_insert_source_width_hint(descriptor, &statement.source);

    if first_width == Some(columns.len()) {
        return columns;
    }

    full_columns
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_insert_literal_patch(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        columns: &[String],
        values: &[Value],
    ) -> Result<AcceptedMutationIntentPatch, QueryError> {
        let mut patch = AcceptedMutationIntentPatch::new();
        for (field, value) in columns.iter().zip(values.iter()) {
            reject_explicit_sql_write_to_generated_field(descriptor, field)?;
            reject_explicit_sql_write_to_managed_field(descriptor, field)?;
            let input =
                sql_write_input_for_accepted_field(descriptor, field, value).map_err(|error| {
                    if descriptor.is_primary_key_field_name(field) {
                        QueryError::sql_write_boundary(
                            SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible,
                        )
                    } else {
                        error
                    }
                })?;
            patch = sql_write_patch_set_accepted_field(descriptor, patch, field, input)?;
        }

        Ok(patch)
    }

    fn sql_insert_values_patch(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        columns: &[String],
        values: &[SqlWriteValue],
    ) -> Result<AcceptedMutationIntentPatch, QueryError> {
        let mut patch = AcceptedMutationIntentPatch::new();
        for (field, value) in columns.iter().zip(values.iter()) {
            patch = match value {
                SqlWriteValue::Literal(value) => {
                    reject_explicit_sql_write_to_generated_field(descriptor, field)?;
                    reject_explicit_sql_write_to_managed_field(descriptor, field)?;
                    let input = sql_write_input_for_accepted_field(descriptor, field, value)
                        .map_err(|error| {
                            if descriptor.is_primary_key_field_name(field) {
                                QueryError::sql_write_boundary(
                                    SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible,
                                )
                            } else {
                                error
                            }
                        })?;
                    sql_write_patch_set_accepted_field(descriptor, patch, field, input)?
                }
                SqlWriteValue::Default => {
                    sql_write_patch_set_insert_default(descriptor, patch, field)?
                }
            };
        }

        Ok(patch)
    }

    fn sql_insert_default_values_patch(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    ) -> Result<AcceptedMutationIntentPatch, QueryError> {
        let mut patch = AcceptedMutationIntentPatch::new();
        for field in descriptor.fields() {
            patch = sql_write_patch_set_insert_default(descriptor, patch, field.name())?;
        }

        Ok(patch)
    }

    // Execute the SELECT source for `INSERT ... SELECT` and consume the
    // projected rows directly into the structural mutation batch. SQL
    // projection still owns row materialization, but write execution no longer
    // exposes that materialized source as a separate helper result.
    fn execute_sql_insert_select_source_patches<E>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        source_query: &StructuralQuery,
        columns: &[String],
        candidate_bounds: SqlWriteCandidateBounds,
    ) -> Result<SqlWriteCandidateCollection<StructuralMutationTargetKey<E::Key>>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (authority, _schema_info) =
            Self::accepted_sql_write_authority_schema_info::<E>(catalog)?;
        let rows = self.collect_bounded_sql_write_candidate_collection_from_structural_query(
            catalog.snapshot(),
            authority,
            source_query,
            candidate_bounds,
            None,
            |row| {
                if row.len() != columns.len() {
                    return Err(QueryError::sql_write_boundary(
                        SqlWriteBoundaryCode::InsertSelectWidthMismatch,
                    ));
                }

                let patch = Self::sql_insert_literal_patch(descriptor, columns, row)?;
                Ok((
                    StructuralMutationTargetKey::resolve_from_after_image(),
                    patch,
                ))
            },
        )?;

        Ok(rows)
    }

    // Convert one already-validated INSERT source row into the structural
    // mutation batch. Keeping this helper at the row boundary lets VALUES and
    // INSERT SELECT feed patches directly without first cloning/staging the
    // whole source row set behind a shared temporary vector.
    fn sql_insert_push_patch_row<E>(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        rows: &mut SqlWriteCandidateCollection<StructuralMutationTargetKey<E::Key>>,
        columns: &[String],
        values: &[SqlWriteValue],
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let patch = Self::sql_insert_values_patch(descriptor, columns, values)?;
        rows.push(
            StructuralMutationTargetKey::resolve_from_after_image(),
            patch,
        );

        Ok(())
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_insert_statement<E>(
        &self,
        statement: &SqlInsertStatement,
        source_query: Option<&StructuralQuery>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_insert_statement_with_execution_bounds::<E>(
            statement,
            source_query,
            catalog,
            None,
        )
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_insert_statement_with_update_surface_bounds<
        E,
    >(
        &self,
        statement: &SqlInsertStatement,
        source_query: Option<&StructuralQuery>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_insert_statement_with_execution_bounds::<E>(
            statement,
            source_query,
            catalog,
            Some(sql_insert_update_surface_execution_bounds(
                statement.returning.as_ref(),
            )),
        )
    }

    fn execute_sql_insert_statement_with_execution_bounds<E>(
        &self,
        statement: &SqlInsertStatement,
        source_query: Option<&StructuralQuery>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        execution_bounds: Option<SqlWriteExecutionBounds>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            catalog,
            statement.returning.as_ref(),
            |catalog, descriptor| {
                let columns = sql_insert_columns(&descriptor, statement);
                if !matches!(statement.source, SqlInsertSource::DefaultValues) {
                    ensure_sql_insert_required_fields(&descriptor, columns.as_slice())?;
                }
                let write_context =
                    SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
                let candidate_bounds =
                    sql_insert_candidate_bounds(execution_bounds, statement.returning.is_some());
                let mut collection = SqlWriteCandidateCollection::new();

                match &statement.source {
                    SqlInsertSource::Values(values) => {
                        candidate_bounds.validate_at(
                            SqlWriteCandidateRows::from_len(values.len()),
                            SqlWriteCandidateBoundCheck::InsertValuesSource,
                        )?;
                        collection
                            .reserve(values.len().min(SQL_INSERT_VALUES_INITIAL_RESERVE_ROWS));
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
                                &mut collection,
                                columns.as_slice(),
                                tuple.as_slice(),
                            )?;
                        }
                    }
                    SqlInsertSource::DefaultValues => {
                        candidate_bounds.validate_at(
                            SqlWriteCandidateRows::from_len(1),
                            SqlWriteCandidateBoundCheck::InsertValuesSource,
                        )?;
                        collection.push(
                            StructuralMutationTargetKey::resolve_from_after_image(),
                            Self::sql_insert_default_values_patch(&descriptor)?,
                        );
                    }
                    SqlInsertSource::Select(_) => {
                        let source_query = source_query.ok_or_else(QueryError::invariant)?;
                        collection = self.execute_sql_insert_select_source_patches::<E>(
                            catalog,
                            &descriptor,
                            source_query,
                            columns.as_slice(),
                            candidate_bounds,
                        )?;
                    }
                }
                let kind = match &statement.source {
                    SqlInsertSource::Values(_) | SqlInsertSource::DefaultValues => {
                        SqlWriteKind::Insert
                    }
                    SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
                };
                self.execute_sql_write_mutation_batch::<E>(
                    catalog,
                    &descriptor,
                    SqlWriteMutationExecution::from_bounded_collection(
                        collection,
                        candidate_bounds,
                        kind,
                        MutationMode::Insert,
                        write_context,
                        execution_bounds.map(|bounds| bounds.returning),
                    )?,
                    statement.returning.as_ref(),
                )
            },
        )
    }
}
