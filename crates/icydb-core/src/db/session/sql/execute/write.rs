use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        data::StructuralPatch,
        executor::{EntityAuthority, MutationMode},
        schema::{ValidateError, field_type_from_model_kind, literal_matches_type},
        session::sql::{
            SqlStatementResult,
            execute::write_returning::{
                sql_returning_statement_projection, sql_write_statement_result,
            },
            projection::{projection_labels_from_fields, sql_projection_rows_from_kernel_rows},
        },
        sql::lowering::{
            bind_prepared_sql_select_statement_structural, canonicalize_sql_predicate_for_model,
            canonicalize_strict_sql_literal_for_kind, extract_prepared_sql_insert_select_source,
            lower_sql_where_expr, prepare_sql_statement,
        },
        sql::parser::{
            SqlExpr, SqlInsertSource, SqlInsertStatement, SqlOrderDirection, SqlOrderTerm,
            SqlProjection, SqlReturningProjection, SqlSelectStatement, SqlStatement,
            SqlUpdateStatement,
        },
    },
    model::field::{FieldInsertGeneration, FieldModel},
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityKind, EntityValue, KeyValueCodec},
    types::{Timestamp, Ulid},
    value::Value,
};

fn sql_write_key_from_literal<E>(value: &Value, pk_name: &str) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let Some(normalized) =
        canonicalize_strict_sql_literal_for_kind(&E::MODEL.primary_key().kind(), value)
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

fn sql_write_generated_field_value(field: &FieldModel) -> Option<Value> {
    field
        .insert_generation()
        .map(|generation| match generation {
            FieldInsertGeneration::Ulid => Value::Ulid(Ulid::generate()),
            FieldInsertGeneration::Timestamp => Value::Timestamp(Timestamp::now()),
        })
}

fn sql_write_order_term_field(term: &SqlOrderTerm) -> Result<&str, QueryError> {
    term.direct_field_name().ok_or_else(|| {
        QueryError::unsupported_query(
            "SQL write ORDER BY only supports direct field targets in this release",
        )
    })
}

fn sql_write_value_for_field<E>(field_name: &str, value: &Value) -> Result<Value, QueryError>
where
    E: EntityKind,
{
    let field_slot = E::MODEL.resolve_field_slot(field_name).ok_or_else(|| {
        QueryError::invariant("SQL write field must resolve against the target entity model")
    })?;
    let field_kind = E::MODEL.fields()[field_slot].kind();
    let normalized = canonicalize_strict_sql_literal_for_kind(&field_kind, value)
        .unwrap_or_else(|| value.clone());

    let field_type = field_type_from_model_kind(&field_kind);
    if !literal_matches_type(&normalized, &field_type) {
        return Err(QueryError::unsupported_query(
            ValidateError::invalid_literal(field_name, "literal type does not match field type")
                .to_string(),
        ));
    }

    Ok(normalized)
}

fn reject_explicit_sql_write_to_managed_field<E>(
    field_name: &str,
    statement_kind: &str,
) -> Result<(), QueryError>
where
    E: EntityKind,
{
    let Some(field_slot) = E::MODEL.resolve_field_slot(field_name) else {
        return Ok(());
    };
    let field = &E::MODEL.fields()[field_slot];

    if field.write_management().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL {statement_kind} does not allow explicit writes to managed field '{field_name}' in this release"
        )));
    }

    Ok(())
}

fn reject_explicit_sql_write_to_generated_field<E>(
    field_name: &str,
    statement_kind: &str,
) -> Result<(), QueryError>
where
    E: EntityKind,
{
    let Some(field_slot) = E::MODEL.resolve_field_slot(field_name) else {
        return Ok(());
    };
    let field = &E::MODEL.fields()[field_slot];

    if field.insert_generation().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL {statement_kind} does not allow explicit writes to generated field '{field_name}' in this release"
        )));
    }

    Ok(())
}

fn sql_insert_field_is_omittable(field: &FieldModel) -> bool {
    if sql_write_generated_field_value(field).is_some() {
        return true;
    }

    field.write_management().is_some()
}

fn ensure_sql_insert_required_fields<E>(columns: &[String]) -> Result<(), QueryError>
where
    E: EntityKind,
{
    let missing_required_fields = E::MODEL
        .fields()
        .iter()
        .filter(|field| !columns.iter().any(|column| column == field.name()))
        .filter(|field| !sql_insert_field_is_omittable(field))
        .map(FieldModel::name)
        .collect::<Vec<_>>();

    if missing_required_fields.is_empty() {
        return Ok(());
    }

    if missing_required_fields.len() == 1
        && missing_required_fields[0] == E::MODEL.primary_key.name()
    {
        return Err(QueryError::unsupported_query(format!(
            "SQL INSERT requires primary key column '{}' in this release",
            E::MODEL.primary_key.name()
        )));
    }

    Err(QueryError::unsupported_query(format!(
        "SQL INSERT requires explicit values for non-generated fields {} in this release",
        missing_required_fields.join(", ")
    )))
}

fn sql_insert_source_width_hint<E>(source: &SqlInsertSource) -> Option<usize>
where
    E: EntityKind,
{
    match source {
        SqlInsertSource::Values(values) => values.first().map(Vec::len),
        SqlInsertSource::Select(select) => match &select.projection {
            SqlProjection::All => Some(
                E::MODEL
                    .fields()
                    .iter()
                    .filter(|field| field.write_management().is_none())
                    .count(),
            ),
            SqlProjection::Items(items) => Some(items.len()),
        },
    }
}

fn sql_insert_columns<E>(statement: &SqlInsertStatement) -> Vec<String>
where
    E: EntityKind,
{
    if !statement.columns.is_empty() {
        return statement.columns.clone();
    }

    let columns: Vec<String> = E::MODEL
        .fields()
        .iter()
        .filter(|field| !sql_insert_field_is_omittable(field))
        .map(|field| field.name().to_string())
        .collect();
    let full_columns: Vec<String> = E::MODEL
        .fields()
        .iter()
        .filter(|field| field.write_management().is_none())
        .map(|field| field.name().to_string())
        .collect();
    let first_width = sql_insert_source_width_hint::<E>(&statement.source);

    if first_width == Some(columns.len()) {
        return columns;
    }

    full_columns
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_insert_patch_and_key<E>(
        columns: &[String],
        values: &[Value],
    ) -> Result<(E::Key, StructuralPatch), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let pk_name = E::MODEL.primary_key.name;
        let generated_fields = E::MODEL
            .fields()
            .iter()
            .filter(|field| !columns.iter().any(|column| column == field.name()))
            .filter_map(|field| {
                sql_write_generated_field_value(field).map(|value| (field.name(), value))
            })
            .collect::<Vec<_>>();
        let key = if let Some(pk_index) = columns.iter().position(|field| field == pk_name) {
            let pk_value = values.get(pk_index).ok_or_else(|| {
                QueryError::invariant(
                    "INSERT primary key column must align with one VALUES literal",
                )
            })?;
            sql_write_key_from_literal::<E>(pk_value, pk_name)?
        } else if let Some((_, pk_value)) = generated_fields
            .iter()
            .find(|(field_name, _)| *field_name == pk_name)
        {
            sql_write_key_from_literal::<E>(pk_value, pk_name)?
        } else {
            return Err(QueryError::unsupported_query(format!(
                "SQL INSERT requires primary key column '{pk_name}' in this release"
            )));
        };

        let mut patch = StructuralPatch::new();
        for (field_name, generated_value) in &generated_fields {
            patch = patch
                .set_field(E::MODEL, field_name, generated_value.clone())
                .map_err(QueryError::execute)?;
        }
        for (field, value) in columns.iter().zip(values.iter()) {
            reject_explicit_sql_write_to_generated_field::<E>(field, "INSERT")?;
            reject_explicit_sql_write_to_managed_field::<E>(field, "INSERT")?;
            let normalized = sql_write_value_for_field::<E>(field, value)?;
            patch = patch
                .set_field(E::MODEL, field, normalized)
                .map_err(QueryError::execute)?;
        }

        Ok((key, patch))
    }

    fn sql_structural_patch<E>(
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralPatch, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let pk_name = E::MODEL.primary_key.name;
        let mut patch = StructuralPatch::new();
        for assignment in &statement.assignments {
            if assignment.field == pk_name {
                return Err(QueryError::unsupported_query(format!(
                    "SQL UPDATE does not allow primary key mutation for '{pk_name}' in this release"
                )));
            }
            reject_explicit_sql_write_to_generated_field::<E>(assignment.field.as_str(), "UPDATE")?;
            reject_explicit_sql_write_to_managed_field::<E>(assignment.field.as_str(), "UPDATE")?;
            let normalized =
                sql_write_value_for_field::<E>(assignment.field.as_str(), &assignment.value)?;

            patch = patch
                .set_field(E::MODEL, assignment.field.as_str(), normalized)
                .map_err(QueryError::execute)?;
        }

        Ok(patch)
    }

    fn sql_update_selector_query<E>(statement: &SqlUpdateStatement) -> Result<Query<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let Some(predicate) = statement.predicate.clone() else {
            return Err(QueryError::unsupported_query(
                "SQL UPDATE requires WHERE predicate in this release",
            ));
        };
        let predicate = canonicalize_sql_predicate_for_model(
            E::MODEL,
            lower_sql_where_expr(&predicate)
                .map_err(|error| QueryError::unsupported_query(error.to_string()))?,
        );
        let pk_name = E::MODEL.primary_key.name;
        let mut selector = Query::<E>::new(MissingRowPolicy::Ignore).filter_predicate(predicate);

        if statement.order_by.is_empty() {
            selector = selector.order_term(crate::db::asc(pk_name));
        } else {
            let mut orders_primary_key = false;

            for term in &statement.order_by {
                let field = sql_write_order_term_field(term)?;
                if field == pk_name {
                    orders_primary_key = true;
                }
                selector = match term.direction {
                    SqlOrderDirection::Asc => selector.order_term(crate::db::asc(field)),
                    SqlOrderDirection::Desc => selector.order_term(crate::db::desc(field)),
                };
            }

            if !orders_primary_key {
                selector = selector.order_term(crate::db::asc(pk_name));
            }
        }

        if let Some(limit) = statement.limit {
            selector = selector.limit(limit);
        }
        if let Some(offset) = statement.offset {
            selector = selector.offset(offset);
        }

        Ok(selector)
    }

    fn sql_insert_select_source_statement<E>(
        statement: &SqlInsertStatement,
    ) -> Result<SqlSelectStatement, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let prepared =
            prepare_sql_statement(SqlStatement::Insert(statement.clone()), E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?;
        let mut select = extract_prepared_sql_insert_select_source(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;
        let pk_name = E::MODEL.primary_key.name;
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

    fn execute_sql_insert_select_source_rows<E>(
        &self,
        source: &SqlSelectStatement,
    ) -> Result<Vec<Vec<Value>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let prepared = prepare_sql_statement(SqlStatement::Select(source.clone()), E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let authority = EntityAuthority::for_type::<E>();
        let query = bind_prepared_sql_select_statement_structural(
            prepared,
            authority.model(),
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let (payload, _) =
            self.execute_structural_sql_projection_without_sql_cache(query, authority)?;
        let (_, _, rows, _) = payload.into_parts();

        Ok(rows)
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_insert_statement<E>(
        &self,
        statement: &SqlInsertStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let columns = sql_insert_columns::<E>(statement);
        ensure_sql_insert_required_fields::<E>(columns.as_slice())?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
        let source_rows = match &statement.source {
            SqlInsertSource::Values(values) => {
                for tuple in values {
                    if tuple.len() != columns.len() {
                        return Err(QueryError::from_sql_parse_error(
                            crate::db::sql::parser::SqlParseError::invalid_syntax(
                                "INSERT column list and VALUES tuple length must match",
                            ),
                        ));
                    }
                }

                values.clone()
            }
            SqlInsertSource::Select(_) => {
                let source = Self::sql_insert_select_source_statement::<E>(statement)?;
                let rows = self.execute_sql_insert_select_source_rows::<E>(&source)?;
                for row in &rows {
                    if row.len() != columns.len() {
                        return Err(QueryError::unsupported_query(
                            "SQL INSERT SELECT projection width must match the target INSERT column list in this release",
                        ));
                    }
                }

                rows
            }
        };
        let mut entities = Vec::with_capacity(source_rows.len());

        for values in &source_rows {
            let (key, patch) = Self::sql_insert_patch_and_key::<E>(columns.as_slice(), values)?;
            let entity = self
                .execute_save_entity::<E>(|save| {
                    save.apply_internal_structural_mutation_with_write_context(
                        MutationMode::Insert,
                        key,
                        patch,
                        write_context,
                    )
                })
                .map_err(QueryError::execute)?;
            entities.push(entity);
        }

        sql_write_statement_result::<C, E>(entities, statement.returning.as_ref())
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_update_statement<E>(
        &self,
        statement: &SqlUpdateStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let selector = Self::sql_update_selector_query::<E>(statement)?;
        let patch = Self::sql_structural_patch::<E>(statement)?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
        let matched = self.execute_query(&selector)?;
        let mut entities = Vec::with_capacity(matched.len());

        for entity in matched.entities() {
            let updated = self
                .execute_save_entity::<E>(|save| {
                    save.apply_internal_structural_mutation_with_write_context(
                        MutationMode::Update,
                        entity.id().key(),
                        patch.clone(),
                        write_context,
                    )
                })
                .map_err(QueryError::execute)?;
            entities.push(updated);
        }

        sql_write_statement_result::<C, E>(entities, statement.returning.as_ref())
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
            None => self
                .execute_delete_count(&typed_query)
                .map(|row_count| SqlStatementResult::Count { row_count }),
            Some(returning) => {
                // Phase 2: returning deletes reuse the structural projection
                // terminal once, then shape the requested outbound row contract
                // locally at the SQL write boundary.
                let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(&typed_query)?;
                let deleted = self
                    .with_metrics(|| {
                        self.delete_executor::<E>()
                            .execute_structural_projection(plan)
                    })
                    .map_err(QueryError::execute)?;
                let (rows, row_count) = deleted.into_parts();
                let rows =
                    sql_projection_rows_from_kernel_rows(rows).map_err(QueryError::execute)?;

                sql_returning_statement_projection(
                    projection_labels_from_fields(E::MODEL.fields()),
                    rows,
                    row_count,
                    returning,
                )
            }
        }
    }
}
