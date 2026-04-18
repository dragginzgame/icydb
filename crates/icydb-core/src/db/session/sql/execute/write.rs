use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        data::UpdatePatch,
        executor::{EntityAuthority, MutationMode},
        identifiers_tail_match,
        schema::{ValidateError, field_type_from_model_kind, literal_matches_type},
        session::sql::{
            SqlStatementResult,
            projection::{
                SqlProjectionPayload, projection_labels_from_fields,
                sql_projection_rows_from_kernel_rows,
            },
        },
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSqlQuery, bind_lowered_sql_query,
            canonicalize_sql_predicate_for_model, canonicalize_strict_sql_literal_for_kind,
            lower_sql_command_from_prepared_statement, lower_sql_where_expr, prepare_sql_statement,
        },
        sql::parser::{
            SqlExpr, SqlInsertSource, SqlInsertStatement, SqlOrderDirection, SqlOrderTerm,
            SqlProjection, SqlReturningProjection, SqlSelectStatement, SqlStatement,
            SqlUpdateStatement,
        },
    },
    model::{
        entity::resolve_field_slot,
        field::{FieldInsertGeneration, FieldModel},
    },
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityKind, EntityValue, FieldValue},
    types::{Timestamp, Ulid},
    value::Value,
};

// Keep typed SQL write routes on the same entity-match contract used by
// lowered query execution, without widening write statements into lowering.
fn ensure_sql_write_entity_matches<E>(sql_entity: &str) -> Result<(), QueryError>
where
    E: EntityKind,
{
    if identifiers_tail_match(sql_entity, E::MODEL.name()) {
        return Ok(());
    }

    Err(QueryError::from_sql_lowering_error(
        crate::db::sql::lowering::SqlLoweringError::EntityMismatch {
            sql_entity: sql_entity.to_string(),
            expected_entity: E::MODEL.name(),
        },
    ))
}

fn sql_write_key_from_literal<E>(value: &Value, pk_name: &str) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if let Some(key) = <E::Key as FieldValue>::from_value(value) {
        return Ok(key);
    }

    let Some(normalized) =
        canonicalize_strict_sql_literal_for_kind(&E::MODEL.primary_key().kind(), value)
    else {
        return Err(QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
        )));
    };

    <E::Key as FieldValue>::from_value(&normalized).ok_or_else(|| {
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
    let SqlExpr::Field(field) = &term.field else {
        return Err(QueryError::unsupported_query(
            "SQL write ORDER BY only supports direct field targets in this release",
        ));
    };

    Ok(field.as_str())
}

fn sql_write_value_for_field<E>(field_name: &str, value: &Value) -> Result<Value, QueryError>
where
    E: EntityKind,
{
    let field_slot = resolve_field_slot(E::MODEL, field_name).ok_or_else(|| {
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
    let Some(field_slot) = resolve_field_slot(E::MODEL, field_name) else {
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

fn reject_explicit_sql_insert_to_generated_field<E>(field_name: &str) -> Result<(), QueryError>
where
    E: EntityKind,
{
    let Some(field_slot) = resolve_field_slot(E::MODEL, field_name) else {
        return Ok(());
    };
    let field = &E::MODEL.fields()[field_slot];

    if field.insert_generation().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL INSERT does not allow explicit writes to generated field '{field_name}' in this release"
        )));
    }

    Ok(())
}

fn reject_explicit_sql_update_to_generated_field<E>(field_name: &str) -> Result<(), QueryError>
where
    E: EntityKind,
{
    let Some(field_slot) = resolve_field_slot(E::MODEL, field_name) else {
        return Ok(());
    };
    let field = &E::MODEL.fields()[field_slot];

    if field.insert_generation().is_some() {
        return Err(QueryError::unsupported_query(format!(
            "SQL UPDATE does not allow explicit writes to generated field '{field_name}' in this release"
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

fn ensure_sql_insert_value_tuples_match_columns(
    columns: &[String],
    values: &[Vec<Value>],
) -> Result<(), QueryError> {
    for tuple in values {
        if tuple.len() != columns.len() {
            return Err(QueryError::from_sql_parse_error(
                crate::db::sql::parser::SqlParseError::invalid_syntax(
                    "INSERT column list and VALUES tuple length must match",
                ),
            ));
        }
    }

    Ok(())
}

fn ensure_sql_insert_selected_rows_match_columns(
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<(), QueryError> {
    for row in rows {
        if row.len() != columns.len() {
            return Err(QueryError::unsupported_query(
                "SQL INSERT SELECT projection width must match the target INSERT column list in this release",
            ));
        }
    }

    Ok(())
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_write_statement_row<E>(entity: E) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let mut row = Vec::with_capacity(E::MODEL.fields().len());

        for index in 0..E::MODEL.fields().len() {
            let value = entity.get_value_by_index(index).ok_or_else(|| {
                QueryError::invariant(
                    "SQL write statement projection row must include every declared field",
                )
            })?;
            row.push(value);
        }

        Ok(row)
    }

    fn sql_write_statement_projection<E>(
        entities: Vec<E>,
    ) -> Result<SqlProjectionPayload, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let columns = projection_labels_from_fields(E::MODEL.fields());
        let rows = entities
            .into_iter()
            .map(Self::sql_write_statement_row)
            .collect::<Result<Vec<_>, _>>()?;
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(SqlProjectionPayload::new(
            columns,
            vec![None; E::MODEL.fields().len()],
            rows,
            row_count,
        ))
    }

    fn sql_write_statement_result<E>(
        entities: Vec<E>,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);

        match returning {
            None => Ok(SqlStatementResult::Count { row_count }),
            Some(returning) => {
                let (columns, _, rows, row_count) =
                    Self::sql_write_statement_projection(entities)?.into_parts();

                Self::sql_returning_statement_projection(columns, rows, row_count, returning)
            }
        }
    }

    fn sql_returning_statement_projection(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
        returning: &SqlReturningProjection,
    ) -> Result<SqlStatementResult, QueryError> {
        match returning {
            SqlReturningProjection::All => Ok(SqlProjectionPayload::new(
                columns,
                vec![None; rows.first().map_or(0, Vec::len)],
                rows,
                row_count,
            )
            .into_statement_result()),
            SqlReturningProjection::Fields(fields) => {
                let mut indices = Vec::with_capacity(fields.len());

                for field in fields {
                    let index = columns
                        .iter()
                        .position(|column| column == field)
                        .ok_or_else(|| {
                            QueryError::unsupported_query(format!(
                                "SQL RETURNING field '{field}' does not exist on the target entity"
                            ))
                        })?;
                    indices.push(index);
                }

                let mut projected_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut projected = Vec::with_capacity(indices.len());
                    for index in &indices {
                        let value = row.get(*index).ok_or_else(|| {
                            QueryError::invariant(
                                "SQL RETURNING projection row must align with declared columns",
                            )
                        })?;
                        projected.push(value.clone());
                    }
                    projected_rows.push(projected);
                }

                Ok(SqlProjectionPayload::new(
                    fields.clone(),
                    vec![None; fields.len()],
                    projected_rows,
                    row_count,
                )
                .into_statement_result())
            }
        }
    }

    fn sql_insert_patch_and_key<E>(
        columns: &[String],
        values: &[Value],
    ) -> Result<(E::Key, UpdatePatch), QueryError>
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

        let mut patch = UpdatePatch::new();
        for (field_name, generated_value) in &generated_fields {
            patch = patch
                .set_field(E::MODEL, field_name, generated_value.clone())
                .map_err(QueryError::execute)?;
        }
        for (field, value) in columns.iter().zip(values.iter()) {
            reject_explicit_sql_insert_to_generated_field::<E>(field)?;
            reject_explicit_sql_write_to_managed_field::<E>(field, "INSERT")?;
            let normalized = sql_write_value_for_field::<E>(field, value)?;
            patch = patch
                .set_field(E::MODEL, field, normalized)
                .map_err(QueryError::execute)?;
        }

        Ok((key, patch))
    }

    fn sql_update_patch<E>(statement: &SqlUpdateStatement) -> Result<UpdatePatch, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let pk_name = E::MODEL.primary_key.name;
        let mut patch = UpdatePatch::new();
        for assignment in &statement.assignments {
            if assignment.field == pk_name {
                return Err(QueryError::unsupported_query(format!(
                    "SQL UPDATE does not allow primary key mutation for '{pk_name}' in this release"
                )));
            }
            reject_explicit_sql_update_to_generated_field::<E>(assignment.field.as_str())?;
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
        let mut selector = Query::<E>::new(MissingRowPolicy::Ignore).filter(predicate);

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
        let SqlStatement::Insert(statement) = prepared.into_statement() else {
            return Err(QueryError::invariant(
                "INSERT SELECT source preparation must preserve INSERT statement ownership",
            ));
        };
        let SqlInsertSource::Select(select) = statement.source else {
            return Err(QueryError::invariant(
                "INSERT SELECT source execution requires prepared SELECT source",
            ));
        };
        let mut select = *select;
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
        let lowered = lower_sql_command_from_prepared_statement(prepared, E::MODEL)
            .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
            return Err(QueryError::invariant(
                "INSERT SELECT source lowering must stay on the scalar SELECT query lane",
            ));
        };

        let payload =
            self.execute_lowered_sql_projection_core(select, EntityAuthority::for_type::<E>())?;
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
        ensure_sql_write_entity_matches::<E>(statement.entity.as_str())?;
        let columns = sql_insert_columns::<E>(statement);
        ensure_sql_insert_required_fields::<E>(columns.as_slice())?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
        let source_rows = match &statement.source {
            SqlInsertSource::Values(values) => {
                ensure_sql_insert_value_tuples_match_columns(
                    columns.as_slice(),
                    values.as_slice(),
                )?;
                values.clone()
            }
            SqlInsertSource::Select(_) => {
                let source = Self::sql_insert_select_source_statement::<E>(statement)?;
                let rows = self.execute_sql_insert_select_source_rows::<E>(&source)?;
                ensure_sql_insert_selected_rows_match_columns(columns.as_slice(), rows.as_slice())?;

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

        Self::sql_write_statement_result(entities, statement.returning.as_ref())
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_update_statement<E>(
        &self,
        statement: &SqlUpdateStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        ensure_sql_write_entity_matches::<E>(statement.entity.as_str())?;
        let selector = Self::sql_update_selector_query::<E>(statement)?;
        let patch = Self::sql_update_patch::<E>(statement)?;
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

        Self::sql_write_statement_result(entities, statement.returning.as_ref())
    }

    fn execute_typed_sql_delete_projection<E>(
        &self,
        query: &Query<E>,
    ) -> Result<SqlProjectionPayload, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;
        let deleted = self
            .with_metrics(|| {
                self.delete_executor::<E>()
                    .execute_structural_projection(plan)
            })
            .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows).map_err(QueryError::execute)?;

        Ok(SqlProjectionPayload::new(
            projection_labels_from_fields(E::MODEL.fields()),
            vec![None; E::MODEL.fields().len()],
            rows,
            row_count,
        ))
    }

    fn execute_typed_sql_delete_count<E>(
        &self,
        query: &Query<E>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let row_count = self.execute_delete_count(query)?;

        Ok(SqlStatementResult::Count { row_count })
    }

    fn execute_typed_sql_delete_returning<E>(
        &self,
        query: &Query<E>,
        returning: &SqlReturningProjection,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (columns, _, rows, row_count) = self
            .execute_typed_sql_delete_projection(query)?
            .into_parts();

        Self::sql_returning_statement_projection(columns, rows, row_count, returning)
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_delete_statement<E>(
        &self,
        delete: LoweredBaseQueryShape,
        statement: &crate::db::sql::parser::SqlDeleteStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let typed_query =
            bind_lowered_sql_query::<E>(LoweredSqlQuery::Delete(delete), MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)?;

        match &statement.returning {
            Some(returning) => self.execute_typed_sql_delete_returning(&typed_query, returning),
            None => self.execute_typed_sql_delete_count(&typed_query),
        }
    }
}
