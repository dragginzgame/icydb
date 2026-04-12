//! Module: db::session::sql::dispatch
//! Responsibility: session-owned SQL dispatch entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL dispatch classification and result packaging.

mod computed;
mod lowered;

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        data::UpdatePatch,
        executor::{EntityAuthority, MutationMode},
        identifiers_tail_match,
        query::{intent::StructuralQuery, plan::AccessPlannedQuery},
        schema::{ValidateError, field_type_from_model_kind, literal_matches_type},
        session::sql::{
            SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
            aggregate::parsed_requires_dedicated_sql_aggregate_lane,
            computed_projection,
            projection::{
                SqlProjectionPayload, execute_sql_projection_rows_for_canister,
                execute_sql_projection_text_rows_for_canister, projection_labels_from_fields,
                projection_labels_from_projection_spec, sql_projection_rows_from_kernel_rows,
            },
        },
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSelectShape, LoweredSqlQuery, SqlLoweringError,
            bind_lowered_sql_query, canonicalize_sql_predicate_for_model,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlInsertSource, SqlInsertStatement,
            SqlOrderDirection, SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement,
            SqlStatement, SqlTextFunction, SqlUpdateStatement,
        },
    },
    model::{
        entity::resolve_field_slot,
        field::{FieldInsertGeneration, FieldKind, FieldModel},
    },
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityKind, EntityValue},
    types::{Timestamp, Ulid},
    value::Value,
};

#[cfg(feature = "perf-attribution")]
pub use lowered::LoweredSqlDispatchExecutorAttribution;

///
/// GeneratedSqlDispatchAttempt
///
/// Hidden generated-query dispatch envelope used by the facade helper to keep
/// generated route ownership in core while preserving the public EXPLAIN error
/// rewrite contract at the outer boundary.
///

#[doc(hidden)]
pub struct GeneratedSqlDispatchAttempt {
    entity_name: &'static str,
    explain_order_field: Option<&'static str>,
    result: Result<SqlDispatchResult, QueryError>,
}

impl GeneratedSqlDispatchAttempt {
    // Build one generated-query dispatch attempt with optional explain-hint context.
    const fn new(
        entity_name: &'static str,
        explain_order_field: Option<&'static str>,
        result: Result<SqlDispatchResult, QueryError>,
    ) -> Self {
        Self {
            entity_name,
            explain_order_field,
            result,
        }
    }

    /// Borrow the resolved entity name for this generated-query attempt.
    #[must_use]
    pub const fn entity_name(&self) -> &'static str {
        self.entity_name
    }

    /// Borrow the suggested deterministic order field for EXPLAIN rewrites.
    #[must_use]
    pub const fn explain_order_field(&self) -> Option<&'static str> {
        self.explain_order_field
    }

    /// Consume and return the generated-query dispatch result.
    pub fn into_result(self) -> Result<SqlDispatchResult, QueryError> {
        self.result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlGroupingSurface {
    Scalar,
    Grouped,
}

const fn unsupported_sql_grouping_message(surface: SqlGroupingSurface) -> &'static str {
    match surface {
        SqlGroupingSurface::Scalar => {
            "execute_sql rejects grouped SELECT; use execute_sql_grouped(...)"
        }
        SqlGroupingSurface::Grouped => "execute_sql_grouped requires grouped SQL query intent",
    }
}

// Enforce the generated canister query contract that empty SQL is unsupported
// before any parser/lowering work occurs.
fn trim_generated_query_sql_input(sql: &str) -> Result<&str, QueryError> {
    let sql_trimmed = sql.trim();
    if sql_trimmed.is_empty() {
        return Err(QueryError::unsupported_query(
            "query endpoint requires a non-empty SQL string",
        ));
    }

    Ok(sql_trimmed)
}

// Render the generated-surface entity list from the descriptor table instead
// of assuming every session-visible entity belongs on the public query export.
fn generated_sql_entities(authorities: &[EntityAuthority]) -> Vec<String> {
    let mut entities = Vec::with_capacity(authorities.len());

    for authority in authorities {
        entities.push(authority.model().name().to_string());
    }

    entities
}

// Project parsed SELECT items into one stable outward column contract while
// allowing parser-owned aliases to override only the final session label.
fn sql_projection_labels_from_select_statement(
    statement: &SqlStatement,
) -> Result<Option<Vec<String>>, QueryError> {
    let SqlStatement::Select(select) = statement else {
        return Err(QueryError::invariant(
            "SQL projection labels require SELECT statement shape",
        ));
    };
    let SqlProjection::Items(items) = &select.projection else {
        return Ok(None);
    };

    Ok(Some(
        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                select
                    .projection_alias(index)
                    .map_or_else(|| grouped_sql_projection_item_label(item), str::to_string)
            })
            .collect(),
    ))
}

// Render one grouped SELECT item into the public grouped-column label used by
// unified dispatch results.
fn grouped_sql_projection_item_label(item: &SqlSelectItem) -> String {
    match item {
        SqlSelectItem::Field(field) => field.clone(),
        SqlSelectItem::Aggregate(aggregate) => grouped_sql_aggregate_call_label(aggregate),
        SqlSelectItem::TextFunction(call) => {
            format!(
                "{}({})",
                grouped_sql_text_function_name(call.function),
                call.field
            )
        }
    }
}

// Keep the dedicated SQL aggregate lane on parser-owned outward labels
// without reopening alias semantics in lowering or runtime strategy state.
fn sql_aggregate_dispatch_label_override(statement: &SqlStatement) -> Option<String> {
    let SqlStatement::Select(select) = statement else {
        return None;
    };

    select.projection_alias(0).map(str::to_string)
}

// Render one aggregate call into one canonical SQL-style label.
fn grouped_sql_aggregate_call_label(aggregate: &SqlAggregateCall) -> String {
    let kind = match aggregate.kind {
        SqlAggregateKind::Count => "COUNT",
        SqlAggregateKind::Sum => "SUM",
        SqlAggregateKind::Avg => "AVG",
        SqlAggregateKind::Min => "MIN",
        SqlAggregateKind::Max => "MAX",
    };

    match aggregate.field.as_deref() {
        Some(field) => format!("{kind}({field})"),
        None => format!("{kind}(*)"),
    }
}

// Render one reduced SQL text-function identifier into one stable uppercase
// SQL label for outward column metadata.
const fn grouped_sql_text_function_name(function: SqlTextFunction) -> &'static str {
    match function {
        SqlTextFunction::Trim => "TRIM",
        SqlTextFunction::Ltrim => "LTRIM",
        SqlTextFunction::Rtrim => "RTRIM",
        SqlTextFunction::Lower => "LOWER",
        SqlTextFunction::Upper => "UPPER",
        SqlTextFunction::Length => "LENGTH",
        SqlTextFunction::Left => "LEFT",
        SqlTextFunction::Right => "RIGHT",
        SqlTextFunction::StartsWith => "STARTS_WITH",
        SqlTextFunction::EndsWith => "ENDS_WITH",
        SqlTextFunction::Contains => "CONTAINS",
        SqlTextFunction::Position => "POSITION",
        SqlTextFunction::Replace => "REPLACE",
        SqlTextFunction::Substring => "SUBSTRING",
    }
}

// Resolve one generated query route onto the descriptor-owned authority table.
fn authority_for_generated_sql_route(
    route: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<EntityAuthority, QueryError> {
    let sql_entity = route.entity();

    for authority in authorities {
        if identifiers_tail_match(sql_entity, authority.model().name()) {
            return Ok(*authority);
        }
    }

    Err(unsupported_generated_sql_entity_error(
        sql_entity,
        authorities,
    ))
}

// Keep the generated query-surface unsupported-entity contract stable while
// moving authority lookup out of the build-generated shim.
fn unsupported_generated_sql_entity_error(
    entity_name: &str,
    authorities: &[EntityAuthority],
) -> QueryError {
    let mut supported = String::new();

    for (index, authority) in authorities.iter().enumerate() {
        if index != 0 {
            supported.push_str(", ");
        }

        supported.push_str(authority.model().name());
    }

    QueryError::unsupported_query(format!(
        "query endpoint does not support entity '{entity_name}'; supported: {supported}"
    ))
}

// Keep typed SQL write routes on the same entity-match contract used by
// lowered query dispatch, without widening write statements into lowering.
fn ensure_sql_write_entity_matches<E>(sql_entity: &str) -> Result<(), QueryError>
where
    E: EntityKind,
{
    if identifiers_tail_match(sql_entity, E::MODEL.name()) {
        return Ok(());
    }

    Err(QueryError::from_sql_lowering_error(
        SqlLoweringError::EntityMismatch {
            sql_entity: sql_entity.to_string(),
            expected_entity: E::MODEL.name(),
        },
    ))
}

// Normalize one reduced-SQL primary-key literal onto the concrete entity key
// type accepted by the structural mutation entrypoint.
fn sql_write_key_from_literal<E>(value: &Value, pk_name: &str) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if let Some(key) = <E::Key as crate::traits::FieldValue>::from_value(value) {
        return Ok(key);
    }

    let widened = match value {
        Value::Int(v) if *v >= 0 => Value::Uint(v.cast_unsigned()),
        Value::Uint(v) if i64::try_from(*v).is_ok() => Value::Int(v.cast_signed()),
        _ => {
            return Err(QueryError::unsupported_query(format!(
                "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
            )));
        }
    };

    <E::Key as crate::traits::FieldValue>::from_value(&widened).ok_or_else(|| {
        QueryError::unsupported_query(format!(
            "SQL write primary key literal for '{pk_name}' is not compatible with entity key type"
        ))
    })
}

// Synthesize one generated SQL insert literal from the schema-owned runtime
// field contract instead of hard-coding generation at the SQL boundary.
fn sql_write_generated_field_value(field: &FieldModel) -> Option<Value> {
    field
        .insert_generation()
        .map(|generation| match generation {
            FieldInsertGeneration::Ulid => Value::Ulid(Ulid::generate()),
            FieldInsertGeneration::Timestamp => Value::Timestamp(Timestamp::now()),
        })
}

// Normalize one reduced-SQL write literal onto the target entity field kind
// when the parser's numeric literal domain is narrower than the runtime field.
fn sql_write_value_for_field<E>(field_name: &str, value: &Value) -> Result<Value, QueryError>
where
    E: EntityKind,
{
    let field_slot = resolve_field_slot(E::MODEL, field_name).ok_or_else(|| {
        QueryError::invariant("SQL write field must resolve against the target entity model")
    })?;
    let field_kind = E::MODEL.fields()[field_slot].kind();

    let normalized = match (field_kind, value) {
        (FieldKind::Uint, Value::Int(v)) if *v >= 0 => Value::Uint(v.cast_unsigned()),
        (FieldKind::Int, Value::Uint(v)) if i64::try_from(*v).is_ok() => {
            Value::Int(v.cast_signed())
        }
        _ => value.clone(),
    };

    let field_type = field_type_from_model_kind(&field_kind);
    if !literal_matches_type(&normalized, &field_type) {
        return Err(QueryError::unsupported_query(
            ValidateError::invalid_literal(field_name, "literal type does not match field type")
                .to_string(),
        ));
    }

    Ok(normalized)
}

// Reject explicit user-authored writes to schema-managed fields so reduced SQL
// does not silently accept values that the write boundary will overwrite.
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

// Reject explicit user-authored INSERT values for schema-generated fields so
// reduced SQL keeps generated insert ownership on the server side.
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

// Reject explicit user-authored UPDATE assignments to insert-generated fields
// so system-owned generation remains immutable after creation.
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

// Determine whether one field may be omitted from reduced SQL INSERT because
// the write lane owns its value synthesis contract.
fn sql_insert_field_is_omittable(field: &FieldModel) -> bool {
    if sql_write_generated_field_value(field).is_some() {
        return true;
    }

    field.write_management().is_some()
}

// Reject explicit INSERT column lists that omit non-generated user fields so
// reduced SQL does not silently consume typed-Rust defaults.
fn validate_sql_insert_required_fields<E>(columns: &[String]) -> Result<(), QueryError>
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

// Resolve the effective INSERT column list for one reduced SQL write:
// explicit column lists pass through, while omitted-column-list INSERT uses
// canonical user-authored model field order and leaves hidden timestamp
// synthesis on the existing write path.
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

// Resolve the effective INSERT column list for one reduced SQL write:
// explicit column lists pass through, while omitted-column-list INSERT uses
// canonical user-authored model field order and leaves hidden timestamp
// synthesis on the existing write path.
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

// Validate one INSERT tuple list against the resolved effective column list so
// every VALUES tuple stays full-width and deterministic.
fn validate_sql_insert_value_tuple_lengths(
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

// Validate one projected `INSERT ... SELECT` row set against the resolved
// effective column list so replayed structural inserts stay deterministic.
fn validate_sql_insert_selected_rows(
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
    // Project one typed SQL write after-image into one outward SQL row using
    // the persisted model field order.
    fn sql_write_dispatch_row<E>(entity: E) -> Result<Vec<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let mut row = Vec::with_capacity(E::MODEL.fields().len());

        for index in 0..E::MODEL.fields().len() {
            let value = entity.get_value_by_index(index).ok_or_else(|| {
                QueryError::invariant(
                    "SQL write dispatch projection row must include every declared field",
                )
            })?;
            row.push(value);
        }

        Ok(row)
    }

    // Render one or more typed entities returned by SQL write dispatch as one
    // projection payload so write statements reuse the same outward result
    // family as row-producing SELECT and DELETE dispatch.
    fn sql_write_dispatch_projection<E>(entities: Vec<E>) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let columns = projection_labels_from_fields(E::MODEL.fields());
        let rows = entities
            .into_iter()
            .map(Self::sql_write_dispatch_row)
            .collect::<Result<Vec<_>, _>>()?;
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(SqlDispatchResult::Projection {
            columns,
            rows,
            row_count,
        })
    }

    // Build the structural insert patch and resolved primary key expected by
    // the shared structural mutation entrypoint.
    fn sql_insert_patch_and_key<E>(
        columns: &[String],
        values: &[Value],
    ) -> Result<(E::Key, UpdatePatch), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: resolve the required primary-key literal from the explicit
        // INSERT column/value list, or synthesize one schema-generated value
        // when the target field contract admits omission on insert.
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

        // Phase 2: lower the explicit column/value pairs onto the structural
        // patch program consumed by the shared save path.
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

    // Build the structural update patch shared by every row selected by one
    // reduced SQL UPDATE statement.
    fn sql_update_patch<E>(statement: &SqlUpdateStatement) -> Result<UpdatePatch, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: lower the `SET` list onto the structural patch program
        // while keeping primary-key mutation out of the reduced SQL write lane.
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

    // Resolve one deterministic typed selector query for reduced SQL UPDATE.
    fn sql_update_selector_query<E>(statement: &SqlUpdateStatement) -> Result<Query<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: keep the widened SQL UPDATE lane explicit about requiring
        // one admitted reduced predicate instead of opening bare full-table
        // updates implicitly.
        let Some(predicate) = statement.predicate.clone() else {
            return Err(QueryError::unsupported_query(
                "SQL UPDATE requires WHERE predicate in this release",
            ));
        };
        let predicate = canonicalize_sql_predicate_for_model(E::MODEL, predicate);
        let pk_name = E::MODEL.primary_key.name;
        let mut selector = Query::<E>::new(MissingRowPolicy::Ignore).filter(predicate);

        // Phase 2: honor one explicit ordered update window when present, and
        // otherwise keep the write target set deterministic on primary-key
        // order exactly as the earlier predicate-only update lane did.
        if statement.order_by.is_empty() {
            selector = selector.order_by(pk_name);
        } else {
            let mut orders_primary_key = false;

            for term in &statement.order_by {
                if term.field == pk_name {
                    orders_primary_key = true;
                }
                selector = match term.direction {
                    SqlOrderDirection::Asc => selector.order_by(term.field.as_str()),
                    SqlOrderDirection::Desc => selector.order_by_desc(term.field.as_str()),
                };
            }

            if !orders_primary_key {
                selector = selector.order_by(pk_name);
            }
        }

        // Phase 3: apply the bounded update window on top of the deterministic
        // selector order before mutation replay begins.
        if let Some(limit) = statement.limit {
            selector = selector.limit(limit);
        }
        if let Some(offset) = statement.offset {
            selector = selector.offset(offset);
        }

        Ok(selector)
    }

    // Validate and normalize the admitted `INSERT ... SELECT` source shape
    // without widening the write lane into grouped, aggregate, or computed
    // projection ownership.
    fn sql_insert_select_source_statement<E>(
        statement: &SqlInsertStatement,
    ) -> Result<SqlSelectStatement, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let SqlInsertSource::Select(select) = statement.source.clone() else {
            return Err(QueryError::invariant(
                "INSERT SELECT source validation requires parsed SELECT source",
            ));
        };
        let mut select = *select;
        ensure_sql_write_entity_matches::<E>(select.entity.as_str())?;

        if !select.group_by.is_empty() || !select.having.is_empty() {
            return Err(QueryError::unsupported_query(
                "SQL INSERT SELECT requires scalar SELECT source in this release",
            ));
        }

        if let SqlProjection::Items(items) = &select.projection {
            for item in items {
                if matches!(item, SqlSelectItem::Aggregate(_)) {
                    return Err(QueryError::unsupported_query(
                        "SQL INSERT SELECT does not support aggregate source projection in this release",
                    ));
                }
            }
        }

        let pk_name = E::MODEL.primary_key.name;
        if select.order_by.is_empty() || !select.order_by.iter().any(|term| term.field == pk_name) {
            select.order_by.push(SqlOrderTerm {
                field: pk_name.to_string(),
                direction: SqlOrderDirection::Asc,
            });
        }

        Ok(select)
    }

    // Execute one admitted `INSERT ... SELECT` source query through the
    // existing scalar SQL projection lane and return the projected value rows
    // that will later feed the ordinary structural insert replay.
    fn execute_sql_insert_select_source_rows<E>(
        &self,
        source: &SqlSelectStatement,
    ) -> Result<Vec<Vec<Value>>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: reuse the already-shipped scalar computed-projection lane
        // when the source SELECT widens beyond plain fields but still fits the
        // admitted session-owned text projection contract.
        if let Some(plan) = computed_projection::computed_sql_projection_plan(
            &SqlStatement::Select(source.clone()),
        )? {
            let result = self.execute_computed_sql_projection_dispatch_for_authority(
                plan,
                EntityAuthority::for_type::<E>(),
            )?;

            return match result {
                SqlDispatchResult::Projection { rows, .. } => Ok(rows),
                other => Err(QueryError::invariant(format!(
                    "INSERT SELECT computed source must produce projection rows, found {other:?}",
                ))),
            };
        }

        // Phase 2: keep the ordinary field-only source path on the shared
        // scalar SQL projection lane.
        let prepared = prepare_sql_statement(SqlStatement::Select(source.clone()), E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?;
        let lowered =
            lower_sql_command_from_prepared_statement(prepared, E::MODEL.primary_key.name)
                .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
            return Err(QueryError::invariant(
                "INSERT SELECT source lowering must stay on the scalar SELECT query lane",
            ));
        };

        let payload =
            self.execute_lowered_sql_projection_core(select, EntityAuthority::for_type::<E>())?;
        let (_, rows, _) = payload.into_parts();

        Ok(rows)
    }

    // Execute one narrow SQL INSERT statement through the existing structural
    // mutation path and project the returned after-image as one SQL row.
    fn execute_sql_insert_dispatch<E>(
        &self,
        statement: &SqlInsertStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        ensure_sql_write_entity_matches::<E>(statement.entity.as_str())?;
        let columns = sql_insert_columns::<E>(statement);
        validate_sql_insert_required_fields::<E>(columns.as_slice())?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Insert, Timestamp::now());
        let source_rows = match &statement.source {
            SqlInsertSource::Values(values) => {
                validate_sql_insert_value_tuple_lengths(columns.as_slice(), values.as_slice())?;
                values.clone()
            }
            SqlInsertSource::Select(_) => {
                let source = Self::sql_insert_select_source_statement::<E>(statement)?;
                let rows = self.execute_sql_insert_select_source_rows::<E>(&source)?;
                validate_sql_insert_selected_rows(columns.as_slice(), rows.as_slice())?;

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

        Self::sql_write_dispatch_projection(entities)
    }

    // Execute one reduced SQL UPDATE statement by selecting deterministic
    // target rows first and then replaying one shared structural patch onto
    // each matched primary key.
    fn execute_sql_update_dispatch<E>(
        &self,
        statement: &SqlUpdateStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        ensure_sql_write_entity_matches::<E>(statement.entity.as_str())?;
        let selector = Self::sql_update_selector_query::<E>(statement)?;
        let patch = Self::sql_update_patch::<E>(statement)?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
        let matched = self.execute_query(&selector)?;
        let mut entities = Vec::with_capacity(matched.len());

        // Phase 1: apply the already-normalized structural patch to every
        // matched row in deterministic primary-key order.
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

        Self::sql_write_dispatch_projection(entities)
    }

    // Build the shared structural SQL projection execution inputs once so
    // value-row and rendered-row dispatch surfaces only differ in final packaging.
    fn prepare_structural_sql_projection_execution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(Vec<String>, AccessPlannedQuery), QueryError> {
        // Phase 1: build the structural access plan once and freeze its outward
        // column contract for all projection materialization surfaces.
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(query, authority)?;
        let projection = plan.projection_spec(authority.model());
        let columns = projection_labels_from_projection_spec(&projection);

        Ok((columns, plan))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        // Phase 1: build the shared structural plan and outward column contract once.
        let (columns, plan) = self.prepare_structural_sql_projection_execution(query, authority)?;

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics.
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    // Execute one structural SQL load query and return render-ready text rows
    // for the dispatch lane when the terminal short path can prove them
    // directly.
    fn execute_structural_sql_projection_text(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        // Phase 1: build the shared structural plan and outward column contract once.
        let (columns, plan) = self.prepare_structural_sql_projection_execution(query, authority)?;

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics while preferring rendered SQL rows.
        let projected =
            execute_sql_projection_text_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlDispatchResult::ProjectionText {
            columns,
            rows,
            row_count,
        })
    }

    // Execute one typed SQL delete query while keeping the row payload on the
    // typed delete executor boundary that still owns non-runtime-hook delete
    // commit-window application.
    fn execute_typed_sql_delete<E>(&self, query: &Query<E>) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();
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
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    // Lower one parsed SQL query/explain route once for one resolved authority
    // and preserve grouped-column metadata for grouped SELECT dispatch.
    fn lowered_sql_query_dispatch_inputs_for_authority(
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
        unsupported_message: &'static str,
    ) -> Result<(LoweredSqlQuery, Option<Vec<String>>), QueryError> {
        let lowered = parsed.lower_query_lane_for_entity(
            authority.model().name(),
            authority.model().primary_key.name,
        )?;
        let projection_columns = matches!(lowered.query(), Some(LoweredSqlQuery::Select(_)))
            .then(|| sql_projection_labels_from_select_statement(&parsed.statement))
            .transpose()?;
        let query = lowered
            .into_query()
            .ok_or_else(|| QueryError::unsupported_query(unsupported_message))?;

        Ok((query, projection_columns.flatten()))
    }

    // Execute one parsed SQL query route through the shared aggregate,
    // computed-projection, and lowered query lane so typed and generated
    // dispatch only differ at the final SELECT/DELETE packaging boundary.
    fn dispatch_sql_query_route_for_authority(
        &self,
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
        unsupported_message: &'static str,
        dispatch_select: impl FnOnce(
            &Self,
            LoweredSelectShape,
            EntityAuthority,
            bool,
            Option<Vec<String>>,
        ) -> Result<SqlDispatchResult, QueryError>,
        dispatch_delete: impl FnOnce(
            &Self,
            LoweredBaseQueryShape,
            EntityAuthority,
        ) -> Result<SqlDispatchResult, QueryError>,
    ) -> Result<SqlDispatchResult, QueryError> {
        // Phase 1: keep aggregate and computed projection classification on the
        // shared parsed route so both dispatch surfaces honor the same lane split.
        if parsed_requires_dedicated_sql_aggregate_lane(parsed) {
            let command =
                Self::compile_sql_aggregate_command_core_for_authority(parsed, authority)?;

            return self.execute_sql_aggregate_dispatch_for_authority(
                command,
                authority,
                sql_aggregate_dispatch_label_override(&parsed.statement),
            );
        }

        if let Some(plan) = computed_projection::computed_sql_projection_plan(&parsed.statement)? {
            return self.execute_computed_sql_projection_dispatch_for_authority(plan, authority);
        }

        // Phase 2: lower the remaining query route once, then let the caller
        // decide only the final outward result packaging.
        let (query, projection_columns) = Self::lowered_sql_query_dispatch_inputs_for_authority(
            parsed,
            authority,
            unsupported_message,
        )?;
        let grouped_surface = query.has_grouping();

        match query {
            LoweredSqlQuery::Select(select) => {
                dispatch_select(self, select, authority, grouped_surface, projection_columns)
            }
            LoweredSqlQuery::Delete(delete) => dispatch_delete(self, delete, authority),
        }
    }

    // Execute one parsed SQL EXPLAIN route through the shared computed-
    // projection and lowered explain lanes so typed and generated dispatch do
    // not duplicate the same explain classification tree.
    fn dispatch_sql_explain_route_for_authority(
        &self,
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        // Phase 1: keep computed-projection explain ownership on the same
        // parsed route boundary as the shared query lane.
        if let Some((mode, plan)) =
            computed_projection::computed_sql_projection_explain_plan(&parsed.statement)?
        {
            return self
                .explain_computed_sql_projection_dispatch_for_authority(mode, plan, authority)
                .map(SqlDispatchResult::Explain);
        }

        // Phase 2: lower once for execution/logical explain and preserve the
        // shared execution-first fallback policy across both callers.
        let lowered = parsed.lower_query_lane_for_entity(
            authority.model().name(),
            authority.model().primary_key.name,
        )?;
        if let Some(explain) =
            self.explain_lowered_sql_execution_for_authority(&lowered, authority)?
        {
            return Ok(SqlDispatchResult::Explain(explain));
        }

        self.explain_lowered_sql_for_authority(&lowered, authority)
            .map(SqlDispatchResult::Explain)
    }

    // Validate that one SQL-derived query intent matches the grouped/scalar
    // execution surface that is about to consume it.
    pub(in crate::db::session::sql) fn ensure_sql_query_grouping<E>(
        query: &Query<E>,
        surface: SqlGroupingSurface,
    ) -> Result<(), QueryError>
    where
        E: EntityKind,
    {
        match (surface, query.has_grouping()) {
            (SqlGroupingSurface::Scalar, false) | (SqlGroupingSurface::Grouped, true) => Ok(()),
            (SqlGroupingSurface::Scalar, true) | (SqlGroupingSurface::Grouped, false) => Err(
                QueryError::unsupported_query(unsupported_sql_grouping_message(surface)),
            ),
        }
    }

    /// Execute one reduced SQL statement into one unified SQL dispatch payload.
    pub fn execute_sql_dispatch<E>(&self, sql: &str) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        self.execute_sql_dispatch_parsed::<E>(&parsed)
    }

    /// Execute one parsed reduced SQL statement into one unified SQL payload.
    pub fn execute_sql_dispatch_parsed<E>(
        &self,
        parsed: &SqlParsedStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => self.dispatch_sql_query_route_for_authority(
                parsed,
                EntityAuthority::for_type::<E>(),
                "execute_sql_dispatch accepts SELECT or DELETE only",
                |session, select, authority, grouped_surface, projection_columns| {
                    if grouped_surface {
                        let columns = projection_columns.ok_or_else(|| {
                            QueryError::unsupported_query(
                                "grouped SQL dispatch requires explicit grouped projection items",
                            )
                        })?;

                        return session.execute_lowered_sql_grouped_dispatch_select_core(
                            select, authority, columns,
                        );
                    }

                    let payload = session.execute_lowered_sql_projection_core(select, authority)?;
                    if let Some(columns) = projection_columns {
                        let (_, rows, row_count) = payload.into_parts();

                        return Ok(SqlProjectionPayload::new(columns, rows, row_count)
                            .into_dispatch_result());
                    }

                    Ok(payload.into_dispatch_result())
                },
                |session, delete, _authority| {
                    let typed_query = bind_lowered_sql_query::<E>(
                        LoweredSqlQuery::Delete(delete),
                        MissingRowPolicy::Ignore,
                    )
                    .map_err(QueryError::from_sql_lowering_error)?;

                    session.execute_typed_sql_delete(&typed_query)
                },
            ),
            SqlStatementRoute::Insert { .. } => {
                let SqlStatement::Insert(statement) = &parsed.statement else {
                    return Err(QueryError::invariant(
                        "INSERT SQL route must carry parsed INSERT statement",
                    ));
                };

                self.execute_sql_insert_dispatch::<E>(statement)
            }
            SqlStatementRoute::Update { .. } => {
                let SqlStatement::Update(statement) = &parsed.statement else {
                    return Err(QueryError::invariant(
                        "UPDATE SQL route must carry parsed UPDATE statement",
                    ));
                };

                self.execute_sql_update_dispatch::<E>(statement)
            }
            SqlStatementRoute::Explain { .. } => self
                .dispatch_sql_explain_route_for_authority(parsed, EntityAuthority::for_type::<E>()),
            SqlStatementRoute::Describe { .. } => {
                Ok(SqlDispatchResult::Describe(self.describe_entity::<E>()))
            }
            SqlStatementRoute::ShowIndexes { .. } => {
                Ok(SqlDispatchResult::ShowIndexes(self.show_indexes::<E>()))
            }
            SqlStatementRoute::ShowColumns { .. } => {
                Ok(SqlDispatchResult::ShowColumns(self.show_columns::<E>()))
            }
            SqlStatementRoute::ShowEntities => {
                Ok(SqlDispatchResult::ShowEntities(self.show_entities()))
            }
        }
    }

    /// Execute one parsed reduced SQL statement through the generated canister
    /// query/explain surface for one already-resolved dynamic authority.
    ///
    /// This keeps the canister SQL facade on the same reduced SQL ownership
    /// boundary as typed dispatch without forcing the outer facade to reopen
    /// typed-generic routing just to preserve parity for computed projections.
    #[doc(hidden)]
    pub fn execute_generated_query_surface_dispatch_for_authority(
        &self,
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => self.dispatch_sql_query_route_for_authority(
                parsed,
                authority,
                "generated SQL query surface requires query or EXPLAIN statement lanes",
                |session, select, authority, grouped_surface, projection_columns| {
                    if grouped_surface {
                        let columns = projection_columns.ok_or_else(|| {
                            QueryError::unsupported_query(
                                "grouped SQL dispatch requires explicit grouped projection items",
                            )
                        })?;

                        return session
                            .execute_lowered_sql_grouped_dispatch_select_core(select, authority, columns);
                    }

                    let result =
                        session.execute_lowered_sql_dispatch_select_text_core(select, authority)?;
                    if let Some(columns) = projection_columns {
                        let SqlDispatchResult::ProjectionText {
                            rows, row_count, ..
                        } = result
                        else {
                            return Err(QueryError::invariant(
                                "generated scalar SQL dispatch text path must emit projection text rows",
                            ));
                        };

                        return Ok(SqlDispatchResult::ProjectionText {
                            columns,
                            rows,
                            row_count,
                        });
                    }

                    Ok(result)
                },
                |session, delete, authority| {
                    session.execute_lowered_sql_dispatch_delete_core(&delete, authority)
                },
            ),
            SqlStatementRoute::Explain { .. } => {
                self.dispatch_sql_explain_route_for_authority(parsed, authority)
            }
            SqlStatementRoute::Insert { .. } | SqlStatementRoute::Update { .. }
            | SqlStatementRoute::Describe { .. }
            | SqlStatementRoute::ShowIndexes { .. }
            | SqlStatementRoute::ShowColumns { .. }
            | SqlStatementRoute::ShowEntities => Err(QueryError::unsupported_query(
                "generated SQL query surface requires SELECT, DELETE, or EXPLAIN statement lanes",
            )),
        }
    }

    /// Execute one raw SQL string through the generated canister query surface.
    ///
    /// This hidden helper keeps parse, route, authority, and metadata/query
    /// dispatch ownership in core so the build-generated `sql_dispatch` shim
    /// stays close to a pure descriptor table plus public ABI wrapper.
    #[doc(hidden)]
    #[must_use]
    pub fn execute_generated_query_surface_sql(
        &self,
        sql: &str,
        authorities: &[EntityAuthority],
    ) -> GeneratedSqlDispatchAttempt {
        // Phase 1: normalize and parse once so every generated route family
        // shares the same SQL ownership boundary.
        let sql_trimmed = match trim_generated_query_sql_input(sql) {
            Ok(sql_trimmed) => sql_trimmed,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };
        let parsed = match self.parse_sql_statement(sql_trimmed) {
            Ok(parsed) => parsed,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };

        // Phase 2: keep SHOW ENTITIES descriptor-owned and resolve all other
        // generated routes against the emitted authority table exactly once.
        if matches!(parsed.route(), SqlStatementRoute::ShowEntities) {
            return GeneratedSqlDispatchAttempt::new(
                "",
                None,
                Ok(SqlDispatchResult::ShowEntities(generated_sql_entities(
                    authorities,
                ))),
            );
        }
        let authority = match authority_for_generated_sql_route(parsed.route(), authorities) {
            Ok(authority) => authority,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };

        // Phase 3: dispatch the resolved route through the existing query,
        // explain, and metadata helpers without rebuilding route ownership in
        // the generated build output.
        let entity_name = authority.model().name();
        let explain_order_field = parsed
            .route()
            .is_explain()
            .then_some(authority.model().primary_key.name);
        let result = match parsed.route() {
            SqlStatementRoute::Query { .. } | SqlStatementRoute::Explain { .. } => {
                self.execute_generated_query_surface_dispatch_for_authority(&parsed, authority)
            }
            SqlStatementRoute::Insert { .. } | SqlStatementRoute::Update { .. } => {
                Err(QueryError::unsupported_query(
                    "generated SQL query surface requires SELECT, DELETE, or EXPLAIN statement lanes",
                ))
            }
            SqlStatementRoute::Describe { .. } => Ok(SqlDispatchResult::Describe(
                self.describe_entity_model(authority.model()),
            )),
            SqlStatementRoute::ShowIndexes { .. } => Ok(SqlDispatchResult::ShowIndexes(
                self.show_indexes_for_store_model(authority.store_path(), authority.model()),
            )),
            SqlStatementRoute::ShowColumns { .. } => Ok(SqlDispatchResult::ShowColumns(
                self.show_columns_for_model(authority.model()),
            )),
            SqlStatementRoute::ShowEntities => unreachable!(
                "SHOW ENTITIES is handled before authority resolution for generated query dispatch"
            ),
        };

        GeneratedSqlDispatchAttempt::new(entity_name, explain_order_field, result)
    }
}
