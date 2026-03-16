//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate, rewrite_field_identifiers},
        query::{
            builder::aggregate::{avg, count, count_by, max_by, min_by, sum},
            intent::{Query, QueryError},
        },
        sql::identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlExplainMode,
            SqlExplainStatement, SqlExplainTarget, SqlOrderDirection, SqlProjection, SqlSelectItem,
            SqlSelectStatement, SqlShowIndexesStatement, SqlStatement, parse_sql,
        },
    },
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// SqlCommand
///
/// Lowered SQL command for one entity-typed query surface.
///
/// `Query` contains executable load/delete intent.
/// `Explain` wraps one executable query intent plus requested explain mode.
/// `ExplainGlobalAggregate` wraps one constrained global aggregate SQL command
/// plus requested explain mode.
///

#[derive(Debug)]
pub(crate) enum SqlCommand<E: EntityKind> {
    Query(Query<E>),
    Explain {
        mode: SqlExplainMode,
        query: Query<E>,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        command: SqlGlobalAggregateCommand<E>,
    },
    DescribeEntity,
    ShowIndexesEntity,
}

///
/// SqlGlobalAggregateTerminal
///
/// Global SQL aggregate terminals currently executable through dedicated
/// aggregate SQL entrypoints.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlGlobalAggregateTerminal {
    CountRows,
    CountField(String),
    SumField(String),
    AvgField(String),
    MinField(String),
    MaxField(String),
}

///
/// SqlGlobalAggregateCommand
///
/// Lowered global SQL aggregate command carrying base query shape plus terminal.
///

#[derive(Debug)]
pub(crate) struct SqlGlobalAggregateCommand<E: EntityKind> {
    query: Query<E>,
    terminal: SqlGlobalAggregateTerminal,
}

impl<E: EntityKind> SqlGlobalAggregateCommand<E> {
    /// Borrow the lowered base query shape for aggregate execution.
    #[must_use]
    pub(crate) const fn query(&self) -> &Query<E> {
        &self.query
    }

    /// Borrow the lowered aggregate terminal.
    #[must_use]
    pub(crate) const fn terminal(&self) -> &SqlGlobalAggregateTerminal {
        &self.terminal
    }
}

///
/// SqlLoweringError
///
/// SQL frontend lowering failures before planner validation/execution.
///

#[derive(Debug, ThisError)]
pub(crate) enum SqlLoweringError {
    #[error("{0}")]
    Parse(#[from] crate::db::sql::parser::SqlParseError),

    #[error("{0}")]
    Query(#[from] QueryError),

    #[error("SQL entity '{sql_entity}' does not match requested entity type '{expected_entity}'")]
    EntityMismatch {
        sql_entity: String,
        expected_entity: &'static str,
    },

    #[error(
        "unsupported SQL SELECT projection in this release; executable forms are SELECT *, direct field lists, or constrained grouped aggregate projection shapes"
    )]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT in this release")]
    UnsupportedSelectDistinct,

    #[error("unsupported SQL GROUP BY projection shape in this release")]
    UnsupportedSelectGroupBy,
}

/// Parse and lower one SQL statement into canonical query intent for `E`.
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = parse_sql(sql)?;
    lower_statement::<E>(statement, consistency)
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
pub(crate) fn compile_sql_global_aggregate_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = parse_sql(sql)?;
    lower_global_aggregate_statement::<E>(statement, consistency)
}

fn lower_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(SqlCommand::Query(lower_select::<E>(
            statement,
            consistency,
        )?)),
        SqlStatement::Delete(statement) => Ok(SqlCommand::Query(lower_delete::<E>(
            statement,
            consistency,
        )?)),
        SqlStatement::Explain(statement) => lower_explain::<E>(statement, consistency),
        SqlStatement::Describe(statement) => lower_describe::<E>(statement.entity.as_str()),
        SqlStatement::ShowIndexes(statement) => lower_show_indexes::<E>(statement),
    }
}

fn lower_describe<E: EntityKind>(sql_entity: &str) -> Result<SqlCommand<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(sql_entity)?;

    Ok(SqlCommand::DescribeEntity)
}

fn lower_show_indexes<E: EntityKind>(
    statement: SqlShowIndexesStatement,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;

    Ok(SqlCommand::ShowIndexesEntity)
}

fn lower_global_aggregate_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = statement else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };

    lower_global_aggregate_select::<E>(statement, consistency)
}

fn lower_explain<E: EntityKind>(
    statement: SqlExplainStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let mode = statement.mode;

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select::<E>(select_statement, mode, consistency)
        }
        SqlExplainTarget::Delete(delete_statement) => Ok(SqlCommand::Explain {
            mode,
            query: lower_delete::<E>(delete_statement, consistency)?,
        }),
    }
}

fn lower_explain_select<E: EntityKind>(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match lower_select::<E>(statement.clone(), consistency) {
        Ok(query) => Ok(SqlCommand::Explain { mode, query }),
        Err(SqlLoweringError::UnsupportedSelectProjection) => {
            let command = lower_global_aggregate_select::<E>(statement, consistency)?;

            Ok(SqlCommand::ExplainGlobalAggregate { mode, command })
        }
        Err(err) => Err(err),
    }
}

fn lower_global_aggregate_select<E: EntityKind>(
    mut statement: SqlSelectStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;
    let entity_scope = sql_entity_scope_candidates::<E>(statement.entity.as_str());
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());

    if statement.distinct {
        return Err(SqlLoweringError::UnsupportedSelectDistinct);
    }
    if !statement.group_by.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }

    let terminal = lower_global_aggregate_terminal(statement.projection)?;

    // Phase 1: lower base scalar query shape for terminal execution.
    let mut query = Query::new(consistency);
    if let Some(predicate) = statement.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, statement.order_by);

    // Phase 2: preserve effective window semantics for aggregate terminals.
    if let Some(limit) = statement.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = statement.offset {
        query = query.offset(offset);
    }

    Ok(SqlGlobalAggregateCommand { query, terminal })
}

fn lower_select<E: EntityKind>(
    mut statement: SqlSelectStatement,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;
    let entity_scope = sql_entity_scope_candidates::<E>(statement.entity.as_str());
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());

    // Phase 1: projection and predicate/order shaping.
    let mut query = Query::new(consistency);
    query = apply_group_by_fields(query, statement.group_by.as_slice())?;
    query = apply_scalar_distinct_flag::<E>(
        query,
        statement.distinct,
        &statement.projection,
        statement.group_by.as_slice(),
    )?;
    query = apply_projection(query, statement.projection, statement.group_by.as_slice())?;
    if let Some(predicate) = statement.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, statement.order_by);

    // Phase 2: page window clauses.
    if let Some(limit) = statement.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = statement.offset {
        query = query.offset(offset);
    }

    Ok(query)
}

fn apply_scalar_distinct_flag<E: EntityKind>(
    query: Query<E>,
    distinct: bool,
    projection: &SqlProjection,
    group_by_fields: &[String],
) -> Result<Query<E>, SqlLoweringError> {
    if !distinct {
        return Ok(query);
    }
    if !group_by_fields.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectDistinct);
    }

    match projection {
        SqlProjection::All => Ok(query.distinct()),
        SqlProjection::Items(items) => {
            if items
                .iter()
                .any(|item| matches!(item, SqlSelectItem::Aggregate(_)))
            {
                return Err(SqlLoweringError::UnsupportedSelectDistinct);
            }

            let has_primary_key_field = items.iter().any(|item| {
                matches!(
                    item,
                    SqlSelectItem::Field(field) if field == E::MODEL.primary_key.name
                )
            });
            if !has_primary_key_field {
                return Err(SqlLoweringError::UnsupportedSelectDistinct);
            }

            Ok(query.distinct())
        }
    }
}

fn lower_global_aggregate_terminal(
    projection: SqlProjection,
) -> Result<SqlGlobalAggregateTerminal, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };
    if items.len() != 1 {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    }

    let Some(SqlSelectItem::Aggregate(aggregate)) = items.into_iter().next() else {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    };

    match (aggregate.kind, aggregate.field) {
        (SqlAggregateKind::Count, None) => Ok(SqlGlobalAggregateTerminal::CountRows),
        (SqlAggregateKind::Count, Some(field)) => Ok(SqlGlobalAggregateTerminal::CountField(field)),
        (SqlAggregateKind::Sum, Some(field)) => Ok(SqlGlobalAggregateTerminal::SumField(field)),
        (SqlAggregateKind::Avg, Some(field)) => Ok(SqlGlobalAggregateTerminal::AvgField(field)),
        (SqlAggregateKind::Min, Some(field)) => Ok(SqlGlobalAggregateTerminal::MinField(field)),
        (SqlAggregateKind::Max, Some(field)) => Ok(SqlGlobalAggregateTerminal::MaxField(field)),
        _ => Err(SqlLoweringError::UnsupportedSelectProjection),
    }
}

fn apply_projection<E: EntityKind>(
    mut query: Query<E>,
    projection: SqlProjection,
    group_by_fields: &[String],
) -> Result<Query<E>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return apply_scalar_projection(query, projection);
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    };

    let mut projected_group_fields = Vec::<String>::new();
    let mut aggregate_calls = Vec::<SqlAggregateCall>::new();
    let mut seen_aggregate = false;

    for item in items {
        match item {
            SqlSelectItem::Field(field) => {
                // Keep grouped projection deterministic and mappable to grouped
                // response contracts: group keys must be declared first.
                if seen_aggregate {
                    return Err(SqlLoweringError::UnsupportedSelectGroupBy);
                }
                projected_group_fields.push(field);
            }
            SqlSelectItem::Aggregate(aggregate) => {
                seen_aggregate = true;
                aggregate_calls.push(aggregate);
            }
        }
    }

    if aggregate_calls.is_empty() || projected_group_fields.as_slice() != group_by_fields {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }

    for aggregate_call in aggregate_calls {
        query = query.aggregate(lower_aggregate_call(aggregate_call)?);
    }

    Ok(query)
}

fn apply_scalar_projection<E: EntityKind>(
    query: Query<E>,
    projection: SqlProjection,
) -> Result<Query<E>, SqlLoweringError> {
    match projection {
        SqlProjection::All => Ok(query),
        SqlProjection::Items(items) => {
            let has_aggregate = items
                .iter()
                .any(|item| matches!(item, SqlSelectItem::Aggregate(_)));
            let has_field = items
                .iter()
                .any(|item| matches!(item, SqlSelectItem::Field(_)));

            if has_aggregate && has_field {
                return Err(SqlLoweringError::UnsupportedSelectProjection);
            }

            if has_aggregate {
                return Err(SqlLoweringError::UnsupportedSelectProjection);
            }

            let mut fields = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    SqlSelectItem::Field(field) => fields.push(field),
                    SqlSelectItem::Aggregate(_) => {
                        return Err(SqlLoweringError::UnsupportedSelectProjection);
                    }
                }
            }

            Ok(query.select_fields(fields))
        }
    }
}

fn apply_group_by_fields<E: EntityKind>(
    mut query: Query<E>,
    group_by_fields: &[String],
) -> Result<Query<E>, SqlLoweringError> {
    for field in group_by_fields {
        query = query.group_by(field)?;
    }

    Ok(query)
}

fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<crate::db::query::builder::AggregateExpr, SqlLoweringError> {
    match (call.kind, call.field) {
        (SqlAggregateKind::Count, None) => Ok(count()),
        (SqlAggregateKind::Count, Some(field)) => Ok(count_by(field)),
        (SqlAggregateKind::Sum, Some(field)) => Ok(sum(field)),
        (SqlAggregateKind::Avg, Some(field)) => Ok(avg(field)),
        (SqlAggregateKind::Min, Some(field)) => Ok(min_by(field)),
        (SqlAggregateKind::Max, Some(field)) => Ok(max_by(field)),
        _ => Err(SqlLoweringError::UnsupportedSelectProjection),
    }
}

fn lower_delete<E: EntityKind>(
    mut statement: SqlDeleteStatement,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;
    let entity_scope = sql_entity_scope_candidates::<E>(statement.entity.as_str());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());

    let mut query = Query::new(consistency).delete();
    if let Some(predicate) = statement.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms(query, statement.order_by);
    if let Some(limit) = statement.limit {
        query = query.limit(limit);
    }

    Ok(query)
}

fn apply_order_terms<E: EntityKind>(
    mut query: Query<E>,
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
) -> Query<E> {
    for term in order_by {
        query = match term.direction {
            SqlOrderDirection::Asc => query.order_by(term.field),
            SqlOrderDirection::Desc => query.order_by_desc(term.field),
        };
    }

    query
}

// Build one identifier scope used for reducing SQL-qualified field references
// (`entity.field`, `schema.entity.field`) into canonical planner field names.
fn sql_entity_scope_candidates<E: EntityKind>(sql_entity: &str) -> Vec<String> {
    let mut out = Vec::new();
    out.push(sql_entity.to_string());
    out.push(E::MODEL.entity_name().to_string());

    if let Some(last) = identifier_last_segment(sql_entity) {
        out.push(last.to_string());
    }
    if let Some(last) = identifier_last_segment(E::MODEL.entity_name()) {
        out.push(last.to_string());
    }

    out
}

fn normalize_projection_identifiers(
    projection: SqlProjection,
    entity_scope: &[String],
) -> SqlProjection {
    match projection {
        SqlProjection::All => SqlProjection::All,
        SqlProjection::Items(items) => SqlProjection::Items(
            items
                .into_iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => {
                        SqlSelectItem::Field(normalize_identifier(field, entity_scope))
                    }
                    SqlSelectItem::Aggregate(aggregate) => {
                        SqlSelectItem::Aggregate(SqlAggregateCall {
                            kind: aggregate.kind,
                            field: aggregate
                                .field
                                .map(|field| normalize_identifier(field, entity_scope)),
                        })
                    }
                })
                .collect(),
        ),
    }
}

fn normalize_order_terms(
    terms: Vec<crate::db::sql::parser::SqlOrderTerm>,
    entity_scope: &[String],
) -> Vec<crate::db::sql::parser::SqlOrderTerm> {
    terms
        .into_iter()
        .map(|term| crate::db::sql::parser::SqlOrderTerm {
            field: normalize_identifier(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
}

fn normalize_identifier_list(fields: Vec<String>, entity_scope: &[String]) -> Vec<String> {
    fields
        .into_iter()
        .map(|field| normalize_identifier(field, entity_scope))
        .collect()
}

// SQL lowering only adapts identifier qualification (`entity.field` -> `field`)
// and delegates predicate-tree traversal ownership to `db::predicate`.
fn adapt_predicate_identifiers_to_scope(
    predicate: Predicate,
    entity_scope: &[String],
) -> Predicate {
    rewrite_field_identifiers(predicate, |field| normalize_identifier(field, entity_scope))
}

fn normalize_identifier(identifier: String, entity_scope: &[String]) -> String {
    normalize_identifier_to_scope(identifier, entity_scope)
}

fn ensure_entity_matches<E: EntityKind>(sql_entity: &str) -> Result<(), SqlLoweringError> {
    let expected = E::MODEL.entity_name();
    if identifiers_tail_match(sql_entity, expected) {
        return Ok(());
    }

    Err(SqlLoweringError::EntityMismatch {
        sql_entity: sql_entity.to_string(),
        expected_entity: expected,
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::ExecutablePlan,
            predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
            query::intent::Query,
            query::plan::{
                QueryMode,
                expr::{Expr, ProjectionField},
            },
            sql::{
                lowering::{
                    SqlCommand, SqlGlobalAggregateTerminal, SqlLoweringError, compile_sql_command,
                    compile_sql_global_aggregate_command,
                },
                parser::SqlExplainMode,
            },
        },
        model::field::FieldKind,
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct SqlLowerEntity {
        id: Ulid,
        name: String,
        age: u64,
    }

    crate::test_canister! {
        ident = SqlLowerCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = SqlLowerDataStore,
        canister = SqlLowerCanister,
    }

    crate::test_entity_schema! {
        ident = SqlLowerEntity,
        id = Ulid,
        entity_name = "SqlLowerEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text),
            ("age", FieldKind::Uint),
        ],
        indexes = [],
        store = SqlLowerDataStore,
        canister = SqlLowerCanister,
    }

    #[test]
    fn compile_sql_command_select_star_lowers_to_load_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 10 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT * should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_select_distinct_star_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT * FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT DISTINCT * should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "SELECT DISTINCT * should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_select_distinct_with_pk_field_list_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT id, age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SELECT DISTINCT with PK-projected field list should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "SELECT DISTINCT field-list including PK should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_rejects_select_distinct_without_pk_projection() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT DISTINCT age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
    }

    #[test]
    fn compile_sql_command_delete_lowers_to_delete_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "DELETE FROM SqlLowerEntity WHERE age < 18 ORDER BY age LIMIT 3",
            MissingRowPolicy::Ignore,
        )
        .expect("DELETE should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Delete(_)));
    }

    #[test]
    fn compile_sql_command_describe_lowers_to_describe_entity_lane() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "DESCRIBE public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("DESCRIBE should lower");

        assert!(
            matches!(command, SqlCommand::DescribeEntity),
            "DESCRIBE should lower to dedicated describe command lane",
        );
    }

    #[test]
    fn compile_sql_command_describe_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "DESCRIBE DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("DESCRIBE entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_command_show_indexes_lowers_to_show_indexes_lane() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SHOW INDEXES public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SHOW INDEXES should lower");

        assert!(
            matches!(command, SqlCommand::ShowIndexesEntity),
            "SHOW INDEXES should lower to dedicated show-indexes command lane",
        );
    }

    #[test]
    fn compile_sql_command_show_indexes_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SHOW INDEXES DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("SHOW INDEXES entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_command_explain_execution_wraps_lowered_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN EXECUTION SELECT * FROM SqlLowerEntity LIMIT 1",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN EXECUTION should lower");

        let SqlCommand::Explain { mode, query } = command else {
            panic!("expected lowered explain command");
        };

        assert_eq!(mode, SqlExplainMode::Execution);
        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_explain_select_distinct_star_lowers_to_distinct_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT DISTINCT * FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN SELECT DISTINCT * should lower");

        let SqlCommand::Explain { mode, query } = command else {
            panic!("expected lowered explain command");
        };
        assert_eq!(mode, SqlExplainMode::Plan);
        assert!(
            query
                .explain()
                .expect("distinct explain should build")
                .distinct(),
            "EXPLAIN SELECT DISTINCT * should preserve scalar distinct intent",
        );
    }

    #[test]
    fn compile_sql_command_explain_select_distinct_without_pk_projection_rejects() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT DISTINCT age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("EXPLAIN SELECT DISTINCT without PK projection should fail closed");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
    }

    #[test]
    fn compile_sql_command_explain_global_aggregate_lowers_to_dedicated_command() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "EXPLAIN SELECT COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("EXPLAIN global aggregate SQL should lower");

        let SqlCommand::ExplainGlobalAggregate { mode, command } = command else {
            panic!("expected lowered explain global aggregate command");
        };

        assert_eq!(mode, SqlExplainMode::Plan);
        assert!(
            matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
            "global aggregate EXPLAIN should preserve aggregate terminal lowering",
        );
    }

    #[test]
    fn compile_sql_command_select_field_projection_lowers_to_scalar_field_selection() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, age FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("field-list projection should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        let projection = query
            .plan()
            .expect("field-list plan should build")
            .projection_spec();
        let field_names = projection
            .fields()
            .map(|field| match field {
                ProjectionField::Scalar {
                    expr: Expr::Field(field),
                    alias: None,
                } => field.as_str().to_string(),
                other @ ProjectionField::Scalar { .. } => {
                    panic!(
                        "scalar field-list projection should lower to plain field exprs: {other:?}"
                    )
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
    }

    #[test]
    fn compile_sql_command_select_table_qualified_fields_parity_matches_unqualified_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT SqlLowerEntity.name, SqlLowerEntity.age \
             FROM SqlLowerEntity \
             WHERE SqlLowerEntity.age >= 21 \
             ORDER BY SqlLowerEntity.age DESC LIMIT 5 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified field-list SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .select_fields(["name", "age"])
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(5)
            .offset(1);

        assert_eq!(
            sql_query
                .plan()
                .expect("qualified SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("unqualified fluent plan should build")
                .into_inner(),
            "qualified SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
        );
    }

    #[test]
    fn compile_sql_command_qualified_nested_predicate_matches_unqualified_fluent_intent() {
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM SqlLowerEntity \
             WHERE (SqlLowerEntity.age >= 21 OR SqlLowerEntity.name = 'Ada') \
             AND NOT (SqlLowerEntity.name = 'Bob')",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified nested-predicate SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query =
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::And(vec![
                Predicate::Or(vec![
                    Predicate::Compare(ComparePredicate::with_coercion(
                        "age",
                        CompareOp::Gte,
                        Value::Int(21),
                        CoercionId::NumericWiden,
                    )),
                    Predicate::Compare(ComparePredicate::eq(
                        "name".to_string(),
                        Value::Text("Ada".to_string()),
                    )),
                ]),
                Predicate::Not(Box::new(Predicate::Compare(ComparePredicate::eq(
                    "name".to_string(),
                    Value::Text("Bob".to_string()),
                )))),
            ]));

        assert_eq!(
            sql_query
                .plan()
                .expect("qualified nested-predicate SQL plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("unqualified fluent nested-predicate plan should build")
                .into_inner(),
            "qualified nested predicate identifiers should normalize to the same canonical planned intent as unqualified fluent predicates",
        );
    }

    #[test]
    fn compile_sql_command_select_schema_qualified_entity_lowers_to_load_query() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM public.SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("schema-qualified entity SQL should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        assert!(matches!(query.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn compile_sql_command_rejects_global_aggregate_select_projection_in_current_slice() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("global aggregate projection should remain gated in this slice");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
    }

    #[test]
    fn compile_sql_command_rejects_mixed_scalar_and_aggregate_projection_in_current_slice() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, COUNT(*) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("mixed scalar+aggregate projection should remain gated in this slice");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
    }

    #[test]
    fn compile_sql_command_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
        let command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped aggregate projection should lower");

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };
        assert!(
            query.has_grouping(),
            "grouped aggregate SQL lowering should produce grouped query intent",
        );
    }

    #[test]
    fn compile_sql_command_select_grouped_qualified_identifiers_match_unqualified_intent() {
        let qualified_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT SqlLowerEntity.age, COUNT(*) \
             FROM public.SqlLowerEntity \
             WHERE SqlLowerEntity.age >= 21 \
             GROUP BY SqlLowerEntity.age \
             ORDER BY SqlLowerEntity.age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified grouped SQL query should lower");
        let unqualified_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("unqualified grouped SQL query should lower");

        let (SqlCommand::Query(qualified_query), SqlCommand::Query(unqualified_query)) =
            (qualified_command, unqualified_command)
        else {
            panic!("expected lowered grouped query commands");
        };

        assert_eq!(
            qualified_query
                .plan()
                .expect("qualified grouped SQL plan should build")
                .into_inner(),
            unqualified_query
                .plan()
                .expect("unqualified grouped SQL plan should build")
                .into_inner(),
            "qualified grouped SQL identifiers should normalize to the same canonical planned intent as unqualified grouped SQL",
        );
    }

    #[test]
    fn compile_sql_command_select_grouped_aggregate_parity_matches_query_and_executable_identity() {
        // Phase 1: lower equivalent grouped SQL and fluent grouped intents.
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT age, COUNT(*) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 3 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("grouped aggregate SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered grouped SQL query command");
        };
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .group_by("age")
            .expect("fluent grouped query should accept grouped field")
            .aggregate(crate::db::count())
            .order_by_desc("age")
            .limit(3)
            .offset(1);

        // Phase 2: assert canonical planned identity + fingerprint parity.
        let sql_compiled = sql_query.plan().expect("grouped SQL plan should build");
        let fluent_compiled = fluent_query
            .plan()
            .expect("fluent grouped plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "grouped SQL lowering and fluent grouped query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_query
                .plan_hash_hex()
                .expect("grouped SQL plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent grouped plan hash should build"),
            "equivalent grouped SQL and fluent grouped queries must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable =
            ExecutablePlan::from(sql_query.plan().expect("grouped SQL executable plan"));
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent grouped executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("grouped SQL execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent grouped execution strategy"),
            "equivalent grouped SQL and fluent grouped queries must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("grouped SQL execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent grouped execution ordering"),
            "equivalent grouped SQL and fluent grouped queries must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_command_select_field_projection_parity_matches_query_and_executable_identity() {
        // Phase 1: lower equivalent SQL and fluent field-list intents.
        let sql_command = compile_sql_command::<SqlLowerEntity>(
            "SELECT name, age FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 5 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("field-list SQL query should lower");
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .select_fields(["name", "age"])
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(5)
            .offset(1);

        // Phase 2: assert canonical planned identity + fingerprint parity.
        let sql_compiled = sql_query.plan().expect("SQL plan should build");
        let fluent_compiled = fluent_query.plan().expect("fluent plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "SQL field-list lowering and fluent field-list query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_query
                .plan_hash_hex()
                .expect("SQL field-list plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent field-list plan hash should build"),
            "equivalent SQL and fluent field-list projections must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable = ExecutablePlan::from(sql_query.plan().expect("SQL executable plan"));
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("SQL execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent execution strategy"),
            "equivalent SQL and fluent field-list projections must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("SQL execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent execution ordering"),
            "equivalent SQL and fluent field-list projections must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_command_rejects_entity_mismatch() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT * FROM DifferentEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("entity mismatch should fail lowering");

        assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
    }

    #[test]
    fn compile_sql_global_aggregate_command_count_star_lowers() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT COUNT(*) FROM SqlLowerEntity WHERE age >= 21",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate count SQL should lower");

        assert!(
            matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
            "COUNT(*) should lower to global count terminal",
        );
        assert!(
            !command.query().has_grouping(),
            "global aggregate SQL command should lower to scalar base query shape",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_count_sum_avg_min_max_lower() {
        let count_by_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT COUNT(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("COUNT(field) SQL should lower");
        let sum_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("SUM(field) SQL should lower");
        let avg_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT AVG(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("AVG(field) SQL should lower");
        let min_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT MIN(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("MIN(field) SQL should lower");
        let max_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT MAX(age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("MAX(field) SQL should lower");

        assert!(
            matches!(
                count_by_command.terminal(),
                SqlGlobalAggregateTerminal::CountField(field) if field == "age"
            ),
            "COUNT(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                sum_command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "SUM(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                avg_command.terminal(),
                SqlGlobalAggregateTerminal::AvgField(field) if field == "age"
            ),
            "AVG(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                min_command.terminal(),
                SqlGlobalAggregateTerminal::MinField(field) if field == "age"
            ),
            "MIN(field) should preserve field target in lowered terminal",
        );
        assert!(
            matches!(
                max_command.terminal(),
                SqlGlobalAggregateTerminal::MaxField(field) if field == "age"
            ),
            "MAX(field) should preserve field target in lowered terminal",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_qualified_field_lowers_to_unqualified_terminal() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(SqlLowerEntity.age) FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect("qualified global aggregate field SQL should lower");

        assert!(
            matches!(
                command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "qualified aggregate target fields should normalize to canonical unqualified field names",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_preserves_base_query_window_semantics() {
        let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate SQL command should lower");
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(2)
            .offset(1);

        assert_eq!(
            command
                .query()
                .plan()
                .expect("SQL global aggregate base query plan should build")
                .into_inner(),
            fluent_query
                .plan()
                .expect("fluent base query plan should build")
                .into_inner(),
            "global aggregate SQL lowering should preserve scalar base query predicate/order/window semantics",
        );
        assert_eq!(
            command
                .query()
                .plan_hash_hex()
                .expect("SQL global aggregate base query plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent base query plan hash should build"),
            "global aggregate SQL lowering should preserve deterministic base query fingerprint semantics",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_parity_matches_fluent_query_and_executable_identity() {
        // Phase 1: lower equivalent global aggregate SQL and fluent scalar base query intent.
        let sql_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
            "SELECT SUM(age) \
             FROM SqlLowerEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 3 OFFSET 1",
            MissingRowPolicy::Ignore,
        )
        .expect("global aggregate SQL should lower");
        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )))
            .order_by_desc("age")
            .limit(3)
            .offset(1);

        // Phase 2: assert aggregate-terminal contract and canonical planned identity + fingerprint parity.
        assert!(
            matches!(
                sql_command.terminal(),
                SqlGlobalAggregateTerminal::SumField(field) if field == "age"
            ),
            "global aggregate SQL SUM terminal should preserve canonical target field",
        );
        let sql_compiled = sql_command
            .query()
            .plan()
            .expect("global aggregate SQL base query plan should build");
        let fluent_compiled = fluent_query
            .plan()
            .expect("fluent scalar base query plan should build");
        assert_eq!(
            sql_compiled.into_inner(),
            fluent_compiled.into_inner(),
            "global aggregate SQL base query lowering and fluent scalar query must produce identical normalized planned intent",
        );
        assert_eq!(
            sql_command
                .query()
                .plan_hash_hex()
                .expect("global aggregate SQL base query plan hash should build"),
            fluent_query
                .plan_hash_hex()
                .expect("fluent scalar base query plan hash should build"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical fingerprints",
        );

        // Phase 3: assert executable-contract parity at route/runtime planning boundary.
        let sql_executable = ExecutablePlan::from(
            sql_command
                .query()
                .plan()
                .expect("global aggregate SQL base executable plan"),
        );
        let fluent_executable =
            ExecutablePlan::from(fluent_query.plan().expect("fluent scalar executable plan"));
        assert_eq!(sql_executable.mode(), fluent_executable.mode());
        assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
        assert_eq!(sql_executable.access(), fluent_executable.access());
        assert_eq!(
            sql_executable.consistency(),
            fluent_executable.consistency()
        );
        assert_eq!(
            sql_executable
                .execution_strategy()
                .expect("global aggregate SQL base execution strategy"),
            fluent_executable
                .execution_strategy()
                .expect("fluent scalar execution strategy"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable strategy",
        );
        assert_eq!(
            sql_executable
                .execution_ordering()
                .expect("global aggregate SQL base execution ordering"),
            fluent_executable
                .execution_ordering()
                .expect("fluent scalar execution ordering"),
            "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable ordering",
        );
    }

    #[test]
    fn compile_sql_global_aggregate_command_rejects_unsupported_shapes() {
        for sql in [
            "SELECT age FROM SqlLowerEntity",
            "SELECT COUNT(*), SUM(age) FROM SqlLowerEntity",
            "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        ] {
            let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
                sql,
                MissingRowPolicy::Ignore,
            )
            .expect_err("unsupported global aggregate SQL shape should fail closed");

            assert!(
                matches!(
                    err,
                    SqlLoweringError::UnsupportedSelectProjection
                        | SqlLoweringError::UnsupportedSelectGroupBy
                ),
                "unsupported global aggregate SQL shape should remain lowering-gated: {sql}",
            );
        }
    }
}
