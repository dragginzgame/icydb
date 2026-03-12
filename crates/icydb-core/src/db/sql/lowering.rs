//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::intent::Query,
        sql::parser::{
            SqlDeleteStatement, SqlExplainMode, SqlExplainStatement, SqlExplainTarget,
            SqlOrderDirection, SqlProjection, SqlSelectStatement, SqlStatement, parse_sql,
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
///

#[derive(Debug)]
pub(crate) enum SqlCommand<E: EntityKind> {
    Query(Query<E>),
    Explain {
        mode: SqlExplainMode,
        query: Query<E>,
    },
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

    #[error("SQL entity '{sql_entity}' does not match requested entity type '{expected_entity}'")]
    EntityMismatch {
        sql_entity: String,
        expected_entity: &'static str,
    },

    #[error("unsupported SQL SELECT projection in this release; only SELECT * is executable")]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT in this release")]
    UnsupportedSelectDistinct,

    #[error("unsupported SQL GROUP BY in this release")]
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
    }
}

fn lower_explain<E: EntityKind>(
    statement: SqlExplainStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let query = match statement.statement {
        SqlExplainTarget::Select(statement) => lower_select::<E>(statement, consistency)?,
        SqlExplainTarget::Delete(statement) => lower_delete::<E>(statement, consistency)?,
    };

    Ok(SqlCommand::Explain {
        mode: statement.mode,
        query,
    })
}

fn lower_select<E: EntityKind>(
    statement: SqlSelectStatement,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;

    // Keep executable SQL lowering intentionally narrow in this slice.
    if !matches!(statement.projection, SqlProjection::All) {
        return Err(SqlLoweringError::UnsupportedSelectProjection);
    }
    if statement.distinct {
        return Err(SqlLoweringError::UnsupportedSelectDistinct);
    }
    if !statement.group_by.is_empty() {
        return Err(SqlLoweringError::UnsupportedSelectGroupBy);
    }

    // Phase 1: predicate and deterministic ordering.
    let mut query = Query::new(consistency);
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

fn lower_delete<E: EntityKind>(
    statement: SqlDeleteStatement,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    ensure_entity_matches::<E>(statement.entity.as_str())?;

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

fn ensure_entity_matches<E: EntityKind>(sql_entity: &str) -> Result<(), SqlLoweringError> {
    let expected = E::MODEL.entity_name();
    if sql_entity.eq_ignore_ascii_case(expected) {
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
            predicate::MissingRowPolicy,
            query::plan::QueryMode,
            sql::{
                lowering::{SqlCommand, SqlLoweringError, compile_sql_command},
                parser::SqlExplainMode,
            },
        },
        model::field::FieldKind,
        types::Ulid,
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
    fn compile_sql_command_rejects_non_star_projection() {
        let err = compile_sql_command::<SqlLowerEntity>(
            "SELECT name FROM SqlLowerEntity",
            MissingRowPolicy::Ignore,
        )
        .expect_err("non-star projection should be rejected in this slice");

        assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
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
}
