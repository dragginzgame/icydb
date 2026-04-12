//! Module: db::session::sql::surface::route
//! Responsibility: classify lowered SQL command results into the outward SQL
//! route/result families returned by session dispatch entrypoints.
//! Does not own: SQL lane selection or executor/planner behavior.
//! Boundary: keeps outward SQL route taxonomy separate from execution internals.

use crate::db::{
    GroupedRow, QueryError,
    sql::lowering::{
        LoweredSqlCommand, LoweredSqlLaneKind, PreparedSqlStatement as CorePreparedSqlStatement,
        lower_sql_command_from_prepared_statement, lowered_sql_command_lane, prepare_sql_statement,
    },
    sql::parser::{SqlExplainTarget, SqlStatement},
};

/// Canonical SQL statement routing metadata derived from reduced SQL parser output.
///
/// Carries surface kind (`Query` / `Insert` / `Update` / `Explain` /
/// `Describe` / `ShowIndexes` / `ShowColumns` / `ShowEntities`) and canonical
/// parsed entity identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlStatementRoute {
    Query { entity: String },
    Insert { entity: String },
    Update { entity: String },
    Explain { entity: String },
    Describe { entity: String },
    ShowIndexes { entity: String },
    ShowColumns { entity: String },
    ShowEntities,
}

/// Unified SQL dispatch payload returned by shared SQL lane execution.
#[derive(Debug)]
pub enum SqlDispatchResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        rows: Vec<Vec<crate::value::Value>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

///
/// SqlParsedStatement
///
/// Opaque parsed SQL statement envelope with stable route metadata.
/// This allows callers to parse once and reuse parsed authority across route
/// classification and typed dispatch lowering.
///

#[derive(Clone, Debug)]
pub struct SqlParsedStatement {
    pub(in crate::db::session::sql) statement: SqlStatement,
    route: SqlStatementRoute,
}

impl SqlParsedStatement {
    // Construct one parsed SQL statement envelope inside the session SQL boundary.
    pub(in crate::db::session::sql) const fn new(
        statement: SqlStatement,
        route: SqlStatementRoute,
    ) -> Self {
        Self { statement, route }
    }

    /// Borrow canonical route metadata for this parsed statement.
    #[must_use]
    pub const fn route(&self) -> &SqlStatementRoute {
        &self.route
    }

    /// Return whether this parsed statement is a SQL mutation statement.
    #[must_use]
    pub const fn is_mutation(&self) -> bool {
        matches!(
            &self.statement,
            SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_)
        )
    }

    // Prepare this parsed statement for one concrete entity route.
    pub(in crate::db::session::sql) fn prepare(
        &self,
        expected_entity: &'static str,
    ) -> Result<CorePreparedSqlStatement, QueryError> {
        prepare_sql_statement(self.statement.clone(), expected_entity)
            .map_err(QueryError::from_sql_lowering_error)
    }

    /// Lower this parsed statement into one shared query-lane shape.
    #[inline(never)]
    pub fn lower_query_lane_for_entity(
        &self,
        expected_entity: &'static str,
        primary_key_field: &str,
    ) -> Result<LoweredSqlCommand, QueryError> {
        let lowered = lower_sql_command_from_prepared_statement(
            self.prepare(expected_entity)?,
            primary_key_field,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let lane = lowered_sql_command_lane(&lowered);

        match lane {
            LoweredSqlLaneKind::Query | LoweredSqlLaneKind::Explain => Ok(lowered),
            LoweredSqlLaneKind::Describe
            | LoweredSqlLaneKind::ShowIndexes
            | LoweredSqlLaneKind::ShowColumns
            | LoweredSqlLaneKind::ShowEntities => {
                Err(QueryError::unsupported_query_lane_dispatch())
            }
        }
    }
}

impl SqlStatementRoute {
    /// Borrow the parsed SQL entity identifier for this statement.
    ///
    /// `SHOW ENTITIES` does not carry an entity identifier and returns an
    /// empty string for this accessor.
    #[must_use]
    pub const fn entity(&self) -> &str {
        match self {
            Self::Query { entity }
            | Self::Insert { entity }
            | Self::Update { entity }
            | Self::Explain { entity }
            | Self::Describe { entity }
            | Self::ShowIndexes { entity }
            | Self::ShowColumns { entity } => entity.as_str(),
            Self::ShowEntities => "",
        }
    }

    /// Return whether this route targets the EXPLAIN surface.
    #[must_use]
    pub const fn is_explain(&self) -> bool {
        matches!(self, Self::Explain { .. })
    }

    /// Return whether this route targets the DESCRIBE surface.
    #[must_use]
    pub const fn is_describe(&self) -> bool {
        matches!(self, Self::Describe { .. })
    }

    /// Return whether this route targets the `SHOW INDEXES` surface.
    #[must_use]
    pub const fn is_show_indexes(&self) -> bool {
        matches!(self, Self::ShowIndexes { .. })
    }

    /// Return whether this route targets the `SHOW COLUMNS` surface.
    #[must_use]
    pub const fn is_show_columns(&self) -> bool {
        matches!(self, Self::ShowColumns { .. })
    }

    /// Return whether this route targets the `SHOW ENTITIES` surface.
    #[must_use]
    pub const fn is_show_entities(&self) -> bool {
        matches!(self, Self::ShowEntities)
    }
}

// Resolve one parsed reduced SQL statement to canonical surface route metadata.
pub(in crate::db::session::sql) fn sql_statement_route_from_statement(
    statement: &SqlStatement,
) -> SqlStatementRoute {
    match statement {
        SqlStatement::Select(select) => SqlStatementRoute::Query {
            entity: select.entity.clone(),
        },
        SqlStatement::Delete(delete) => SqlStatementRoute::Query {
            entity: delete.entity.clone(),
        },
        SqlStatement::Insert(insert) => SqlStatementRoute::Insert {
            entity: insert.entity.clone(),
        },
        SqlStatement::Update(update) => SqlStatementRoute::Update {
            entity: update.entity.clone(),
        },
        SqlStatement::Explain(explain) => match &explain.statement {
            SqlExplainTarget::Select(select) => SqlStatementRoute::Explain {
                entity: select.entity.clone(),
            },
            SqlExplainTarget::Delete(delete) => SqlStatementRoute::Explain {
                entity: delete.entity.clone(),
            },
        },
        SqlStatement::Describe(describe) => SqlStatementRoute::Describe {
            entity: describe.entity.clone(),
        },
        SqlStatement::ShowIndexes(show_indexes) => SqlStatementRoute::ShowIndexes {
            entity: show_indexes.entity.clone(),
        },
        SqlStatement::ShowColumns(show_columns) => SqlStatementRoute::ShowColumns {
            entity: show_columns.entity.clone(),
        },
        SqlStatement::ShowEntities(_) => SqlStatementRoute::ShowEntities,
    }
}
