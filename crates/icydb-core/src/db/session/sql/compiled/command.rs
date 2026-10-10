//! Generic-free compiled SQL command artifacts.
//! Does not own: accepted-schema execution context handoff.

#[cfg(feature = "sql")]
use crate::db::sql::lowering::LoweredSqlCommand;
use crate::db::{
    query::intent::StructuralQuery,
    sql::{
        lowering::SqlGlobalAggregateCommand,
        parser::{SqlDescribeMode, SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};

///
/// CompiledSqlCommand
///
/// CompiledSqlCommand is the generic-free SQL compile artifact stored in the
/// session SQL cache and later dispatched by the SQL execution boundary.
/// It deliberately carries syntax-surface commands, not executor scratch state.
/// Cached syntax owns its queries; execution clones cannot populate resident memo cells.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select {
        query: Box<StructuralQuery>,
    },
    Delete {
        query: Box<StructuralQuery>,
        returning: Option<SqlReturningProjection>,
    },
    GlobalAggregate {
        command: Box<SqlGlobalAggregateCommand>,
    },
    #[cfg(feature = "sql")]
    Explain(Box<LoweredSqlCommand>),
    Insert(CompiledSqlInsertCommand),
    Update(SqlUpdateStatement),
    DescribeEntity {
        mode: SqlDescribeMode,
    },
    ShowConstraintsEntity,
    ShowIndexesEntity,
    ShowColumnsEntity {
        mode: SqlDescribeMode,
    },
    ShowRelationsEntity,
    ShowEntities {
        entity: Option<String>,
        verbose: bool,
    },
    ShowStores {
        verbose: bool,
    },
    ShowMemory,
}

///
/// CompiledSqlInsertCommand
///
/// CompiledSqlInsertCommand carries one normalized INSERT statement plus the
/// optional bound source query for `INSERT ... SELECT`.
/// VALUES inserts keep no source query; SELECT inserts reuse the compiled
/// source artifact during execution instead of preparing and binding it again.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledSqlInsertCommand {
    statement: SqlInsertStatement,
    source_query: Option<Box<StructuralQuery>>,
}

impl CompiledSqlInsertCommand {
    /// Build one compiled INSERT command from its normalized statement and
    /// optional already-bound source query.
    #[must_use]
    pub(in crate::db) fn new(
        statement: SqlInsertStatement,
        source_query: Option<StructuralQuery>,
    ) -> Self {
        Self {
            statement,
            source_query: source_query.map(Box::new),
        }
    }

    /// Borrow the normalized INSERT syntax surface.
    #[must_use]
    pub(in crate::db) const fn statement(&self) -> &SqlInsertStatement {
        &self.statement
    }

    /// Borrow the bound INSERT SELECT source query when this command uses a
    /// SELECT source.
    #[must_use]
    pub(in crate::db) fn source_query(&self) -> Option<&StructuralQuery> {
        self.source_query.as_deref()
    }
}

impl CompiledSqlCommand {
    #[must_use]
    pub(in crate::db) fn select(query: StructuralQuery) -> Self {
        Self::Select {
            query: Box::new(query),
        }
    }

    #[must_use]
    pub(in crate::db) fn global_aggregate(command: SqlGlobalAggregateCommand) -> Self {
        Self::GlobalAggregate {
            command: Box::new(command),
        }
    }
}

crate::retained::retained_fields!(CompiledSqlInsertCommand {
    Self { statement, source_query } => [statement, source_query],
});
crate::retained::retained_fields!(CompiledSqlCommand {
    Self::Select { query } => [query],
    Self::Delete { query, returning } => [query, returning],
    Self::GlobalAggregate { command } => [command],
    #[cfg(feature = "sql")]
    Self::Explain(command) => [command],
    Self::Insert(command) => [command],
    Self::Update(statement) => [statement],
    Self::ShowEntities { entity, verbose: _ } => [entity],
    Self::DescribeEntity { mode: _ } | Self::ShowConstraintsEntity | Self::ShowIndexesEntity
        | Self::ShowColumnsEntity { mode: _ } | Self::ShowRelationsEntity | Self::ShowStores { verbose: _ }
        | Self::ShowMemory => [],
});
