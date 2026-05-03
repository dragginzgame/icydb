//! Module: db::session::sql::compiled
//! Responsibility: session-owned compiled SQL command artifacts.
//! Does not own: SQL parsing/lowering or execution dispatch.
//! Boundary: carries generic-free compiled SQL state between session compile and execute phases.

use crate::db::{
    query::intent::StructuralQuery,
    sql::{
        lowering::{LoweredSqlCommand, SqlGlobalAggregateCommandCore},
        parser::{SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};
use std::sync::Arc;

///
/// CompiledSqlCommand
///
/// CompiledSqlCommand is the generic-free SQL compile artifact stored in the
/// session SQL cache and later dispatched by the SQL execution boundary.
/// It deliberately carries syntax-surface commands, not executor scratch state.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select {
        query: Arc<StructuralQuery>,
    },
    Delete {
        query: Arc<StructuralQuery>,
        returning: Option<SqlReturningProjection>,
    },
    GlobalAggregate {
        command: Box<SqlGlobalAggregateCommandCore>,
    },
    Explain(Box<LoweredSqlCommand>),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

///
/// SqlProjectionContract
///
/// SqlProjectionContract is the outward SQL projection contract derived from
/// one shared lower prepared plan. SQL execution keeps this wrapper so
/// statement shaping stays owner-local while prepared-plan reuse lives below it.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl SqlProjectionContract {
    #[must_use]
    pub(in crate::db) const fn new(columns: Vec<String>, fixed_scales: Vec<Option<u32>>) -> Self {
        Self {
            columns,
            fixed_scales,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
}
