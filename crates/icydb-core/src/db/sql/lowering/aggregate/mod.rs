mod command;
mod distinct;
mod grouped;
mod lowering;
mod projection;
mod semantics;
mod strategy;
mod terminal;

use crate::db::sql::parser::{SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement};

#[cfg(test)]
pub(crate) use command::SqlGlobalAggregateCommand;
#[cfg(test)]
pub(crate) use command::compile_sql_global_aggregate_command;
pub(in crate::db) use command::compile_sql_global_aggregate_command_core_from_prepared;
pub(in crate::db::sql::lowering) use command::{
    LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape,
};
pub(crate) use command::{
    SqlGlobalAggregateCommandCore, bind_lowered_sql_explain_global_aggregate_structural,
};
#[cfg(test)]
pub(in crate::db::sql::lowering) use command::{
    bind_lowered_sql_global_aggregate_command, compile_sql_global_aggregate_command_from_prepared,
};
pub(in crate::db::sql::lowering) use grouped::{
    extend_unique_sql_expr_aggregate_calls, grouped_projection_aggregate_calls,
    resolve_having_aggregate_expr_index,
};
pub(in crate::db::sql::lowering) use lowering::{
    lower_aggregate_call, lower_grouped_aggregate_call,
};
pub(in crate::db::sql::lowering) use projection::expr_references_global_direct_fields;
#[cfg(test)]
pub(crate) use strategy::PreparedSqlScalarAggregateDescriptorShape;
pub(crate) use strategy::{
    PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
};

impl SqlStatement {
    /// Return whether this parsed SQL statement is an executable constrained
    /// global aggregate shape owned by the dedicated aggregate lane.
    #[must_use]
    pub(in crate::db) fn is_global_aggregate_lane_shape(&self) -> bool {
        let Self::Select(statement) = self else {
            return false;
        };

        statement.is_global_aggregate_lane_shape()
    }
}

impl SqlSelectStatement {
    /// Return whether this parsed SELECT shape can route onto the dedicated
    /// global aggregate lowering lane.
    #[must_use]
    fn is_global_aggregate_lane_shape(&self) -> bool {
        if self.distinct || !self.group_by.is_empty() {
            return false;
        }

        // Skip the heavier global-aggregate shape lowering when one plain scalar
        // SELECT cannot possibly route onto the dedicated aggregate lane.
        if !self.might_require_global_aggregate_lane() {
            return false;
        }

        lower_global_aggregate_select_shape(self.clone()).is_ok()
    }

    // Use one cheap parsed-shape screen before the dedicated aggregate lane opens
    // the full lowering path. Plain scalar selects with no HAVING and no aggregate
    // projection items can never become executable global aggregates.
    fn might_require_global_aggregate_lane(&self) -> bool {
        if !self.having.is_empty() {
            return true;
        }

        match &self.projection {
            SqlProjection::Items(items) => items.iter().any(SqlSelectItem::contains_aggregate),
            SqlProjection::All => false,
        }
    }
}
