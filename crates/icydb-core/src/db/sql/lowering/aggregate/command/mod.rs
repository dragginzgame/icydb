mod binding;
#[cfg(feature = "sql-explain")]
mod explain;
mod global;

pub(crate) use binding::SqlGlobalAggregateCommand;
pub(in crate::db) use binding::compile_sql_global_aggregate_command_from_prepared_with_schema;
#[cfg(feature = "sql-explain")]
pub(crate) use explain::bind_lowered_sql_explain_global_aggregate_with_schema;
pub(in crate::db::sql::lowering) use global::{
    LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape,
};
