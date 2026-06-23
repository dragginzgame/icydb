mod binding;
mod explain;
mod global;

pub(crate) use binding::SqlGlobalAggregateCommand;
#[cfg(test)]
pub(crate) use binding::TypedSqlGlobalAggregateCommand;
#[cfg(test)]
pub(crate) use binding::compile_sql_global_aggregate_command_for_model_only;
pub(in crate::db) use binding::compile_sql_global_aggregate_command_from_prepared_with_schema;
#[cfg(test)]
pub(in crate::db::sql::lowering) use binding::{
    bind_lowered_sql_global_aggregate_command_for_model_only,
    compile_sql_global_aggregate_command_from_prepared_for_model_only,
};
pub(crate) use explain::bind_lowered_sql_explain_global_aggregate_with_schema;
pub(in crate::db::sql::lowering) use global::{
    LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape,
};
