use crate::{
    db::{
        predicate::MissingRowPolicy,
        schema::SchemaInfo,
        sql::{
            lowering::{
                LoweredSqlCommand, LoweredSqlCommandInner, SqlLoweringError,
                aggregate::command::{
                    SqlGlobalAggregateCommand,
                    binding::bind_lowered_sql_global_aggregate_command_with_schema,
                },
            },
            parser::SqlExplainMode,
        },
    },
    model::entity::EntityModel,
};

/// Bind one lowered global aggregate EXPLAIN shape with explicit schema
/// projection.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_with_schema(
    lowered: &LoweredSqlCommand,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<Option<(SqlExplainMode, bool, SqlGlobalAggregateCommand)>, SqlLoweringError> {
    let LoweredSqlCommandInner::ExplainGlobalAggregate {
        mode,
        verbose,
        command,
    } = &lowered.0
    else {
        return Ok(None);
    };

    Ok(Some((
        *mode,
        *verbose,
        bind_lowered_sql_global_aggregate_command_with_schema(
            model,
            command.clone(),
            consistency,
            schema,
        )?,
    )))
}
