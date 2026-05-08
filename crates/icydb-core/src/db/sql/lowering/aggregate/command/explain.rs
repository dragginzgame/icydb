use crate::{
    db::{
        predicate::MissingRowPolicy,
        schema::SchemaInfo,
        sql::{
            lowering::{
                LoweredSqlCommand, LoweredSqlCommandInner, SqlLoweringError,
                aggregate::command::{
                    SqlGlobalAggregateCommandCore,
                    binding::bind_lowered_sql_global_aggregate_command_structural,
                },
            },
            parser::SqlExplainMode,
        },
    },
    model::entity::EntityModel,
};

/// Bind one lowered global aggregate EXPLAIN shape with explicit schema
/// projection.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_structural_with_schema(
    lowered: &LoweredSqlCommand,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<Option<(SqlExplainMode, bool, SqlGlobalAggregateCommandCore)>, SqlLoweringError> {
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
        bind_lowered_sql_global_aggregate_command_structural(
            model,
            command.clone(),
            consistency,
            schema,
        )?,
    )))
}
