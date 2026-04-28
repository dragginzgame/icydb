use crate::{
    db::{
        predicate::MissingRowPolicy,
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

/// Bind one lowered global aggregate EXPLAIN shape onto the structural query
/// surface when the explain command carries that specialized form.
pub(crate) fn bind_lowered_sql_explain_global_aggregate_structural(
    lowered: &LoweredSqlCommand,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
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
        bind_lowered_sql_global_aggregate_command_structural(model, command.clone(), consistency)?,
    )))
}
