//! SQL `DELETE` policy parsing, shape classification, and admission planning.
//! Does not own: public DTO definitions or delete execution.

use super::model::*;
#[cfg(test)]
use crate::db::{QueryError, sql::parser::parse_sql};
use crate::db::{
    session::sql::write_policy::{
        SqlWriteExecutionBounds, SqlWritePlanCore, SqlWriteStatementShape,
        SqlWriteStatementShapeInput, classify_write_statement_shape,
    },
    sql::parser::{SqlDeleteStatement, SqlStatement},
};

/// Classify one SQL statement under an explicit `DELETE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided primary
/// key context.
#[cfg(test)]
pub(in crate::db) fn classify_sql_delete_policy(
    sql: &str,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> Result<SqlDeletePolicyResult, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(classify_sql_delete_statement_policy(
        &statement, policy, context,
    ))
}

pub(in crate::db) fn classify_sql_delete_statement_policy(
    statement: &SqlStatement,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeletePolicyResult {
    let SqlStatement::Delete(statement) = statement else {
        return Err(SqlDeletePolicyRejection::NotDelete);
    };

    let write_shape = classify_write_shape(statement, context);
    if let Some(rejection) = delete_policy_rejection(policy, &write_shape, context) {
        return Err(rejection);
    }

    Ok(validated_delete_plan(
        statement,
        policy,
        &write_shape,
        context,
    ))
}

fn delete_policy_rejection(
    policy: SqlDeleteExposurePolicy,
    write_shape: &SqlWriteStatementShape,
    context: SqlDeletePolicyContext<'_>,
) -> Option<SqlDeletePolicyRejection> {
    let rejection = match policy {
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => write_shape.primary_key_policy_rejection(),
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => {
            write_shape.bounded_deterministic_policy_rejection(context.write_bounds())
        }
    };

    rejection.map(SqlDeletePolicyRejection::WriteShape)
}

fn validated_delete_plan(
    statement: &SqlDeleteStatement,
    policy: SqlDeleteExposurePolicy,
    write_shape: &SqlWriteStatementShape,
    context: SqlDeletePolicyContext<'_>,
) -> SqlValidatedDeletePlan {
    let execution_bounds = execution_bounds(policy, write_shape, context);
    match policy {
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => {
            SqlValidatedDeletePlan::PublicPrimaryKeyOnly(SqlPublicPrimaryKeyDeletePlan {
                core: SqlWritePlanCore::from_borrowed(statement, execution_bounds),
            })
        }
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => {
            SqlValidatedDeletePlan::PublicBoundedDeterministic(SqlPublicBoundedDeletePlan {
                core: SqlWritePlanCore::from_borrowed(statement, execution_bounds),
            })
        }
    }
}

const fn execution_bounds(
    policy: SqlDeleteExposurePolicy,
    write_shape: &SqlWriteStatementShape,
    context: SqlDeletePolicyContext<'_>,
) -> SqlWriteExecutionBounds {
    write_shape.execution_bounds_for_exposure_class(policy.exposure_class(), context.write_bounds())
}

fn classify_write_shape(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlWriteStatementShape {
    classify_write_statement_shape(SqlWriteStatementShapeInput {
        predicate: statement.predicate.as_ref(),
        entity: statement.entity.as_str(),
        table_alias: statement.table_alias.as_deref(),
        order_by: statement.order_by.as_slice(),
        limit: statement.limit,
        offset: statement.offset,
        returning: statement.returning.as_ref(),
        primary_key_fields: context.primary_key_fields,
    })
}
