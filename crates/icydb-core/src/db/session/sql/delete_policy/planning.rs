//! SQL `DELETE` policy parsing, shape classification, and admission planning.
//! Does not own: public DTO definitions or delete execution.

use super::model::*;
use crate::db::{
    QueryError,
    session::sql::write_policy::{
        SqlWriteExecutionBounds, SqlWritePlanCore, SqlWriteShapePolicyRejection,
        SqlWriteStatementShape, SqlWriteStatementShapeInput, classify_write_statement_shape,
    },
    sql::parser::{SqlDeleteStatement, SqlStatement, parse_sql_with_attribution},
};

/// Classify one SQL statement under an explicit `DELETE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided primary
/// key context.
pub(in crate::db) fn classify_sql_delete_policy(
    sql: &str,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> Result<SqlDeletePolicyReport, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(classify_sql_delete_statement_policy(
        &statement, policy, context,
    ))
}

pub(in crate::db) fn classify_sql_delete_statement_policy(
    statement: &SqlStatement,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeletePolicyReport {
    let SqlStatement::Delete(statement) = statement else {
        return SqlDeletePolicyReport::rejected(SqlDeletePolicyRejection::NotDelete);
    };

    let classification = classify_delete_statement(statement, context);
    let rejection = delete_policy_rejection(policy, &classification, context);
    let plan = if rejection.is_none() {
        Some(validated_delete_plan(
            statement,
            policy,
            &classification,
            context,
        ))
    } else {
        None
    };

    SqlDeletePolicyReport {
        classification: Some(classification),
        plan,
        rejection,
    }
}

fn classify_delete_statement(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeleteStatementClassification {
    SqlDeleteStatementClassification {
        target_entity: statement.entity.clone(),
        write_shape: classify_write_shape(statement, context),
    }
}

const fn delete_policy_rejection(
    policy: SqlDeleteExposurePolicy,
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> Option<SqlDeletePolicyRejection> {
    match policy {
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => {
            write_shape_policy_rejection(classification.write_shape.primary_key_policy_rejection())
        }
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => write_shape_policy_rejection(
            classification
                .write_shape
                .bounded_deterministic_policy_rejection(context.write_bounds()),
        ),
    }
}

const fn write_shape_policy_rejection(
    rejection: Option<SqlWriteShapePolicyRejection>,
) -> Option<SqlDeletePolicyRejection> {
    match rejection {
        Some(rejection) => Some(SqlDeletePolicyRejection::from_write_shape_rejection(
            rejection,
        )),
        None => None,
    }
}

fn validated_delete_plan(
    statement: &SqlDeleteStatement,
    policy: SqlDeleteExposurePolicy,
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> SqlValidatedDeletePlan {
    let execution_bounds = execution_bounds(policy, classification, context);
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
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> SqlWriteExecutionBounds {
    classification
        .write_shape
        .execution_bounds_for_exposure_class(policy.exposure_class(), context.write_bounds())
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
