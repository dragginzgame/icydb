//! SQL `UPDATE` policy parsing, shape classification, and admission planning.
//! Does not own: public DTO definitions or update execution.

use super::model::*;
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
use crate::db::{
    QueryError, SqlStatementDispatch,
    schema::{AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField},
    session::sql::write_policy::{
        SqlWriteExecutionBounds, SqlWriteOrderProof, SqlWritePlanCore, SqlWriteStatementShape,
        SqlWriteStatementShapeInput, classify_write_statement_shape, contains_field,
        current_table_field_name,
    },
    sql::{
        lowering::prepare_sql_statement,
        parser::{SqlStatement, SqlUpdateStatement},
    },
};

/// Run one classifier with field-ownership context derived from accepted schema.
///
/// Keeping this projection beside SQL update policy prevents exact, prefix,
/// and resumable entrypoints from maintaining separate generated/managed
/// field lists.
pub(in crate::db) fn with_accepted_sql_update_policy_context<T>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    run: impl FnOnce(SqlUpdatePolicyContext<'_>) -> T,
) -> T {
    let generated_fields = descriptor
        .fields()
        .iter()
        .filter(|field| field.write_policy().insert_generation().is_some())
        .map(AcceptedRowLayoutRuntimeField::name)
        .collect::<Vec<_>>();
    let managed_fields = descriptor
        .fields()
        .iter()
        .filter(|field| field.write_policy().write_management().is_some())
        .map(AcceptedRowLayoutRuntimeField::name)
        .collect::<Vec<_>>();

    run(SqlUpdatePolicyContext::public_generated(
        descriptor.primary_key_names(),
        generated_fields.as_slice(),
        managed_fields.as_slice(),
    ))
}

/// Classify one SQL statement under an explicit `UPDATE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided schema
/// field categories.
#[cfg(test)]
pub(in crate::db) fn classify_sql_update_policy(
    sql: &str,
    policy: SqlUpdateExposurePolicy,
    context: SqlUpdatePolicyContext<'_>,
) -> Result<SqlUpdatePolicyResult, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(classify_sql_update_statement_policy(
        &statement, policy, context,
    ))
}

/// Classify one SQL statement against one concrete expected entity.
///
/// Direct typed execution boundaries use this adapter so their generic entity
/// cannot silently override the entity named by the SQL statement.
pub(in crate::db) fn classify_sql_update_policy_for_entity(
    dispatch: &SqlStatementDispatch<'_>,
    expected_entity: &str,
    policy: SqlUpdateExposurePolicy,
    context: SqlUpdatePolicyContext<'_>,
) -> Result<SqlUpdatePolicyResult, QueryError> {
    let statement = prepare_dispatched_sql_statement(dispatch.statement(), expected_entity)?;

    Ok(classify_sql_update_statement_policy(
        &statement, policy, context,
    ))
}

/// Classify one SQL statement for trusted resumable-update preparation.
///
/// This is the single frontend-shape owner for the resumable lane. Catalog
/// constraints and scope dependency closure remain schema-owned and are
/// checked by the preparation boundary after this function succeeds.
pub(in crate::db) fn classify_sql_resumable_update_policy(
    dispatch: &SqlStatementDispatch<'_>,
    expected_entity: &str,
    context: SqlUpdatePolicyContext<'_>,
) -> Result<SqlResumableUpdatePolicyReport, QueryError> {
    let statement = prepare_dispatched_sql_statement(dispatch.statement(), expected_entity)?;
    let SqlStatement::Update(statement) = statement else {
        return Ok(Err(SqlUpdatePolicyRejection::NotUpdate));
    };
    let write_shape = classify_write_shape(&statement, context);

    if let Some(rejection) = write_shape.required_where_rejection() {
        return Ok(Err(SqlUpdatePolicyRejection::WriteShape(rejection)));
    }
    if let Some(rejection) = unsafe_assignment_rejection(&statement, context) {
        return Ok(Err(rejection));
    }
    if statement.returning.is_some() {
        return Ok(Err(SqlUpdatePolicyRejection::ResumableReturningUnsupported));
    }
    if !exact_update_window_supported(&write_shape) {
        return Ok(Err(SqlUpdatePolicyRejection::ResumableWindowUnsupported));
    }

    Ok(Ok(SqlTrustedResumableUpdatePlan { statement }))
}

fn prepare_dispatched_sql_statement(
    statement: &SqlStatement,
    expected_entity: &str,
) -> Result<SqlStatement, QueryError> {
    prepare_sql_statement(statement, expected_entity)
        .map(crate::db::sql::lowering::PreparedSqlStatement::into_statement)
        .map_err(QueryError::from_sql_lowering_error)
}

pub(in crate::db) fn classify_sql_update_statement_policy(
    statement: &SqlStatement,
    policy: SqlUpdateExposurePolicy,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdatePolicyResult {
    let SqlStatement::Update(statement) = statement else {
        return Err(SqlUpdatePolicyRejection::NotUpdate);
    };

    let write_shape = classify_write_shape(statement, context);
    if let Some(rejection) = update_policy_rejection(policy, statement, &write_shape, context) {
        return Err(rejection);
    }

    Ok(validated_update_plan(
        statement,
        policy,
        &write_shape,
        context,
    ))
}

fn update_policy_rejection(
    policy: SqlUpdateExposurePolicy,
    statement: &SqlUpdateStatement,
    write_shape: &SqlWriteStatementShape,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlUpdatePolicyRejection> {
    if let Some(rejection) = write_shape.required_where_rejection() {
        return Some(SqlUpdatePolicyRejection::WriteShape(rejection));
    }

    if let Some(rejection) = unsafe_assignment_rejection(statement, context) {
        return Some(rejection);
    }

    match policy {
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => write_shape
            .primary_key_policy_rejection()
            .map(SqlUpdatePolicyRejection::WriteShape),
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => write_shape
            .bounded_deterministic_policy_rejection(context.write_bounds())
            .map(SqlUpdatePolicyRejection::WriteShape),
        SqlUpdateExposurePolicy::TrustedExact(_) => {
            if exact_update_window_supported(write_shape) {
                None
            } else {
                Some(SqlUpdatePolicyRejection::ExactWindowUnsupported)
            }
        }
    }
}

const fn exact_update_window_supported(shape: &SqlWriteStatementShape) -> bool {
    shape.limit.is_none()
        && shape.offset.is_none()
        && matches!(
            shape.order_proof,
            SqlWriteOrderProof::Missing | SqlWriteOrderProof::CanonicalPrimaryKey
        )
}

fn validated_update_plan(
    statement: &SqlUpdateStatement,
    policy: SqlUpdateExposurePolicy,
    write_shape: &SqlWriteStatementShape,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlValidatedUpdatePlan {
    let execution_bounds = execution_bounds(policy, write_shape, context);
    match policy {
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => {
            SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(SqlPublicPrimaryKeyUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, execution_bounds),
            })
        }
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => {
            SqlValidatedUpdatePlan::PublicBoundedDeterministic(SqlPublicBoundedUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, execution_bounds),
            })
        }
        SqlUpdateExposurePolicy::TrustedExact(policy) => {
            SqlValidatedUpdatePlan::TrustedExact(SqlTrustedExactUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, execution_bounds),
                policy,
            })
        }
    }
}

const fn execution_bounds(
    policy: SqlUpdateExposurePolicy,
    write_shape: &SqlWriteStatementShape,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlWriteExecutionBounds {
    match policy {
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => write_shape
            .execution_bounds_for_exposure_class(
                crate::db::session::sql::write_policy::SqlWriteExposureClass::PublicPrimaryKeyOnly,
                context.write_bounds(),
            ),
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => write_shape
            .execution_bounds_for_exposure_class(
                crate::db::session::sql::write_policy::SqlWriteExposureClass::PublicBoundedDeterministic,
                context.write_bounds(),
            ),
        SqlUpdateExposurePolicy::TrustedExact(policy) => {
            crate::db::session::sql::write_policy::sql_write_execution_bounds_for_exact_update(
                policy.require_affected_at_most(),
                write_shape.returning_shape.is_requested(),
                context.max_returning_rows,
                context.max_returning_response_bytes,
            )
        }
    }
}

fn unsafe_assignment_rejection(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlUpdatePolicyRejection> {
    if assignments_contain_field(statement, context.primary_key_fields) {
        Some(SqlUpdatePolicyRejection::PrimaryKeyMutation)
    } else if assignments_contain_field(statement, context.generated_fields) {
        Some(SqlUpdatePolicyRejection::GeneratedFieldMutation)
    } else if assignments_contain_field(statement, context.managed_fields) {
        Some(SqlUpdatePolicyRejection::ManagedFieldMutation)
    } else {
        None
    }
}

fn assignments_contain_field(statement: &SqlUpdateStatement, fields: &[&str]) -> bool {
    statement.assignments.iter().any(|assignment| {
        assignment_field_name(statement, assignment.field.as_str())
            .is_some_and(|field| contains_field(fields, field))
    })
}

fn classify_write_shape(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
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

fn assignment_field_name<'a>(statement: &SqlUpdateStatement, field: &'a str) -> Option<&'a str> {
    current_table_field_name(
        field,
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
    )
}
