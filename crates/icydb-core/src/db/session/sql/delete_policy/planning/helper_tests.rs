//! Test-only helpers for the parent module.

use super::*;

/// Classify one SQL statement under an explicit `DELETE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided primary
/// key context.
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
