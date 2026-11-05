//! Module: db::session::sql::write_policy::bounds
//! Responsibility: shared SQL write row/returning bound derivation.
//! Does not own: parser-shape classification or statement-family policy.
//! Boundary: maps admitted write lanes into execution and returning caps.

use super::model::{
    SqlWriteBoundedPolicyRejection, SqlWriteExecutionBounds, SqlWriteOrderProof,
    SqlWriteReturningBounds,
};

pub(super) const fn combined_optional_row_bound(
    policy_max_rows: Option<u32>,
    configured_max_rows: Option<u32>,
) -> Option<u32> {
    match (policy_max_rows, configured_max_rows) {
        (Some(policy), Some(configured)) => Some(if policy < configured {
            policy
        } else {
            configured
        }),
        (Some(policy), None) => Some(policy),
        (None, Some(configured)) => Some(configured),
        (None, None) => None,
    }
}

pub(super) const fn bounded_write_policy_rejection(
    offset: Option<u32>,
    limit: Option<u32>,
    max_limit: u32,
    order_proof: SqlWriteOrderProof,
) -> Option<SqlWriteBoundedPolicyRejection> {
    if offset.is_some() {
        return Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported);
    }

    let Some(limit) = limit else {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    };
    if limit == 0 {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    }
    if limit > max_limit {
        return Some(SqlWriteBoundedPolicyRejection::LimitTooHigh);
    }

    match order_proof {
        SqlWriteOrderProof::CanonicalPrimaryKey => None,
        SqlWriteOrderProof::DescendingPrimaryKey => {
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder)
        }
        SqlWriteOrderProof::Missing | SqlWriteOrderProof::Other => {
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder)
        }
    }
}

pub(super) const fn sql_write_execution_bounds(
    max_staged_rows: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    let max_rows = if returning_requested {
        combined_optional_row_bound(max_staged_rows, max_returning_rows)
    } else {
        None
    };

    SqlWriteExecutionBounds {
        max_staged_rows,
        returning: SqlWriteReturningBounds {
            max_rows,
            max_response_bytes: max_returning_response_bytes,
        },
    }
}

pub(in crate::db::session::sql) const fn sql_write_execution_bounds_for_exact_update(
    require_affected_at_most: u32,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    sql_write_execution_bounds(
        Some(require_affected_at_most),
        returning_requested,
        max_returning_rows,
        max_returning_response_bytes,
    )
}

#[cfg(test)]
mod tests;
