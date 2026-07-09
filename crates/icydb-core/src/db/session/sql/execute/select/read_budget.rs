//! Module: db::session::sql::execute::select::read_budget
//! Responsibility: test-only SQL SELECT read-admission response budget probes.
//! Does not own: production SELECT execution or public read-admission policy.
//! Boundary: keeps test response-size encoding fixtures out of production SELECT execution code.

use crate::{
    db::{
        GroupedRow, QueryError, SqlStatementResult,
        executor::{SharedPreparedExecutionPlan, initial_read_plan_requires_materialized_sort},
        query::{
            admission::{QueryAdmissionPolicy, QueryAdmissionSummary, QueryMaterializationSummary},
            plan::GroupedExecutionConfig,
        },
    },
    error::InternalError,
    value::OutputValue,
};
use candid::{CandidType, Encode};

#[derive(CandidType)]
enum SqlReadResponseSizeProbe {
    Projection(SqlReadProjectionSizeProbe),
    Grouped(SqlReadGroupedSizeProbe),
}

#[derive(CandidType)]
struct SqlReadProjectionSizeProbe {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
}

#[derive(CandidType)]
struct SqlReadGroupedSizeProbe {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<SqlReadGroupedRowSizeProbe>,
    row_count: u32,
    next_cursor: Option<String>,
}

#[derive(CandidType)]
struct SqlReadGroupedRowSizeProbe {
    group_key: Vec<OutputValue>,
    aggregate_values: Vec<OutputValue>,
}

pub(in crate::db::session::sql::execute) fn enforce_read_admission_policy(
    policy: &QueryAdmissionPolicy,
    prepared_plan: &SharedPreparedExecutionPlan,
) -> Result<(), QueryError> {
    let mut summary = QueryAdmissionSummary::from_plan(policy.lane(), prepared_plan.logical_plan());
    if initial_read_plan_requires_materialized_sort(prepared_plan).map_err(QueryError::execute)? {
        let returned_row_bound = summary.returned_row_bound();
        let returned_row_bound_kind = summary.returned_row_bound_kind();
        summary = summary.with_materialization(QueryMaterializationSummary::sort(
            returned_row_bound,
            returned_row_bound_kind,
        ));
    }
    let admission = policy.evaluate(summary);

    if let Some(rejection) = admission.rejection() {
        Err(QueryError::from(rejection.code()))
    } else {
        Ok(())
    }
}

pub(super) fn prepared_read_admission_plan_with_execution_caps(
    policy: &QueryAdmissionPolicy,
    prepared_plan: SharedPreparedExecutionPlan,
) -> Result<SharedPreparedExecutionPlan, QueryError> {
    let Some(execution) = grouped_execution_config_for_read_admission(policy, &prepared_plan)
    else {
        return Ok(prepared_plan);
    };

    prepared_plan
        .with_grouped_execution_config(execution)
        .map_err(QueryError::execute)
}

fn grouped_execution_config_for_read_admission(
    policy: &QueryAdmissionPolicy,
    prepared_plan: &SharedPreparedExecutionPlan,
) -> Option<GroupedExecutionConfig> {
    let policy_execution = policy.grouped().execution_config()?;
    let grouped_plan = prepared_plan.logical_plan().grouped_plan()?;
    let plan_execution = grouped_plan.group.execution;

    Some(GroupedExecutionConfig::with_hard_limits(
        plan_execution
            .max_groups()
            .min(policy_execution.max_groups()),
        plan_execution
            .max_group_bytes()
            .min(policy_execution.max_group_bytes()),
    ))
}

pub(in crate::db::session::sql::execute) fn enforce_sql_read_response_byte_policy(
    policy: &QueryAdmissionPolicy,
    result: &SqlStatementResult,
) -> Result<(), QueryError> {
    let Some(max_response_bytes) = policy.max_response_bytes() else {
        return Ok(());
    };

    let max_response_bytes = usize::try_from(max_response_bytes.get()).unwrap_or(usize::MAX);
    if sql_read_projection_response_len_exceeds_max(result, max_response_bytes)? {
        Err(QueryError::from(
            icydb_diagnostic_code::QueryReadAdmissionCode::ProjectionResponseMayExceedLimit,
        ))
    } else {
        Ok(())
    }
}

fn sql_read_projection_response_len_exceeds_max(
    result: &SqlStatementResult,
    max_response_bytes: usize,
) -> Result<bool, QueryError> {
    match result {
        SqlStatementResult::Projection {
            columns,
            fixed_scales,
            rows,
            row_count,
        } => {
            let base_len = encoded_sql_read_projection_response_len(
                columns.clone(),
                fixed_scales.clone(),
                Vec::new(),
                *row_count,
            )?;
            encoded_sql_read_projection_rows_len_exceeds_max(base_len, max_response_bytes, rows)
        }
        SqlStatementResult::Grouped {
            columns,
            fixed_scales,
            rows,
            row_count,
            next_cursor,
        } => {
            let base_len = encoded_sql_read_grouped_response_len(
                columns.clone(),
                fixed_scales.clone(),
                Vec::new(),
                *row_count,
                next_cursor.clone(),
            )?;
            encoded_sql_read_grouped_rows_len_exceeds_max(base_len, max_response_bytes, rows)
        }
        _ => Ok(false),
    }
}

fn encoded_sql_read_projection_rows_len_exceeds_max(
    mut estimated_payload_len: usize,
    max_response_bytes: usize,
    rows: &[Vec<OutputValue>],
) -> Result<bool, QueryError> {
    if estimated_payload_len > max_response_bytes {
        return Ok(true);
    }

    for row in rows {
        let row_len = Encode!(row)
            .map_err(|_| QueryError::execute(InternalError::query_executor_invariant()))?
            .len();
        estimated_payload_len = estimated_payload_len.saturating_add(row_len);
        if estimated_payload_len > max_response_bytes {
            return Ok(true);
        }
    }

    Ok(false)
}

fn encoded_sql_read_grouped_rows_len_exceeds_max(
    mut estimated_payload_len: usize,
    max_response_bytes: usize,
    rows: &[GroupedRow],
) -> Result<bool, QueryError> {
    if estimated_payload_len > max_response_bytes {
        return Ok(true);
    }

    for row in rows {
        let row_len = Encode!(&grouped_row_size_probe(row))
            .map_err(|_| QueryError::execute(InternalError::query_executor_invariant()))?
            .len();
        estimated_payload_len = estimated_payload_len.saturating_add(row_len);
        if estimated_payload_len > max_response_bytes {
            return Ok(true);
        }
    }

    Ok(false)
}

fn grouped_row_size_probe(row: &GroupedRow) -> SqlReadGroupedRowSizeProbe {
    SqlReadGroupedRowSizeProbe {
        group_key: row.group_key().to_vec(),
        aggregate_values: row.aggregate_values().to_vec(),
    }
}

fn encoded_sql_read_projection_response_len(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
) -> Result<usize, QueryError> {
    let payload = SqlReadResponseSizeProbe::Projection(SqlReadProjectionSizeProbe {
        columns,
        fixed_scales,
        rows,
        row_count,
    });
    let encoded = Encode!(&payload)
        .map_err(|_| QueryError::execute(InternalError::query_executor_invariant()))?;

    Ok(encoded.len())
}

fn encoded_sql_read_grouped_response_len(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<SqlReadGroupedRowSizeProbe>,
    row_count: u32,
    next_cursor: Option<String>,
) -> Result<usize, QueryError> {
    let payload = SqlReadResponseSizeProbe::Grouped(SqlReadGroupedSizeProbe {
        columns,
        fixed_scales,
        rows,
        row_count,
        next_cursor,
    });
    let encoded = Encode!(&payload)
        .map_err(|_| QueryError::execute(InternalError::query_executor_invariant()))?;

    Ok(encoded.len())
}
