//!
//! Test-only SQL perf harness for quickstart canister integration sampling.
//!

use crate::{db, sql_dispatch};
use candid::{CandidType, Deserialize};
use icydb::{
    Error,
    db::sql::{SqlQueryResult, normalize_sql_input},
    db::{
        query::Predicate,
        response::{PagedGroupedResponse, PagedResponse, Response},
    },
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    value::Value,
};
use icydb_testing_quickstart_fixtures::schema::User;

const MAX_REPEAT_COUNT: u32 = 32;

///
/// SqlPerfSurface
///
/// One measured SQL surface owned by the quickstart canister perf harness.
/// This stays intentionally narrow so the harness can compare generated SQL
/// dispatch against representative typed session surfaces without pretending to
/// cover every possible query front in one first pass.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchUser,
    TypedQueryFromSqlUserExecute,
    TypedExecuteSqlUser,
    TypedExecuteSqlGroupedUser,
    TypedExecuteSqlGroupedUserSecondPage,
    TypedExecuteSqlAggregateUser,
    FluentLoadUserOrderIdLimit2,
    FluentLoadUserNameEqLimit1,
    FluentPagedUserOrderIdLimit2FirstPage,
    FluentPagedUserOrderIdLimit2SecondPage,
    FluentPagedUserOrderIdLimit2InvalidCursor,
}

///
/// SqlPerfRequest
///
/// One perf-harness request for one SQL surface and one query shape.
/// `repeat_count` runs happen inside one wasm call so the sample can report
/// both the first execution cost and the warmed repeated-run range.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfRequest {
    pub surface: SqlPerfSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
    pub repeat_count: u32,
}

///
/// SqlPerfOutcome
///
/// Compact result summary for one measured SQL surface.
/// The audit only needs stable payload shape, cardinality, and failure class
/// signals here; full query payload rendering stays outside the perf harness.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfOutcome {
    pub success: bool,
    pub result_kind: String,
    pub entity: Option<String>,
    pub row_count: Option<u32>,
    pub detail_count: Option<u32>,
    pub has_cursor: Option<bool>,
    pub rendered_value: Option<String>,
    pub error_kind: Option<String>,
    pub error_origin: Option<String>,
    pub error_message: Option<String>,
}

///
/// SqlPerfSample
///
/// One repeated wasm-side instruction sample for one SQL surface.
/// This reports first/min/max/avg/total local instruction deltas so the audit
/// can see cold-vs-warm behavior without relying on host-side zeroed counters.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfSample {
    pub surface: SqlPerfSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
    pub repeat_count: u32,
    pub first_local_instructions: u64,
    pub min_local_instructions: u64,
    pub max_local_instructions: u64,
    pub total_local_instructions: u64,
    pub avg_local_instructions: u64,
    pub outcome_stable: bool,
    pub outcome: SqlPerfOutcome,
}

/// Measure one SQL surface request inside the running canister.
pub fn sample_sql_surface(request: SqlPerfRequest) -> Result<SqlPerfSample, Error> {
    validate_perf_request(&request)?;

    let sql = normalize_sql_input(request.sql.as_str())?.to_string();
    let repeat_count = request.repeat_count;

    // Measure each execution independently so the sample retains first-run and
    // warmed-run spread instead of only one merged total.
    let mut first_local_instructions = 0_u64;
    let mut min_local_instructions = u64::MAX;
    let mut max_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut last_outcome = None;
    let mut outcome_stable = true;

    for iteration in 0..repeat_count {
        let (delta, outcome) = measure_once(
            request.surface,
            sql.as_str(),
            request.cursor_token.as_deref(),
        );

        if iteration == 0 {
            first_local_instructions = delta;
        } else if last_outcome.as_ref() != Some(&outcome) {
            outcome_stable = false;
        }

        min_local_instructions = min_local_instructions.min(delta);
        max_local_instructions = max_local_instructions.max(delta);
        total_local_instructions = total_local_instructions.saturating_add(delta);
        last_outcome = Some(outcome);
    }

    let avg_local_instructions = total_local_instructions / u64::from(repeat_count);
    let outcome = last_outcome.expect("repeat_count validation guarantees at least one run");

    Ok(SqlPerfSample {
        surface: request.surface,
        sql,
        cursor_token: request.cursor_token,
        repeat_count,
        first_local_instructions,
        min_local_instructions,
        max_local_instructions,
        total_local_instructions,
        avg_local_instructions,
        outcome_stable,
        outcome,
    })
}

fn validate_perf_request(request: &SqlPerfRequest) -> Result<(), Error> {
    if request.repeat_count == 0 {
        return Err(invalid_perf_request_error(
            "sql_perf repeat_count must be at least 1",
        ));
    }

    if request.repeat_count > MAX_REPEAT_COUNT {
        return Err(invalid_perf_request_error(format!(
            "sql_perf repeat_count must be <= {MAX_REPEAT_COUNT}"
        )));
    }

    Ok(())
}

fn invalid_perf_request_error(message: impl Into<String>) -> Error {
    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        message,
    )
}

#[cfg(target_arch = "wasm32")]
fn read_local_instruction_counter() -> u64 {
    canic_cdk::api::performance_counter(1)
}

#[cfg(not(target_arch = "wasm32"))]
const fn read_local_instruction_counter() -> u64 {
    0
}

fn missing_continuation_sample(message: &'static str) -> (u64, SqlPerfOutcome) {
    (0, outcome_from_error(invalid_perf_request_error(message)))
}

fn measure_typed_grouped_second_page(sql: &str) -> (u64, SqlPerfOutcome) {
    let bootstrap = db().execute_sql_grouped::<User>(sql, None);
    let outcome = match bootstrap {
        Ok(first_page) => {
            let Some(cursor_token) = first_page.next_cursor() else {
                return missing_continuation_sample(
                    "typed grouped second-page sample requires a continuation cursor",
                );
            };

            return measure_surface_call(|| {
                db().execute_sql_grouped::<User>(sql, Some(cursor_token))
                    .map_or_else(outcome_from_error, outcome_from_grouped_response)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_fluent_paged_second_page() -> (u64, SqlPerfOutcome) {
    let bootstrap = db().load::<User>().order_by("id").limit(2).execute_paged();
    let outcome = match bootstrap {
        Ok(first_page) => {
            let Some(cursor_token) = first_page.next_cursor() else {
                return missing_continuation_sample(
                    "fluent paged second-page sample requires a continuation cursor",
                );
            };

            return measure_surface_call(|| {
                db().load::<User>()
                    .order_by("id")
                    .limit(2)
                    .cursor(cursor_token.to_string())
                    .execute_paged()
                    .map_or_else(outcome_from_error, outcome_from_paged_response)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_once(
    surface: SqlPerfSurface,
    sql: &str,
    cursor_token: Option<&str>,
) -> (u64, SqlPerfOutcome) {
    match surface {
        SqlPerfSurface::GeneratedDispatch => measure_surface_call(|| {
            sql_dispatch::query(sql).map_or_else(outcome_from_error, outcome_from_sql_query_result)
        }),
        SqlPerfSurface::TypedDispatchUser => measure_surface_call(|| {
            db().execute_sql_dispatch::<User>(sql)
                .map_or_else(outcome_from_error, outcome_from_sql_query_result)
        }),
        SqlPerfSurface::TypedQueryFromSqlUserExecute => measure_surface_call(|| {
            let session = db();
            session
                .query_from_sql::<User>(sql)
                .and_then(|query| session.execute_query(&query))
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedExecuteSqlUser => measure_surface_call(|| {
            db().execute_sql::<User>(sql)
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedExecuteSqlGroupedUser => measure_surface_call(|| {
            db().execute_sql_grouped::<User>(sql, cursor_token)
                .map_or_else(outcome_from_error, outcome_from_grouped_response)
        }),
        SqlPerfSurface::TypedExecuteSqlGroupedUserSecondPage => {
            measure_typed_grouped_second_page(sql)
        }
        SqlPerfSurface::TypedExecuteSqlAggregateUser => measure_surface_call(|| {
            db().execute_sql_aggregate::<User>(sql)
                .map_or_else(outcome_from_error, outcome_from_value)
        }),
        SqlPerfSurface::FluentLoadUserOrderIdLimit2 => measure_surface_call(|| {
            db().load::<User>()
                .order_by("id")
                .limit(2)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentLoadUserNameEqLimit1 => measure_surface_call(|| {
            db().load::<User>()
                .filter(Predicate::eq("name".to_string(), "alice".into()))
                .order_by("id")
                .limit(1)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentPagedUserOrderIdLimit2FirstPage => measure_surface_call(|| {
            db().load::<User>()
                .order_by("id")
                .limit(2)
                .execute_paged()
                .map_or_else(outcome_from_error, outcome_from_paged_response)
        }),
        SqlPerfSurface::FluentPagedUserOrderIdLimit2SecondPage => {
            measure_fluent_paged_second_page()
        }
        SqlPerfSurface::FluentPagedUserOrderIdLimit2InvalidCursor => measure_surface_call(|| {
            db().load::<User>()
                .order_by("id")
                .limit(2)
                .cursor(cursor_token.unwrap_or("zz").to_string())
                .execute_paged()
                .map_or_else(outcome_from_error, outcome_from_paged_response)
        }),
    }
}

fn measure_surface_call(run: impl FnOnce() -> SqlPerfOutcome) -> (u64, SqlPerfOutcome) {
    let start = read_local_instruction_counter();
    let outcome = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, outcome)
}

// Keep perf outcome counters on the stable `u32` wire type without silently
// truncating host-side `usize` lengths if a future harness shape grows.
fn checked_perf_count(count: usize, label: &str) -> u32 {
    u32::try_from(count).unwrap_or_else(|_| panic!("perf harness {label} exceeds u32"))
}

fn outcome_from_sql_query_result(result: SqlQueryResult) -> SqlPerfOutcome {
    match result {
        SqlQueryResult::Projection(rows) => SqlPerfOutcome {
            success: true,
            result_kind: "projection".to_string(),
            entity: Some(rows.entity),
            row_count: Some(rows.row_count),
            detail_count: Some(checked_perf_count(
                rows.columns.len(),
                "projection column count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
        SqlQueryResult::Explain { entity, explain } => SqlPerfOutcome {
            success: true,
            result_kind: "explain".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(
                explain.lines().count(),
                "explain line count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
        SqlQueryResult::Describe(description) => SqlPerfOutcome {
            success: true,
            result_kind: "describe".to_string(),
            entity: Some(description.entity_name().to_string()),
            row_count: None,
            detail_count: Some(checked_perf_count(
                description.fields().len(),
                "describe field count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => SqlPerfOutcome {
            success: true,
            result_kind: "show_indexes".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(indexes.len(), "show indexes count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
        SqlQueryResult::ShowColumns { entity, columns } => SqlPerfOutcome {
            success: true,
            result_kind: "show_columns".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(columns.len(), "show columns count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
        SqlQueryResult::ShowEntities { entities } => SqlPerfOutcome {
            success: true,
            result_kind: "show_entities".to_string(),
            entity: None,
            row_count: None,
            detail_count: Some(checked_perf_count(entities.len(), "show entities count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
        },
    }
}

fn outcome_from_response(result: Response<User>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "typed_response".to_string(),
        entity: Some("User".to_string()),
        row_count: Some(result.count()),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_paged_response(result: PagedResponse<User>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "paged_response".to_string(),
        entity: Some("User".to_string()),
        row_count: Some(checked_perf_count(
            result.items().len(),
            "paged response row count",
        )),
        detail_count: None,
        has_cursor: Some(result.next_cursor().is_some()),
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_grouped_response(result: PagedGroupedResponse) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "grouped_response".to_string(),
        entity: Some("User".to_string()),
        row_count: Some(checked_perf_count(
            result.items().len(),
            "grouped response row count",
        )),
        detail_count: None,
        has_cursor: Some(result.next_cursor().is_some()),
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_value(result: Value) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "aggregate_value".to_string(),
        entity: Some("User".to_string()),
        row_count: None,
        detail_count: None,
        has_cursor: None,
        rendered_value: Some(format!("{result:?}")),
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_error(err: Error) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: false,
        result_kind: "error".to_string(),
        entity: None,
        row_count: None,
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: Some(format!("{:?}", err.kind())),
        error_origin: Some(format!("{:?}", err.origin())),
        error_message: Some(err.message().to_string()),
    }
}
