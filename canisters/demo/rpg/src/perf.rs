//!
//! Demo-local SQL perf harness for the Character-only RPG surface.
//!

use crate::{core_db, db, sql_dispatch};
use candid::{CandidType, Deserialize};
use icydb::{
    Error,
    db::{
        EntityAuthority, PersistedRow, SqlStatementRoute, identifiers_tail_match,
        sql::SqlQueryResult,
    },
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::EntityValue,
};
use icydb_testing_demo_rpg_fixtures::schema::{Character, DemoRpgCanister};

const MAX_REPEAT_COUNT: u32 = 100;

///
/// SqlPerfSurface
///
/// Stable perf-surface selector shared with the PocketIC SQL integration
/// harness.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchCharacter,
}

///
/// SqlPerfRequest
///
/// One repeated perf sample request for one SQL surface and query shape.
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
/// Compact perf-harness outcome summary shared with integration tests.
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
/// One repeated wasm-side perf sample.
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

///
/// SqlPerfAttributionSurface
///
/// Stable attribution selector shared with the PocketIC SQL integration
/// harness.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchCharacter,
}

///
/// SqlPerfAttributionRequest
///
/// One fixed-cost attribution request for one representative SQL surface.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfAttributionRequest {
    pub surface: SqlPerfAttributionSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
}

///
/// SqlPerfAttributionSample
///
/// One fixed-cost wasm attribution sample.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfAttributionSample {
    pub surface: SqlPerfAttributionSurface,
    pub sql: String,
    pub parse_local_instructions: u64,
    pub route_local_instructions: u64,
    pub lower_local_instructions: u64,
    pub dispatch_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub wrapper_local_instructions: u64,
    pub total_local_instructions: u64,
    pub outcome: SqlPerfOutcome,
}

/// Measure one SQL surface request inside the running canister.
pub fn sample_sql_surface(request: SqlPerfRequest) -> Result<SqlPerfSample, Error> {
    validate_perf_request(&request)?;

    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();
    let repeat_count = request.repeat_count;
    let mut first_local_instructions = 0_u64;
    let mut min_local_instructions = u64::MAX;
    let mut max_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut last_outcome = None;
    let mut outcome_stable = true;

    for iteration in 0..repeat_count {
        let (delta, outcome) = measure_once(request.surface, sql.as_str());

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

/// Attribute one representative SQL query surface into fixed-cost wasm phases.
pub fn attribute_sql_surface(
    request: SqlPerfAttributionRequest,
) -> Result<SqlPerfAttributionSample, Error> {
    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();

    match request.surface {
        SqlPerfAttributionSurface::GeneratedDispatch => {
            attribute_generated_dispatch_surface(sql.as_str())
        }
        SqlPerfAttributionSurface::TypedDispatchCharacter => {
            attribute_typed_dispatch_surface::<Character>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCharacter,
            )
        }
    }
}

fn normalize_perf_sql_input(sql: &str) -> Result<&str, Error> {
    let sql_trimmed = sql.trim();
    if sql_trimmed.is_empty() {
        return Err(Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Query,
            "query endpoint requires a non-empty SQL string",
        ));
    }

    Ok(sql_trimmed)
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

fn measure_result<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

const fn attributed_total(
    parse_local_instructions: u64,
    route_local_instructions: u64,
    lower_local_instructions: u64,
    dispatch_local_instructions: u64,
    execute_local_instructions: u64,
    wrapper_local_instructions: u64,
) -> u64 {
    parse_local_instructions
        .saturating_add(route_local_instructions)
        .saturating_add(lower_local_instructions)
        .saturating_add(dispatch_local_instructions)
        .saturating_add(execute_local_instructions)
        .saturating_add(wrapper_local_instructions)
}

fn generated_query_authority(
    route: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<EntityAuthority, Error> {
    if matches!(route, SqlStatementRoute::ShowEntities) {
        return Err(invalid_perf_request_error(
            "sql attribution requires entity-scoped generated SQL route",
        ));
    }

    let sql_entity = route.entity();
    for authority in authorities {
        if identifiers_tail_match(sql_entity, authority.model().name()) {
            return Ok(*authority);
        }
    }

    Err(invalid_perf_request_error(format!(
        "sql attribution unsupported entity '{sql_entity}'"
    )))
}

fn attribute_generated_dispatch_surface(sql: &str) -> Result<SqlPerfAttributionSample, Error> {
    let core = core_db();
    let authorities = sql_dispatch::authorities();

    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (route_local_instructions, authority_result) =
        measure_result(|| generated_query_authority(parsed.route(), authorities));
    let authority = authority_result?;

    let (core_dispatch_total, core_dispatch_result) = measure_result(|| {
        core.execute_generated_query_surface_dispatch_for_authority(&parsed, authority)
            .map_err(Error::from)
    });
    let _core_dispatch_result = core_dispatch_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(
                authority.model().name(),
                authority.model().primary_key().name(),
            )
            .map_err(Error::from)
    });
    let lowered = lowered_result?;

    let (execute_local_instructions, execute_result) = measure_result(|| {
        core.execute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
            .map_err(Error::from)
    });
    let _execute_result = execute_result?;

    let dispatch_local_instructions = core_dispatch_total
        .saturating_sub(lower_local_instructions.saturating_add(execute_local_instructions));

    let (total_local_instructions, outcome) = measure_surface_call(|| {
        sql_dispatch::query(sql).map_or_else(outcome_from_error, outcome_from_sql_query_result)
    });
    let attributed_core = parse_local_instructions
        .saturating_add(route_local_instructions)
        .saturating_add(core_dispatch_total);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface: SqlPerfAttributionSurface::GeneratedDispatch,
        sql: sql.to_string(),
        parse_local_instructions,
        route_local_instructions,
        lower_local_instructions,
        dispatch_local_instructions,
        execute_local_instructions,
        wrapper_local_instructions,
        total_local_instructions: attributed_total(
            parse_local_instructions,
            route_local_instructions,
            lower_local_instructions,
            dispatch_local_instructions,
            execute_local_instructions,
            wrapper_local_instructions,
        ),
        outcome,
    })
}

fn attribute_typed_dispatch_surface<E>(
    sql: &str,
    surface: SqlPerfAttributionSurface,
) -> Result<SqlPerfAttributionSample, Error>
where
    E: PersistedRow<Canister = DemoRpgCanister> + EntityValue,
{
    let core = core_db();
    let authority = EntityAuthority::for_type::<E>();

    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (core_dispatch_total, core_dispatch_result) = measure_result(|| {
        core.execute_sql_dispatch_parsed::<E>(&parsed)
            .map_err(Error::from)
    });
    let _core_dispatch_result = core_dispatch_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key().name())
            .map_err(Error::from)
    });
    let lowered = lowered_result?;

    let (execute_local_instructions, execute_result) = measure_result(|| {
        core.execute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
            .map_err(Error::from)
    });
    let _execute_result = execute_result?;

    let dispatch_local_instructions = core_dispatch_total
        .saturating_sub(lower_local_instructions.saturating_add(execute_local_instructions));

    let (total_local_instructions, outcome) = measure_surface_call(|| {
        db().execute_sql_dispatch::<E>(sql)
            .map_or_else(outcome_from_error, outcome_from_sql_query_result)
    });
    let attributed_core = parse_local_instructions.saturating_add(core_dispatch_total);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface,
        sql: sql.to_string(),
        parse_local_instructions,
        route_local_instructions: 0,
        lower_local_instructions,
        dispatch_local_instructions,
        execute_local_instructions,
        wrapper_local_instructions,
        total_local_instructions: attributed_total(
            parse_local_instructions,
            0,
            lower_local_instructions,
            dispatch_local_instructions,
            execute_local_instructions,
            wrapper_local_instructions,
        ),
        outcome,
    })
}

fn measure_typed_dispatch_surface<E>(sql: &str) -> (u64, SqlPerfOutcome)
where
    E: PersistedRow<Canister = DemoRpgCanister> + EntityValue,
{
    measure_surface_call(|| {
        db().execute_sql_dispatch::<E>(sql)
            .map_or_else(outcome_from_error, outcome_from_sql_query_result)
    })
}

fn measure_once(surface: SqlPerfSurface, sql: &str) -> (u64, SqlPerfOutcome) {
    match surface {
        SqlPerfSurface::GeneratedDispatch => measure_surface_call(|| {
            sql_dispatch::query(sql).map_or_else(outcome_from_error, outcome_from_sql_query_result)
        }),
        SqlPerfSurface::TypedDispatchCharacter => measure_typed_dispatch_surface::<Character>(sql),
    }
}

fn measure_surface_call(run: impl FnOnce() -> SqlPerfOutcome) -> (u64, SqlPerfOutcome) {
    let start = read_local_instruction_counter();
    let outcome = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, outcome)
}

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
