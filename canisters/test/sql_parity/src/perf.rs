//!
//! Test-only SQL perf harness for broad sql_parity integration sampling.
//!

use crate::{core_db, db, sql_dispatch};
use candid::{CandidType, Deserialize};
use icydb::{
    Error,
    db::{
        EntityAuthority, PersistedRow, SqlStatementRoute, identifiers_tail_match,
        query::Predicate,
        response::{
            PagedGroupedResponse, PagedResponse, Response, WriteBatchResponse, WriteResponse,
        },
        sql::SqlQueryResult,
    },
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::{EntitySchema, EntityValue},
    value::Value,
};
use icydb_testing_test_sql_parity_fixtures::schema::{
    Customer, CustomerAccount, CustomerOrder, SqlParityCanister,
};

const MAX_REPEAT_COUNT: u32 = 100;

//
// SqlPerfSurface
//
// One measured SQL surface owned by the sql_parity canister perf harness.
// This stays intentionally narrow so the harness can compare generated SQL
// dispatch against representative typed session surfaces without pretending to
// cover every possible query front in one first pass.
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedQueryFromSqlCustomerExecute,
    TypedExecuteSqlCustomer,
    TypedInsertCustomer,
    TypedInsertManyAtomicCustomer10,
    TypedInsertManyAtomicCustomer100,
    TypedInsertManyAtomicCustomer1000,
    TypedInsertManyNonAtomicCustomer10,
    TypedInsertManyNonAtomicCustomer100,
    TypedInsertManyNonAtomicCustomer1000,
    TypedUpdateCustomer,
    FluentDeleteCustomerByIdLimit1Count,
    FluentDeletePerfCustomerCount,
    TypedExecuteSqlGroupedCustomer,
    TypedExecuteSqlGroupedCustomerSecondPage,
    TypedExecuteSqlAggregateCustomer,
    FluentLoadCustomerByIdLimit2,
    FluentLoadCustomerNameEqLimit1,
    FluentPagedCustomerByIdLimit2FirstPage,
    FluentPagedCustomerByIdLimit2SecondPage,
    FluentPagedCustomerByIdLimit2InvalidCursor,
}

//
// SqlPerfRequest
//
// One perf-harness request for one SQL surface and one query shape.
// `repeat_count` runs happen inside one wasm call so the sample can report
// both the first execution cost and the warmed repeated-run range.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfRequest {
    pub surface: SqlPerfSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
    pub repeat_count: u32,
}

//
// SqlPerfOutcome
//
// Compact result summary for one measured SQL surface.
// The audit only needs stable payload shape, cardinality, and failure class
// signals here; full query payload rendering stays outside the perf harness.
//

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

//
// SqlPerfSample
//
// One repeated wasm-side instruction sample for one SQL surface.
// This reports first/min/max/avg/total local instruction deltas so the audit
// can see cold-vs-warm behavior without relying on host-side zeroed counters.
//

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

//
// SqlPerfAttributionSurface
//
// Representative SQL query surfaces used for fixed-cost phase attribution.
// This stays intentionally narrow so attribution can isolate the shared read
// stack for representative scalar and grouped SELECT shapes.
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedGroupedCustomer,
    TypedGroupedCustomerSecondPage,
}

//
// SqlPerfAttributionRequest
//
// One phase-attribution request for one representative SQL surface.
// This measures one single execution and breaks the total into parse, route,
// lower, core-dispatch overhead, executor, and outer-wrapper costs.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfAttributionRequest {
    pub surface: SqlPerfAttributionSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
}

//
// SqlPerfAttributionSample
//
// One fixed-cost SQL query attribution sample measured inside wasm.
// `dispatch_local_instructions` captures the core path between lowering and
// executor entry, while `wrapper_local_instructions` captures the remaining
// surface work above the attributed core path.
//

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

// Measure one SQL surface request inside the running canister.
pub fn sample_sql_surface(request: SqlPerfRequest) -> Result<SqlPerfSample, Error> {
    validate_perf_request(&request)?;

    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();
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

// Attribute one representative SQL query surface into fixed-cost wasm phases.
pub fn attribute_sql_surface(
    request: SqlPerfAttributionRequest,
) -> Result<SqlPerfAttributionSample, Error> {
    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();

    match request.surface {
        SqlPerfAttributionSurface::GeneratedDispatch => {
            attribute_generated_dispatch_surface(sql.as_str())
        }
        SqlPerfAttributionSurface::TypedDispatchCustomer => {
            attribute_typed_dispatch_surface::<Customer>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomer,
            )
        }
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder => {
            attribute_typed_dispatch_surface::<CustomerOrder>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
            )
        }
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount => {
            attribute_typed_dispatch_surface::<CustomerAccount>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
            )
        }
        SqlPerfAttributionSurface::TypedGroupedCustomer => {
            attribute_typed_grouped_surface(sql.as_str(), request.cursor_token.as_deref())
        }
        SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage => {
            attribute_typed_grouped_second_page_surface(sql.as_str())
        }
    }
}

// Keep perf-harness input validation local so the public `icydb` SQL facade
// does not need to retain generated query-surface adapter helpers.
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

fn missing_continuation_sample(message: &'static str) -> (u64, SqlPerfOutcome) {
    (0, outcome_from_error(invalid_perf_request_error(message)))
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

    // Phase 1: attribute the core generated-query path with parse kept
    // separate from authority resolution, lowering, and shared dispatch.
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

    // Phase 2: measure the real generated surface total and assign the
    // remaining cost to the outer wrapper above the attributed core path.
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

// Attribute one typed `execute_sql_dispatch::<E>` surface through the same
// core phases used by the generated lane while keeping the entity binding
// explicit at the typed facade boundary.
fn attribute_typed_dispatch_surface<E>(
    sql: &str,
    surface: SqlPerfAttributionSurface,
) -> Result<SqlPerfAttributionSample, Error>
where
    E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
{
    let core = core_db();
    let authority = EntityAuthority::for_type::<E>();

    // Phase 1: parse once and attribute the core typed dispatch path after
    // parse so the remaining outer facade wrapper can be measured cleanly.
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

    // Phase 2: measure the full facade surface total and assign the remaining
    // cost above the attributed core path to the outer wrapper.
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

fn attribute_typed_grouped_surface(
    sql: &str,
    cursor_token: Option<&str>,
) -> Result<SqlPerfAttributionSample, Error> {
    let core = core_db();

    // Phase 1: parse once so grouped attribution can isolate the typed
    // query-intent build from the shared grouped execute path.
    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (core_grouped_total, query_result) =
        measure_result(|| core.query_from_sql::<Customer>(sql).map_err(Error::from));
    let query = query_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(
                Customer::MODEL.name(),
                Customer::MODEL.primary_key().name(),
            )
            .map_err(Error::from)
    });
    let _lowered = lowered_result?;

    let dispatch_local_instructions = core_grouped_total
        .saturating_sub(parse_local_instructions.saturating_add(lower_local_instructions));

    // Phase 2: execute the already-built grouped intent so the remaining
    // facade work can be attributed as one outer wrapper cost.
    let (execute_local_instructions, execute_result) = measure_result(|| {
        core.execute_grouped(&query, cursor_token)
            .map_err(Error::from)
    });
    let _execute_result = execute_result?;

    let (total_local_instructions, outcome) = measure_surface_call(|| {
        db().execute_sql_grouped::<Customer>(sql, cursor_token)
            .map_or_else(outcome_from_error, outcome_from_grouped_response)
    });
    let attributed_core = core_grouped_total.saturating_add(execute_local_instructions);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
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

// Bootstrap one grouped query cursor token from the typed grouped facade so
// second-page attribution can isolate resumed grouped execution separately
// from first-page cursor emission.
fn bootstrap_typed_grouped_cursor_token(sql: &str) -> Result<String, Error> {
    let first_page = db().execute_sql_grouped::<Customer>(sql, None)?;

    first_page.next_cursor().map(str::to_string).ok_or_else(|| {
        Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Query,
            "grouped second-page attribution requires a continuation cursor",
        )
    })
}

fn attribute_typed_grouped_second_page_surface(
    sql: &str,
) -> Result<SqlPerfAttributionSample, Error> {
    // Phase 1: bootstrap one continuation cursor outside the measured resumed
    // call so attribution stays focused on the second-page grouped request.
    let cursor_token = bootstrap_typed_grouped_cursor_token(sql)?;

    let mut sample = attribute_typed_grouped_surface(sql, Some(cursor_token.as_str()))?;
    sample.surface = SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage;

    Ok(sample)
}

fn measure_typed_grouped_second_page(sql: &str) -> (u64, SqlPerfOutcome) {
    let cursor_token = match bootstrap_typed_grouped_cursor_token(sql) {
        Ok(cursor_token) => cursor_token,
        Err(err) => return (0, outcome_from_error(err)),
    };

    measure_surface_call(|| {
        db().execute_sql_grouped::<Customer>(sql, Some(cursor_token.as_str()))
            .map_or_else(outcome_from_error, outcome_from_grouped_response)
    })
}

fn measure_fluent_paged_second_page() -> (u64, SqlPerfOutcome) {
    let bootstrap = db()
        .load::<Customer>()
        .order_by("id")
        .limit(2)
        .execute_paged();
    let outcome = match bootstrap {
        Ok(first_page) => {
            let Some(cursor_token) = first_page.next_cursor() else {
                return missing_continuation_sample(
                    "fluent paged second-page sample requires a continuation cursor",
                );
            };

            return measure_surface_call(|| {
                db().load::<Customer>()
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

fn perf_insert_customer() -> Customer {
    Customer {
        name: "perf-insert-customer".to_string(),
        age: 29,
        ..Default::default()
    }
}

fn perf_insert_customer_for_batch(batch_size: u32, offset: u32) -> Customer {
    let age_offset = i32::try_from(offset % 50).expect("offset modulo 50 must fit in i32");

    Customer {
        name: format!("perf-insert-customer-{batch_size}-{offset}"),
        age: 20 + age_offset,
        ..Default::default()
    }
}

fn perf_insert_customer_batch(batch_size: u32) -> Vec<Customer> {
    (0..batch_size)
        .map(|offset| perf_insert_customer_for_batch(batch_size, offset))
        .collect()
}

fn perf_update_customers() -> (Customer, Customer) {
    let base = Customer {
        name: "perf-update-customer-before".to_string(),
        age: 33,
        ..Default::default()
    };
    let inserted = base.clone();
    let updated = Customer {
        name: "perf-update-customer-after".to_string(),
        age: 34,
        ..base
    };

    (inserted, updated)
}

fn perf_delete_customer() -> Customer {
    Customer {
        name: "perf-delete-customer".to_string(),
        age: 35,
        ..Default::default()
    }
}

fn measure_typed_update_customer() -> (u64, SqlPerfOutcome) {
    let (inserted, updated) = perf_update_customers();
    let outcome = match db().insert(inserted) {
        Ok(_) => {
            return measure_surface_call(|| {
                db().update(updated)
                    .map_or_else(outcome_from_error, outcome_from_write_response)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_fluent_delete_perf_customer_count() -> (u64, SqlPerfOutcome) {
    let inserted = perf_delete_customer();
    let outcome = match db().insert(inserted) {
        Ok(_) => {
            return measure_surface_call(|| {
                db().delete::<Customer>()
                    .filter(Predicate::eq(
                        "name".to_string(),
                        "perf-delete-customer".into(),
                    ))
                    .order_by("id")
                    .limit(1)
                    .execute_count_only()
                    .map_or_else(outcome_from_error, outcome_from_delete_count)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_typed_insert_many_atomic_customer(batch_size: u32) -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().insert_many_atomic(perf_insert_customer_batch(batch_size))
            .map_or_else(outcome_from_error, outcome_from_write_batch_response)
    })
}

fn measure_typed_insert_many_non_atomic_customer(batch_size: u32) -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().insert_many_non_atomic(perf_insert_customer_batch(batch_size))
            .map_or_else(outcome_from_error, outcome_from_write_batch_response)
    })
}

// Measure one typed `execute_sql_dispatch::<E>` projection surface while
// keeping the entity binding explicit in the checked-in perf harness.
fn measure_typed_dispatch_surface<E>(sql: &str) -> (u64, SqlPerfOutcome)
where
    E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
{
    measure_surface_call(|| {
        db().execute_sql_dispatch::<E>(sql)
            .map_or_else(outcome_from_error, outcome_from_sql_query_result)
    })
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
        SqlPerfSurface::TypedDispatchCustomer => measure_typed_dispatch_surface::<Customer>(sql),
        SqlPerfSurface::TypedDispatchCustomerOrder => {
            measure_typed_dispatch_surface::<CustomerOrder>(sql)
        }
        SqlPerfSurface::TypedDispatchCustomerAccount => {
            measure_typed_dispatch_surface::<CustomerAccount>(sql)
        }
        SqlPerfSurface::TypedQueryFromSqlCustomerExecute => measure_surface_call(|| {
            let session = db();
            session
                .query_from_sql::<Customer>(sql)
                .and_then(|query| session.execute_query(&query))
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedExecuteSqlCustomer => measure_surface_call(|| {
            db().execute_sql::<Customer>(sql)
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedInsertCustomer => measure_surface_call(|| {
            db().insert(perf_insert_customer())
                .map_or_else(outcome_from_error, outcome_from_write_response)
        }),
        SqlPerfSurface::TypedInsertManyAtomicCustomer10 => {
            measure_typed_insert_many_atomic_customer(10)
        }
        SqlPerfSurface::TypedInsertManyAtomicCustomer100 => {
            measure_typed_insert_many_atomic_customer(100)
        }
        SqlPerfSurface::TypedInsertManyAtomicCustomer1000 => {
            measure_typed_insert_many_atomic_customer(1000)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer10 => {
            measure_typed_insert_many_non_atomic_customer(10)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer100 => {
            measure_typed_insert_many_non_atomic_customer(100)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer1000 => {
            measure_typed_insert_many_non_atomic_customer(1000)
        }
        SqlPerfSurface::TypedUpdateCustomer => measure_typed_update_customer(),
        SqlPerfSurface::FluentDeleteCustomerByIdLimit1Count => measure_surface_call(|| {
            db().delete::<Customer>()
                .order_by("id")
                .limit(1)
                .execute_count_only()
                .map_or_else(outcome_from_error, outcome_from_delete_count)
        }),
        SqlPerfSurface::FluentDeletePerfCustomerCount => {
            measure_fluent_delete_perf_customer_count()
        }
        SqlPerfSurface::TypedExecuteSqlGroupedCustomer => measure_surface_call(|| {
            db().execute_sql_grouped::<Customer>(sql, cursor_token)
                .map_or_else(outcome_from_error, outcome_from_grouped_response)
        }),
        SqlPerfSurface::TypedExecuteSqlGroupedCustomerSecondPage => {
            measure_typed_grouped_second_page(sql)
        }
        SqlPerfSurface::TypedExecuteSqlAggregateCustomer => measure_surface_call(|| {
            db().execute_sql_aggregate::<Customer>(sql)
                .map_or_else(outcome_from_error, outcome_from_value)
        }),
        SqlPerfSurface::FluentLoadCustomerByIdLimit2 => measure_surface_call(|| {
            db().load::<Customer>()
                .order_by("id")
                .limit(2)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentLoadCustomerNameEqLimit1 => measure_surface_call(|| {
            db().load::<Customer>()
                .filter(Predicate::eq("name".to_string(), "alice".into()))
                .order_by("id")
                .limit(1)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentPagedCustomerByIdLimit2FirstPage => measure_surface_call(|| {
            db().load::<Customer>()
                .order_by("id")
                .limit(2)
                .execute_paged()
                .map_or_else(outcome_from_error, outcome_from_paged_response)
        }),
        SqlPerfSurface::FluentPagedCustomerByIdLimit2SecondPage => {
            measure_fluent_paged_second_page()
        }
        SqlPerfSurface::FluentPagedCustomerByIdLimit2InvalidCursor => measure_surface_call(|| {
            db().load::<Customer>()
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

fn outcome_from_response(result: Response<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "typed_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(result.count()),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_paged_response(result: PagedResponse<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "paged_response".to_string(),
        entity: Some("Customer".to_string()),
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
        entity: Some("Customer".to_string()),
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
        entity: Some("Customer".to_string()),
        row_count: None,
        detail_count: None,
        has_cursor: None,
        rendered_value: Some(format!("{result:?}")),
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_write_response(result: WriteResponse<Customer>) -> SqlPerfOutcome {
    let _ = result.id();

    SqlPerfOutcome {
        success: true,
        result_kind: "write_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(1),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_write_batch_response(result: WriteBatchResponse<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "write_batch_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(checked_perf_count(result.len(), "write batch row count")),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
    }
}

fn outcome_from_delete_count(row_count: u32) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "delete_count".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(row_count),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
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
