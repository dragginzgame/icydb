//!
//! One-entity dynamic-query canister used for wasm-footprint attribution.
//!

// IC counter 1 preserves the call-context sample boundary through Platform.
// Native system calls provide no IC measurement evidence.
use ic_platform::ic0::performance_counter;

use ic_wire::native;
use icydb::{
    db::{
        DynamicQuery,
        query::{FieldRef, asc, count},
    },
    types::Ulid,
};
use icydb_testing_wasm_helpers::wire::DynamicRowsQuery;

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants() -> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(vec![icydb::db::MemoryAuthority::new(
        "icydb_one_simple",
        "icydb.one_simple.",
    )?])
}

canic_icydb::start!(icydb);

const MAX_REPEATED_QUERIES: u16 = 1_000;

// The Canic adapter exports this raw IC Wire method.
fn query_one_entity_dynamic_rows() {
    // Admission precedes argument allocation/copy and all database work.
    if ic_platform::ic0::msg_arg_data_size() > DynamicRowsQuery::REQUEST_LIMITS.bytes {
        ic_platform::ic0::msg_reject(b"dynamic row query request limit");
        return;
    }
    match dynamic_rows_reply(&ic_platform::cdk::api::msg_arg_data(), dynamic_row_count) {
        Ok(reply) => {
            ic_platform::ic0::msg_reply_data_append(&reply);
            ic_platform::ic0::msg_reply();
        }
        Err(error) => ic_platform::ic0::msg_reject(error.to_string().as_bytes()),
    }
}

canic_icydb::wire_endpoint!(
    query,
    "query_one_entity_dynamic_rows",
    DynamicRowsQuery::ID,
    {
        query_one_entity_dynamic_rows();
    }
);

fn dynamic_rows_reply(
    request: &[u8],
    query: impl FnOnce() -> u32,
) -> Result<Vec<u8>, ic_wire::Error> {
    native::decode_request::<()>(
        DynamicRowsQuery::ID,
        request,
        DynamicRowsQuery::REQUEST_LIMITS,
    )?;
    native::encode_response(
        DynamicRowsQuery::ID,
        &query(),
        DynamicRowsQuery::RESPONSE_LIMITS,
    )
}

fn dynamic_row_count() -> u32 {
    icydb::db::with_request_execution(|| {
        let Ok(database) = icydb::db!() else {
            return 0;
        };
        let request =
            DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(Ulid::MIN));
        let Ok(output) = database.execute_live_page(&request, None) else {
            return 0;
        };

        output.row_count
    })
}

/// Measure a bounded run of identical point queries inside one query message.

fn measure_repeated_point_queries(repetitions: u16) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let executions = repetitions.min(MAX_REPEATED_QUERIES);
        let Ok(database) = icydb::db!() else {
            return ((0, executions, 0, 0),);
        };
        let request =
            DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(Ulid::MIN));
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = performance_counter(1);
        for _ in 0..executions {
            match database.execute_trusted_live_page(&request, None) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}
icydb_testing_canic_adapter::export_wire!(query, "measure_repeated_point_queries", MeasureRepeatedPointQueries, measure_repeated_point_queries, (repetitions: u16), ((u16, u16, u32, u64),));

/// Measure point queries with one different key binding per execution.

fn measure_parameterized_point_queries(repetitions: u16) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let executions = repetitions.min(MAX_REPEATED_QUERIES);
        let Ok(database) = icydb::db!() else {
            return ((0, executions, 0, 0),);
        };
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = performance_counter(1);
        for value in 0..executions {
            let request = DynamicQuery::new("OneSimpleEntity01")
                .filter(FieldRef::new("id").eq(Ulid::from_u128(u128::from(value))));
            match database.execute_trusted_live_page(&request, None) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}
icydb_testing_canic_adapter::export_wire!(query, "measure_parameterized_point_queries", MeasureParameterizedPointQueries, measure_parameterized_point_queries, (repetitions: u16), ((u16, u16, u32, u64),));

/// Measure a bounded run of empty scans inside one query message.

fn measure_repeated_scan_queries(repetitions: u16) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let executions = repetitions.min(MAX_REPEATED_QUERIES);
        let Ok(database) = icydb::db!() else {
            return ((0, executions, 0, 0),);
        };
        let request = DynamicQuery::new("OneSimpleEntity01")
            .order_by(asc("id"))
            .limit(1);
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = performance_counter(1);
        for _ in 0..executions {
            match database.execute_trusted_live_page(&request, None) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}
icydb_testing_canic_adapter::export_wire!(query, "measure_repeated_scan_queries", MeasureRepeatedScanQueries, measure_repeated_scan_queries, (repetitions: u16), ((u16, u16, u32, u64),));

/// Measure grouped declaration preparation and empty execution on warm calls.

fn measure_repeated_grouped_queries(repetitions: u16) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let executions = repetitions.min(MAX_REPEATED_QUERIES);
        let Ok(database) = icydb::db!() else {
            return ((0, executions, 0, 0),);
        };
        let request = DynamicQuery::new("OneSimpleEntity01")
            .group_by("name")
            .aggregate(count())
            .grouped_limits(1, 16 * 1024)
            .limit(1);
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = performance_counter(1);
        for _ in 0..executions {
            match database.execute_trusted_dynamic_grouped_query(&request) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}
icydb_testing_canic_adapter::export_wire!(query, "measure_repeated_grouped_queries", MeasureRepeatedGroupedQueries, measure_repeated_grouped_queries, (repetitions: u16), ((u16, u16, u32, u64),));

// Export after all endpoints so the generated interface includes every method.

#[cfg(test)]
mod wire_tests;
