//!
//! One-entity dynamic-query canister used for wasm-footprint attribution.
//!

use icydb::{
    db::{
        DynamicQuery,
        query::{FieldRef, asc},
    },
    types::Ulid,
};

icydb::start!();

const MAX_REPEATED_QUERIES: u16 = 1_000;

#[ic_cdk::query]
fn query_one_entity_dynamic_rows() -> u32 {
    icydb::db::with_request_execution(|| {
        let Ok(database) = icydb::db!() else {
            return 0;
        };
        let request =
            DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(Ulid::MIN));
        let Ok(output) = database.execute_public_dynamic_query(&request) else {
            return 0;
        };

        output.row_count
    })
}

/// Measure a bounded run of identical point queries inside one query message.
#[ic_cdk::query]
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
        let start = ic_cdk::api::performance_counter(1);
        for _ in 0..executions {
            match database.execute_trusted_dynamic_query(&request) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}

/// Measure point queries with one different key binding per execution.
#[ic_cdk::query]
fn measure_parameterized_point_queries(repetitions: u16) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let executions = repetitions.min(MAX_REPEATED_QUERIES);
        let Ok(database) = icydb::db!() else {
            return ((0, executions, 0, 0),);
        };
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = ic_cdk::api::performance_counter(1);
        for value in 0..executions {
            let request = DynamicQuery::new("OneSimpleEntity01")
                .filter(FieldRef::new("id").eq(Ulid::from_u128(u128::from(value))));
            match database.execute_trusted_dynamic_query(&request) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}

/// Measure a bounded run of empty scans inside one query message.
#[ic_cdk::query]
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
        let start = ic_cdk::api::performance_counter(1);
        for _ in 0..executions {
            match database.execute_trusted_dynamic_query(&request) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        ((executions, failures, rows, local_instructions),)
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
