//! Test-only collection query measurement through the maintained session API.

use crate::typed_fixture_invariant_error;
use candid::CandidType;
use ic_cdk::{api::performance_counter, update};
use icydb::{
    Error,
    db::{
        DynamicQuery, StructuralPatch, WriteCell,
        query::{FieldRef, FilterExpr},
        with_request_execution,
    },
    value::{InputValue, OutputValue},
};

const ENTITY: &str = "SqlTestCollectionProbe";

#[derive(CandidType)]
pub(crate) struct CollectionWorkloadSample {
    result: Result<Vec<Vec<OutputValue>>, Error>,
    query_instructions: u64,
}

// Installation and seed are deliberately separate from measured query messages.
#[update]
fn seed_collection_workload(length: u32) -> Result<(), icydb::Error> {
    with_request_execution(|| {
        if !matches!(length, 16 | 256 | 1_024) {
            return Err(typed_fixture_invariant_error());
        }
        let session = icydb::db!()?;
        let values = [
            InputValue::list((0..u64::from(length)).map(InputValue::nat64).collect()),
            InputValue::list(Vec::new()),
            InputValue::null(),
        ];
        let patches = (1_u64..)
            .zip(values)
            .map(|(id, items)| {
                StructuralPatch::new()
                    .field("id", WriteCell::Value(InputValue::nat64(id)))
                    .field("marker", WriteCell::Value(InputValue::nat64(7)))
                    .field("items", WriteCell::Value(items))
            })
            .collect();
        session.execute_trusted_structural_insert_batch(ENTITY, patches)?;
        Ok(())
    })
}

// Fixed scenarios keep SQL parsing and query construction outside the measured
// interval. No collection-specific runtime implementation is introduced here.
#[update]
fn measure_collection_workload(length: u32, scenario: u8) -> CollectionWorkloadSample {
    let mut query_instructions = 0;
    let result = with_request_execution(|| {
        if !matches!(length, 16 | 256 | 1_024) {
            return Err(typed_fixture_invariant_error());
        }
        let (id, predicate) = match scenario {
            0 | 7 => (1_u64, FieldRef::new("marker").eq(7_u64)),
            1 | 8 => (1, FilterExpr::contains("items", 0_u64)),
            2 => (1, FilterExpr::contains("items", u64::from(length) - 1)),
            3 => (1, FilterExpr::contains("items", u64::from(length))),
            4 => (1, FilterExpr::is_not_empty("items")),
            5 => (2, FilterExpr::is_empty("items")),
            6 => (3, FilterExpr::is_empty("items")),
            _ => return Err(typed_fixture_invariant_error()),
        };
        let fields = if scenario >= 7 {
            vec!["id", "items"]
        } else {
            vec!["id"]
        };
        let query = DynamicQuery::new(ENTITY)
            .filter(FilterExpr::and(vec![FieldRef::new("id").eq(id), predicate]))
            .select(fields)
            .limit(1);
        let session = icydb::db!()?;
        let start = performance_counter(1);
        let result = session.execute_trusted_live_page(&query, None);
        query_instructions = performance_counter(1).saturating_sub(start);
        result.map(|page| page.rows)
    });
    CollectionWorkloadSample {
        result,
        query_instructions,
    }
}
