//!
//! One-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::types::{Id, Ulid};
#[cfg(feature = "exact-key-measurement")]
use icydb::{db::DynamicQuery, db::query::FieldRef};
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[cfg(feature = "exact-key-measurement")]
const MAX_EXACT_KEY_MEASUREMENT_ITEMS: u16 = 1_000;

#[cfg(feature = "exact-key-measurement")]
fn measurement_keys(count: u16, distinct: bool) -> Vec<Id<OneSimpleEntity01>> {
    (0..count)
        .map(|index| {
            let value = if distinct { u128::from(index) + 1 } else { 1 };
            Id::from_key(Ulid::from_u128(value))
        })
        .collect()
}

#[ic_cdk::query]
#[cfg(not(feature = "lifecycle-audit"))]
fn query_one_entity_typed_rows() -> u32 {
    let Ok(database) = db() else {
        return 0;
    };
    database
        .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
        .map_or(0, |row| u32::from(row.is_some()))
}

/// Measure one planner-free batch of distinct missing primary keys.
#[ic_cdk::query]
#[cfg(feature = "exact-key-measurement")]
fn measure_exact_key_batch(items: u16, distinct: bool) -> ((u16, u16, u32, u64),) {
    let items = items.min(MAX_EXACT_KEY_MEASUREMENT_ITEMS);
    let keys = measurement_keys(items, distinct);
    let Ok(database) = db() else {
        return ((0, 1, 0, 0),);
    };
    let start = ic_cdk::api::performance_counter(1);
    let result = database.get_many::<OneSimpleEntity01>(&keys);
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    match result {
        Ok(rows) => {
            let found = rows.iter().filter(|row| row.is_some()).count();
            ((
                items,
                0,
                u32::try_from(found).unwrap_or(u32::MAX),
                local_instructions,
            ),)
        }
        Err(_) => ((items, 1, 0, local_instructions),),
    }
}

/// Measure the former one-dynamic-query-per-key workload in the same binary.
#[ic_cdk::query]
#[cfg(feature = "exact-key-measurement")]
fn measure_dynamic_key_loop(items: u16, distinct: bool) -> ((u16, u16, u32, u64),) {
    let items = items.min(MAX_EXACT_KEY_MEASUREMENT_ITEMS);
    let keys = measurement_keys(items, distinct);
    let Ok(database) = db() else {
        return ((0, 1, 0, 0),);
    };
    let mut failures = 0_u16;
    let mut rows = 0_u32;
    let start = ic_cdk::api::performance_counter(1);
    for key in keys {
        let request =
            DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(key.key()));
        match database.execute_public_dynamic_query(&request) {
            Ok(output) => rows = rows.saturating_add(output.row_count),
            Err(_) => failures = failures.saturating_add(1),
        }
    }
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    ((items, failures, rows, local_instructions),)
}

#[ic_cdk::query]
#[cfg(feature = "lifecycle-audit")]
fn query_one_entity_typed_rows() -> Result<u32, u16> {
    let database = db().map_err(|error| error.code().raw())?;
    let row = database
        .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
        .map_err(|error| match error {
            icydb::db::query::TypedQueryError::Database(error) => error.code().raw(),
            icydb::db::query::TypedQueryError::Row(_) => u16::MAX,
        })?;

    Ok(u32::from(row.is_some()))
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();

#[cfg(test)]
mod tests {
    use crate::db;
    use icydb::{
        db::{
            StructuralPatch, WriteCell,
            query::{FieldRef, TypedQueryError, count},
        },
        diagnostic::{DiagnosticCode, DiagnosticDetail, ErrorOrigin, QueryReadAdmissionCode},
        types::{Id, Ulid},
        value::{InputValue, OutputValue},
    };
    use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

    fn insert_one_native_row(name: &str) -> Ulid {
        let patch = StructuralPatch::new()
            .field("name", WriteCell::Value(InputValue::Text(name.to_string())));
        let result = db()
            .expect("native database should initialize")
            .execute_trusted_structural_insert_batch("OneSimpleEntity01", vec![patch])
            .expect("native insert should succeed");
        let id_slot = result
            .columns
            .iter()
            .position(|column| column == "id")
            .expect("insert result should include the accepted identity field");
        match result.rows.first().and_then(|row| row.get(id_slot)) {
            Some(OutputValue::Ulid(id)) => *id,
            _ => panic!("generated identity should be returned as an Ulid"),
        }
    }

    #[test]
    fn first_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("first");
    }

    #[test]
    fn second_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("second");
    }

    #[test]
    fn generated_typed_grouped_terminal_executes_without_sql() {
        let id = insert_one_native_row("grouped");
        let grouped = db()
            .expect("native database should initialize")
            .query::<OneSimpleEntity01>()
            .expect("generated typed adapter should bind")
            .filter(FieldRef::new("id").eq(id))
            .group_by("name")
            .aggregate(count())
            .grouped_limits(1, 1024)
            .limit(1)
            .execute_grouped()
            .expect("generated typed grouped query should execute");

        assert_eq!(grouped.row_count, 1);
        assert_eq!(
            grouped.rows[0].group_key(),
            &[OutputValue::Text("grouped".to_string())]
        );
        assert_eq!(grouped.rows[0].aggregate_values(), &[OutputValue::Nat64(1)]);
        assert_eq!(grouped.next_cursor, None);

        let error = db()
            .expect("native database should initialize")
            .query::<OneSimpleEntity01>()
            .expect("generated typed adapter should bind")
            .group_by("name")
            .aggregate(count())
            .execute_grouped()
            .expect_err("generated typed grouped query must require explicit limits");
        let TypedQueryError::Database(error) = error else {
            panic!("grouped limit rejection should cross the typed database boundary");
        };
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.code(), DiagnosticCode::QueryReadAdmission);
        assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
        assert_eq!(
            diagnostic.detail(),
            Some(&DiagnosticDetail::QueryReadAdmission {
                reason: QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            })
        );
    }

    #[test]
    fn generated_exact_key_reads_preserve_order_missing_and_duplicates() {
        let first = insert_one_native_row("first-exact");
        let second = insert_one_native_row("second-exact");
        let missing = Ulid::MAX;
        let database = db().expect("native database should initialize");

        let rows = database
            .get_many::<OneSimpleEntity01>(&[
                Id::from_key(second),
                Id::from_key(missing),
                Id::from_key(first),
                Id::from_key(second),
            ])
            .expect("bounded exact-key batch should execute");

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].as_ref().map(|row| row.id), Some(second));
        assert!(rows[1].is_none());
        assert_eq!(rows[2].as_ref().map(|row| row.id), Some(first));
        assert_eq!(rows[3].as_ref().map(|row| row.id), Some(second));
        assert_eq!(
            database
                .get::<OneSimpleEntity01>(Id::from_key(first))
                .expect("single exact-key read should execute")
                .map(|row| row.id),
            Some(first),
        );
    }
}
