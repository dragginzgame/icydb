//!
//! One-entity typed-query canister used for wasm-footprint auditing.
//!

#[cfg(any(feature = "typed-explain-measurement", feature = "sql"))]
mod explain_measurement;

// IC counter 1 preserves the call-context sample boundary through Platform.
// Native system calls provide no IC measurement evidence.
#[cfg(any(
    feature = "exact-key-measurement",
    feature = "typed-explain-measurement",
    feature = "sql-explain-measurement"
))]
use ic_platform::ic0::performance_counter;

#[cfg(feature = "lifecycle-participant")]
use icydb as __icydb_facade;
use icydb::types::{Id, Ulid};
#[cfg(feature = "exact-key-measurement")]
use icydb::{db::DynamicQuery, db::query::FieldRef};
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants() -> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(vec![icydb::db::MemoryAuthority::new(
        "icydb_one_simple",
        "icydb.one_simple.",
    )?])
}

#[cfg(not(feature = "lifecycle-participant"))]
canic_icydb::start!(icydb);

#[cfg(feature = "lifecycle-participant")]
icydb::__icydb_start_actor!();
// This audit isolates private participant code without mounting host lifecycle
// exports. Applications select the adapter's complete public host composition.
#[cfg(feature = "lifecycle-participant")]
canic_icydb::__icydb_host_module!();
#[cfg(feature = "lifecycle-participant")]
canic_icydb::__icydb_start_participant_lifecycle!();

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

#[cfg(not(feature = "lifecycle-audit"))]
fn query_one_entity_typed_rows() -> u32 {
    icydb::db::with_request_execution(|| {
        let Ok(database) = db() else {
            return 0;
        };
        database
            .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
            .map_or(0, |row| u32::from(row.is_some()))
    })
}
#[cfg(not(feature = "lifecycle-audit"))]
icydb_testing_canic_adapter::export_wire!(
    query,
    "query_one_entity_typed_rows",
    QueryOneEntityTypedRows,
    query_one_entity_typed_rows,
    (),
    u32
);

/// Measure one planner-free batch of distinct missing primary keys.

#[cfg(feature = "exact-key-measurement")]
fn measure_exact_key_batch(items: u16, distinct: bool) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let items = items.min(MAX_EXACT_KEY_MEASUREMENT_ITEMS);
        let keys = measurement_keys(items, distinct);
        let Ok(database) = db() else {
            return ((0, 1, 0, 0),);
        };
        let start = performance_counter(1);
        let result = database.get_many::<OneSimpleEntity01>(&keys);
        let local_instructions = performance_counter(1).saturating_sub(start);
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
    })
}
#[cfg(feature = "exact-key-measurement")]
icydb_testing_canic_adapter::export_wire!(query, "measure_exact_key_batch", MeasureExactKeyBatch, measure_exact_key_batch, (items: u16, distinct: bool), ((u16, u16, u32, u64),));

/// Measure the former one-dynamic-query-per-key workload in the same binary.

#[cfg(feature = "exact-key-measurement")]
fn measure_dynamic_key_loop(items: u16, distinct: bool) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
        let items = items.min(MAX_EXACT_KEY_MEASUREMENT_ITEMS);
        let keys = measurement_keys(items, distinct);
        let Ok(database) = db() else {
            return ((0, 1, 0, 0),);
        };
        let mut failures = 0_u16;
        let mut rows = 0_u32;
        let start = performance_counter(1);
        for key in keys {
            let request =
                DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(key.key()));
            match database.execute_live_page(&request, None) {
                Ok(output) => {
                    rows = rows.saturating_add(u32::try_from(output.len()).unwrap_or(u32::MAX));
                }
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = performance_counter(1).saturating_sub(start);

        ((items, failures, rows, local_instructions),)
    })
}
#[cfg(feature = "exact-key-measurement")]
icydb_testing_canic_adapter::export_wire!(query, "measure_dynamic_key_loop", MeasureDynamicKeyLoop, measure_dynamic_key_loop, (items: u16, distinct: bool), ((u16, u16, u32, u64),));

#[cfg(feature = "lifecycle-audit")]
fn query_one_entity_typed_lifecycle_rows() -> Result<u32, u16> {
    icydb::db::with_request_execution(|| {
        let database = db().map_err(|error| error.code().raw())?;
        let row = database
            .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
            .map_err(|error| match error {
                icydb::db::TypedOperationError::Database(error) => error.code().raw(),
                icydb::db::TypedOperationError::Adapter(_) => u16::MAX,
            })?;

        Ok(u32::from(row.is_some()))
    })
}
#[cfg(feature = "lifecycle-audit")]
icydb_testing_canic_adapter::export_wire!(query, "query_one_entity_typed_lifecycle_rows", QueryOneEntityTypedLifecycleRows, query_one_entity_typed_lifecycle_rows, (), Result<u32, u16>);

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "u256-audit"))]
mod u256_tests {
    use crate::db;
    use icydb::{
        U256,
        db::{
            DynamicQuery, StructuralPatch, TypedAdapterError, TypedOperationError, TypedWrite,
            WriteCell,
            query::{FieldRef, asc, count, max_by, min_by},
        },
        traits::EntitySource,
        types::Id,
        value::{InputValue, OutputValue},
    };
    use icydb_testing_audit_one_simple_fixtures::one_simple::{OneSimpleEntity01, U256AuditEntity};

    fn u256_patch(
        id: U256,
        amount: U256,
        optional_amount: Option<U256>,
        bucket: u64,
        label: &str,
    ) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::u256(id)))
            .field("amount", WriteCell::Value(InputValue::u256(amount)))
            .field(
                "optional_amount",
                optional_amount.map_or(WriteCell::Null, |value| {
                    WriteCell::Value(InputValue::u256(value))
                }),
            )
            .field("bucket", WriteCell::Value(InputValue::nat64(bucket)))
            .field(
                "label",
                WriteCell::Value(InputValue::text(label.to_string())),
            )
    }

    fn output_u256(output: &icydb::db::LiveQueryPageOutput, row: usize, field: &str) -> U256 {
        let column = output
            .columns
            .iter()
            .position(|column| column == field)
            .expect("U256 query output should retain the requested field");
        match output.rows.get(row).and_then(|row| row.get(column)) {
            Some(value) => match value.as_public() {
                icydb::value::PublicValue::U256(value) => *value,
                _ => panic!("U256 query output should retain its exact value kind"),
            },
            _ => panic!("U256 query output should retain its exact value kind"),
        }
    }

    #[test]
    fn native_u256_storage_indexes_queries_grouping_extrema_and_cursor_converge() {
        crate::__icydb_generated::__drive_native_database_for_tests()
            .expect("fresh native database startup should complete");
        icydb::db::with_request_execution(|| {
            let database = db().expect("native database should initialize");
            database
                .execute_trusted_structural_insert_batch(
                    U256AuditEntity::ENTITY,
                    vec![
                        u256_patch(U256::MIN, U256::MAX, None, 1, "maximum"),
                        u256_patch(U256::ONE, U256::ONE, Some(U256::MAX), 1, "one"),
                        u256_patch(
                            U256::from(2_u64),
                            U256::from(7_u64),
                            Some(U256::MIN),
                            2,
                            "seven",
                        ),
                    ],
                )
                .expect("U256 rows should persist and index");

            let exact = database
                .get::<U256AuditEntity>(Id::from_key(U256::MIN))
                .expect("U256 exact primary-key lookup should execute")
                .expect("U256 exact primary-key lookup should find the row");
            assert_eq!(exact.amount, U256::MAX);

            let ordered = DynamicQuery::new(U256AuditEntity::ENTITY)
                .filter(FieldRef::new("amount").gte(U256::ONE))
                .order_by(asc("amount"))
                .order_by(asc("id"))
                .limit(1);
            let first = database
                .execute_live_page(&ordered, None)
                .expect("indexed U256 range page should execute");
            assert_eq!(output_u256(&first, 0, "amount"), U256::ONE);

            let first_group_page = database
                .query::<U256AuditEntity>()
                .expect("generated U256 adapter should bind")
                .group_by("amount")
                .aggregate(count())
                .order_by(asc("amount"))
                .grouped_limits(4, 16 * 1024)
                .limit(1)
                .execute_grouped()
                .expect("first U256 grouped page should execute");
            assert_eq!(
                first_group_page.rows[0].group_key(),
                &[OutputValue::u256(U256::ONE)],
            );
            let cursor = first_group_page
                .next_cursor
                .expect("bounded U256 grouping should return a cursor");
            let second_group_page = database
                .query::<U256AuditEntity>()
                .expect("generated U256 adapter should bind")
                .group_by("amount")
                .aggregate(count())
                .order_by(asc("amount"))
                .grouped_limits(4, 16 * 1024)
                .limit(1)
                .cursor(cursor)
                .execute_grouped()
                .expect("U256 grouped cursor continuation should execute");
            assert_eq!(
                second_group_page.rows[0].group_key(),
                &[OutputValue::u256(U256::from(7_u64))],
            );

            let grouped = database
                .query::<U256AuditEntity>()
                .expect("generated U256 adapter should bind")
                .filter(FieldRef::new("amount").gte(U256::ONE))
                .group_by("bucket")
                .aggregate(min_by("amount"))
                .aggregate(max_by("amount"))
                .grouped_limits(4, 16 * 1024)
                .limit(4)
                .execute_grouped()
                .expect("U256 grouped extrema should execute");
            assert_eq!(grouped.row_count, 2);
            assert!(grouped.rows.iter().any(|row| {
                row.group_key() == [OutputValue::nat64(1)]
                    && row.aggregate_values()
                        == [OutputValue::u256(U256::ONE), OutputValue::u256(U256::MAX)]
            }));

            database
                .execute_trusted_structural_insert_batch(
                    U256AuditEntity::ENTITY,
                    vec![u256_patch(
                        U256::from(3_u64),
                        U256::ONE,
                        None,
                        3,
                        "duplicate",
                    )],
                )
                .expect_err("unique U256 index should reject a duplicate value");
        });
    }

    #[test]
    fn same_entity_typed_write_rows_reject_a_foreign_binding_before_execution() {
        crate::__icydb_generated::__drive_native_database_for_tests()
            .expect("fresh native database startup should complete");
        icydb::db::with_request_execution(|| {
            let database = db().expect("native database should initialize");
            let one_binding =
                OneSimpleEntity01::typed_binding(&database).expect("simple entity should bind");
            let u256_binding =
                U256AuditEntity::typed_binding(&database).expect("U256 entity should bind");
            let foreign = TypedWrite::delete(&u256_binding, InputValue::u256(U256::MIN));

            assert!(matches!(
                database.execute_trusted_typed_write_batch_rows(&one_binding, vec![foreign]),
                Err(TypedOperationError::Adapter(
                    TypedAdapterError::EntityMismatch
                )),
            ));
        });
    }
}
