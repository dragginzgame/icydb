//!
//! One-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::types::{Id, Ulid};
#[cfg(feature = "exact-key-measurement")]
use icydb::{db::DynamicQuery, db::query::FieldRef};
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

#[cfg(not(feature = "lifecycle-participant"))]
icydb::start!();

#[cfg(feature = "lifecycle-participant")]
icydb::start!(participant);

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
    icydb::db::with_request_execution(|| {
        let Ok(database) = db() else {
            return 0;
        };
        database
            .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
            .map_or(0, |row| u32::from(row.is_some()))
    })
}

/// Measure one planner-free batch of distinct missing primary keys.
#[ic_cdk::query]
#[cfg(feature = "exact-key-measurement")]
fn measure_exact_key_batch(items: u16, distinct: bool) -> ((u16, u16, u32, u64),) {
    icydb::db::with_request_execution(|| {
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
    })
}

/// Measure the former one-dynamic-query-per-key workload in the same binary.
#[ic_cdk::query]
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
        let start = ic_cdk::api::performance_counter(1);
        for key in keys {
            let request =
                DynamicQuery::new("OneSimpleEntity01").filter(FieldRef::new("id").eq(key.key()));
            match database.execute_live_page(&request, None) {
                Ok(output) => rows = rows.saturating_add(output.row_count),
                Err(_) => failures = failures.saturating_add(1),
            }
        }
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        ((items, failures, rows, local_instructions),)
    })
}

#[ic_cdk::query]
#[cfg(feature = "lifecycle-audit")]
fn query_one_entity_typed_rows() -> Result<u32, u16> {
    icydb::db::with_request_execution(|| {
        let database = db().map_err(|error| error.code().raw())?;
        let row = database
            .get::<OneSimpleEntity01>(Id::from_key(Ulid::MIN))
            .map_err(|error| match error {
                icydb::db::query::TypedQueryError::Database(error) => error.code().raw(),
                icydb::db::query::TypedQueryError::Row(_) => u16::MAX,
            })?;

        Ok(u32::from(row.is_some()))
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();

#[cfg(test)]
mod tests {
    use crate::db;
    use icydb::{
        db::{
            StructuralMutation, StructuralPatch, TypedAdapterError, TypedRowAdapter,
            TypedWriteAdapter, TypedWriteError, WriteCell,
            query::{FieldRef, TypedQueryError, count},
        },
        diagnostic::{DiagnosticCode, DiagnosticDetail, ErrorOrigin, QueryReadAdmissionCode},
        traits::EntitySource,
        types::{Id, Ulid},
        value::{InputValue, OutputValue},
    };
    use icydb_testing_audit_one_simple_fixtures::one_simple::{
        OneSimpleEntity01, OneSimpleEntity01Insert,
    };

    fn insert_one_native_row(name: &str) -> Ulid {
        crate::__icydb_generated::__initialize_native_database_for_tests()
            .expect("fresh native database startup should complete");
        icydb::db::with_request_execution(|| {
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
        })
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
        icydb::db::with_request_execution(|| {
            let grouped = db()
                .expect("native database should initialize")
                .query::<OneSimpleEntity01>()
                .expect("generated typed adapter should bind")
                .filter(FieldRef::new("id").eq(id))
                .group_by("name")
                .aggregate(count())
                .grouped_limits(1, 16 * 1024)
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
        });
    }

    #[test]
    fn generated_exact_key_reads_preserve_order_missing_and_duplicates() {
        let first = insert_one_native_row("first-exact");
        let second = insert_one_native_row("second-exact");
        let missing = Ulid::MAX;
        icydb::db::with_request_execution(|| {
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
        });
    }

    #[test]
    fn concrete_mutation_projection_terminals_return_typed_rows() {
        crate::__icydb_generated::__initialize_native_database_for_tests()
            .expect("fresh native database startup should complete");
        icydb::db::with_request_execution(|| {
            let database = db().expect("native database should initialize");
            let binding = OneSimpleEntity01::typed_binding(&database)
                .expect("generated entity should bind to accepted authority");
            let write = OneSimpleEntity01Insert {
                name: WriteCell::Value("single".to_string()),
            }
            .encode_write(&binding)
            .expect("generated insert should encode");
            let inserted = database
                .execute_trusted_typed_write_row(write)
                .and_then(|row| {
                    OneSimpleEntity01::decode_row(&binding, row).map_err(TypedWriteError::Adapter)
                })
                .expect("single typed mutation should return its projected row");
            assert_eq!(inserted.name, "single");

            let mutations = ["batch-one", "batch-two"]
                .into_iter()
                .map(|name| StructuralMutation::Insert {
                    entity: OneSimpleEntity01::ENTITY.to_string(),
                    patch: StructuralPatch::new()
                        .field("name", WriteCell::Value(InputValue::Text(name.to_string()))),
                })
                .collect();
            let inserted = database
                .execute_trusted_structural_mutation_batch_rows(&binding, mutations)
                .and_then(|rows| {
                    rows.into_iter()
                        .map(|row| {
                            OneSimpleEntity01::decode_row(&binding, row)
                                .map_err(TypedWriteError::Adapter)
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .expect("structural mutation batch should return projected rows");
            assert_eq!(
                inserted
                    .iter()
                    .map(|row| row.name.as_str())
                    .collect::<Vec<_>>(),
                ["batch-one", "batch-two"],
            );

            let error = database
                .execute_trusted_structural_mutation_batch_rows(
                    &binding,
                    vec![StructuralMutation::Insert {
                        entity: "OtherEntity".to_string(),
                        patch: StructuralPatch::new(),
                    }],
                )
                .expect_err("a foreign entity must reject before structural execution");
            assert!(matches!(
                error,
                TypedWriteError::Adapter(TypedAdapterError::EntityMismatch)
            ));
        });
    }
}

#[cfg(all(test, feature = "u256-audit"))]
mod u256_tests {
    use crate::db;
    use icydb::{
        U256,
        db::{
            DynamicQuery, StructuralPatch, WriteCell,
            query::{FieldRef, asc, count, max_by, min_by},
        },
        traits::EntitySource,
        types::Id,
        value::{InputValue, OutputValue},
    };
    use icydb_testing_audit_one_simple_fixtures::one_simple::U256AuditEntity;

    fn u256_patch(
        id: U256,
        amount: U256,
        optional_amount: Option<U256>,
        bucket: u64,
        label: &str,
    ) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::U256(id)))
            .field("amount", WriteCell::Value(InputValue::U256(amount)))
            .field(
                "optional_amount",
                optional_amount.map_or(WriteCell::Null, |value| {
                    WriteCell::Value(InputValue::U256(value))
                }),
            )
            .field("bucket", WriteCell::Value(InputValue::Nat64(bucket)))
            .field(
                "label",
                WriteCell::Value(InputValue::Text(label.to_string())),
            )
    }

    fn output_u256(output: &icydb::db::LiveQueryPageOutput, row: usize, field: &str) -> U256 {
        let column = output
            .columns
            .iter()
            .position(|column| column == field)
            .expect("U256 query output should retain the requested field");
        match output.rows.get(row).and_then(|row| row.get(column)) {
            Some(OutputValue::U256(value)) => *value,
            _ => panic!("U256 query output should retain its exact value kind"),
        }
    }

    #[test]
    fn native_u256_storage_indexes_queries_grouping_extrema_and_cursor_converge() {
        crate::__icydb_generated::__initialize_native_database_for_tests()
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
                &[OutputValue::U256(U256::ONE)],
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
                &[OutputValue::U256(U256::from(7_u64))],
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
                row.group_key() == [OutputValue::Nat64(1)]
                    && row.aggregate_values()
                        == [OutputValue::U256(U256::ONE), OutputValue::U256(U256::MAX)]
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
}
