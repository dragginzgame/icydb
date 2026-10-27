//! Full-row primary traversal shares physical rows without per-key point reads.

use super::*;
use crate::{
    db::{
        data::{RawDataStoreKey, RawRow, StoreVisit},
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    },
    testing::test_memory,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

fn seeded_rows() -> Vec<(RawDataStoreKey, RawRow)> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "payload", 1, AcceptedFieldKind::Blob { max_len: None }),
        ],
        Vec::new(),
    );
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    for first in (0..130_u64).step_by(32) {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                (first..(first + 32).min(130))
                    .map(|id| {
                        DynamicStructuralPatch::new(vec![
                            ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                            (
                                "payload".into(),
                                DynamicWriteCell::Value(InputValue::blob(vec![
                                    u8::try_from(id)
                                        .unwrap();
                                    256
                                ])),
                            ),
                        ])
                    })
                    .collect(),
            )
            .unwrap();
    }
    DATA_STORE.with_borrow(|store| {
        let mut rows = Vec::new();
        store
            .visit_entries(|key, row| {
                rows.push((key.clone(), row.clone()));
                Ok::<_, std::convert::Infallible>(StoreVisit::Continue)
            })
            .unwrap();
        rows
    })
}

#[test]
fn full_row_primary_ranges_preserve_backing_order_payloads_and_read_admission() {
    let rows = seeded_rows();
    // Heap, live-only journal, canonical-only journal and mixed journal use
    // identical rows. Only the mixed overlay must use the point-read route.
    for backing in 0..4_u8 {
        if backing != 0 {
            let mut store = DataStore::init_journaled(test_memory(190 + backing));
            for (position, (key, row)) in rows.iter().enumerate() {
                if backing == 2 || (backing == 3 && position < 65) {
                    store
                        .fold_recovered_journal_put(key.clone(), row.clone())
                        .unwrap();
                } else {
                    store.insert_raw_for_test(key.clone(), row.clone());
                }
            }
            DATA_STORE.with(|target| *target.borrow_mut() = store);
        }
        for descending in [false, true] {
            let direction = if descending { "DESC" } else { "ASC" };
            let sql = format!(
                "SELECT * FROM PlannerRow WHERE id >= 3 AND id < 127 ORDER BY id {direction}"
            );
            let mut expected = (3..127_u64)
                .map(|id| {
                    vec![
                        OutputValue::nat64(id),
                        OutputValue::blob(vec![u8::try_from(id).unwrap(); 256]),
                    ]
                })
                .collect::<Vec<_>>();
            if descending {
                expected.reverse();
            }
            let before = DataStore::current_get_call_count();
            let read = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
            assert_eq!(projection_rows(&read, &sql), expected);
            let point_reads = DataStore::current_get_call_count() - before;
            assert_eq!(point_reads == 0, backing != 3);
            assert!(
                projection_rows(
                    &read,
                    "SELECT * FROM PlannerRow WHERE id >= 140 ORDER BY id"
                )
                .is_empty()
            );
            assert_eq!(
                projection_rows(&read, &format!("{sql} LIMIT 3")),
                expected[..3]
            );

            let root = crate::db::RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::StoredBytesRead, 256),
            );
            let error = new_request_session(&root)
                .execute_trusted_sql_query(&sql)
                .unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::StoredBytesRead.raw(),
            )));
            // A rejected read neither consumes nor changes the source rows.
            let read = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
            assert_eq!(projection_rows(&read, &sql), expected);
        }
        assert_malformed_row_refused(backing, &rows[13]);
    }
}

fn assert_malformed_row_refused(backing: u8, original: &(RawDataStoreKey, RawRow)) {
    let replace = |row: RawRow| {
        DATA_STORE.with_borrow_mut(|store| {
            if backing == 2 {
                store
                    .fold_recovered_journal_put(original.0.clone(), row)
                    .unwrap();
            } else {
                store.insert_raw_for_test(original.0.clone(), row);
            }
        });
    };
    replace(RawRow::try_new(vec![0]).unwrap());
    for direction in ["ASC", "DESC"] {
        let read = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        let error = read
            .execute_trusted_sql_query(&format!(
                "SELECT * FROM PlannerRow WHERE id >= 3 AND id < 127 ORDER BY id {direction}"
            ))
            .unwrap_err();
        assert_eq!(
            error.diagnostic_code().class(),
            icydb_diagnostic_code::ErrorClass::Corruption
        );
    }
    replace(original.1.clone());
}
