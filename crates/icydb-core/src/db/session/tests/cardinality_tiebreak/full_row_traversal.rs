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
    // identical rows through one traversal without per-row point reads.
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
            assert_eq!(point_reads, 0);
            assert!(
                projection_rows(
                    &read,
                    "SELECT * FROM PlannerRow WHERE id >= 140 ORDER BY id"
                )
                .is_empty()
            );
            assert_collection_windows(&read, &sql, &expected);
            assert_cursor_pages(descending, &expected);
            let filtered_sql = format!(
                "SELECT * FROM PlannerRow WHERE id >= 3 AND id < 127 AND MOD(id, 2) = 0 ORDER BY id {direction}"
            );
            let filtered = expected
                .iter()
                .enumerate()
                .filter(|(index, _)| index % 2 == usize::from(!descending))
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            assert_collection_windows(&read, &filtered_sql, &filtered);
            assert_collection_windows(
                &read,
                &filtered_sql.replace("MOD(id, 2) = 0", "MOD(id, 2) = 3"),
                &[],
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

fn assert_collection_windows(
    read: &DbSession<TestCanister>,
    sql: &str,
    expected: &[Vec<OutputValue>],
) {
    for (limit, offset) in [(0, 0), (1, 0), (3, 2), (128, 120), (3, 130)] {
        let before = DataStore::current_get_call_count();
        let actual = projection_rows(read, &format!("{sql} LIMIT {limit} OFFSET {offset}"));
        assert_eq!(
            actual,
            expected
                .iter()
                .skip(offset)
                .take(limit)
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(DataStore::current_get_call_count(), before);
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
        // The malformed payload is beyond the first row in both directions;
        // a satisfied finite window must not decode it.
        assert_eq!(projection_rows(&read, &format!(
            "SELECT * FROM PlannerRow WHERE id >= 3 AND id < 127 ORDER BY id {direction} LIMIT 1"
        )).len(), 1);
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

// Full payload projection forces the full-row reader. Resume through small
// physical pages, including empty residual-filtered pages, on every backing.
fn assert_cursor_pages(descending: bool, expected: &[Vec<OutputValue>]) {
    for selective in [false, true] {
        let mut filters = vec![
            FieldRef::new("id").gte(InputValue::nat64(3)),
            FieldRef::new("id").lt(InputValue::nat64(127)),
        ];
        if selective {
            filters.push(FieldRef::new("payload").eq(InputValue::blob(vec![64; 256])));
        }
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id", "payload"])
            .filter(FilterExpr::and(filters))
            .order_by(if descending {
                crate::db::desc("id")
            } else {
                asc("id")
            });
        let mut continuation = None;
        let mut actual = Vec::new();
        let mut empty_pages = 0;
        let before = DataStore::current_get_call_count();
        for _ in 0..132 {
            let root = crate::db::RequestExecutionRoot::__new_runtime_root();
            let page = new_request_session(&root)
                .execute_trusted_live_page(&query, continuation.as_deref())
                .unwrap();
            if page.rows.is_empty() && page.continuation.is_some() {
                empty_pages += 1;
            }
            actual.extend(page.rows);
            if page.continuation.is_some() {
                assert_ne!(page.continuation, continuation);
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert!(continuation.is_none());
        let expected = expected
            .iter()
            .filter(|row| !selective || row[0] == OutputValue::nat64(64))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(DataStore::current_get_call_count(), before);
        if selective {
            assert!(empty_pages > 0);
        }
    }
}
