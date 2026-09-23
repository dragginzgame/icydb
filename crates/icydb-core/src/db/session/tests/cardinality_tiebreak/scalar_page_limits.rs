//! Authored scalar limits bound reads without changing continuation identity.

use super::*;
use crate::db::{
    RequestExecutionRoot, desc,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::ops::Range;

const PAYLOAD_BYTES: usize = 2048;
// Two payloads leave room for one result and the maintained lookahead, not a
// whole page. Encoding overhead is bounded independently of the store size.
const SMALL_READ_BYTES: u64 = 2 * (PAYLOAD_BYTES as u64 + 256);

fn limited_request(bytes: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::StoredBytesRead, bytes),
    )
}

// Reuse the session harness's registry with a payload-only accepted schema.
// No generated model or secondary index can stand in for primary-row reads.
fn initialize_payload_store() {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let setup = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    setup.db.drive_startup_recovery_page().unwrap();
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        field(2, "payload", 1, AcceptedFieldKind::Blob { max_len: None }),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.into(),
        ENTITY_NAME.into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        BTreeMap::from([
            ((ENTITY_TAG, field_source("id")), FieldId::new(1)),
            ((ENTITY_TAG, field_source("payload")), FieldId::new(2)),
        ]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        setup.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
}

fn insert_payload_rows(ids: Range<u64>) {
    for start in ids.clone().step_by(8) {
        let rows = (start..(start + 8).min(ids.end))
            .map(|id| {
                DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "payload".into(),
                        DynamicWriteCell::Value(InputValue::blob(vec![7; PAYLOAD_BYTES])),
                    ),
                ])
            })
            .collect();
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, rows)
            .unwrap();
    }
}

#[test]
fn small_limits_bound_reads_as_payload_store_grows() {
    initialize_payload_store();
    let mut previous_count = 0;
    let mut baseline = None;
    for count in [16, 128] {
        insert_payload_rows(previous_count..count);
        previous_count = count;
        let mut observed = Vec::new();
        for order in [None, Some(asc("id")), Some(desc("id"))] {
            let expected_id = if order == Some(desc("id")) {
                count - 1
            } else {
                0
            };
            for id_only in [false, true] {
                let mut query = DynamicQuery::new(ENTITY_NAME).limit(1);
                if let Some(order) = &order {
                    query = query.order_by(order.clone());
                }
                if id_only {
                    query = query.select(["id"]);
                }
                // Cold/warm reuse must never cache a page-sized physical limit.
                for _ in 0..2 {
                    let root = limited_request(SMALL_READ_BYTES);
                    let page = new_request_session(&root)
                        .execute_trusted_live_page(&query, None)
                        .unwrap();
                    let mut expected = vec![OutputValue::nat64(expected_id)];
                    if !id_only {
                        expected.push(OutputValue::blob(vec![7; PAYLOAD_BYTES]));
                    }
                    assert_eq!(page.rows, vec![expected]);
                    assert!(page.continuation.is_none());
                    assert!(root.observed(Resource::RowsVisited) <= 2);
                    observed.push(root.observed(Resource::StoredBytesRead));
                }
            }
        }
        if let Some(baseline) = &baseline {
            assert_eq!(&observed, baseline);
        } else {
            baseline = Some(observed);
        }
        let root = limited_request(0);
        let page = new_request_session(&root)
            .execute_trusted_live_page(
                &DynamicQuery::new(ENTITY_NAME).order_by(asc("id")).limit(0),
                None,
            )
            .unwrap();
        assert!(page.rows.is_empty() && page.continuation.is_none());
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn exhaustive_small_limit_uses_the_same_bounded_window() {
    initialize_payload_store();
    insert_payload_rows(0..128);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .order_by(asc("id"))
        .limit(1);
    let root = limited_request(SMALL_READ_BYTES);
    let page = new_request_session(&root)
        .execute_trusted_exhaustive_page(&query, None, None)
        .unwrap();
    assert_eq!(page.rows, vec![vec![OutputValue::nat64(0)]]);
    assert!(page.continuation.is_none());
    assert!(root.observed(Resource::RowsVisited) <= 2);
}

#[test]
fn selective_small_limit_preserves_empty_page_progress() {
    initialize_payload_store();
    insert_payload_rows(0..16);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .filter(FieldRef::new("id").gt(InputValue::nat64(14)))
        .order_by(asc("id"))
        .limit(1);
    let mut continuation = None;
    let mut actual = Vec::new();
    for _ in 0..16 {
        let root = RequestExecutionRoot::__new_runtime_root();
        let page = new_request_session(&root)
            .execute_trusted_live_page(&query, continuation.as_deref())
            .unwrap();
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
    assert_eq!(actual, vec![vec![OutputValue::nat64(15)]]);
}

#[test]
fn authored_total_limit_stays_stable_across_filtered_pages() {
    let setup = initialize();
    seed_rows(&setup);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .filter(FilterExpr::and(vec![
            FieldRef::new("rare").gte("group-a"),
            FieldRef::new("rare").lt("group-b"),
        ]))
        .order_by(asc("id"))
        .limit(5);
    let mut continuation = None;
    let mut actual = Vec::new();
    for _ in 0..16 {
        let root = RequestExecutionRoot::__new_runtime_root();
        let page = new_request_session(&root)
            .execute_trusted_live_page(&query, continuation.as_deref())
            .unwrap();
        actual.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(continuation.is_none());
    assert_eq!(
        actual,
        (0..5)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>()
    );
}
