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
    initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "payload", 1, AcceptedFieldKind::Blob { max_len: None }),
        ],
        Vec::new(),
    );
}

pub(super) fn initialize_payload_schema(
    fields: Vec<PersistedFieldSnapshot>,
    indexes: Vec<PersistedIndexSnapshot>,
) {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let setup = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    setup.db.drive_startup_recovery_page().unwrap();
    let bindings = fields
        .iter()
        .map(|field| ((ENTITY_TAG, field_source(field.name())), field.id()))
        .collect();
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
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
        indexes,
    );
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        bindings,
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        setup.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
}

#[test]
fn unique_numeric_ranges_preserve_page_union_and_every_resume_suffix() {
    fn collect_pages(
        query: &DynamicQuery,
        mut continuation: Option<String>,
        tokens: &mut Vec<(String, usize)>,
    ) -> Vec<Vec<OutputValue>> {
        let mut rows = Vec::new();
        for _ in 0..16 {
            let root = RequestExecutionRoot::__new_runtime_root();
            let page = new_request_session(&root)
                .execute_trusted_live_page(query, continuation.as_deref())
                .unwrap();
            rows.extend(page.rows);
            let Some(next) = page.continuation else {
                return rows;
            };
            assert_ne!(Some(&next), continuation.as_ref());
            assert!(!tokens.iter().any(|(token, _)| token == &next));
            tokens.push((next.clone(), rows.len()));
            continuation = Some(next);
        }
        panic!("numeric range must exhaust within the bounded page count");
    }

    // Fixed minimized cases: modulo selection yields [1, 5) over 1..=13,
    // and [1, 2) over 1..=5. Current scalar LIMIT is a total result limit;
    // use the maintained physical page window to verify complete pagination.
    for (count, upper) in [(13_u64, 5_u64), (5, 2)] {
        initialize_payload_schema(
            vec![
                field(1, "id", 0, AcceptedFieldKind::Nat64),
                field(2, "code", 1, AcceptedFieldKind::Nat64),
            ],
            vec![PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).unwrap(),
                1,
                "code_idx".into(),
                STORE_PATH.into(),
                true,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["code".into()],
                    AcceptedFieldKind::Nat64,
                    false,
                )]),
                None,
            )],
        );
        for code in 1..=count {
            new_request_session(&RequestExecutionRoot::__new_runtime_root())
                .execute_trusted_dynamic_insert_batch(
                    ENTITY_NAME,
                    vec![DynamicStructuralPatch::new(vec![
                        (
                            "id".into(),
                            DynamicWriteCell::Value(InputValue::nat64(count - code)),
                        ),
                        (
                            "code".into(),
                            DynamicWriteCell::Value(InputValue::nat64(code)),
                        ),
                    ])],
                )
                .unwrap();
        }
        for (order, descending) in [(asc("code"), false), (desc("code"), true)] {
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["code", "id"])
                .filter(FilterExpr::and(vec![
                    FieldRef::new("code").gte(InputValue::nat64(1)),
                    FieldRef::new("code").lt(InputValue::nat64(upper)),
                ]))
                .order_by(order);
            let mut expected = (1..upper)
                .map(|code| vec![OutputValue::nat64(code), OutputValue::nat64(count - code)])
                .collect::<Vec<_>>();
            if descending {
                expected.reverse();
            }
            let mut tokens = Vec::new();
            let actual = collect_pages(&query, None, &mut tokens);
            assert_eq!(actual, expected);
            assert_eq!(tokens.is_empty(), upper == 2);
            for (token, offset) in tokens {
                let suffix = collect_pages(&query, Some(token), &mut Vec::new());
                assert_eq!(suffix, expected[offset..]);
            }
            let limited = new_request_session(&RequestExecutionRoot::__new_runtime_root())
                .execute_trusted_live_page(&query.limit(1), None)
                .unwrap();
            assert_eq!(limited.rows, expected[..1]);
            assert!(limited.continuation.is_none());
        }
    }
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

#[test]
fn unfiltered_small_pages_resume_until_physical_exhaustion() {
    initialize_payload_store();
    insert_payload_rows(0..16);
    for (order, expected) in [
        (asc("id"), (0..16).collect::<Vec<_>>()),
        (desc("id"), (0..16).rev().collect::<Vec<_>>()),
    ] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .order_by(order);
        let mut continuation = None;
        let mut actual = Vec::new();
        for _ in 0..16 {
            let root = RequestExecutionRoot::__new_runtime_root();
            let page = new_request_session(&root)
                .execute_trusted_live_page(&query, continuation.as_deref())
                .unwrap();
            assert!(root.observed(Resource::RowsVisited) <= 3);
            actual.extend(page.rows);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert!(continuation.is_none());
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|id| vec![OutputValue::nat64(id)])
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn one_sided_primary_ranges_resume_across_small_pages() {
    initialize_payload_store();
    insert_payload_rows(0..16);

    for (order, descending) in [(asc("id"), false), (desc("id"), true)] {
        for (filter, mut expected) in [
            (
                FieldRef::new("id").gt(InputValue::nat64(7)),
                (8..16).collect::<Vec<_>>(),
            ),
            (
                FieldRef::new("id").gte(InputValue::nat64(7)),
                (7..16).collect::<Vec<_>>(),
            ),
            (
                FieldRef::new("id").lt(InputValue::nat64(7)),
                (0..7).collect::<Vec<_>>(),
            ),
            (
                FieldRef::new("id").lte(InputValue::nat64(7)),
                (0..8).collect::<Vec<_>>(),
            ),
        ] {
            if descending {
                expected.reverse();
            }
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["id"])
                .filter(filter)
                .order_by(order.clone());
            let mut continuation = None;
            let mut actual = Vec::new();
            let mut pages = 0;
            for _ in 0..16 {
                let root = RequestExecutionRoot::__new_runtime_root();
                let page = new_request_session(&root)
                    .execute_trusted_live_page(&query, continuation.as_deref())
                    .unwrap();
                assert!(root.observed(Resource::RowsVisited) <= 4);
                assert!(page.continuation.is_none() || page.continuation != continuation);
                actual.extend(page.rows);
                pages += 1;
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            assert!(continuation.is_none());
            assert!(pages > 1);
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|id| vec![OutputValue::nat64(id)])
                    .collect::<Vec<_>>()
            );
        }
    }
}
