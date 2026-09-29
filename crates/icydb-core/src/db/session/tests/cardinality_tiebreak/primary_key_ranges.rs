//! Primary-key range endpoints agree across reads, mutations and resumed pages.

use super::*;
use crate::{db::desc, types::Ulid};

#[test]
fn primary_key_range_sql_reads_and_counts_respect_endpoints() {
    let session = initialize();
    seed_rows(&session);
    for (predicate, expected) in [
        ("id >= 2 AND id < 5", vec![2, 3, 4]),
        ("id > 2 AND id <= 5", vec![3, 4, 5]),
        ("id > 2 AND id < 5", vec![3, 4]),
        ("id >= 2 AND id <= 5", vec![2, 3, 4, 5]),
        ("id BETWEEN 2 AND 5", vec![2, 3, 4, 5]),
        ("id >= 2 AND id < 2", vec![]),
        ("id >= 2 AND id <= 2", vec![2]),
        ("id = 2", vec![2]),
        ("id IN (2, 4)", vec![2, 4]),
    ] {
        let rows = expected
            .iter()
            .map(|id| vec![OutputValue::nat64(*id)])
            .collect::<Vec<_>>();
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id"),
            ),
            rows,
            "{predicate}",
        );
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT COUNT(*) FROM PlannerRow WHERE {predicate}")
            ),
            vec![vec![OutputValue::nat64(
                u64::try_from(expected.len()).unwrap()
            )]],
            "{predicate}",
        );
    }
}

#[test]
fn primary_key_range_sql_update_preserves_excluded_rows() {
    let session = initialize();
    seed_rows(&session);
    let result = session
        .execute_trusted_sql_exact_update(
            "UPDATE PlannerRow SET common = 'changed' WHERE id >= 2 AND id < 5",
            12,
        )
        .unwrap();
    assert!(matches!(result, SqlStatementResult::Count { row_count: 3 }));
    assert_eq!(
        projection_rows(&session, "SELECT id, common FROM PlannerRow ORDER BY id"),
        (0..12)
            .map(|id| vec![
                OutputValue::nat64(id),
                OutputValue::text(
                    if (2..5).contains(&id) {
                        "changed"
                    } else {
                        "everyone"
                    }
                    .into()
                ),
            ])
            .collect::<Vec<_>>(),
    );
}

#[test]
fn primary_key_range_sql_delete_preserves_excluded_rows() {
    let session = initialize();
    seed_rows(&session);
    let result = session
        .execute_trusted_sql_mutation("DELETE FROM PlannerRow WHERE id >= 2 AND id < 5")
        .unwrap();
    assert!(matches!(result, SqlStatementResult::Count { row_count: 3 }));
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id"),
        (0..12)
            .filter(|id| !(2..5).contains(id))
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>(),
    );
}

// Publish a small accepted schema so numeric and non-numeric primary keys use
// the same real session path, without a secondary index influencing selection.
fn initialize_range_store(kind: AcceptedFieldKind) -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    session.db.drive_startup_recovery_page().unwrap();
    let fields = vec![
        field(1, "id", 0, kind),
        field(2, "payload", 1, AcceptedFieldKind::Nat64),
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
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    session
}

#[test]
fn numeric_primary_key_range_pages_preserve_bounds_direction_and_total_limit() {
    assert_range_pages(
        AcceptedFieldKind::Nat64,
        &(0..12).map(InputValue::nat64).collect::<Vec<_>>(),
        &(0..12).map(OutputValue::nat64).collect::<Vec<_>>(),
    );
}

#[test]
fn ulid_primary_key_range_pages_preserve_bounds_direction_and_total_limit() {
    assert_range_pages(
        AcceptedFieldKind::Ulid,
        &(0..12)
            .map(|id| InputValue::ulid(Ulid::from_u128(id)))
            .collect::<Vec<_>>(),
        &(0..12)
            .map(|id| OutputValue::ulid(Ulid::from_u128(id)))
            .collect::<Vec<_>>(),
    );
}

fn assert_range_pages(kind: AcceptedFieldKind, keys: &[InputValue], outputs: &[OutputValue]) {
    let session = initialize_range_store(kind);
    for (id, key) in keys.iter().enumerate() {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(key.clone())),
                    (
                        "payload".into(),
                        DynamicWriteCell::Value(InputValue::nat64(id as u64)),
                    ),
                ])],
            )
            .unwrap();
    }
    for lower_inclusive in [false, true] {
        for upper_inclusive in [false, true] {
            for descending in [false, true] {
                for limit in [None, Some(0), Some(1), Some(3)] {
                    for id_only in [false, true] {
                        let lower = FieldRef::new("id");
                        let upper = FieldRef::new("id");
                        let mut query = DynamicQuery::new(ENTITY_NAME)
                            .filter(FilterExpr::and(vec![
                                if lower_inclusive {
                                    lower.gte(keys[2].clone())
                                } else {
                                    lower.gt(keys[2].clone())
                                },
                                if upper_inclusive {
                                    upper.lte(keys[8].clone())
                                } else {
                                    upper.lt(keys[8].clone())
                                },
                            ]))
                            .order_by(if descending { desc("id") } else { asc("id") });
                        if let Some(limit) = limit {
                            query = query.limit(limit);
                        }
                        if id_only {
                            query = query.select(["id"]);
                        }
                        let mut expected = ((if lower_inclusive { 2 } else { 3 })
                            ..(if upper_inclusive { 9 } else { 8 }))
                            .map(|id| {
                                let mut row = vec![outputs[id].clone()];
                                if !id_only {
                                    row.push(OutputValue::nat64(id as u64));
                                }
                                row
                            })
                            .collect::<Vec<_>>();
                        if descending {
                            expected.reverse();
                        }
                        if let Some(limit) = limit {
                            expected.truncate(limit as usize);
                        }
                        let actual = drain_range_pages(&query);
                        assert_eq!(actual, expected, "{query:?}");
                    }
                }
            }
        }
    }
}

fn drain_range_pages(query: &DynamicQuery) -> Vec<Vec<OutputValue>> {
    let mut continuation = None;
    let mut actual = Vec::new();
    for _ in 0..16 {
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        let page = new_request_session(&root)
            .execute_trusted_live_page(query, continuation.as_deref())
            .unwrap();
        assert!(page.continuation.is_none() || page.continuation != continuation);
        actual.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(continuation.is_none());
    actual
}
