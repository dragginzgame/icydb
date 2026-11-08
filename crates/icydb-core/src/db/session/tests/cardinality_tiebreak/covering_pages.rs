//! Covering payloads use scalar progress, hidden slots and live-row authority.

use super::*;
use crate::db::{
    ExhaustiveQueryPageOutput, ExhaustiveReadError, ReadSetRevisionError, ReadSetRevisionProof,
    RequestExecutionRoot,
    data::{RawRow, StoreVisit},
    desc,
    schema::AcceptedIndexPredicate,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, ErrorCode};

fn initialize_covering_rows(nullable: bool, partial: bool) {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        PersistedFieldSnapshot::new_initial_test_field(
            2,
            "code",
            1,
            AcceptedFieldKind::Nat64,
            nullable,
        ),
        field(3, "payload", 2, AcceptedFieldKind::Blob { max_len: None }),
    ];
    initialize_field_schema(
        fields,
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "code_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::from_field_paths(vec![
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["code".into()],
                    AcceptedFieldKind::Nat64,
                    nullable,
                ),
            ]),
            partial.then(|| AcceptedIndexPredicate::test_non_null(2)),
        )],
    );
    for id in 0..18_u64 {
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "code".into(),
                        if nullable && id % 7 == 0 {
                            DynamicWriteCell::Null
                        } else {
                            DynamicWriteCell::Value(InputValue::nat64(id / 3))
                        },
                    ),
                    (
                        "payload".into(),
                        DynamicWriteCell::Value(InputValue::blob(vec![7; 2048])),
                    ),
                ])],
            )
            .unwrap();
    }
}

#[test]
fn covering_pages_preserve_hidden_order_slots_duplicates_and_every_resume_suffix() {
    initialize_covering_rows(false, false);
    for descending in [false, true] {
        for prefix in [false, true] {
            let filter = if prefix {
                FieldRef::new("code").eq(InputValue::nat64(2))
            } else {
                FilterExpr::and(vec![
                    FieldRef::new("code").gte(InputValue::nat64(1)),
                    FieldRef::new("code").lt(InputValue::nat64(5)),
                ])
            };
            // Selecting id hides the sort field; selecting code hides the PK
            // suffix. Duplicate code values put boundaries inside equal groups.
            for selected in ["id", "code"] {
                let query = DynamicQuery::new(ENTITY_NAME)
                    .select([selected])
                    .filter(filter.clone())
                    .order_by(if descending {
                        desc("code")
                    } else {
                        asc("code")
                    });
                let ids = if prefix { 6..9 } else { 3..15 };
                let mut expected = ids
                    .map(|id| {
                        vec![OutputValue::nat64(if selected == "id" {
                            id
                        } else {
                            id / 3
                        })]
                    })
                    .collect::<Vec<_>>();
                if descending {
                    expected.reverse();
                }
                for public in [false, true] {
                    let before = DataStore::current_get_call_count();
                    let (actual, tokens) =
                        secondary_order::collect_bounded_secondary_pages(&query, public, None);
                    assert_eq!(actual, expected);
                    assert!(!tokens.is_empty());
                    for (token, offset) in tokens {
                        assert_eq!(
                            secondary_order::collect_bounded_secondary_pages(
                                &query,
                                public,
                                Some(token)
                            )
                            .0,
                            expected[offset..]
                        );
                    }
                    assert_eq!(
                        DataStore::current_get_call_count(),
                        before,
                        "covering pages must not load row payloads"
                    );
                    let root = secondary_order::bounded_secondary_request(2);
                    let page = new_request_session(&root)
                        .execute_trusted_live_page(&query.clone().limit(1), None)
                        .unwrap();
                    assert_eq!(page.rows, expected[..1]);
                    assert!(page.continuation.is_none());
                }
            }
        }
        let order = if descending { "DESC" } else { "ASC" };
        let mut expected = (3..15_u64)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>();
        if descending {
            expected.reverse();
        }
        for _ in 0..2 {
            let before = DataStore::current_get_call_count();
            let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
            assert_eq!(
                projection_rows(
                    &session,
                    &format!(
                        "SELECT id FROM PlannerRow WHERE code >= 1 AND code < 5 ORDER BY code {order} LIMIT 3 OFFSET 2"
                    )
                ),
                expected[2..5]
            );
            assert_eq!(DataStore::current_get_call_count(), before);
            // An expression residual is deliberately row-backed, with the same
            // ordering/window owner and result semantics.
            assert_eq!(projection_rows(&session, &format!("SELECT id FROM PlannerRow WHERE code >= 1 AND code < 5 AND MOD(id, 2) = 0 ORDER BY code {order}")), expected.iter().filter(|row| matches!(row[0].as_public(), crate::value::PublicValue::Nat64(id) if id % 2 == 0)).cloned().collect::<Vec<_>>());
        }
    }
}

fn assert_nullable_completeness(partial: bool) {
    initialize_covering_rows(true, partial);
    for descending in [false, true] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FieldRef::new("code").gte(InputValue::nat64(1)))
            .order_by(if descending {
                desc("code")
            } else {
                asc("code")
            });
        let mut expected = (3..18_u64)
            .filter(|id| id % 7 != 0)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>();
        if descending {
            expected.reverse();
        }
        for public in [false, true] {
            let before = DataStore::current_get_call_count();
            assert_eq!(
                secondary_order::collect_bounded_secondary_pages(&query, public, None).0,
                expected
            );
            assert_eq!(DataStore::current_get_call_count(), before);
        }
    }
    // A sparse/partial index cannot stand in for NULL rows. Retain the
    // complete trusted fallback when no predicate proves index membership.
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    let rows = projection_rows(&session, "SELECT code FROM PlannerRow ORDER BY code");
    assert_eq!(rows.len(), 18);
    assert_eq!(
        rows.iter()
            .filter(|row| row[0] == OutputValue::null())
            .count(),
        3
    );
}

#[test]
fn covering_pages_preserve_nullable_completeness() {
    assert_nullable_completeness(false);
}

#[test]
fn covering_pages_preserve_partial_index_completeness() {
    assert_nullable_completeness(true);
}

#[test]
fn covering_pages_check_liveness_without_decoding_persisted_payloads() {
    initialize_covering_rows(false, false);
    let raw_key = DATA_STORE.with_borrow(|store| {
        let mut first = None;
        store
            .visit_entries(|key, _| {
                first = Some(key.clone());
                Ok::<_, crate::error::InternalError>(StoreVisit::Stop)
            })
            .unwrap();
        first.unwrap()
    });
    let original = DATA_STORE.with_borrow(|store| store.get(&raw_key).unwrap());
    DATA_STORE.with_borrow_mut(|store| {
        store
            .insert_raw_for_test(raw_key.clone(), RawRow::try_new(vec![0]).unwrap())
            .unwrap()
    });
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .filter(FieldRef::new("code").gte(InputValue::nat64(0)))
        .order_by(asc("code"));
    let before = DataStore::current_get_call_count();
    let page = new_request_session(&RequestExecutionRoot::__new_runtime_root())
        .execute_trusted_live_page(&query, None)
        .unwrap();
    assert_eq!(
        page.rows,
        vec![vec![OutputValue::nat64(0)], vec![OutputValue::nat64(1)]]
    );
    assert_eq!(DataStore::current_get_call_count(), before);
    DATA_STORE.with_borrow_mut(|store| store.remove(&raw_key).unwrap());
    let error = new_request_session(&RequestExecutionRoot::__new_runtime_root())
        .execute_trusted_live_page(&query, None)
        .unwrap_err();
    assert_eq!(error.diagnostic().error_code(), ErrorCode::STORE_CORRUPTION);
    assert_eq!(DataStore::current_get_call_count(), before);
    DATA_STORE.with_borrow_mut(|store| store.insert_raw_for_test(raw_key, original));
    assert_eq!(
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_live_page(&query, None)
            .unwrap()
            .rows,
        page.rows
    );
}

#[test]
fn covering_exhaustive_pages_retain_proof_and_reject_tampered_live_cursors() {
    use crate::db::cursor::{decode_optional_cursor_token, encode_cursor};
    // Exhaustive proofs require durable source stores; reuse the maintained
    // journaled fixture rather than weakening that admission for heap storage.
    let setup = initialize_journaled();
    seed_rows(&setup);
    for descending in [false, true] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FieldRef::new("rare").gte("group-a"))
            .order_by(if descending {
                desc("rare")
            } else {
                asc("rare")
            });
        let mut expected = (0..12_u64)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>();
        if descending {
            expected.reverse();
        }
        for public in [false, true] {
            let before = DataStore::current_get_call_count();
            let mut cursor = None;
            let mut proof = None;
            let mut rows = Vec::new();
            for _ in 0..16 {
                let root = secondary_order::bounded_secondary_request(3);
                let session =
                    DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
                let page = if public {
                    session.execute_public_exhaustive_page(
                        &query,
                        cursor.as_deref(),
                        proof.as_ref(),
                    )
                } else {
                    session.execute_trusted_exhaustive_page(
                        &query,
                        cursor.as_deref(),
                        proof.as_ref(),
                    )
                }
                .unwrap();
                if let Some(previous) = &proof {
                    assert_eq!(&page.proof, previous);
                }
                proof = Some(page.proof);
                rows.extend(page.rows);
                assert!(page.continuation.is_none() || page.continuation != cursor);
                cursor = page.continuation;
                if cursor.is_none() {
                    break;
                }
            }
            assert!(cursor.is_none());
            assert_eq!(rows, expected);
            assert_eq!(DataStore::current_get_call_count(), before);

            let root = RequestExecutionRoot::__new_runtime_root();
            let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
            let page = if public {
                session.execute_public_live_page(&query, None)
            } else {
                session.execute_trusted_live_page(&query, None)
            }
            .unwrap();
            let mut bytes = decode_optional_cursor_token(page.continuation.as_deref())
                .unwrap()
                .unwrap();
            *bytes.last_mut().unwrap() ^= 1;
            let tampered = encode_cursor(&bytes);
            let root = secondary_order::bounded_secondary_request(0);
            let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
            let result = if public {
                session.execute_public_live_page(&query, Some(&tampered))
            } else {
                session.execute_trusted_live_page(&query, Some(&tampered))
            };
            assert_eq!(
                result.unwrap_err().diagnostic().error_code(),
                ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
            );
            assert_eq!(DataStore::current_get_call_count(), before);
        }
    }
}

// Exercise the actual public/trusted entrypoints with fresh request budgets.
// Source-proof rejection must precede both index traversal and row reads.
fn exhaustive_covering_page(
    root: &RequestExecutionRoot,
    query: &DynamicQuery,
    public: bool,
    cursor: Option<&str>,
    proof: Option<&ReadSetRevisionProof>,
) -> Result<ExhaustiveQueryPageOutput, ExhaustiveReadError> {
    let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, root);
    if public {
        session.execute_public_exhaustive_page(query, cursor, proof)
    } else {
        session.execute_trusted_exhaustive_page(query, cursor, proof)
    }
}

fn assert_covering_source_revision_refusal(access_changed: bool) {
    let setup = initialize_journaled();
    seed_rows(&setup);
    let mut issued = Vec::new();
    for descending in [false, true] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FieldRef::new("rare").gte("group-a"))
            .order_by(if descending {
                desc("rare")
            } else {
                asc("rare")
            });
        for public in [false, true] {
            let before = DataStore::current_get_call_count();
            let root = secondary_order::bounded_secondary_request(3);
            let page = exhaustive_covering_page(&root, &query, public, None, None).unwrap();
            let cursor = page.continuation.as_deref().unwrap();
            let root = secondary_order::bounded_secondary_request(0);
            assert!(matches!(
                exhaustive_covering_page(&root, &query, public, Some(cursor), None),
                Err(ExhaustiveReadError::Revision(
                    ReadSetRevisionError::ResumeProofRequired
                ))
            ));
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
            let root = secondary_order::bounded_secondary_request(3);
            let resumed =
                exhaustive_covering_page(&root, &query, public, Some(cursor), Some(&page.proof))
                    .unwrap();
            assert_eq!(resumed.proof, page.proof);
            assert_eq!(
                resumed.rows,
                if descending {
                    vec![vec![OutputValue::nat64(9)], vec![OutputValue::nat64(8)]]
                } else {
                    vec![vec![OutputValue::nat64(2)], vec![OutputValue::nat64(3)]]
                }
            );
            assert_eq!(DataStore::current_get_call_count(), before);
            issued.push((query.clone(), public, page));
        }
    }

    if access_changed {
        let store = setup.db.store_handle(JOURNALED_STORE_PATH).unwrap();
        store.mark_index_building().unwrap();
        store.mark_index_ready().unwrap();
    } else {
        insert_row(&setup, 99, "other", "group-c");
    }

    for (query, public, page) in issued {
        let before = DataStore::current_get_call_count();
        let cursor = page.continuation.as_deref().unwrap();
        let root = secondary_order::bounded_secondary_request(0);
        let error =
            exhaustive_covering_page(&root, &query, public, Some(cursor), Some(&page.proof))
                .unwrap_err();
        assert!(matches!(
            (access_changed, error),
            (
                true,
                ExhaustiveReadError::Revision(ReadSetRevisionError::StoreAccessChanged { .. })
            ) | (
                false,
                ExhaustiveReadError::Revision(ReadSetRevisionError::StoreDataChanged { .. })
            )
        ));
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
        assert_eq!(DataStore::current_get_call_count(), before);

        // A caller cannot repair an old cursor by capturing a new proof. The
        // unchanged authenticated token remains bound to its original source.
        let root = secondary_order::bounded_secondary_request(3);
        let fresh = exhaustive_covering_page(&root, &query, public, None, None).unwrap();
        assert_ne!(fresh.proof, page.proof);
        let root = secondary_order::bounded_secondary_request(0);
        let error =
            exhaustive_covering_page(&root, &query, public, Some(cursor), Some(&fresh.proof))
                .unwrap_err();
        let ExhaustiveReadError::Query(error) = error else {
            panic!("refreshed proof must not rebind the old token");
        };
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
        assert_eq!(DataStore::current_get_call_count(), before);
    }
}

#[test]
fn covering_exhaustive_pages_reject_changed_data_before_scanning() {
    assert_covering_source_revision_refusal(false);
}

#[test]
fn covering_exhaustive_pages_reject_changed_access_before_scanning() {
    assert_covering_source_revision_refusal(true);
}
