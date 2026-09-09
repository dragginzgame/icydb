//! Session regressions for complete access through nullable composite indexes.

use super::*;
use crate::types::Principal;

fn initialize_sparse_indexes(complete_index: bool) -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session.db.drive_startup_recovery_page().unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &sparse_schema_candidate(complete_index),
    )
    .unwrap();
    session
}

fn sparse_schema_candidate(complete_index: bool) -> CandidateSchemaRevision {
    let fields = [
        ("id", AcceptedFieldKind::Nat64, false),
        ("recipient", AcceptedFieldKind::Principal, false),
        ("last_event_at", AcceptedFieldKind::Int64, false),
        ("read_at", AcceptedFieldKind::Int64, true),
        ("resolved_at", AcceptedFieldKind::Int64, true),
        ("label", AcceptedFieldKind::Text { max_len: None }, true),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind, nullable))| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot + 1).unwrap()),
            name.to_string(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind.clone(),
            Vec::new(),
            nullable,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
        )
    })
    .collect::<Vec<_>>();
    let index_fields: &[(&str, &[usize])] = if complete_index {
        &[
            ("a_read", &[1, 3]),
            ("b_resolved", &[1, 4]),
            ("c_label", &[5]),
            ("z_events", &[1, 2]),
        ]
    } else {
        &[
            ("a_read", &[1, 3]),
            ("b_resolved", &[1, 4]),
            ("c_label", &[5]),
        ]
    };
    let indexes = index_fields
        .iter()
        .enumerate()
        .map(|(ordinal, (name, slots))| {
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::try_from(ordinal + 1).unwrap()).unwrap(),
                u16::try_from(ordinal + 1).unwrap(),
                (*name).to_string(),
                STORE_PATH.to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(
                    slots
                        .iter()
                        .map(|slot| {
                            let field = &fields[*slot];
                            PersistedIndexFieldPathSnapshot::new(
                                field.id(),
                                field.slot(),
                                vec![field.name().to_string()],
                                field.kind().clone(),
                                field.nullable(),
                            )
                        })
                        .collect(),
                ),
                None,
            )
        })
        .collect();
    let bindings = fields
        .iter()
        .map(|field| ((ENTITY_TAG, field_source(field.name())), field.id()))
        .collect();
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
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
    accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        bindings,
    )
}

fn sparse_row(id: u64, read_at: Option<i64>) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        (
            "id".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(id)),
        ),
        (
            "recipient".to_string(),
            DynamicWriteCell::Value(InputValue::principal(Principal::anonymous())),
        ),
        (
            "last_event_at".to_string(),
            DynamicWriteCell::Value(InputValue::int64(100)),
        ),
        (
            "read_at".to_string(),
            read_at.map_or(DynamicWriteCell::Null, |value| {
                DynamicWriteCell::Value(InputValue::int64(value))
            }),
        ),
        ("resolved_at".to_string(), DynamicWriteCell::Null),
        (
            "label".to_string(),
            read_at.map_or(DynamicWriteCell::Null, |_| {
                DynamicWriteCell::Value(InputValue::text("read".to_string()))
            }),
        ),
    ])
}

fn recipient_query() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME).filter(FieldRef::new("recipient").eq(Principal::anonymous()))
}

#[test]
fn nullable_composite_competitors_preserve_public_trusted_and_sql_rows() {
    let session = initialize_sparse_indexes(true);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![sparse_row(1, None)])
        .unwrap();
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![sparse_row(2, Some(101))])
        .unwrap();
    for query in [
        recipient_query(),
        recipient_query().filter(FieldRef::new("last_event_at").eq(100_i64)),
    ] {
        // Repeat to protect warm plan selection as well as the first cardinality tie-break.
        for _ in 0..2 {
            assert_eq!(
                session
                    .execute_public_live_page(&query, None)
                    .unwrap()
                    .row_count,
                2
            );
            assert_eq!(
                session
                    .execute_trusted_live_page(&query, None)
                    .unwrap()
                    .row_count,
                2
            );
        }
    }
    assert_eq!(
        session
            .execute_public_exact_count(&recipient_query())
            .unwrap(),
        2
    );
    let dispatch = crate::db::sql_statement_dispatch(
        "SELECT id FROM PlannerRow WHERE recipient = ? ORDER BY id LIMIT 20",
    )
    .unwrap();
    let (SqlStatementResult::Projection { rows, .. }, _) = session
        .execute_trusted_sql_query_with_entity_name(
            &dispatch,
            &[InputValue::principal(Principal::anonymous())],
        )
        .unwrap()
    else {
        panic!("SQL must project notification rows");
    };
    assert_eq!(rows.len(), 2);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![sparse_row(3, None)])
        .unwrap();
    let paged = recipient_query().order_by(asc("id")).limit(3);
    let first = session.execute_public_live_page(&paged, None).unwrap();
    assert_eq!(first.row_count, 2);
    let next = first
        .continuation
        .expect("three rows exceed the native test page bound");
    let second = session
        .execute_public_live_page(&paged, Some(&next))
        .unwrap();
    assert_eq!(second.row_count, 1);
    assert!(second.continuation.is_none());
}

#[test]
fn nullable_composite_without_complete_alternative_rejects_unsafe_public_read_and_count() {
    let session = initialize_sparse_indexes(false);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![sparse_row(1, None)])
        .unwrap();
    assert!(
        session
            .execute_public_live_page(&recipient_query(), None)
            .is_err()
    );
    assert!(
        session
            .execute_public_exact_count(&recipient_query())
            .is_err()
    );
    let all = recipient_query().limit(20);
    assert_eq!(
        session
            .execute_trusted_live_page(&all, None)
            .unwrap()
            .row_count,
        1
    );
}

#[test]
fn nullable_index_membership_proofs_preserve_eq_in_range_and_text_prefix() {
    let session = initialize_sparse_indexes(false);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![sparse_row(1, None), sparse_row(2, Some(101))],
        )
        .unwrap();
    let filters = [
        FieldRef::new("read_at").eq(101_i64),
        FieldRef::new("read_at").in_list([101_i64, 102]),
        FieldRef::new("read_at").gte(101_i64),
        FieldRef::new("read_at").is_not_null(),
    ];
    for filter in filters {
        let query = recipient_query().filter(filter);
        assert_eq!(
            session
                .execute_public_live_page(&query, None)
                .unwrap()
                .row_count,
            1
        );
    }
    for filter in [
        FieldRef::new("label").eq("read"),
        FieldRef::new("label").in_list(["read", "other"]),
        FieldRef::new("label").text_starts_with("re"),
    ] {
        let query = DynamicQuery::new(ENTITY_NAME).filter(filter);
        assert_eq!(
            session
                .execute_public_live_page(&query, None)
                .unwrap()
                .row_count,
            1
        );
    }
    let label_count =
        DynamicQuery::new(ENTITY_NAME).filter(FieldRef::new("label").in_list(["read", "other"]));
    assert_eq!(session.execute_public_exact_count(&label_count).unwrap(), 1);
    let null_query = recipient_query().filter(FieldRef::new("read_at").is_null());
    assert!(session.execute_public_live_page(&null_query, None).is_err());
}
