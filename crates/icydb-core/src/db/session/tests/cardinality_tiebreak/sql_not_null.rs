//! SQL NOT preserves UNKNOWN across predicate/expression reads and mutations.

use super::*;

fn initialize_nullable_rows() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    session.db.drive_startup_recovery_page().unwrap();
    let fields = [
        ("id", AcceptedFieldKind::Nat64, false),
        ("status", AcceptedFieldKind::Text { max_len: None }, true),
        ("peer", AcceptedFieldKind::Text { max_len: None }, true),
        ("qty", AcceptedFieldKind::Nat64, true),
        ("other", AcceptedFieldKind::Nat64, true),
        ("flag", AcceptedFieldKind::Bool, true),
        ("marked", AcceptedFieldKind::Bool, false),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind, nullable))| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot + 1).unwrap()),
            name.into(),
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
    let bindings = fields
        .iter()
        .map(|field| ((ENTITY_TAG, field_source(field.name())), field.id()))
        .collect();
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
        bindings,
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    for values in [
        "1, 'archived', 'archived', 1, 1, TRUE, FALSE",
        "2, 'active', 'closed', 2, 3, FALSE, FALSE",
        "3, NULL, 'active', NULL, 2, NULL, FALSE",
        "4, 'closed', NULL, 3, NULL, TRUE, FALSE",
        "5, '', '', 0, 0, FALSE, FALSE",
        "6, NULL, NULL, NULL, NULL, NULL, FALSE",
    ] {
        session
            .execute_trusted_sql_mutation(&format!(
                "INSERT INTO PlannerRow (id, status, peer, qty, other, flag, marked) VALUES ({values})"
            ))
            .unwrap();
    }
    session
}

fn sql_rows(session: &DbSession<TestCanister>, sql: &str) -> Vec<Vec<OutputValue>> {
    let result = session
        .execute_trusted_sql_query(sql)
        .unwrap_or_else(|error| panic!("query {sql} failed: {error:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("query {sql} did not return a projection");
    };
    rows
}

fn assert_ids(session: &DbSession<TestCanister>, predicate: &str, expected: &[u64]) {
    assert_eq!(
        sql_rows(
            session,
            &format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id"),
        ),
        expected
            .iter()
            .map(|id| vec![OutputValue::nat64(*id)])
            .collect::<Vec<_>>(),
        "{predicate}",
    );
}

#[test]
fn sql_not_null_reads_and_counts_agree_across_execution_lanes() {
    let session = initialize_nullable_rows();
    for (predicate, expected) in [
        ("NOT (status = 'archived')", vec![2, 4, 5]),
        ("NOT (status <> 'archived')", vec![1]),
        ("status NOT LIKE 'a%'", vec![4, 5]),
        ("status NOT ILIKE 'a%'", vec![4, 5]),
        ("NOT (LOWER(status) = 'archived')", vec![2, 4, 5]),
        ("NOT STARTS_WITH(status, 'a')", vec![4, 5]),
        ("NOT ENDS_WITH(status, 'ed')", vec![2, 5]),
        ("NOT CONTAINS(status, 'iv')", vec![4, 5]),
        ("NOT CONTAINS(LOWER(status), 'iv')", vec![4, 5]),
        ("NOT (qty > 0)", vec![5]),
        ("NOT (0 < qty)", vec![5]),
        ("NOT (qty >= 2)", vec![1, 5]),
        ("NOT (qty < 2)", vec![2, 4]),
        ("NOT (qty <= 2)", vec![4]),
        ("NOT (qty = other)", vec![2]),
        ("NOT (status = peer)", vec![2]),
        ("NOT flag", vec![2, 5]),
        ("NOT NOT (status = 'archived')", vec![1]),
        ("NOT (status = 'archived' OR qty = 3)", vec![2, 5]),
        ("NOT (status = 'archived' AND qty = 3)", vec![1, 2, 4, 5]),
        ("NOT (status = 'archived' AND id = 1)", vec![2, 3, 4, 5, 6]),
        ("NOT (status = 'archived' OR id = 3)", vec![2, 4, 5]),
        ("NOT (status = 'archived') OR id = 3", vec![2, 3, 4, 5]),
        ("NOT (status IS NULL)", vec![1, 2, 4, 5]),
        ("NOT (status IS NOT NULL)", vec![3, 6]),
        ("NOT (status = NULL)", vec![]),
        ("NOT (status IN ('archived', 'active'))", vec![4, 5]),
        ("NOT (status IN ('archived', NULL))", vec![]),
    ] {
        // Arithmetic keeps the same truth condition in the expression lane.
        for condition in [
            predicate.to_string(),
            format!("({predicate}) AND id + 0 = id"),
        ] {
            assert_ids(&session, &condition, &expected);
        }
        // Global COUNT requires a predicate subset; scalar SELECT above also
        // qualifies the equivalent expression-backed execution.
        assert_eq!(
            sql_rows(
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

fn assert_update_preserves_unknown(predicate: &str) {
    let session = initialize_nullable_rows();
    let result = session
        .execute_trusted_sql_exact_update(
            &format!("UPDATE PlannerRow SET marked = TRUE WHERE {predicate}"),
            6,
        )
        .unwrap();
    assert!(matches!(result, SqlStatementResult::Count { row_count: 3 }));
    assert_ids(&session, "marked = TRUE", &[1, 4, 5]);
    assert_ids(&session, "marked = FALSE", &[2, 3, 6]);
}

#[test]
fn sql_not_null_update_preserves_unknown_rows() {
    assert_update_preserves_unknown("NOT (status = 'active')");
}

#[test]
fn sql_not_null_not_like_update_preserves_unknown_rows() {
    assert_update_preserves_unknown("status NOT LIKE 'act%'");
}

fn assert_delete_preserves_unknown(predicate: &str) {
    let session = initialize_nullable_rows();
    let result = session
        .execute_trusted_sql_mutation(&format!("DELETE FROM PlannerRow WHERE {predicate}"))
        .unwrap();
    assert!(matches!(result, SqlStatementResult::Count { row_count: 3 }));
    assert_ids(&session, "id > 0", &[2, 3, 6]);
}

#[test]
fn sql_not_null_delete_preserves_unknown_rows() {
    assert_delete_preserves_unknown("NOT (status = 'active')");
}

#[test]
fn sql_not_null_not_like_delete_preserves_unknown_rows() {
    assert_delete_preserves_unknown("status NOT LIKE 'act%'");
}
