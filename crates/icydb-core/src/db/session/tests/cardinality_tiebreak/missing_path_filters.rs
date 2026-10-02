//! Accepted optional-record filters share read, grouped and mutation semantics.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    executor::{StructuralProjectionRequest, execute_structural_projection_rows},
    query::{
        admission::{QueryAdmissionPolicy, QueryAdmissionSummary},
        preparation::with_preparation_work,
    },
    schema::{
        AcceptedSourceBindingCatalog, LeafCodec, PersistedNestedLeafSnapshot,
        accepted_schema_candidate_with_catalogs_for_tests,
        build_record_composite_catalog_for_tests, empty_accepted_enum_catalog_for_tests,
    },
    sql::{
        lowering::{
            bind_lowered_sql_select_query_structural_with_schema,
            lower_prepared_sql_select_statement_with_schema, prepare_sql_statement,
        },
        parser::parse_sql,
    },
};
use crate::value::Value;
use icydb_schema::EntitySourceKey;

fn initialize_profiles() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    session.db.drive_startup_recovery_page().unwrap();
    let enums = empty_accepted_enum_catalog_for_tests();
    let (composites, profile_type) = build_record_composite_catalog_for_tests(
        "tests::Profile".into(),
        "rank".into(),
        AcceptedFieldKind::Nat64,
        true,
        &enums,
    );
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        field(2, "common", 1, AcceptedFieldKind::Text { max_len: None }),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "profile".into(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Composite {
                type_id: profile_type,
            },
            vec![PersistedNestedLeafSnapshot::new(
                vec!["rank".into()],
                AcceptedFieldKind::Nat64,
                true,
            )],
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
        field(4, "marker", 3, AcceptedFieldKind::Nat64),
    ];
    let bindings = AcceptedSourceBindingCatalog::initial_for_tests(
        BTreeMap::from([(EntitySourceKey::try_new(ENTITY_SOURCE).unwrap(), ENTITY_TAG)]),
        fields
            .iter()
            .map(|field| ((ENTITY_TAG, field_source(field.name())), field.id()))
            .collect(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
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
        vec![],
    );
    let candidate = accepted_schema_candidate_with_catalogs_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        enums,
        composites,
        bindings,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    seed_profiles(&session);
    session
}

fn seed_profiles(session: &DbSession<TestCanister>) {
    for (id, name, rank) in [
        (1_u64, "bob", None),
        (2, "alice", Some(InputValue::nat64(5))),
        (3, "carol", Some(InputValue::nat64(7))),
        (4, "bob", Some(InputValue::nat64(7))),
        (5, "alice", None),
        (6, "alice", Some(InputValue::null())),
    ] {
        let profile = rank.map_or(DynamicWriteCell::Null, |rank| {
            DynamicWriteCell::Value(InputValue::map(vec![(
                InputValue::text("rank".into()),
                rank,
            )]))
        });
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "common".into(),
                        DynamicWriteCell::Value(InputValue::text(name.into())),
                    ),
                    ("profile".into(), profile),
                    (
                        "marker".into(),
                        DynamicWriteCell::Value(InputValue::nat64(5)),
                    ),
                ])],
            )
            .unwrap();
    }
}

fn structural_select(session: &DbSession<TestCanister>, sql: &str) -> StructuralQuery {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let statement = parse_sql(sql).unwrap();
    with_preparation_work(|work| {
        let prepared = prepare_sql_statement(&statement, ENTITY_NAME, work).unwrap();
        let select = lower_prepared_sql_select_statement_with_schema(
            prepared,
            catalog.accepted_schema_info(),
            work,
        )
        .unwrap();
        bind_lowered_sql_select_query_structural_with_schema(
            select,
            MissingRowPolicy::Ignore,
            catalog.accepted_schema_info(),
            work,
        )
    })
    .unwrap()
}

#[test]
fn missing_path_filters_preserve_admitted_structural_and_sql_rows() {
    let session = initialize_profiles();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let policy = QueryAdmissionPolicy::default_bounded_read();
    for condition in [
        "common = 'bob' OR profile.rank = 5",
        "profile.rank = 5 OR common = 'bob'",
    ] {
        for direction in ["ASC", "DESC"] {
            let query = structural_select(
                &session,
                &format!(
                    "SELECT id FROM PlannerRow WHERE id IN (1,2,3,4,5,6) AND ({condition}) ORDER BY id {direction} LIMIT 6"
                ),
            );
            let expected = if direction == "DESC" {
                [4, 2, 1]
            } else {
                [1, 2, 4]
            }
            .map(|id| vec![Value::Nat64(id)]);
            for _ in 0..2 {
                for lane in [
                    DiagnosticExecutionLane::PublicRead,
                    DiagnosticExecutionLane::TrustedRead,
                ] {
                    let plan = session
                        .cached_shared_query_plan_for_accepted_authority_with_catalog(
                            catalog.accepted_entity_authority(),
                            &catalog,
                            &query,
                            lane,
                        )
                        .unwrap();
                    assert_eq!(
                        policy
                            .evaluate(
                                QueryAdmissionSummary::from_plan(
                                    policy.lane(),
                                    plan.logical_plan()
                                )
                                .unwrap()
                            )
                            .rejection(),
                        None
                    );
                    let rows = execute_structural_projection_rows(
                        &session.db,
                        StructuralProjectionRequest::new(plan, lane),
                    )
                    .unwrap()
                    .into_value_rows();
                    assert_eq!(rows, expected);
                }
            }
        }
    }
    for condition in [
        "common = 'bob' OR profile.rank = 5",
        "profile.rank = 5 OR common = 'bob'",
    ] {
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                ),
                [1, 2, 4].map(|id| vec![OutputValue::nat64(id)])
            );
        }
    }
    // A missing descendant must not become a terminal NULL observation.
    for condition in [
        "profile.rank IS NULL",
        "NOT (profile.rank = 5)",
        "COALESCE(profile.rank, 0) = 0",
    ] {
        let expected = if condition.starts_with("NOT") {
            vec![vec![OutputValue::nat64(3)], vec![OutputValue::nat64(4)]]
        } else {
            vec![vec![OutputValue::nat64(6)]]
        };
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
            ),
            expected
        );
    }
}

#[test]
fn missing_path_filters_preserve_grouped_counts_and_continuation() {
    let session = initialize_profiles();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let policy = QueryAdmissionPolicy::default_bounded_read();
    for condition in [
        "common = 'bob' OR profile.rank = 5",
        "profile.rank = 5 OR common = 'bob'",
    ] {
        for public in [true, false] {
            for _ in 0..2 {
                let query = structural_select(&session, &format!("SELECT common, COUNT(*) FROM PlannerRow WHERE id IN (1,2,3,4,5,6) AND ({condition}) GROUP BY common ORDER BY common LIMIT 1"))
                    .grouped_limits(4, 16 * 1024);
                let mut cursor = None;
                let mut groups = vec![];
                for _ in 0..8 {
                    let page = session
                        .execute_structural_grouped_from_query(
                            &query,
                            &catalog,
                            public.then_some(&policy),
                            cursor.as_deref(),
                        )
                        .unwrap();
                    groups.extend(
                        page.rows
                            .into_iter()
                            .map(|row| (row.group_key().to_vec(), row.aggregate_values().to_vec())),
                    );
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert!(cursor.is_none());
                assert_eq!(
                    groups,
                    vec![
                        (
                            vec![OutputValue::text("alice".into())],
                            vec![OutputValue::nat64(1)]
                        ),
                        (
                            vec![OutputValue::text("bob".into())],
                            vec![OutputValue::nat64(2)]
                        ),
                    ]
                );
            }
        }
    }
}

#[test]
fn missing_path_filters_preserve_update_and_delete_scopes() {
    let session = initialize_profiles();
    let condition = "common = 'bob' OR profile.rank = 5";
    for _ in 0..2 {
        let result = session
            .execute_trusted_sql_exact_update(
                &format!("UPDATE PlannerRow SET marker = 1 WHERE {condition}"),
                5,
            )
            .unwrap();
        assert!(matches!(result, SqlStatementResult::Count { row_count: 3 }));
        assert_eq!(
            projection_rows(
                &session,
                "SELECT id FROM PlannerRow WHERE marker = 1 ORDER BY id"
            ),
            [1, 2, 4].map(|id| vec![OutputValue::nat64(id)])
        );
    }
    let result = session
        .execute_trusted_sql_mutation(
            "DELETE FROM PlannerRow WHERE profile.rank = 5 OR common = 'bob' RETURNING id",
        )
        .unwrap();
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("delete returns before images");
    };
    assert_eq!(rows, [1, 2, 4].map(|id| vec![OutputValue::nat64(id)]));
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id"),
        [3, 5, 6].map(|id| vec![OutputValue::nat64(id)])
    );
}
