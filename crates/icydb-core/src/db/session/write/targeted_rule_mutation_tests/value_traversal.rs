//! Published catalog reads agree across full materialization and nested paths.

use super::*;
use crate::{
    db::{
        DynamicQuery, SqlStatementResult, asc,
        schema::{
            CandidateSchemaRevision, PersistedNestedLeafSnapshot, TestEnumDefinition,
            TestEnumVariant, build_accepted_enum_catalog_for_tests,
            build_record_members_catalog_for_tests,
        },
    },
    value::OutputValue,
};

fn candidate() -> CandidateSchemaRevision {
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "State",
        vec![
            TestEnumVariant::unit("Idle"),
            TestEnumVariant::payload(
                "Ready",
                AcceptedFieldKind::Nat64,
                FieldStorageDecode::CatalogValue,
            ),
        ],
    )])
    .unwrap();
    let state_kind = AcceptedFieldKind::Enum {
        type_id: enums.type_id("State").unwrap(),
    };
    let mut deep_kind = AcceptedFieldKind::Nat64;
    for _ in 0..48 {
        deep_kind = AcceptedFieldKind::List(Box::new(deep_kind));
    }
    let members = [
        ("a_before", AcceptedFieldKind::Nat64),
        ("b_state", state_kind),
        ("c_after", AcceptedFieldKind::Nat64),
        ("d_deep", deep_kind),
    ];
    let (composites, record_id) = build_record_members_catalog_for_tests(
        "Profile".into(),
        members
            .iter()
            .map(|(name, kind)| ((*name).into(), kind.clone(), false))
            .collect(),
        &enums,
    );
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            vec![],
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "profile".into(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Composite { type_id: record_id },
            members
                .into_iter()
                .map(|(name, kind)| {
                    PersistedNestedLeafSnapshot::new(vec![name.into()], kind, false)
                })
                .collect(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let tag = EntityTag::new(93);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.into(),
        "Traversal".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let bindings = AcceptedSourceBindingCatalog::initial_for_tests(
        BTreeMap::from([(source(ENTITY_SOURCE, EntitySourceKey::try_new), tag)]),
        BTreeMap::from([
            (
                (tag, source(ID_SOURCE, FieldSourceKey::try_new)),
                FieldId::new(1),
            ),
            (
                (tag, source(PROFILE_SOURCE, FieldSourceKey::try_new)),
                FieldId::new(2),
            ),
        ]),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    accepted_schema_candidate_with_catalogs_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        enums,
        composites,
        bindings,
        BTreeMap::from([(tag, snapshot)]),
    )
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let candidate = candidate();
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session.db.drive_startup_recovery_page().unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    session
}

fn profile(payload: bool) -> InputValue {
    let mut deep = InputValue::nat64(7);
    for _ in 0..48 {
        deep = InputValue::list(vec![deep]);
    }
    let state = if payload {
        InputValue::enum_value("Ready", Some("State"))
            .with_enum_payload(InputValue::nat64(5))
            .unwrap()
    } else {
        InputValue::enum_value("Idle", Some("State"))
    };
    InputValue::map(vec![
        (InputValue::from("a_before"), InputValue::nat64(9)),
        (InputValue::from("b_state"), state),
        (InputValue::from("c_after"), InputValue::nat64(11)),
        (InputValue::from("d_deep"), deep),
    ])
}

fn projection(session: &DbSession<TestCanister>, sql: &str) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } =
        session.execute_trusted_sql_query(sql).unwrap()
    else {
        panic!("projection expected")
    };
    rows
}

#[test]
fn canonical_traversal_published_reads_preserve_enum_siblings_and_deep_leaves() {
    let session = initialize();
    for id in 1..=2 {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Traversal".into(),
                patch: DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    ("profile".into(), DynamicWriteCell::Value(profile(id == 2))),
                ]),
            })
            .unwrap();
    }
    for _ in 0..2 {
        let query = DynamicQuery::new("Traversal")
            .select(["id", "profile"])
            .filter(crate::db::FilterExpr::in_list(
                "id",
                [InputValue::nat64(1), InputValue::nat64(2)],
            ))
            .order_by(asc("id"))
            .limit(2);
        let full = session.execute_public_live_page(&query, None).unwrap().rows;
        assert_eq!(
            full,
            session
                .execute_trusted_live_page(&query, None)
                .unwrap()
                .rows
        );
        assert_eq!(full.len(), 2);
        let leaves = projection(
            &session,
            "SELECT profile.a_before, profile.c_after, profile.b_state, profile.d_deep FROM Traversal ORDER BY id ASC",
        );
        for (row, selected) in full.iter().zip(&leaves) {
            let crate::value::PublicValue::Map(entries) = row[1].as_public() else {
                panic!("record expected")
            };
            for (name, leaf) in ["a_before", "c_after", "b_state", "d_deep"]
                .into_iter()
                .zip(selected)
            {
                assert_eq!(
                    entries
                        .iter()
                        .find(|(key, _)| matches!(key, crate::value::PublicValue::Text(text) if text == name))
                        .unwrap()
                        .1,
                    *leaf.as_public()
                );
            }
        }
        assert_eq!(
            projection(
                &session,
                "SELECT id FROM Traversal WHERE profile.a_before = 9 AND profile.c_after = 11 ORDER BY id ASC"
            ),
            vec![vec![OutputValue::nat64(1)], vec![OutputValue::nat64(2)]]
        );
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(
                "SELECT profile.a_before, COUNT(*) FROM Traversal GROUP BY profile.a_before",
            )
            .unwrap()
        else {
            panic!("grouped result expected")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].group_key(), [OutputValue::nat64(9)]);
        assert_eq!(rows[0].aggregate_values(), [OutputValue::nat64(2)]);
    }
}
