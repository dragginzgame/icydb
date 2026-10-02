//! Hybrid projection declines unsupported index decoding without losing rows.

use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        predicate::Predicate,
        query::plan::{
            CoveringReadFieldSource, covering_hybrid_projection_execution_plan_with_schema_info,
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedEnumCatalog, AcceptedSourceBindingCatalog,
            TestEnumDefinition, TestEnumVariant, accepted_schema_candidate_with_catalogs_for_tests,
            build_accepted_enum_catalog_for_tests, empty_accepted_enum_catalog_for_tests,
        },
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
    value::{PublicEnumValue, PublicValue, Value},
};
use icydb_schema::EntitySourceKey;

// Each named case has its own accepted registry identity. All fixtures publish
// the actual kind and index contract rather than relying on generated models.
pub(super) fn initialize_component_schema(kind: AcceptedFieldKind, enums: AcceptedEnumCatalog) {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let setup = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    setup.db.drive_startup_recovery_page().unwrap();
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        field(2, "category", 1, AcceptedFieldKind::Nat64),
        field(3, "operand", 2, kind.clone()),
        field(4, "label", 3, AcceptedFieldKind::Text { max_len: None }),
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
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "category_operand_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["category".into()],
                    AcceptedFieldKind::Nat64,
                    false,
                ),
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(3),
                    SchemaFieldSlot::new(2),
                    vec!["operand".into()],
                    kind,
                    false,
                ),
            ]),
            None,
        )],
    );
    let candidate = accepted_schema_candidate_with_catalogs_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        enums,
        AcceptedCompositeCatalog::empty(),
        bindings,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        setup.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
}

fn assert_component_projection(
    kind: AcceptedFieldKind,
    input: InputValue,
    expected: OutputValue,
    enums: AcceptedEnumCatalog,
) {
    let ordered = !matches!(kind, AcceptedFieldKind::Enum { .. });
    initialize_component_schema(kind, enums);
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    seed_component_rows(&session, &input);
    assert_hybrid_admission(
        &session,
        Predicate::eq("category".into(), Value::Nat64(3)),
        false,
    );
    if ordered {
        assert_hybrid_admission(
            &session,
            Predicate::gte("category".into(), Value::Nat64(3)),
            true,
        );
    }

    for _ in 0..2 {
        for (suffix, ids) in [
            ("", vec![1, 2]),
            ("ORDER BY id ASC", vec![1, 2]),
            ("ORDER BY id DESC", vec![2, 1]),
            ("ORDER BY id DESC LIMIT 1 OFFSET 1", vec![1]),
        ] {
            let rows = ids
                .into_iter()
                .map(|id| vec![expected.clone(), OutputValue::text(format!("row-{id}"))])
                .collect::<Vec<_>>();
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT operand, label FROM PlannerRow WHERE category = 3 {suffix}")
                ),
                rows
            );
        }
        // Membership spans two physical prefixes, including the other category.
        assert_eq!(
            projection_rows(
                &session,
                "SELECT operand, label FROM PlannerRow WHERE category IN (3, 9) ORDER BY id"
            ),
            (1..=3)
                .map(|id| vec![expected.clone(), OutputValue::text(format!("row-{id}"))])
                .collect::<Vec<_>>()
        );

        if ordered {
            // A range has no exact prefix row-presence proof, so it exercises
            // checked hybrid rows as well as the proven prefix route above.
            assert_eq!(
                projection_rows(
                    &session,
                    "SELECT operand, label FROM PlannerRow WHERE category >= 3 ORDER BY id"
                ),
                (1..=3)
                    .map(|id| vec![expected.clone(), OutputValue::text(format!("row-{id}"))])
                    .collect::<Vec<_>>()
            );
            for (direction, ids) in [("ASC", vec![1, 2]), ("DESC", vec![2, 1])] {
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!(
                            "SELECT operand, label FROM PlannerRow WHERE category = 3 ORDER BY operand {direction}"
                        )
                    ),
                    ids.into_iter()
                        .map(|id| vec![expected.clone(), OutputValue::text(format!("row-{id}"))])
                        .collect::<Vec<_>>()
                );
            }
        }
        if ordered {
            // Equality-bound operands come from the accepted predicate constant,
            // so even unsupported decoder tags can remain on hybrid execution.
            // SQL parameter coercion excludes enums; their accepted projection
            // is qualified through the category predicate above.
            let dispatch = crate::db::sql_statement_dispatch(
            "SELECT operand, label FROM PlannerRow WHERE category = 3 AND operand = ? ORDER BY id",
        )
        .unwrap();
            let (SqlStatementResult::Projection { rows, .. }, _) = session
                .execute_trusted_sql_query_with_entity_name(&dispatch, std::slice::from_ref(&input))
                .unwrap()
            else {
                panic!("bound component query must return a projection");
            };
            assert_eq!(
                rows,
                (1..=2)
                    .map(|id| vec![expected.clone(), OutputValue::text(format!("row-{id}"))])
                    .collect::<Vec<_>>()
            );
        }
    }
}

fn seed_component_rows<C: CanisterKind>(session: &DbSession<C>, input: &InputValue) {
    for id in 1..=3 {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(if id == 3 { 9 } else { 3 })),
                    ),
                    ("operand".into(), DynamicWriteCell::Value(input.clone())),
                    (
                        "label".into(),
                        DynamicWriteCell::Value(InputValue::text(format!("row-{id}"))),
                    ),
                ])],
            )
            .unwrap();
    }
}

fn assert_hybrid_admission<C: CanisterKind>(
    session: &DbSession<C>,
    predicate: Predicate,
    range: bool,
) {
    // Prove this is the admitted hybrid route: operand comes from the index,
    // while label requires the row. A scalar-only plan would mask the defect.
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = StructuralQuery::new(MissingRowPolicy::Ignore)
        .filter_normalized_predicate(predicate)
        .select_fields(["operand", "label"])
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        });
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &query,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    assert_eq!(
        prepared
            .logical_plan()
            .access
            .as_index_range_path()
            .is_some(),
        range
    );
    let hybrid = covering_hybrid_projection_execution_plan_with_schema_info(
        catalog.accepted_schema_info(),
        prepared.logical_plan(),
        true,
    )
    .unwrap();
    assert!(matches!(
        hybrid.fields[0].source,
        CoveringReadFieldSource::IndexComponent { .. }
    ));
    assert!(matches!(
        hybrid.fields[1].source,
        CoveringReadFieldSource::RowField
    ));
}

fn projection_rows<C: CanisterKind>(session: &DbSession<C>, sql: &str) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
    else {
        panic!("component query must return a projection");
    };
    rows
}

macro_rules! component_case {
    ($name:ident, $kind:expr, $value:expr) => {
        #[test]
        fn $name() {
            let value = $value;
            assert_component_projection(
                $kind,
                InputValue::from(value.clone()),
                OutputValue::from(value),
                empty_accepted_enum_catalog_for_tests(),
            );
        }
    };
}

component_case!(
    hybrid_decimal,
    AcceptedFieldKind::Decimal { scale: 2 },
    Value::Decimal(Decimal::new(1250, 2))
);
component_case!(
    hybrid_timestamp,
    AcceptedFieldKind::Timestamp,
    Value::Timestamp(Timestamp::from_millis(-42))
);
component_case!(
    hybrid_date,
    AcceptedFieldKind::Date,
    Value::Date(Date::EPOCH)
);
component_case!(
    hybrid_duration,
    AcceptedFieldKind::Duration,
    Value::Duration(Duration::from_millis(5))
);
component_case!(
    hybrid_float32,
    AcceptedFieldKind::Float32,
    Value::Float32(Float32::try_new(1.25).unwrap())
);
component_case!(
    hybrid_float64,
    AcceptedFieldKind::Float64,
    Value::Float64(Float64::try_new(1.25).unwrap())
);
component_case!(hybrid_int128, AcceptedFieldKind::Int128, Value::Int128(-42));
component_case!(hybrid_nat128, AcceptedFieldKind::Nat128, Value::Nat128(42));
component_case!(
    hybrid_int_big,
    AcceptedFieldKind::IntBig { max_bytes: 64 },
    Value::IntBig(IntBig::from(-42_i64))
);
component_case!(
    hybrid_nat_big,
    AcceptedFieldKind::NatBig { max_bytes: 64 },
    Value::NatBig(NatBig::from(42_u64))
);
component_case!(hybrid_u256, AcceptedFieldKind::U256, Value::U256(U256::MAX));
component_case!(
    hybrid_principal,
    AcceptedFieldKind::Principal,
    Value::Principal(Principal::anonymous())
);
component_case!(
    hybrid_subaccount,
    AcceptedFieldKind::Subaccount,
    Value::Subaccount(Subaccount::from_array([9; 32]))
);
component_case!(
    hybrid_account,
    AcceptedFieldKind::Account,
    Value::Account(Account::from_owner_and_subaccount(
        Principal::anonymous(),
        Some(Subaccount::from_array([9; 32]))
    ))
);
component_case!(hybrid_bool, AcceptedFieldKind::Bool, Value::Bool(true));
component_case!(hybrid_int64, AcceptedFieldKind::Int64, Value::Int64(-42));
component_case!(hybrid_nat64, AcceptedFieldKind::Nat64, Value::Nat64(42));
component_case!(
    hybrid_text,
    AcceptedFieldKind::Text { max_len: None },
    Value::Text("sample".into())
);
component_case!(
    hybrid_ulid,
    AcceptedFieldKind::Ulid,
    Value::Ulid(Ulid::from_u128(7))
);
component_case!(hybrid_unit, AcceptedFieldKind::Unit, Value::Unit);

#[test]
fn hybrid_unit_enum() {
    const ENUM_PATH: &str = "db::session::tests::cardinality_tiebreak::Choice";
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        ENUM_PATH,
        vec![
            TestEnumVariant::unit("First"),
            TestEnumVariant::unit("Second"),
        ],
    )])
    .unwrap();
    let kind = AcceptedFieldKind::Enum {
        type_id: enums.type_id(ENUM_PATH).unwrap(),
    };
    assert_component_projection(
        kind,
        InputValue::enum_value("First", Some(ENUM_PATH)),
        OutputValue::from_public(PublicValue::Enum(PublicEnumValue::new(
            "First",
            Some(ENUM_PATH),
        ))),
        enums,
    );
}
