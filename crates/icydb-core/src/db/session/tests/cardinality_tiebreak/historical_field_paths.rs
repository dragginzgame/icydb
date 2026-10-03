//! Accepted record additions must expose the same logical paths on old and new rows.

use super::*;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        codec::{decode_row_payload_bytes, serialize_row_payload},
        data::{DecodedDataStoreKey, RawRow, encode_canonical_value_storage_bytes},
        executor::{
            StructuralProjectionRequest,
            budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            execute_structural_projection_rows,
        },
        query::{
            admission::{QueryAdmissionPolicy, QueryAdmissionSummary},
            preparation::with_preparation_work,
        },
        schema::{
            AcceptedSourceBindingCatalog, LeafCodec, PersistedFieldOrigin,
            PersistedNestedLeafSnapshot, RowLayoutVersion, SchemaFieldWritePolicy,
            SchemaHistoricalFill, accepted_schema_candidate_with_catalogs_for_tests,
            build_record_composite_catalog_for_tests, empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};
use icydb_schema::EntitySourceKey;

fn historical_profiles(fill: Option<Value>) -> DbSession<TestCanister> {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        field(2, "bucket", 1, AcceptedFieldKind::Nat64),
    ];
    scalar_page_limits::initialize_payload_schema(fields.clone(), vec![]);
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for id in 1..=2 {
        insert_profile(&session, id, None);
    }
    let old_rows = [1, 2].map(|id| {
        let key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(id))
            .unwrap()
            .to_raw()
            .unwrap();
        let row = DATA_STORE.with(|store| store.borrow().get(&key).unwrap());
        assert_eq!(
            decode_row_payload_bytes(row.as_bytes())
                .unwrap()
                .layout_version(),
            RowLayoutVersion::INITIAL
        );
        (key, row)
    });

    let candidate = record_addition_candidate(fields, fill.as_ref());
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .unwrap();
    insert_profile(&session, 3, Some(InputValue::null()));
    insert_profile(&session, 4, Some(profile_input(InputValue::nat64(7))));
    insert_profile(&session, 5, Some(profile_input(InputValue::null())));

    // Establish that the accepted logical-slot owner already resolves this fill.
    // Rows 1 and 2 were written before the field existed and are never rewritten.
    let expected = OutputValue::from(fill.unwrap_or(Value::Null));
    assert_eq!(
        projection_rows(
            &session,
            "SELECT profile FROM PlannerRow WHERE id IN (1,2) ORDER BY id"
        ),
        vec![vec![expected.clone()], vec![expected]]
    );
    for (key, row) in old_rows {
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().get(&key).unwrap()),
            row
        );
    }
    session
}

// Retain the initial historical floor while constructing the accepted record addition.
fn record_addition_candidate(
    mut fields: Vec<PersistedFieldSnapshot>,
    fill: Option<&Value>,
) -> CandidateSchemaRevision {
    let enums = empty_accepted_enum_catalog_for_tests();
    let (composites, profile_type) = build_record_composite_catalog_for_tests(
        "tests::Profile".into(),
        "rank".into(),
        AcceptedFieldKind::Nat64,
        true,
        &enums,
    );
    let layout = RowLayoutVersion::INITIAL.checked_next().unwrap();
    let historical_fill = fill.map_or(SchemaHistoricalFill::Null, |value| {
        SchemaHistoricalFill::SlotPayload(encode_canonical_value_storage_bytes(value).unwrap())
    });
    fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
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
        layout,
        SchemaInsertDefault::None,
        historical_fill,
        SchemaFieldWritePolicy::none(),
        PersistedFieldOrigin::SqlDdl,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    ));
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
        SchemaRowLayout::new(
            layout,
            RowLayoutVersion::INITIAL,
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        vec![],
    );
    accepted_schema_candidate_with_catalogs_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL.checked_next().unwrap(),
        enums,
        composites,
        bindings,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
    )
}

fn profile_input(rank: InputValue) -> InputValue {
    InputValue::map(vec![(InputValue::text("rank".into()), rank)])
}

fn insert_profile(session: &DbSession<TestCanister>, id: u64, profile: Option<InputValue>) {
    let mut cells = vec![
        ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
        (
            "bucket".into(),
            DynamicWriteCell::Value(InputValue::nat64(7)),
        ),
    ];
    if let Some(profile) = profile {
        cells.push(("profile".into(), DynamicWriteCell::Value(profile)));
    }
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![DynamicStructuralPatch::new(cells)])
        .unwrap();
}

fn fills() -> [Option<Value>; 3] {
    [
        None,
        Some(Value::Map(vec![(
            Value::Text("rank".into()),
            Value::Nat64(5),
        )])),
        Some(Value::Map(vec![(Value::Text("rank".into()), Value::Null)])),
    ]
}

fn historical_field_paths_scalar_projection(fill: Option<Value>) {
    let historical_rank = match &fill {
        Some(Value::Map(entries)) => OutputValue::from(entries[0].1.clone()),
        _ => OutputValue::null(),
    };
    let session = historical_profiles(fill);
    for _ in 0..2 {
        assert_eq!(
            projection_rows(&session, "SELECT profile.rank FROM PlannerRow ORDER BY id"),
            vec![
                vec![historical_rank.clone()],
                vec![historical_rank.clone()],
                vec![OutputValue::null()],
                vec![OutputValue::nat64(7)],
                vec![OutputValue::null()]
            ]
        );
    }
    assert_admitted_rows(
        &session,
        "SELECT profile.rank FROM PlannerRow",
        FieldRef::new("id").in_list([1, 2, 3, 4, 5].map(InputValue::nat64)),
        vec![
            vec![historical_rank.clone()],
            vec![historical_rank],
            vec![OutputValue::null()],
            vec![OutputValue::nat64(7)],
            vec![OutputValue::null()],
        ],
    );
}

fn historical_field_paths_single_path_group(fill: Option<Value>) {
    let numeric_fill =
        matches!(&fill, Some(Value::Map(entries)) if entries[0].1 == Value::Nat64(5));
    let session = historical_profiles(fill);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let policy = QueryAdmissionPolicy::default_bounded_read();
    // An extra direct key selects retained-slot decoding instead of the
    // single-path decoder. Both must observe the same historical leaf.
    for keys in ["profile.rank", "profile.rank, bucket"] {
        let query = missing_path_filters::structural_select(&session,
        &format!("SELECT {keys}, COUNT(*) FROM PlannerRow WHERE id IN (1,2,3,4,5) GROUP BY {keys} ORDER BY {keys} LIMIT 1"))
        .grouped_limits(8, 16 * 1024);
        for _ in 0..2 {
            for public in [true, false] {
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
                    groups.extend(page.rows.into_iter().map(|row| {
                        if keys.contains("bucket") {
                            assert_eq!(row.group_key()[1], OutputValue::nat64(7));
                        }
                        (
                            vec![row.group_key()[0].clone()],
                            row.aggregate_values().to_vec(),
                        )
                    }));
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert!(cursor.is_none());
                let mut expected = vec![
                    (
                        vec![OutputValue::null()],
                        vec![OutputValue::nat64(if numeric_fill { 2 } else { 4 })],
                    ),
                    (vec![OutputValue::nat64(7)], vec![OutputValue::nat64(1)]),
                ];
                if numeric_fill {
                    expected.push((vec![OutputValue::nat64(5)], vec![OutputValue::nat64(2)]));
                }
                assert_eq!(groups.len(), expected.len());
                for group in expected {
                    assert!(
                        groups.contains(&group),
                        "missing group {group:?} in {groups:?}"
                    );
                }
            }
        }
    }
}

// Exercise plan admission and both compiled-reader lanes, including warm reuse.
fn assert_admitted_rows(
    session: &DbSession<TestCanister>,
    select: &str,
    primary_key_filter: FilterExpr,
    expected: Vec<Vec<OutputValue>>,
) {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let policy = QueryAdmissionPolicy::default_bounded_read();
    for direction in ["ASC", "DESC"] {
        let query = missing_path_filters::structural_select(
            session,
            &format!("{select} ORDER BY id {direction} LIMIT 5"),
        );
        // Scalar SQL filters do not always expose a planner predicate. Supply
        // the accepted structural key guard through its normal preparation owner.
        let query = with_preparation_work(|work| {
            query.filter_for_schema(catalog.accepted_schema_info(), &primary_key_filter, work)
        })
        .unwrap();
        let mut expected = expected.clone();
        if direction == "DESC" {
            expected.reverse();
        }
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
                            QueryAdmissionSummary::from_plan(policy.lane(), plan.logical_plan())
                                .unwrap()
                        )
                        .rejection(),
                    None,
                    "{select} must retain its structural primary-key bound"
                );
                let rows = execute_structural_projection_rows(
                    &session.db,
                    StructuralProjectionRequest::new(plan, lane),
                )
                .unwrap()
                .into_value_rows();
                assert_eq!(
                    rows.into_iter()
                        .map(|row| row.into_iter().map(OutputValue::from).collect::<Vec<_>>())
                        .collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }
}

fn historical_field_paths_filters_preserve_missing_and_terminal_null(fill: Option<Value>) {
    let old_numeric = matches!(&fill, Some(Value::Map(entries)) if entries[0].1 == Value::Nat64(5));
    let old_null_leaf = matches!(&fill, Some(Value::Map(entries)) if entries[0].1 == Value::Null);
    let session = historical_profiles(fill);
    for (condition, ids) in [
        (
            "profile.rank = 5",
            if old_numeric { vec![1, 2] } else { vec![] },
        ),
        (
            "profile.rank IS NULL",
            if old_null_leaf {
                vec![1, 2, 5]
            } else {
                vec![5]
            },
        ),
        ("profile.rank = 5 OR bucket = 7", vec![1, 2, 3, 4, 5]),
        (
            "NOT (profile.rank = 7)",
            if old_numeric { vec![1, 2] } else { vec![] },
        ),
    ] {
        let select = format!("SELECT id FROM PlannerRow WHERE ({condition})");
        assert_eq!(
            projection_rows(&session, &format!("{select} ORDER BY id")),
            ids.iter()
                .map(|id| vec![OutputValue::nat64(*id)])
                .collect::<Vec<_>>()
        );
        // Public reads retain an explicit structural key guard, while the SQL
        // scan above qualifies the expression-owned filter without that guard.
        for id in 1..=5 {
            assert_admitted_rows(
                &session,
                &select,
                FieldRef::new("id").eq(InputValue::nat64(id)),
                if ids.contains(&id) {
                    vec![vec![OutputValue::nat64(id)]]
                } else {
                    vec![]
                },
            );
        }
    }
}

#[test]
fn historical_field_paths_preserve_execution_limits() {
    historical_profiles(fills()[1].clone());
    for (sql, resource) in [
        (
            "SELECT profile.rank FROM PlannerRow WHERE id = 1",
            Resource::PredicateExpressionSteps,
        ),
        (
            "SELECT profile.rank, COUNT(*) FROM PlannerRow WHERE id = 1 GROUP BY profile.rank",
            Resource::DecodedBytes,
        ),
    ] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, 0),
        );
        let error = new_request_session(&root)
            .execute_trusted_sql_query(sql)
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert!(root.observed(resource) > 0);
    }
}

#[test]
fn historical_field_paths_reject_current_slot_absence_and_truncated_payloads() {
    let session = historical_profiles(None);
    let key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(1))
        .unwrap()
        .to_raw()
        .unwrap();
    let old = DATA_STORE.with(|store| store.borrow().get(&key).unwrap());
    for truncate in [false, true] {
        DATA_STORE.with(|store| {
            let mut store = store.borrow_mut();
            let payload = decode_row_payload_bytes(old.as_bytes())
                .unwrap()
                .into_payload();
            // Keep a valid current-format envelope. A current-layout row may
            // not borrow a historical fill to excuse its missing physical slot.
            let payload = if truncate {
                &payload[..payload.len() - 1]
            } else {
                payload
            };
            let bytes = serialize_row_payload(
                if truncate {
                    RowLayoutVersion::INITIAL
                } else {
                    RowLayoutVersion::INITIAL.checked_next().unwrap()
                },
                payload.len(),
                |out| {
                    out.extend_from_slice(payload);
                    Ok(())
                },
            )
            .unwrap();
            store.insert_raw_for_test(key.clone(), RawRow::try_new(bytes).unwrap());
        });
        for sql in [
            "SELECT profile.rank FROM PlannerRow WHERE id = 1",
            "SELECT profile.rank, COUNT(*) FROM PlannerRow WHERE id = 1 GROUP BY profile.rank",
        ] {
            let error = session.execute_trusted_sql_query(sql).unwrap_err();
            assert!(
                matches!(error, QueryError::Execute(error) if error.as_internal().class() == ErrorClass::Corruption)
            );
        }
    }
}

#[test]
fn historical_field_paths_null_fill_scalar_projection() {
    historical_field_paths_scalar_projection(fills()[0].clone());
}

#[test]
fn historical_field_paths_null_fill_single_path_group() {
    historical_field_paths_single_path_group(fills()[0].clone());
}

#[test]
fn historical_field_paths_null_fill_filters_preserve_missing_and_terminal_null() {
    historical_field_paths_filters_preserve_missing_and_terminal_null(fills()[0].clone());
}

#[test]
fn historical_field_paths_value_fill_scalar_projection() {
    historical_field_paths_scalar_projection(fills()[1].clone());
}

#[test]
fn historical_field_paths_value_fill_single_path_group() {
    historical_field_paths_single_path_group(fills()[1].clone());
}

#[test]
fn historical_field_paths_value_fill_filters_preserve_missing_and_terminal_null() {
    historical_field_paths_filters_preserve_missing_and_terminal_null(fills()[1].clone());
}

#[test]
fn historical_field_paths_null_leaf_fill_scalar_projection() {
    historical_field_paths_scalar_projection(fills()[2].clone());
}

#[test]
fn historical_field_paths_null_leaf_fill_single_path_group() {
    historical_field_paths_single_path_group(fills()[2].clone());
}

#[test]
fn historical_field_paths_null_leaf_fill_filters_preserve_missing_and_terminal_null() {
    historical_field_paths_filters_preserve_missing_and_terminal_null(fills()[2].clone());
}
