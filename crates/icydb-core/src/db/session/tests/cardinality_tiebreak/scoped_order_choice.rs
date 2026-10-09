//! Public scoped reads choose the accepted index that supplies their ordering.

use super::{materialized_sort_admission::summary, scalar_page_limits, *};
use crate::{
    db::{OrderTerm, RequestExecutionRoot, desc, query::preparation::with_preparation_work},
    types::Ulid,
};
use icydb_diagnostic_code::{DiagnosticDetail, QueryReadAdmissionCode};

fn initialize_scoped_serials(unique: bool) {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Ulid),
        field(2, "robot_id", 1, AcceptedFieldKind::Ulid),
        field(3, "location_id", 2, AcceptedFieldKind::Ulid),
        field(4, "serial_number", 3, AcceptedFieldKind::Nat64),
    ];
    let indexes = [("a_scope_idx", false, 2), ("z_serial_idx", unique, 3)]
        .into_iter()
        .enumerate()
        .map(|(offset, (name, unique, arity))| {
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::try_from(offset + 1).unwrap()).unwrap(),
                u16::try_from(offset + 1).unwrap(),
                name.into(),
                STORE_PATH.into(),
                unique,
                PersistedIndexKeySnapshot::FieldPath(
                    fields[1..=arity]
                        .iter()
                        .map(|field| {
                            PersistedIndexFieldPathSnapshot::new(
                                field.id(),
                                field.slot(),
                                vec![field.name().into()],
                                field.kind().clone(),
                                false,
                            )
                        })
                        .collect(),
                ),
                None,
            )
        })
        .collect();
    scalar_page_limits::initialize_payload_schema(fields, indexes);
}

fn scoped_filter() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("robot_id").eq(InputValue::ulid(Ulid::from_u128(100))))
        .filter(FieldRef::new("location_id").eq(InputValue::ulid(Ulid::from_u128(200))))
}

fn scoped_query(descending: bool) -> DynamicQuery {
    scoped_filter().order_by(if descending {
        desc("serial_number")
    } else {
        asc("serial_number")
    })
}

fn assert_order_choice(session: &DbSession<TestCanister>, descending: bool) {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = scoped_query(descending);
    let structural = with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
            catalog.accepted_schema_info(),
            query.filter_expr().unwrap(),
            work,
        )
    })
    .unwrap()
    .order_spec(OrderSpec {
        fields: query
            .order_terms()
            .iter()
            .cloned()
            .map(OrderTerm::lower)
            .collect(),
    })
    .limit(1);
    let prepared = session
        .structural_projection_prepared_plan_for_accepted_authority(
            &structural,
            catalog.accepted_entity_authority(),
            catalog.snapshot(),
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap()
        .0;
    // Execution caches omit explain-only candidates. Project them through the
    // same accepted visible-index owner used by verbose execution EXPLAIN.
    let visible = session
        .visible_indexes_for_store_accepted_schema(STORE_PATH, catalog.accepted_schema_info())
        .unwrap();
    let mut plan = prepared.logical_plan().clone();
    with_preparation_work(|work| {
        plan.finalize_access_choice_with_semantic_indexes_and_schema(
            visible.accepted_semantic_index_contracts(),
            catalog.accepted_schema_info(),
            work,
        )
    })
    .unwrap();
    let choice = &plan.access_choice;
    assert_eq!(choice.chosen_reason.code(), "order_compatible_preferred");
    let rejected = choice
        .rejected
        .iter()
        .find(|index| index.index_name() == "a_scope_idx")
        .unwrap();
    assert_eq!(rejected.reason_code(), "order_compatible_preferred");
}

fn scoped_rows(unique: bool) -> Vec<Vec<OutputValue>> {
    let mut rows = vec![
        (5, 100, 200, 0),
        (4, 100, 200, 7),
        (3, 100, 200, 16),
        (2, 100, 200, u64::MAX),
        (1, 101, 200, u64::MAX),
        (6, 100, 201, 9),
    ];
    if !unique {
        rows.push((7, 100, 200, 7));
    }
    for &(id, robot, location, serial) in &rows {
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    (
                        "id".into(),
                        DynamicWriteCell::Value(InputValue::ulid(Ulid::from_u128(id))),
                    ),
                    (
                        "robot_id".into(),
                        DynamicWriteCell::Value(InputValue::ulid(Ulid::from_u128(robot))),
                    ),
                    (
                        "location_id".into(),
                        DynamicWriteCell::Value(InputValue::ulid(Ulid::from_u128(location))),
                    ),
                    (
                        "serial_number".into(),
                        DynamicWriteCell::Value(InputValue::nat64(serial)),
                    ),
                ])],
            )
            .unwrap();
    }
    rows.retain(|(_, robot, location, _)| *robot == 100 && *location == 200);
    rows.sort_unstable_by_key(|(id, _, _, serial)| (*serial, *id));
    rows.into_iter()
        .map(|(id, robot, location, serial)| {
            vec![
                OutputValue::ulid(Ulid::from_u128(id)),
                OutputValue::ulid(Ulid::from_u128(robot)),
                OutputValue::ulid(Ulid::from_u128(location)),
                OutputValue::nat64(serial),
            ]
        })
        .collect()
}

#[test]
fn scoped_order_choice_preserves_public_top_one_and_resume_without_extra_predicates() {
    for unique in [true, false] {
        // Accepted startup authority is retained outside the live stores.
        // Each schema variant needs a complete isolated native fixture.
        std::thread::spawn(move || {
            initialize_scoped_serials(unique);
            let setup = new_request_session(&RequestExecutionRoot::__new_runtime_root());
            for descending in [false, true] {
                let query = scoped_query(descending).limit(1);
                for _ in 0..2 {
                    let page = setup.execute_public_live_page(&query, None).unwrap();
                    assert!(page.rows.is_empty());
                    assert!(page.continuation.is_none());
                    let facts = summary(&setup, &query);
                    assert_eq!(facts.selected_index(), Some("z_serial_idx"));
                    assert!(!facts.materialization().materialized_sort());
                    assert_order_choice(&setup, descending);
                }
            }
            let expected = scoped_rows(unique);
            for descending in [false, true] {
                let mut expected = expected.clone();
                if descending {
                    expected.reverse();
                }
                for _ in 0..2 {
                    let query = scoped_query(descending);
                    let top = setup
                        .execute_public_live_page(&query.clone().limit(1), None)
                        .unwrap();
                    assert_eq!(top.rows, expected[..1]);
                    assert!(top.continuation.is_none());
                    let trusted = setup
                        .execute_trusted_live_page(&query.clone().limit(1), None)
                        .unwrap();
                    assert_eq!(top.rows, trusted.rows);
                    assert_order_choice(&setup, descending);
                    let mut actual = Vec::new();
                    let mut cursor = None;
                    for _ in 0..8 {
                        let page = setup
                            .execute_public_live_page(&query, cursor.as_deref())
                            .unwrap();
                        actual.extend(page.rows);
                        cursor = page.continuation;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    assert!(cursor.is_none());
                    assert_eq!(actual, expected);
                }
            }
            for _ in 0..2 {
                let no_order = scoped_filter().limit(1);
                assert_eq!(
                    summary(&setup, &no_order).selected_index(),
                    Some("a_scope_idx")
                );
                assert!(setup.execute_public_live_page(&no_order, None).is_ok());
                let unsupported = scoped_query(false).order_by(desc("id")).limit(1);
                let error = setup
                    .execute_public_live_page(&unsupported, None)
                    .unwrap_err();
                assert_eq!(
                    error.diagnostic().detail(),
                    Some(&DiagnosticDetail::QueryReadAdmission {
                        reason: QueryReadAdmissionCode::SortRequiresMaterialization,
                    })
                );
                let whole_index = DynamicQuery::new(ENTITY_NAME)
                    .order_by(asc("robot_id"))
                    .order_by(asc("location_id"))
                    .order_by(asc("serial_number"))
                    .limit(1);
                let error = setup
                    .execute_public_live_page(&whole_index, None)
                    .unwrap_err();
                assert_eq!(
                    error.diagnostic().detail(),
                    Some(&DiagnosticDetail::QueryReadAdmission {
                        reason: QueryReadAdmissionCode::UnboundedFullScanRejected,
                    })
                );
            }
        })
        .join()
        .unwrap();
    }
}
