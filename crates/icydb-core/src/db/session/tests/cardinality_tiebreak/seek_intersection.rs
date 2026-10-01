//! Live accepted-catalog intersections and engine-issued small-page suffixes.

use super::*;
use crate::db::{RequestExecutionRoot, desc};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

fn initialize_intersection(case: u8, signed: bool) {
    let kind = if signed {
        AcceptedFieldKind::Int32
    } else {
        AcceptedFieldKind::Nat64
    };
    let fields = ["id", "lane_a", "lane_b", "group_key", "sort_key"]
        .into_iter()
        .zip(0_u16..)
        .map(|(name, slot)| field(u32::from(slot) + 1, name, slot, kind.clone()))
        .collect();
    let indexes = ["lane_a", "lane_b", "group_key"]
        .into_iter()
        .zip(1_u16..)
        .map(|(name, ordinal)| {
            let paths = [(u32::from(ordinal) + 1, ordinal, name), (1, 0, "id")]
                .into_iter()
                .map(|(id, slot, name)| {
                    PersistedIndexFieldPathSnapshot::new(
                        FieldId::new(id),
                        SchemaFieldSlot::new(slot),
                        vec![name.into()],
                        kind.clone(),
                        false,
                    )
                })
                .collect();
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::from(ordinal)).unwrap(),
                ordinal,
                format!("{name}_idx"),
                STORE_PATH.into(),
                false,
                PersistedIndexKeySnapshot::FieldPath(paths),
                None,
            )
        })
        .collect();
    crate::db::session::tests::cardinality_tiebreak::scalar_page_limits::initialize_payload_schema(
        fields, indexes,
    );
    for start in (0..160).step_by(8) {
        let rows = (start..start + 8)
            .map(|id| {
                let (a, b, c) = match case {
                    0 => (id < 128, id < 128 && id % 8 == 7, id < 128 && id % 16 == 15),
                    1 => (id < 128, (128..144).contains(&id), (128..136).contains(&id)),
                    2 => (id < 128, (112..128).contains(&id), (120..128).contains(&id)),
                    3 => (id < 128, id < 128, id < 128),
                    _ => ((112..128).contains(&id), (120..128).contains(&id), id < 128),
                };
                DynamicStructuralPatch::new(
                    [
                        ("id", id),
                        ("lane_a", u64::from(!a)),
                        ("lane_b", u64::from(!b)),
                        ("group_key", u64::from(!c)),
                        ("sort_key", id % 2),
                    ]
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name.into(),
                            DynamicWriteCell::Value(if signed {
                                InputValue::int64(i64::try_from(value).unwrap())
                            } else {
                                InputValue::nat64(value)
                            }),
                        )
                    })
                    .collect(),
                )
            })
            .collect();
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, rows)
            .unwrap();
    }
}

fn expected(
    case: u8,
    children: u8,
    descending: bool,
    residual: bool,
    limit: Option<u32>,
    signed: bool,
) -> Vec<Vec<OutputValue>> {
    let mut ids: Vec<i64> = match (case, children) {
        (0, 2) => (7..128).step_by(8).collect(),
        (0, 3) => (15..128).step_by(16).collect(),
        (1, _) => Vec::new(),
        (2, 2) => (112..128).collect(),
        (2, 3) | (4, _) => (120..128).collect(),
        (3, _) => (0..128).collect(),
        _ => panic!("unknown workload"),
    };
    if residual {
        ids.retain(|id| id % 2 == 1);
    }
    if descending {
        ids.reverse();
    }
    if let Some(limit) = limit {
        ids.truncate(limit as usize);
    }
    ids.into_iter()
        .map(|id| {
            vec![if signed {
                OutputValue::int64(id)
            } else {
                OutputValue::nat64(u64::try_from(id).unwrap())
            }]
        })
        .collect()
}

fn collect_pages(
    query: &DynamicQuery,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..256 {
        let root = RequestExecutionRoot::__new_runtime_root();
        let page = new_request_session(&root)
            .execute_trusted_live_page(query, cursor.as_deref())
            .unwrap();
        assert_eq!(page.row_count as usize, page.rows.len());
        assert_eq!(page.work.result_rows, page.row_count);
        assert!(root.observed(Resource::KeyIndexEntriesVisited) >= page.work.entries_visited);
        rows.extend(page.rows);
        if page.continuation.is_some() {
            assert_ne!(page.continuation, cursor);
        }
        cursor = page.continuation;
        if let Some(token) = &cursor {
            tokens.push((token.clone(), rows.len()));
        } else {
            break;
        }
    }
    assert!(cursor.is_none(), "all pages must terminate");
    (rows, tokens)
}

fn assert_planned_access(query: &DynamicQuery, case: u8, children: u8, descending: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let structural = crate::db::query::preparation::with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
            catalog.accepted_schema_info(),
            query.filter_expr().unwrap(),
            work,
        )
    })
    .unwrap()
    .select_fields(["id"])
    .order_spec(OrderSpec {
        fields: vec![if descending {
            desc("id").lower()
        } else {
            asc("id").lower()
        }],
    });
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &structural,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    let crate::db::access::AccessPlan::Intersection(selected) = &prepared.logical_plan().access
    else {
        panic!("live workload must select an intersection");
    };
    assert_eq!(selected.len(), usize::from(children));
    // A logical intersection alone does not prove that the bounded probe ran.
    // These unequal-prefix populations must charge the complete cursorless
    // prefix inspection, even when the authored limit returns only one row.
    let expected_probe_entries = match (case, children) {
        (0..=2, 2) => Some(144),
        (4, 2) => Some(24),
        (4, 3) => Some(152),
        _ => None,
    };
    let page = session.execute_trusted_live_page(query, None).unwrap();
    if case == 1 && children == 2 {
        assert!(
            page.continuation.is_none(),
            "empty cursorless probe is exhausted"
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
    if let Some(entries) = expected_probe_entries {
        assert!(root.observed(Resource::KeyIndexEntriesVisited) >= entries);
    }
}

fn qualify_live_pages(signed: bool) {
    for case in 0..=4 {
        initialize_intersection(case, signed);
        for children in [2_u8, 3] {
            for descending in [false, true] {
                for (residual, limit) in [
                    (false, None),
                    (true, None),
                    (false, Some(1)),
                    (true, Some(5)),
                ] {
                    let mut filters = vec![
                        FieldRef::new("lane_a").eq(InputValue::int64(0)),
                        FieldRef::new("lane_b").eq(InputValue::int64(0)),
                    ];
                    if children == 3 {
                        filters.push(FieldRef::new("group_key").eq(InputValue::int64(0)));
                    }
                    if residual {
                        filters.push(FieldRef::new("sort_key").eq(InputValue::int64(1)));
                    }
                    let mut query = DynamicQuery::new(ENTITY_NAME)
                        .select(["id"])
                        .filter(FilterExpr::and(filters))
                        .order_by(if descending { desc("id") } else { asc("id") });
                    if let Some(limit) = limit {
                        query = query.limit(limit);
                    }
                    assert_planned_access(&query, case, children, descending);
                    let expected = expected(case, children, descending, residual, limit, signed);
                    let (rows, tokens) = collect_pages(&query, None);
                    assert_eq!(
                        rows, expected,
                        "case={case} children={children} desc={descending} residual={residual} limit={limit:?}"
                    );
                    for (token, offset) in tokens {
                        assert_eq!(collect_pages(&query, Some(token)).0, expected[offset..]);
                    }
                }
            }
        }
    }
}

// Keep each accepted schema in its own test database incarnation rather than
// replacing primitive kinds behind one live registry's retained authority.
#[test]
fn unsigned_live_intersections_preserve_order_limits_residuals_and_every_resume_suffix() {
    qualify_live_pages(false);
}

#[test]
fn signed_dynamic_filters_preserve_order_limits_residuals_and_every_resume_suffix() {
    qualify_live_pages(true);
}

// Each accepted primitive kind gets a separate database incarnation. The
// maintained dynamic frontend must expose the same exact index contract for
// every signed width, including values near its persisted boundaries.
fn qualify_signed_lookups(kind: AcceptedFieldKind, values: &[i64]) {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "value", 1, kind.clone()),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "value_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["value".into()],
                kind,
                false,
            )]),
            None,
        )],
    );
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            values
                .iter()
                .enumerate()
                .map(|(id, value)| {
                    DynamicStructuralPatch::new(vec![
                        (
                            "id".into(),
                            DynamicWriteCell::Value(InputValue::nat64(id as u64)),
                        ),
                        (
                            "value".into(),
                            DynamicWriteCell::Value(InputValue::int64(*value)),
                        ),
                    ])
                })
                .collect(),
        )
        .unwrap();
    for descending in [false, true] {
        for (filter, selected, membership) in [
            (
                FieldRef::new("value").eq(InputValue::int64(values[0])),
                vec![0],
                false,
            ),
            (
                FieldRef::new("value").eq(InputValue::int64(values[values.len() - 1])),
                vec![values.len() - 1],
                false,
            ),
            (
                FieldRef::new("value").in_list([
                    InputValue::int64(values[0]),
                    InputValue::int64(values[values.len() - 1]),
                    InputValue::int64(values[0]),
                ]),
                vec![0, values.len() - 1],
                true,
            ),
            (
                FilterExpr::or(vec![
                    FieldRef::new("value").eq(InputValue::int64(values[0])),
                    FieldRef::new("value").eq(InputValue::int64(values[values.len() - 1])),
                ]),
                vec![0, values.len() - 1],
                true,
            ),
        ] {
            assert_signed_scalar_access(&session, &filter, descending, membership);
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["id"])
                .filter(filter)
                .order_by(if descending { desc("id") } else { asc("id") });
            let mut selected = selected;
            if descending {
                selected.reverse();
            }
            let expected = selected
                .into_iter()
                .map(|id| vec![OutputValue::nat64(id as u64)])
                .collect::<Vec<_>>();
            let (rows, tokens) = collect_pages(&query, None);
            assert_eq!(rows, expected);
            for (token, offset) in tokens {
                assert_eq!(collect_pages(&query, Some(token)).0, expected[offset..]);
            }
        }
    }
}

fn assert_signed_scalar_access(
    session: &DbSession<TestCanister>,
    filter: &FilterExpr,
    descending: bool,
    membership: bool,
) {
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let structural = crate::db::query::preparation::with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
            catalog.accepted_schema_info(),
            filter,
            work,
        )
    })
    .unwrap()
    .select_fields(["id"])
    .order_spec(OrderSpec {
        fields: vec![if descending {
            desc("id").lower()
        } else {
            asc("id").lower()
        }],
    });
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &structural,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    let path = prepared.logical_plan().access.as_path().unwrap();
    if membership {
        assert!(matches!(
            path,
            crate::db::access::AccessPath::IndexMultiLookup { .. }
        ));
    } else {
        assert!(matches!(
            path,
            crate::db::access::AccessPath::IndexPrefix { .. }
        ));
    }
}

#[test]
fn signed_int8_secondary_equality_and_membership_use_exact_indexes() {
    qualify_signed_lookups(AcceptedFieldKind::Int8, &[-128, -1, 0, 1, 127]);
}

#[test]
fn signed_int16_secondary_equality_and_membership_use_exact_indexes() {
    qualify_signed_lookups(AcceptedFieldKind::Int16, &[-32768, -1, 0, 1, 32767]);
}

#[test]
fn signed_int32_secondary_equality_and_membership_use_exact_indexes() {
    qualify_signed_lookups(
        AcceptedFieldKind::Int32,
        &[i64::from(i32::MIN), -1, 0, 1, i64::from(i32::MAX)],
    );
}

#[test]
fn signed_int64_secondary_equality_and_membership_use_exact_indexes() {
    qualify_signed_lookups(AcceptedFieldKind::Int64, &[i64::MIN, -1, 0, 1, i64::MAX]);
}
