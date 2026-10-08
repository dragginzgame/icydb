//! Generic hash grouping reuses canonical owned keys across nonadjacent rows.

use super::*;
use crate::{
    db::{QueryError, count, sum},
    types::Decimal,
    value::Value,
};

fn initialize_group_schema(kind: AcceptedFieldKind) {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "category", 1, AcceptedFieldKind::Nat64),
            field(3, "operand", 2, kind),
            field(4, "label", 3, AcceptedFieldKind::Text { max_len: None }),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "category_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["category".into()],
                AcceptedFieldKind::Nat64,
                false,
            )]),
            None,
        )],
    );
}

fn assert_owned_group_results(values: &[(InputValue, OutputValue)]) {
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    for id in 1_u64..=5 {
        let key_index = usize::try_from((id - 1) % u64::try_from(values.len()).unwrap()).unwrap();
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(3)),
                    ),
                    (
                        "operand".into(),
                        DynamicWriteCell::Value(values[key_index].0.clone()),
                    ),
                    (
                        "label".into(),
                        DynamicWriteCell::Value(InputValue::text("row".into())),
                    ),
                ])],
            )
            .unwrap();
    }
    for multi_field in [false, true] {
        let mut base = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("category").eq(InputValue::nat64(3)))
            .group_by("operand")
            .aggregate(count())
            .grouped_limits(8, 64 * 1024);
        if multi_field {
            base = base.group_by("category");
        }
        // SUM selects the generic bundle; COUNT alone is the maintained
        // dedicated-fold control. An unrelated index admits the public lane.
        let generic = base.clone().aggregate(sum("id"));
        for _ in 0..2 {
            let counts = session
                .execute_trusted_dynamic_grouped_query(&base)
                .unwrap();
            let grouped = session
                .execute_trusted_dynamic_grouped_query(&generic)
                .unwrap();
            assert_eq!(counts.rows.len(), values.len());
            assert_eq!(grouped.rows.len(), values.len());
            for (count, generic) in counts.rows.iter().zip(&grouped.rows) {
                assert_eq!(count.group_key(), generic.group_key());
            }
            for (key_index, (_, output)) in values.iter().enumerate() {
                let mut key = vec![output.clone()];
                if multi_field {
                    key.push(OutputValue::nat64(3));
                }
                let ids = (1_u64..=5)
                    .filter(|id| {
                        usize::try_from((id - 1) % u64::try_from(values.len()).unwrap()).unwrap()
                            == key_index
                    })
                    .collect::<Vec<_>>();
                let count = OutputValue::nat64(u64::try_from(ids.len()).unwrap());
                let sum = OutputValue::decimal(Decimal::from(ids.into_iter().sum::<u64>()));
                assert_eq!(
                    counts
                        .rows
                        .iter()
                        .find(|row| row.group_key() == key)
                        .unwrap()
                        .aggregate_values(),
                    std::slice::from_ref(&count)
                );
                assert_eq!(
                    grouped
                        .rows
                        .iter()
                        .find(|row| row.group_key() == key)
                        .unwrap()
                        .aggregate_values(),
                    &[count, sum]
                );
            }
            // Limits count distinct groups, rather than the five input rows.
            let tight = generic
                .clone()
                .grouped_limits(u32::try_from(values.len()).unwrap(), 64 * 1024);
            assert_eq!(
                session
                    .execute_trusted_dynamic_grouped_query(&tight)
                    .unwrap()
                    .rows,
                grouped.rows
            );
            assert_eq!(
                session
                    .execute_public_dynamic_grouped_query(&tight)
                    .unwrap()
                    .rows,
                grouped.rows
            );
        }
    }
}

fn assert_group_kind(kind: AcceptedFieldKind, values: Vec<Value>) {
    initialize_group_schema(kind);
    let values = values
        .into_iter()
        .map(|value| (InputValue::from(value.clone()), OutputValue::from(value)))
        .collect::<Vec<_>>();
    assert_owned_group_results(&values);
}

#[test]
fn owned_group_unit_reuses_one_group() {
    assert_group_kind(AcceptedFieldKind::Unit, vec![Value::Unit]);
}

#[test]
fn owned_group_lists_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        [7, 9]
            .map(|key| Value::List(vec![Value::Nat64(key)]))
            .to_vec(),
    );
}

#[test]
fn owned_group_sets_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
        [7, 9]
            .map(|key| Value::List(vec![Value::Nat64(key)]))
            .to_vec(),
    );
}

#[test]
fn owned_group_maps_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat64),
        },
        [7, 9]
            .map(|key| Value::Map(vec![(Value::Text("value".into()), Value::Nat64(key))]))
            .to_vec(),
    );
}

#[test]
fn owned_group_enum_uses_accepted_catalog_identity() {
    use crate::{
        db::schema::{TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests},
        value::{PublicEnumValue, PublicValue},
    };
    const ENUM_PATH: &str = "db::session::tests::cardinality_tiebreak::GroupStatus";
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        ENUM_PATH,
        vec![
            TestEnumVariant::unit("Paid"),
            TestEnumVariant::unit("Pending"),
        ],
    )])
    .unwrap();
    let kind = AcceptedFieldKind::Enum {
        type_id: enums.type_id(ENUM_PATH).unwrap(),
    };
    hybrid_components::initialize_component_schema(kind, enums);
    let values = ["Paid", "Pending"].map(|name| {
        (
            InputValue::enum_value(name, Some(ENUM_PATH)),
            OutputValue::from_public(PublicValue::Enum(PublicEnumValue::new(
                name,
                Some(ENUM_PATH),
            ))),
        )
    });
    assert_owned_group_results(&values);
}

#[test]
fn owned_group_change_preserves_borrowed_scalar_groups() {
    assert_group_kind(
        AcceptedFieldKind::Text { max_len: None },
        ["first", "second"]
            .map(|key| Value::Text(key.into()))
            .to_vec(),
    );
}

// Interleave groups and vary a second key so expected order cannot come from
// insertion order or from reversing only the first component's buckets.
const ORDER_ROWS: [(usize, &str); 8] = [
    (2, "b"),
    (0, "b"),
    (1, "a"),
    (0, "a"),
    (2, "a"),
    (1, "b"),
    (0, "a"),
    (2, "b"),
];

fn seed_order_groups(kind: AcceptedFieldKind, values: &[Value]) {
    initialize_group_schema(kind);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for (index, (key, label)) in ORDER_ROWS.iter().enumerate() {
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    (
                        "id".into(),
                        DynamicWriteCell::Value(InputValue::nat64(
                            u64::try_from(index + 1).unwrap(),
                        )),
                    ),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(3)),
                    ),
                    (
                        "operand".into(),
                        DynamicWriteCell::Value(InputValue::from(values[*key].clone())),
                    ),
                    (
                        "label".into(),
                        DynamicWriteCell::Value(InputValue::text((*label).into())),
                    ),
                ])],
            )
            .unwrap();
    }
}

fn expected_order_groups(
    values: &[Value],
    compound: bool,
    descending: bool,
) -> Vec<(Vec<OutputValue>, Vec<OutputValue>)> {
    // Ordinal fixture keys supply an independent lexicographic reference;
    // this oracle does not call the product's grouped ordering comparator.
    let mut groups = BTreeMap::<(usize, &str), Vec<u64>>::new();
    for (index, (key, label)) in ORDER_ROWS.iter().enumerate() {
        groups
            .entry((*key, if compound { label } else { "" }))
            .or_default()
            .push(u64::try_from(index + 1).unwrap());
    }
    let mut rows = groups
        .into_iter()
        .map(|((key, label), ids)| {
            let mut key = vec![OutputValue::from(values[key].clone())];
            if compound {
                key.push(OutputValue::text(label.into()));
            }
            (
                key,
                vec![
                    OutputValue::nat64(u64::try_from(ids.len()).unwrap()),
                    OutputValue::decimal(Decimal::from(ids.into_iter().sum::<u64>())),
                ],
            )
        })
        .collect::<Vec<_>>();
    if descending {
        rows.reverse();
    }
    rows
}

fn assert_group_order_matrix(kind: AcceptedFieldKind, values: Vec<Value>) {
    use crate::db::desc;
    seed_order_groups(kind, &values);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for compound in [false, true] {
        for descending in [false, true] {
            let expected = expected_order_groups(&values, compound, descending);
            let mut base = DynamicQuery::new(ENTITY_NAME)
                .filter(FieldRef::new("category").eq(InputValue::nat64(3)))
                .group_by("operand")
                .order_by(if descending {
                    desc("operand")
                } else {
                    asc("operand")
                })
                .grouped_limits(8, 64 * 1024);
            if compound {
                base = base.group_by("label").order_by(if descending {
                    desc("label")
                } else {
                    asc("label")
                });
            }
            for aggregate_case in 0..3 {
                let query = match aggregate_case {
                    0 => base.clone().aggregate(count()),
                    1 => base.clone().aggregate(sum("id")),
                    _ => base.clone().aggregate(count()).aggregate(sum("id")),
                };
                for limit in [None, Some(2), Some(20)] {
                    let query =
                        limit.map_or_else(|| query.clone(), |limit| query.clone().limit(limit));
                    for warm in [false, true] {
                        let mut next = Some(query.clone());
                        let mut observed = Vec::new();
                        let mut pages = 0;
                        while let Some(current) = next.take() {
                            let page = session
                                .execute_public_dynamic_grouped_query(&current)
                                .unwrap();
                            assert!(limit.is_none_or(
                                |limit| page.rows.len() <= usize::try_from(limit).unwrap()
                            ));
                            observed.extend(page.rows.iter().map(|row| {
                                (row.group_key().to_vec(), row.aggregate_values().to_vec())
                            }));
                            next = page.next_cursor.map(|cursor| query.clone().cursor(cursor));
                            pages += 1;
                            assert!(
                                pages <= expected.len() + 1,
                                "continuation must make progress"
                            );
                        }
                        let expected = expected
                            .iter()
                            .map(|(key, aggregates)| {
                                (
                                    key.clone(),
                                    match aggregate_case {
                                        0 => aggregates[..1].to_vec(),
                                        1 => aggregates[1..].to_vec(),
                                        _ => aggregates.clone(),
                                    },
                                )
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(
                            observed, expected,
                            "compound={compound} descending={descending} aggregate_case={aggregate_case} limit={limit:?} warm={warm}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn owned_group_order_scalar_matrix() {
    assert_group_order_matrix(
        AcceptedFieldKind::Text { max_len: None },
        ["eng", "ops", "sales"]
            .map(|key| Value::Text(key.into()))
            .to_vec(),
    );
}

#[test]
fn owned_group_order_signed_matrix() {
    assert_group_order_matrix(
        AcceptedFieldKind::Int128,
        [-7, 0, 9].map(Value::Int128).to_vec(),
    );
}

fn assert_canonical_order_rejection(error: QueryError) {
    use crate::db::query::plan::validate::{GroupPlanError, PlanErrorKind, PlanPolicyError};
    let QueryError::Plan(error) = error else {
        panic!("expected grouped planning rejection");
    };
    let PlanErrorKind::Policy(error) = error.into_kind() else {
        panic!("expected grouped order policy rejection");
    };
    let PlanPolicyError::Group(error) = *error else {
        panic!("expected grouped order error");
    };
    assert_eq!(*error, GroupPlanError::OrderPrefixNotAlignedWithGroupKeys);
}

#[test]
fn owned_group_order_rejects_mixed_canonical_directions() {
    assert_mixed_group_direction_rejection(
        AcceptedFieldKind::Text { max_len: None },
        ["eng", "ops", "sales"]
            .map(|key| Value::Text(key.into()))
            .to_vec(),
    );
}

#[test]
fn owned_group_order_rejects_signed_mixed_canonical_directions() {
    assert_mixed_group_direction_rejection(
        AcceptedFieldKind::Int128,
        [-7, 0, 9].map(Value::Int128).to_vec(),
    );
}

fn assert_mixed_group_direction_rejection(kind: AcceptedFieldKind, values: Vec<Value>) {
    use crate::db::desc;
    seed_order_groups(kind, &values);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for descending in [false, true] {
        let base = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("category").eq(InputValue::nat64(3)))
            .group_by("operand")
            .group_by("label")
            .order_by(if descending {
                desc("operand")
            } else {
                asc("operand")
            })
            .order_by(if descending {
                asc("label")
            } else {
                desc("label")
            })
            .grouped_limits(8, 64 * 1024);
        for aggregates in 0..3 {
            let query = match aggregates {
                0 => base.clone().aggregate(count()),
                1 => base.clone().aggregate(sum("id")),
                _ => base.clone().aggregate(count()).aggregate(sum("id")),
            };
            for limit in [None, Some(2), Some(20)] {
                let query = limit.map_or_else(|| query.clone(), |limit| query.clone().limit(limit));
                session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
                for _ in 0..2 {
                    assert_canonical_order_rejection(
                        session
                            .execute_public_dynamic_grouped_query(&query)
                            .expect_err("mixed group-key directions must reject before execution"),
                    );
                    assert_canonical_order_rejection(
                        session
                            .execute_trusted_dynamic_grouped_query(&query)
                            .expect_err("trusted execution retains grouped order admission"),
                    );
                }
            }
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn owned_group_order_sql_rejects_mixed_keys_and_preserves_top_k() {
    let values = ["eng", "ops", "sales"].map(|key| Value::Text(key.into()));
    seed_order_groups(AcceptedFieldKind::Text { max_len: None }, &values);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for order in ["operand ASC, label DESC", "operand DESC, label ASC"] {
        for aggregate in ["COUNT(*)", "SUM(id)", "COUNT(*), SUM(id)"] {
            for window in ["", " LIMIT 2", " LIMIT 20 OFFSET 1"] {
                let sql = format!(
                    "SELECT operand, label, {aggregate} FROM PlannerRow WHERE category = 3 GROUP BY operand, label ORDER BY {order}{window}"
                );
                for _ in 0..2 {
                    assert_canonical_order_rejection(
                        session.execute_trusted_sql_query(&sql).unwrap_err(),
                    );
                }
            }
        }
    }
    // An aggregate term selects the existing per-term, non-resumable Top-K lane.
    // Its mixed key directions are valid and must not inherit canonical rejection.
    for order in [
        "operand ASC, label DESC, COUNT(*) DESC",
        "operand DESC, label ASC, COUNT(*) ASC",
    ] {
        let descending = order.starts_with("operand DESC");
        let mut counts = BTreeMap::<(usize, &str), u64>::new();
        for (key, label) in ORDER_ROWS {
            *counts.entry((key, label)).or_default() += 1;
        }
        let mut expected = counts.into_iter().collect::<Vec<_>>();
        expected.sort_by(
            |((left_key, left_label), _), ((right_key, right_label), _)| {
                let cmp = left_key.cmp(right_key);
                let cmp = if descending { cmp.reverse() } else { cmp };
                cmp.then_with(|| {
                    let cmp = left_label.cmp(right_label);
                    if descending { cmp } else { cmp.reverse() }
                })
            },
        );
        expected.truncate(3);
        let sql = format!(
            "SELECT operand, label, COUNT(*) FROM PlannerRow WHERE category = 3 GROUP BY operand, label ORDER BY {order} LIMIT 3"
        );
        for _ in 0..2 {
            let SqlStatementResult::Grouped {
                rows, next_cursor, ..
            } = session.execute_trusted_sql_query(&sql).unwrap()
            else {
                panic!("expected grouped Top-K rows");
            };
            assert!(next_cursor.is_none());
            assert_eq!(
                rows.iter()
                    .map(|row| (row.group_key().to_vec(), row.aggregate_values().to_vec()))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|((key, label), count)| (
                        vec![
                            OutputValue::from(values[*key].clone()),
                            OutputValue::text((*label).into())
                        ],
                        vec![OutputValue::nat64(*count)]
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn owned_group_order_keeps_sort_budget_errors_typed() {
    use crate::db::{desc, test_support::request_with_limit};
    use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};
    let values = ["eng", "ops", "sales"].map(|key| Value::Text(key.into()));
    seed_order_groups(AcceptedFieldKind::Text { max_len: None }, &values);
    for order in [asc("operand"), desc("operand")] {
        let base = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("category").eq(InputValue::nat64(3)))
            .group_by("operand")
            .aggregate(count())
            .order_by(order)
            .grouped_limits(8, 64 * 1024);
        // COUNT alone selects the dedicated fold; SUM forces the generic fold.
        // Both must enforce the same maintained sort resources on every window.
        for generic in [false, true] {
            let query = if generic {
                base.clone().aggregate(sum("id"))
            } else {
                base.clone()
            };
            for limit in [None, Some(1), Some(20)] {
                let query = limit.map_or_else(|| query.clone(), |limit| query.clone().limit(limit));
                for resource in [
                    Resource::SortEntries,
                    Resource::SortComparisons,
                    Resource::SortTemporaryBytes,
                ] {
                    for trusted in [false, true] {
                        let root = request_with_limit(resource, 0);
                        let session = new_request_session(&root);
                        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
                        for _ in 0..2 {
                            let error = if trusted {
                                session.execute_trusted_dynamic_grouped_query(&query)
                            } else {
                                session.execute_public_dynamic_grouped_query(&query)
                            }
                            .expect_err("grouped sorting must honor the request budget");
                            assert!(
                                error
                                    .diagnostic_facts()
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            assert!(root.observed(resource) > 0);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn owned_group_order_having_offset_and_projection() {
    let values = ["eng", "ops", "sales"].map(|key| Value::Text(key.into()));
    seed_order_groups(AcceptedFieldKind::Text { max_len: None }, &values);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    for compound in [false, true] {
        for descending in [false, true] {
            let fields = if compound {
                "operand, label"
            } else {
                "operand"
            };
            let order = if compound {
                if descending {
                    "operand DESC, label DESC"
                } else {
                    "operand ASC, label ASC"
                }
            } else if descending {
                "operand DESC"
            } else {
                "operand ASC"
            };
            let expected = expected_order_groups(&values, compound, descending)
                .into_iter()
                .filter(|(_, aggregates)| {
                    aggregates[0] == OutputValue::nat64(3) || aggregates[0] == OutputValue::nat64(2)
                })
                .collect::<Vec<_>>();
            for offset in [0, 1, 9] {
                for (window, limit) in [("", usize::MAX), (" LIMIT 1", 1), (" LIMIT 20", 20)] {
                    for generic in [false, true] {
                        let aggregates = if generic {
                            "COUNT(*), SUM(id)"
                        } else {
                            "COUNT(*)"
                        };
                        let sql = format!(
                            "SELECT {fields}, {aggregates} FROM PlannerRow WHERE category = 3 GROUP BY {fields} HAVING COUNT(*) >= 2 ORDER BY {order}{window} OFFSET {offset}"
                        );
                        let expected = expected
                            .iter()
                            .skip(offset)
                            .take(limit)
                            .map(|(key, aggregates)| {
                                (
                                    key.clone(),
                                    if generic {
                                        aggregates.clone()
                                    } else {
                                        aggregates[..1].to_vec()
                                    },
                                )
                            })
                            .collect::<Vec<_>>();
                        for _ in 0..2 {
                            let SqlStatementResult::Grouped { rows, .. } =
                                session.execute_trusted_sql_query(&sql).unwrap()
                            else {
                                panic!("expected grouped projection");
                            };
                            let observed = rows
                                .iter()
                                .map(|row| {
                                    (row.group_key().to_vec(), row.aggregate_values().to_vec())
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(observed, expected, "{sql}");
                        }
                    }
                }
            }
        }
    }
}
