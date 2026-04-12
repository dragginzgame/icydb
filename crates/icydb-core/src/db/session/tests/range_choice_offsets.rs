use super::*;

// Expected ordered-route shape for one range-ordered window shape.
enum RangeOrderedRouteExpectation {
    TopNSeekAccessSatisfied,
    MaterializedSort,
}

// Expected EXPLAIN route properties for one index-range ordered window shape.
struct RangeRouteExpectations<'a> {
    access_name: &'a str,
    ordered_route: RangeOrderedRouteExpectation,
}

// Build the shared bounded range filter once so the individual cases differ
// only on direction and optional offset.
fn deterministic_range_choice_predicate() -> Predicate {
    Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Gt,
            Value::Uint(10),
            CoercionId::Strict,
        )),
    ])
}

// Build one deterministic range-choice execution descriptor for the requested
// direction and optional offset window.
fn deterministic_range_choice_descriptor(
    session: &DbSession<SessionSqlCanister>,
    descending: bool,
    offset: Option<u32>,
) -> ExplainExecutionNodeDescriptor {
    let mut load = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(deterministic_range_choice_predicate());
    load = if descending {
        load.order_by_desc("score")
            .order_by_desc("label")
            .order_by_desc("id")
    } else {
        load.order_by("score").order_by("label").order_by("id")
    };
    if let Some(offset) = offset {
        load = load.offset(offset);
    }

    load.limit(2)
        .explain_execution()
        .expect("session deterministic range explain_execution should build")
}

// Build one fallback order-only execution descriptor for either the scalar or
// composite route family.
fn order_only_fallback_descriptor(
    session: &DbSession<SessionSqlCanister>,
    composite: bool,
    descending: bool,
    offset: Option<u32>,
) -> ExplainExecutionNodeDescriptor {
    if composite {
        let mut load = session.load::<SessionDeterministicChoiceEntity>();
        load = if descending {
            load.order_by_desc("tier")
                .order_by_desc("handle")
                .order_by_desc("id")
        } else {
            load.order_by("tier").order_by("handle").order_by("id")
        };
        if let Some(offset) = offset {
            load = load.offset(offset);
        }

        return load
            .limit(2)
            .explain_execution()
            .expect("session deterministic composite order-only explain_execution should build");
    }

    let mut load = session.load::<SessionOrderOnlyChoiceEntity>();
    load = if descending {
        load.order_by_desc("alpha").order_by_desc("id")
    } else {
        load.order_by("alpha").order_by("id")
    };
    if let Some(offset) = offset {
        load = load.offset(offset);
    }

    load.limit(2)
        .explain_execution()
        .expect("session deterministic order-only explain_execution should build")
}

// Assert the shared EXPLAIN contract for index-range ordered windows while
// letting each case override only the route properties that differ.
fn assert_range_route_descriptor(
    descriptor: &ExplainExecutionNodeDescriptor,
    expectations: RangeRouteExpectations<'_>,
    context: &str,
) {
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should stay on the chosen index-range route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == expectations.access_name)
        ),
        "{context} should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "{context} should derive bounded index-range limit pushdown",
    );
    match expectations.ordered_route {
        RangeOrderedRouteExpectation::TopNSeekAccessSatisfied => {
            assert!(
                explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek)
                    .is_some(),
                "{context} should expose bounded Top-N seek routing",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByAccessSatisfied
                )
                .is_some(),
                "{context} should keep access-satisfied ordering",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByMaterializedSort
                )
                .is_none(),
                "{context} should stay off the materialized-sort fallback",
            );
        }
        RangeOrderedRouteExpectation::MaterializedSort => {
            assert!(
                explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek)
                    .is_none(),
                "{context} should stay off the bounded Top-N seek route",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByAccessSatisfied
                )
                .is_none(),
                "{context} should stay off the access-satisfied ordering route",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByMaterializedSort
                )
                .is_some(),
                "{context} should expose the materialized-sort fallback",
            );
        }
    }
}

#[test]
fn session_explain_execution_range_choice_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: keep the bounded range-choice family under one matrix so the
    // route stays visibly materialized across direction and optional offset.
    let cases = [
        (
            false,
            None,
            RangeRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: RangeOrderedRouteExpectation::MaterializedSort,
            },
            "range-choice roots",
        ),
        (
            true,
            None,
            RangeRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: RangeOrderedRouteExpectation::MaterializedSort,
            },
            "descending range-choice roots",
        ),
        (
            false,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: RangeOrderedRouteExpectation::MaterializedSort,
            },
            "range-choice offset roots",
        ),
        (
            true,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: RangeOrderedRouteExpectation::MaterializedSort,
            },
            "descending range-choice offset roots",
        ),
    ];

    // Phase 2: reuse the shared assertion across all range-choice variants.
    for (descending, offset, expectations, context) in cases {
        let descriptor = deterministic_range_choice_descriptor(&session, descending, offset);
        assert_range_route_descriptor(&descriptor, expectations, context);
    }
}

#[test]
fn session_execute_order_only_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_order_only_choice_session_entities(
        &session,
        &[
            (9_971, "delta", "alpha"),
            (9_972, "alpha", "echo"),
            (9_973, "bravo", "delta"),
            (9_974, "foxtrot", "golf"),
            (9_975, "charlie", "charlie"),
            (9_976, "hotel", "india"),
        ],
    );

    let asc = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("ascending order-only offset window should execute");
    let asc_alpha = asc
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();
    let asc_paged = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute_paged()
        .expect("ascending order-only offset paged window should execute");
    let asc_paged_alpha = asc_paged
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();

    let desc = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by_desc("alpha")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("descending order-only offset window should execute");
    let desc_alpha = desc
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_paged_alpha,
        vec!["bravo", "charlie"],
        "order-only paged windows should preserve the same shifted fallback index order",
    );
    assert_eq!(
        asc_alpha,
        vec!["bravo", "charlie"],
        "ascending order-only offset windows should preserve the shifted fallback index order",
    );
    assert_eq!(
        desc_alpha,
        vec!["foxtrot", "delta"],
        "descending order-only offset windows should preserve the reversed shifted fallback index order",
    );
}

#[test]
fn session_explain_execution_order_only_choice_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: keep the scalar and composite fallback families explicit while
    // removing the repetitive one-wrapper-per-variant shape.
    let cases = [
        (
            false,
            false,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_alpha_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "order-only offset roots",
        ),
        (
            false,
            true,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_alpha_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "descending order-only offset roots",
        ),
        (
            true,
            false,
            None,
            RangeRouteExpectations {
                access_name: "z_tier_handle_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "composite order-only roots",
        ),
        (
            true,
            true,
            None,
            RangeRouteExpectations {
                access_name: "z_tier_handle_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "descending composite order-only roots",
        ),
        (
            true,
            false,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_tier_handle_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "composite order-only offset roots",
        ),
        (
            true,
            true,
            Some(1),
            RangeRouteExpectations {
                access_name: "z_tier_handle_idx",
                ordered_route: RangeOrderedRouteExpectation::TopNSeekAccessSatisfied,
            },
            "descending composite order-only offset roots",
        ),
    ];

    // Phase 2: reuse the shared assertion across every fallback variant.
    for (composite, descending, offset, expectations, context) in cases {
        let descriptor = order_only_fallback_descriptor(&session, composite, descending, offset);
        assert_range_route_descriptor(&descriptor, expectations, context);
    }
}

#[test]
fn session_execute_composite_order_only_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one minimal composite order-only dataset so the offset
    // window must skip the first ordered row instead of merely truncating.
    for (id, tier, handle, label) in [
        (9_981_u128, "gold", "bravo", "amber"),
        (9_982_u128, "gold", "charlie", "bravo"),
        (9_983_u128, "silver", "delta", "delta"),
    ] {
        session
            .insert(SessionDeterministicChoiceEntity {
                id: Ulid::from_u128(id),
                tier: tier.to_string(),
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("composite order-only offset seed insert should succeed");
    }

    // Phase 2: assert the compiled query still carries the logical offset so
    // the runtime check isolates window application, not planning.
    let planned = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .planned()
        .expect("composite order-only offset plan should build");
    assert_eq!(
        planned.explain().page(),
        &crate::db::query::explain::ExplainPagination::Page {
            limit: Some(2),
            offset: 1,
        },
        "composite order-only offset plans must preserve the logical offset at the planner boundary",
    );

    // Phase 3: execute the public entity surface and lock the shifted ordered
    // window directly.
    let response = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("composite order-only offset window should execute");
    let handles = response
        .iter()
        .map(|row| row.entity_ref().handle.as_str())
        .collect::<Vec<_>>();
    let paged = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute_paged()
        .expect("composite order-only offset paged window should execute");
    let paged_handles = paged
        .iter()
        .map(|row| row.entity_ref().handle.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        paged_handles,
        vec!["charlie", "delta"],
        "composite order-only paged windows should preserve the same shifted index order",
    );
    assert_eq!(
        handles,
        vec!["charlie", "delta"],
        "composite order-only offset windows should preserve the shifted index order on the public entity surface",
    );
}
