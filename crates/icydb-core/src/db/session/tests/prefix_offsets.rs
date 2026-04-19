use super::*;
use crate::db::FieldRef;

// Expected ordered-route shape for one prefix-ordered window shape.
enum PrefixOrderedRouteExpectation {
    TopNSeekAccessSatisfied,
    MaterializedSort,
}

// Expected index-range limit-pushdown visibility for one prefix-ordered
// window shape.
enum IndexRangeLimitPushdownExpectation {
    Allowed,
    Forbidden,
}

// Expected EXPLAIN route properties for one prefix-ordered window shape.
struct PrefixRouteExpectations<'a> {
    access_name: &'a str,
    ordered_route: PrefixOrderedRouteExpectation,
    index_range_limit_pushdown: IndexRangeLimitPushdownExpectation,
}

// Build the shared equality-prefix suffix-order filter once so the descriptor
// cases differ only on direction and pagination.
fn equality_prefix_suffix_order_filter() -> crate::db::FilterExpr {
    crate::db::FilterExpr::and(vec![
        crate::db::FieldRef::new("tier").eq("gold"),
        crate::db::FieldRef::new("score").eq(20_u64),
    ])
}

// Build one equality-prefix suffix-order execution descriptor for the requested
// direction and optional offset window.
fn equality_prefix_suffix_order_descriptor(
    session: &DbSession<SessionSqlCanister>,
    descending: bool,
    offset: Option<u32>,
) -> ExplainExecutionNodeDescriptor {
    let mut load = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(equality_prefix_suffix_order_filter());
    load = if descending {
        load.order_term(crate::db::desc("label"))
            .order_term(crate::db::desc("id"))
    } else {
        load.order_term(crate::db::asc("label"))
            .order_term(crate::db::asc("id"))
    };
    if let Some(offset) = offset {
        load = load.offset(offset);
    }

    load.limit(2)
        .explain_execution()
        .expect("session equality-prefix suffix-order explain_execution should build")
}

// Build one unique-prefix offset execution descriptor for the requested
// direction so the tests only state the expected route properties.
fn unique_prefix_offset_descriptor(
    session: &DbSession<SessionSqlCanister>,
    descending: bool,
) -> ExplainExecutionNodeDescriptor {
    let mut load = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(crate::db::FieldRef::new("tier").eq("gold"));
    load = if descending {
        load.order_term(crate::db::desc("handle"))
            .order_term(crate::db::desc("id"))
    } else {
        load.order_term(crate::db::asc("handle"))
            .order_term(crate::db::asc("id"))
    };

    load.limit(2)
        .offset(1)
        .explain_execution()
        .expect("session unique-prefix offset explain_execution should build")
}

// Assert the shared EXPLAIN contract for index-prefix ordered windows while
// letting each test override only the route properties that actually differ.
fn assert_prefix_route_descriptor(
    descriptor: &ExplainExecutionNodeDescriptor,
    expectations: PrefixRouteExpectations<'_>,
    context: &str,
) {
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "{context} should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == expectations.access_name)
        ),
        "{context} should expose the chosen order-compatible composite index",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should expose secondary order pushdown",
    );
    match expectations.ordered_route {
        PrefixOrderedRouteExpectation::TopNSeekAccessSatisfied => {
            assert!(
                explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek)
                    .is_some(),
                "{context} should keep the expected Top-N seek behavior",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByAccessSatisfied
                )
                .is_some(),
                "{context} should keep the expected access-satisfied ordering behavior",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByMaterializedSort
                )
                .is_none(),
                "{context} should not materialize ordering on this route",
            );
        }
        PrefixOrderedRouteExpectation::MaterializedSort => {
            assert!(
                explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek)
                    .is_none(),
                "{context} should not keep Top-N seek on this route",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByAccessSatisfied
                )
                .is_none(),
                "{context} should not mark ordering as access satisfied",
            );
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::OrderByMaterializedSort
                )
                .is_some(),
                "{context} should keep the expected materialized-sort behavior",
            );
        }
    }
    match expectations.index_range_limit_pushdown {
        IndexRangeLimitPushdownExpectation::Allowed => {}
        IndexRangeLimitPushdownExpectation::Forbidden => {
            assert!(
                explain_execution_find_first_node(
                    descriptor,
                    ExplainExecutionNodeType::IndexRangeLimitPushdown
                )
                .is_none(),
                "{context} must not pretend to be an index-range limit-pushdown shape",
            );
        }
    }
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: lock the four directional/window variants for the same
    // equality-prefix suffix-order family under one matrix so the explain
    // contract stays explicit without four near-identical wrappers.
    let cases = [
        (
            false,
            None,
            PrefixRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: PrefixOrderedRouteExpectation::TopNSeekAccessSatisfied,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Forbidden,
            },
            "equality-prefix suffix-order roots",
        ),
        (
            true,
            None,
            PrefixRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: PrefixOrderedRouteExpectation::MaterializedSort,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Forbidden,
            },
            "descending equality-prefix suffix-order roots",
        ),
        (
            false,
            Some(1),
            PrefixRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: PrefixOrderedRouteExpectation::TopNSeekAccessSatisfied,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Allowed,
            },
            "equality-prefix suffix-order offset roots",
        ),
        (
            true,
            Some(1),
            PrefixRouteExpectations {
                access_name: "z_tier_score_label_idx",
                ordered_route: PrefixOrderedRouteExpectation::MaterializedSort,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Allowed,
            },
            "descending equality-prefix suffix-order offset roots",
        ),
    ];

    // Phase 2: run the shared descriptor assertion across every route variant.
    for (descending, offset, expectations, context) in cases {
        let descriptor = equality_prefix_suffix_order_descriptor(&session, descending, offset);
        assert_prefix_route_descriptor(&descriptor, expectations, context);
    }
}

#[test]
fn session_execute_equality_prefix_suffix_order_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic equality-prefix suffix-order dataset so
    // the offset window can validate the retained ordered page on both scan
    // directions.
    for (id, tier, score, handle, label) in [
        (9_041_u128, "gold", 20_u64, "h-amber", "amber"),
        (9_042_u128, "gold", 20_u64, "h-bravo", "bravo"),
        (9_043_u128, "gold", 20_u64, "h-charlie", "charlie"),
        (9_044_u128, "gold", 20_u64, "h-delta", "delta"),
        (9_045_u128, "silver", 20_u64, "h-echo", "echo"),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::from_u128(id),
                tier: tier.to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("equality-prefix suffix-order offset seed insert should succeed");
    }

    // Phase 2: execute one ascending and one descending offset window on the
    // same equality-prefix suffix-order shape.
    let asc = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(equality_prefix_suffix_order_filter())
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .offset(1)
        .limit(2)
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("ascending equality-prefix suffix-order offset window should execute");
    let desc = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(equality_prefix_suffix_order_filter())
        .order_term(crate::db::desc("label"))
        .order_term(crate::db::desc("id"))
        .offset(1)
        .limit(2)
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("descending equality-prefix suffix-order offset window should execute");

    let asc_labels = asc
        .iter()
        .map(|row| row.entity_ref().label.as_str())
        .collect::<Vec<_>>();
    let desc_labels = desc
        .iter()
        .map(|row| row.entity_ref().label.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_labels,
        vec!["bravo", "charlie"],
        "ascending equality-prefix suffix-order offset windows should preserve the chosen suffix order",
    );
    assert_eq!(
        desc_labels,
        vec!["charlie", "bravo"],
        "descending equality-prefix suffix-order offset windows should preserve the reversed ordered window even when execution falls back downstream",
    );
}

#[test]
fn session_execute_unique_prefix_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_unique_prefix_offset_session_entities(
        &session,
        &[
            (9_881, "gold", "amber", "A"),
            (9_882, "gold", "bravo", "B"),
            (9_883, "gold", "charlie", "C"),
            (9_884, "gold", "delta", "D"),
            (9_885, "silver", "echo", "E"),
        ],
    );

    let asc = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::asc("handle"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .offset(1)
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("unique-prefix ascending offset window should execute");
    let asc_handles = asc
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    let desc = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::desc("handle"))
        .order_term(crate::db::desc("id"))
        .limit(2)
        .offset(1)
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("unique-prefix descending offset window should execute");
    let desc_handles = desc
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_handles,
        vec!["bravo".to_string(), "charlie".to_string()],
        "unique-prefix ascending offset windows should preserve the secondary index order without materialized drift",
    );
    assert_eq!(
        desc_handles,
        vec!["charlie".to_string(), "bravo".to_string()],
        "unique-prefix descending offset windows should preserve the reversed secondary index order without materialized drift",
    );
}

#[test]
fn session_explain_execution_unique_prefix_offset_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: lock the ascending and descending unique-prefix offset routes
    // under one small matrix because both variants share the same direct
    // Top-N contract with only scan direction changing.
    let cases = [
        (
            false,
            PrefixRouteExpectations {
                access_name: "tier_handle_unique",
                ordered_route: PrefixOrderedRouteExpectation::TopNSeekAccessSatisfied,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Allowed,
            },
            "unique-prefix offset roots",
        ),
        (
            true,
            PrefixRouteExpectations {
                access_name: "tier_handle_unique",
                ordered_route: PrefixOrderedRouteExpectation::TopNSeekAccessSatisfied,
                index_range_limit_pushdown: IndexRangeLimitPushdownExpectation::Allowed,
            },
            "descending unique-prefix offset roots",
        ),
    ];

    // Phase 2: reuse the shared explain assertion across both directions.
    for (descending, expectations, context) in cases {
        let descriptor = unique_prefix_offset_descriptor(&session, descending);
        assert_prefix_route_descriptor(&descriptor, expectations, context);
    }
}
