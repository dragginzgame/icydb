use super::*;

// Expected EXPLAIN route properties for one prefix-ordered window shape.
struct PrefixRouteExpectations<'a> {
    access_name: &'a str,
    expect_desc_scan: bool,
    expect_top_n_seek: bool,
    expect_access_satisfied: bool,
    expect_materialized_sort: bool,
    forbid_index_range_limit_pushdown: bool,
}

// Build the shared equality-prefix suffix-order filter once so the descriptor
// cases differ only on direction and pagination.
fn equality_prefix_suffix_order_predicate() -> Predicate {
    Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "score",
            CompareOp::Eq,
            Value::Uint(20),
            CoercionId::Strict,
        )),
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
        .filter(equality_prefix_suffix_order_predicate());
    load = if descending {
        load.order_by_desc("label").order_by_desc("id")
    } else {
        load.order_by("label").order_by("id")
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
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )));
    load = if descending {
        load.order_by_desc("handle").order_by_desc("id")
    } else {
        load.order_by("handle").order_by("id")
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
    if expectations.expect_desc_scan {
        assert_eq!(
            descriptor.node_properties().get("scan_dir"),
            Some(&Value::Text("desc".to_string())),
            "{context} should expose the descending scan direction",
        );
    }
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should expose secondary order pushdown",
    );
    assert_eq!(
        explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek).is_some(),
        expectations.expect_top_n_seek,
        "{context} should keep the expected Top-N seek behavior",
    );
    assert_eq!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        expectations.expect_access_satisfied,
        "{context} should keep the expected access-satisfied ordering behavior",
    );
    assert_eq!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        expectations.expect_materialized_sort,
        "{context} should keep the expected materialized-sort behavior",
    );
    if expectations.forbid_index_range_limit_pushdown {
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

#[test]
fn session_explain_execution_equality_prefix_suffix_order_uses_top_n_seek_on_chosen_prefix_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = equality_prefix_suffix_order_descriptor(&session, false, None);

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "z_tier_score_label_idx",
            expect_desc_scan: false,
            expect_top_n_seek: true,
            expect_access_satisfied: true,
            expect_materialized_sort: false,
            forbid_index_range_limit_pushdown: true,
        },
        "equality-prefix suffix-order roots",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = equality_prefix_suffix_order_descriptor(&session, true, None);

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "z_tier_score_label_idx",
            expect_desc_scan: true,
            expect_top_n_seek: false,
            expect_access_satisfied: false,
            expect_materialized_sort: true,
            forbid_index_range_limit_pushdown: true,
        },
        "descending equality-prefix suffix-order roots",
    );
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
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("ascending equality-prefix suffix-order offset window should execute");
    let desc = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .execute()
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
fn session_explain_execution_equality_prefix_suffix_order_offset_uses_top_n_seek_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = equality_prefix_suffix_order_descriptor(&session, false, Some(1));

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "z_tier_score_label_idx",
            expect_desc_scan: false,
            expect_top_n_seek: true,
            expect_access_satisfied: true,
            expect_materialized_sort: false,
            forbid_index_range_limit_pushdown: false,
        },
        "equality-prefix suffix-order offset roots",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_offset_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = equality_prefix_suffix_order_descriptor(&session, true, Some(1));

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "z_tier_score_label_idx",
            expect_desc_scan: true,
            expect_top_n_seek: false,
            expect_access_satisfied: false,
            expect_materialized_sort: true,
            forbid_index_range_limit_pushdown: false,
        },
        "descending equality-prefix suffix-order offset roots",
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
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .limit(2)
        .offset(1)
        .execute()
        .expect("unique-prefix ascending offset window should execute");
    let asc_handles = asc
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    let desc = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by_desc("handle")
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .execute()
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
fn session_explain_execution_unique_prefix_offset_uses_top_n_seek() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = unique_prefix_offset_descriptor(&session, false);

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "tier_handle_unique",
            expect_desc_scan: false,
            expect_top_n_seek: true,
            expect_access_satisfied: true,
            expect_materialized_sort: false,
            forbid_index_range_limit_pushdown: false,
        },
        "unique-prefix offset roots",
    );
}

#[test]
fn session_explain_execution_unique_prefix_offset_desc_uses_top_n_seek() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let descriptor = unique_prefix_offset_descriptor(&session, true);

    assert_prefix_route_descriptor(
        &descriptor,
        PrefixRouteExpectations {
            access_name: "tier_handle_unique",
            expect_desc_scan: true,
            expect_top_n_seek: true,
            expect_access_satisfied: true,
            expect_materialized_sort: false,
            forbid_index_range_limit_pushdown: false,
        },
        "descending unique-prefix offset roots",
    );
}
