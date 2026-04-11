use super::*;

#[test]
fn session_explain_execution_equality_prefix_suffix_order_uses_top_n_seek_on_chosen_prefix_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
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
        .limit(2)
        .explain_execution()
        .expect(
            "session deterministic equality-prefix suffix-order explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "equality-prefix suffix-order roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "equality-prefix suffix-order roots should expose the chosen order-compatible composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "equality-prefix suffix-order roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "equality-prefix suffix-order roots should derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_none(),
        "equality-prefix suffix-order prefix routes must not pretend to be index-range limit-pushdown shapes",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "equality-prefix suffix-order roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
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
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic equality-prefix suffix-order explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending equality-prefix suffix-order roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending equality-prefix suffix-order roots should expose the chosen order-compatible composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending equality-prefix suffix-order roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending equality-prefix suffix-order roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending equality-prefix suffix-order roots should fail closed to a materialized order stage on the chosen prefix route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_none(),
        "descending equality-prefix suffix-order prefix routes must not pretend to be index-range limit-pushdown shapes",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending equality-prefix suffix-order roots should stay off the ascending prefix Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending equality-prefix suffix-order roots should not claim access-satisfied ordering once they materialize sort order",
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

    let descriptor = session
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
        .explain_execution()
        .expect("session equality-prefix suffix-order offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "equality-prefix suffix-order offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "equality-prefix suffix-order offset roots should expose the chosen order-compatible composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "equality-prefix suffix-order offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "equality-prefix suffix-order offset roots should derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "equality-prefix suffix-order offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "equality-prefix suffix-order offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_offset_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
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
        .explain_execution()
        .expect(
            "session descending equality-prefix suffix-order offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending equality-prefix suffix-order offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending equality-prefix suffix-order offset roots should expose the chosen order-compatible composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending equality-prefix suffix-order offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending equality-prefix suffix-order offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending equality-prefix suffix-order offset roots should fail closed to a materialized order stage on the chosen prefix route",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending equality-prefix suffix-order offset roots should stay off the ascending prefix Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending equality-prefix suffix-order offset roots should not claim access-satisfied ordering once they materialize sort order",
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

    let descriptor = session
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
        .explain_execution()
        .expect("session unique-prefix offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "unique-prefix offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "tier_handle_unique")
        ),
        "unique-prefix offset roots should expose the chosen unique composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "unique-prefix offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "unique-prefix offset roots should derive one offset-aware Top-N seek window",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "unique-prefix offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "unique-prefix offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_unique_prefix_offset_desc_uses_top_n_seek() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
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
        .explain_execution()
        .expect("session descending unique-prefix offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending unique-prefix offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "tier_handle_unique")
        ),
        "descending unique-prefix offset roots should expose the chosen unique composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending unique-prefix offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending unique-prefix offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending unique-prefix offset roots should derive one offset-aware Top-N seek window",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending unique-prefix offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending unique-prefix offset roots should stay off the materialized order fallback lane",
    );
}
