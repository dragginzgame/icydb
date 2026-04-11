use super::*;

#[test]
fn session_explain_execution_range_choice_uses_bounded_index_range_hints() {
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
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("session deterministic range explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "range-choice roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "range-choice roots should expose the chosen order-compatible range index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "range-choice roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "range-choice roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "range-choice roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "range-choice roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_desc_uses_bounded_index_range_hints() {
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
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .limit(2)
        .explain_execution()
        .expect("session descending deterministic range explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending range-choice roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending range-choice roots should expose the chosen order-compatible range index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending range-choice roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending range-choice roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending range-choice roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending range-choice roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending range-choice roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_offset_uses_bounded_index_range_hints() {
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
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic range offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "range-choice offset roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "range-choice offset roots should expose the chosen order-compatible range index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "range-choice offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "range-choice offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "range-choice offset roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "range-choice offset roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_desc_offset_uses_bounded_index_range_hints() {
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
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session descending deterministic range offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending range-choice offset roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending range-choice offset roots should expose the chosen order-compatible range index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending range-choice offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending range-choice offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending range-choice offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending range-choice offset roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending range-choice offset roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("session deterministic composite order-only explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "composite order-only roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
        ),
        "composite order-only roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "composite order-only roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "composite order-only roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "composite order-only roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "composite order-only roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_choice_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic order-only offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_alpha_idx")
        ),
        "order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "order-only offset roots should stay off the materialized order fallback lane",
    );
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
fn session_explain_execution_order_only_choice_desc_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by_desc("alpha")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic order-only offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_alpha_idx")
        ),
        "descending order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending order-only offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending order-only offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_desc_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic composite order-only explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending composite order-only roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
        ),
        "descending composite order-only roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending composite order-only roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending composite order-only roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending composite order-only roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending composite order-only roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending composite order-only roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic composite order-only offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "composite order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
        ),
        "composite order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "composite order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "composite order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "composite order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "composite order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "composite order-only offset roots should stay off the materialized order fallback lane",
    );
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

#[test]
fn session_explain_execution_composite_order_only_choice_desc_offset_uses_bounded_index_range_hints()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic composite order-only offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending composite order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
        ),
        "descending composite order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending composite order-only offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending composite order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending composite order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending composite order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending composite order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending composite order-only offset roots should stay off the materialized order fallback lane",
    );
}
