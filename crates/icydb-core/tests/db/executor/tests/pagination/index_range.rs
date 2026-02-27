use super::*;

#[test]
fn load_index_pushdown_eligible_order_matches_index_scan_order() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group8(10_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index,
                prefix_len
            } if index == PUSHDOWN_PARITY_INDEX_MODELS[0].name && prefix_len == 1
        ),
        "query shape should be pushdown-eligible for group+rank index traversal"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .expect("parity load plan should build");
    let response = load.execute(plan).expect("parity load should execute");
    let actual_ids: Vec<Ulid> = ids_from_items(&response.0);

    let expected_ids = ordered_ids_from_group_rank_index(7);
    assert_eq!(
        actual_ids, expected_ids,
        "fallback post-access ordering must match canonical index traversal order for eligible plans"
    );
}

#[test]
fn load_index_prefix_spec_closed_bounds_preserve_prefix_window_end_to_end() {
    setup_pagination_test();

    let rows = [
        (10_301, 7, 10, "g7-r10"),
        (10_302, 7, 20, "g7-r20"),
        (10_303, 7, u32::MAX, "g7-rmax"),
        (10_304, 8, 5, "g8-r5"),
        (10_305, 8, u32::MAX, "g8-rmax"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .expect("pushdown plan should build");
    let prefix_specs = pushdown_plan
        .index_prefix_specs()
        .expect("prefix specs should materialize");
    assert_eq!(
        prefix_specs.len(),
        1,
        "single index-prefix path should lower to exactly one prefix spec"
    );
    assert!(
        matches!(prefix_specs[0].lower(), Bound::Included(_)),
        "index-prefix lower bound should stay closed"
    );
    assert!(
        matches!(prefix_specs[0].upper(), Bound::Included(_)),
        "index-prefix upper bound should stay closed"
    );
    let pushdown_response = load
        .execute(pushdown_plan)
        .expect("pushdown execution should succeed");

    let group7_ids = pushdown_group_ids(&rows, 7);
    let fallback_response = load
        .execute(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(group7_ids.iter().copied())
                .order_by("rank")
                .plan()
                .expect("fallback plan should build"),
        )
        .expect("fallback execution should succeed");

    let pushdown_ids = ids_from_items(&pushdown_response.0);
    let fallback_ids = ids_from_items(&fallback_response.0);
    assert_eq!(
        pushdown_ids, fallback_ids,
        "closed prefix bounds should preserve the exact group window"
    );
    assert!(
        pushdown_response
            .0
            .iter()
            .all(|(_, entity)| entity.group == 7),
        "closed prefix bounds must not leak adjacent prefix rows"
    );
}

#[test]
fn load_index_pushdown_desc_with_explicit_pk_desc_is_eligible_and_ordered() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group8(10_500);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .order_by_desc("id")
        .explain()
        .expect("descending parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index,
                prefix_len
            } if index == PUSHDOWN_PARITY_INDEX_MODELS[0].name && prefix_len == 1
        ),
        "descending uniform order should be pushdown-eligible for group+rank index traversal"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("rank")
        .order_by_desc("id")
        .plan()
        .expect("descending parity load plan should build");
    let response = load
        .execute(plan)
        .expect("descending parity load should execute");
    let actual_ids: Vec<Ulid> = ids_from_items(&response.0);

    let mut expected_ids = ordered_ids_from_group_rank_index(7);
    expected_ids.reverse();
    assert_eq!(
        actual_ids, expected_ids,
        "descending pushdown order should match reversed canonical index traversal"
    );
}

#[test]
fn load_index_pushdown_eligible_paged_results_match_index_scan_window() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(11_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page1 parity plan should build");
    let page1_boundary = page1_plan
        .prepare_cursor(None)
        .expect("page1 parity boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("page1 parity should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);

    let expected_all = ordered_ids_from_group_rank_index(7);
    let expected_page1: Vec<Ulid> = expected_all.iter().copied().take(2).collect();
    assert_eq!(
        page1_ids, expected_page1,
        "page1 fallback output must match the canonical index-order window"
    );

    let page2_cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 parity should emit continuation cursor")
        .clone();
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page2 parity plan should build");
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            page2_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("page2 parity boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 parity should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);

    let expected_page2: Vec<Ulid> = expected_all.iter().copied().skip(2).take(2).collect();
    assert_eq!(
        page2_ids, expected_page2,
        "page2 fallback continuation must match the canonical index-order window"
    );
}

#[test]
fn load_index_pushdown_and_fallback_emit_equivalent_cursor_boundaries() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(12_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown plan should build");
    let pushdown_page = load
        .execute_paged_with_cursor(pushdown_plan, None)
        .expect("pushdown page should execute");

    let fallback_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback plan should build");
    let fallback_page = load
        .execute_paged_with_cursor(fallback_plan, None)
        .expect("fallback page should execute");

    let pushdown_ids: Vec<Ulid> = ids_from_items(&pushdown_page.items.0);
    let fallback_ids: Vec<Ulid> = ids_from_items(&fallback_page.items.0);
    assert_eq!(
        pushdown_ids, fallback_ids,
        "pushdown and fallback page windows should match"
    );

    let pushdown_cursor = pushdown_page
        .next_cursor
        .as_ref()
        .expect("pushdown page should emit continuation cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback page should emit continuation cursor");
    let pushdown_boundary = pushdown_cursor.boundary().clone();
    let fallback_boundary = fallback_cursor.boundary().clone();
    assert_eq!(
        &pushdown_boundary, &fallback_boundary,
        "pushdown and fallback cursors should encode the same continuation boundary"
    );
}

#[test]
fn load_index_pushdown_and_fallback_resume_equivalently_from_shared_boundary() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(13_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let seed_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("seed plan should build");
    let seed_page = load
        .execute_paged_with_cursor(seed_plan, None)
        .expect("seed page should execute");
    let seed_cursor = seed_page
        .next_cursor
        .as_ref()
        .expect("seed page should emit continuation cursor");
    let shared_boundary = seed_cursor.boundary().clone();

    let pushdown_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown page2 plan should build");
    let pushdown_page2 = load
        .execute_paged_with_cursor(pushdown_page2_plan, Some(shared_boundary.clone()))
        .expect("pushdown page2 should execute");

    let fallback_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback page2 plan should build");
    let fallback_page2 = load
        .execute_paged_with_cursor(fallback_page2_plan, Some(shared_boundary))
        .expect("fallback page2 should execute");

    let pushdown_page2_ids: Vec<Ulid> = ids_from_items(&pushdown_page2.items.0);
    let fallback_page2_ids: Vec<Ulid> = ids_from_items(&fallback_page2.items.0);
    assert_eq!(
        pushdown_page2_ids, fallback_page2_ids,
        "pushdown and fallback should return the same rows for a shared continuation boundary"
    );

    let pushdown_next = pushdown_page2
        .next_cursor
        .as_ref()
        .expect("pushdown page2 should emit continuation cursor");
    let fallback_next = fallback_page2
        .next_cursor
        .as_ref()
        .expect("fallback page2 should emit continuation cursor");
    let pushdown_next_boundary = pushdown_next.boundary().clone();
    let fallback_next_boundary = fallback_next.boundary().clone();
    assert_eq!(
        &pushdown_next_boundary, &fallback_next_boundary,
        "pushdown and fallback page2 cursors should encode identical boundaries"
    );
}

#[test]
fn load_index_desc_order_with_ties_matches_for_index_and_by_ids_paths() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(14_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .explain()
        .expect("desc explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible { field }
            ) if field == "rank"
        ),
        "descending rank order should be ineligible and use fallback execution"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let index_path_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page1 plan should build");
    let index_path_page1 = load
        .execute_paged_with_cursor(index_path_page1_plan, None)
        .expect("index-path desc page1 should execute");

    let by_ids_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page1 plan should build");
    let by_ids_page1 = load
        .execute_paged_with_cursor(by_ids_page1_plan, None)
        .expect("by-ids desc page1 should execute");

    let index_path_page1_ids: Vec<Ulid> = ids_from_items(&index_path_page1.items.0);
    let by_ids_page1_ids: Vec<Ulid> = ids_from_items(&by_ids_page1.items.0);
    assert_eq!(
        index_path_page1_ids, by_ids_page1_ids,
        "descending page1 should match across index-prefix and by-ids paths"
    );

    let shared_boundary = index_path_page1
        .next_cursor
        .as_ref()
        .expect("index-path desc page1 should emit cursor")
        .boundary()
        .clone();
    let index_path_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page2 plan should build");
    let index_path_page2 = load
        .execute_paged_with_cursor(index_path_page2_plan, Some(shared_boundary.clone()))
        .expect("index-path desc page2 should execute");

    let by_ids_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page2 plan should build");
    let by_ids_page2 = load
        .execute_paged_with_cursor(by_ids_page2_plan, Some(shared_boundary))
        .expect("by-ids desc page2 should execute");

    let index_path_page2_ids: Vec<Ulid> = ids_from_items(&index_path_page2.items.0);
    let by_ids_page2_ids: Vec<Ulid> = ids_from_items(&by_ids_page2.items.0);
    assert_eq!(
        index_path_page2_ids, by_ids_page2_ids,
        "descending page2 should match across index-prefix and by-ids paths"
    );
}

#[test]
fn load_index_prefix_window_cursor_past_end_returns_empty_page() {
    setup_pagination_test();

    let rows = pushdown_rows_window(15_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, None)
        .expect("prefix window page1 should execute");

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("prefix window page1 should emit continuation cursor");
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page2 plan should build");
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            page1_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("prefix window page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("prefix window page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return final row only");
    assert!(
        page2.next_cursor.is_none(),
        "final prefix window page should not emit continuation cursor"
    );

    let terminal_entity = &page2.items.0[0].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(pushdown_group_predicate(7))
                .order_by("rank")
                .limit(2)
        },
        terminal_entity,
        "cursor boundary at final prefix row should yield an empty continuation page",
        "empty continuation page should not emit a cursor",
    );
}

#[test]
fn load_single_field_range_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = [
        (18_001, 30, "t30"),
        (18_002, 10, "t10-a"),
        (18_003, 10, "t10-b"),
        (18_004, 20, "t20"),
        (18_005, 40, "t40"),
        (18_006, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field range should plan an IndexRange access path"
    );

    let fallback_ids = indexed_metrics_ids_in_tag_range(&rows, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
        },
        fallback_ids,
        |query| query.order_by("tag"),
    );
}

#[test]
fn load_composite_prefix_range_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(19_000);
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite prefix+range should plan an IndexRange access path"
    );

    let fallback_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
        },
        fallback_ids,
        |query| query.order_by("rank"),
    );
}

#[test]
fn load_single_field_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    // Phase 1: seed unique range values so ASC and DESC are strict inverses.
    let rows = [
        (20_101, 10, "t10"),
        (20_102, 20, "t20"),
        (20_103, 30, "t30"),
        (20_104, 40, "t40"),
        (20_105, 50, "t50"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    // Phase 2: verify the surface still plans as IndexRange.
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field asc explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field asc query should plan an IndexRange access path"
    );

    // Phase 3: assert full-result directional symmetry.
    let asc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
                .plan()
                .expect("single-field asc plan should build"),
        )
        .expect("single-field asc execution should succeed");
    let desc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate)
                .order_by_desc("tag")
                .plan()
                .expect("single-field desc plan should build"),
        )
        .expect("single-field desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc.0);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc.0),
        "full DESC result stream should match reversed full ASC result stream"
    );
}

#[test]
fn load_composite_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    // Phase 1: seed deterministic composite rows in one prefix group.
    let rows = [
        (20_201, 7, 10, "g7-r10"),
        (20_202, 7, 20, "g7-r20"),
        (20_203, 7, 30, "g7-r30"),
        (20_204, 7, 40, "g7-r40"),
        (20_205, 7, 50, "g7-r50"),
        (20_206, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 60);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 2: verify IndexRange planning for the composite prefix+range shape.
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite asc explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite asc query should plan an IndexRange access path"
    );

    // Phase 3: assert full-result directional symmetry.
    let asc = load
        .execute(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .plan()
                .expect("composite asc plan should build"),
        )
        .expect("composite asc execution should succeed");
    let desc = load
        .execute(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate)
                .order_by_desc("rank")
                .plan()
                .expect("composite desc plan should build"),
        )
        .expect("composite desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc.0);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc.0),
        "full DESC composite stream should match reversed full ASC stream"
    );
}

#[test]
fn load_unique_index_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    // Phase 1: seed deterministic unique-index rows.
    let rows = [
        (20_301, 10, "c10"),
        (20_302, 20, "c20"),
        (20_303, 30, "c30"),
        (20_304, 40, "c40"),
        (20_305, 50, "c50"),
        (20_306, 70, "c70"),
    ];
    seed_unique_index_range_rows(&rows);

    let predicate = unique_code_range_predicate(10, 60);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    // Phase 2: verify IndexRange planning for the unique range shape.
    let explain = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("code")
        .explain()
        .expect("unique asc explain should build");
    assert!(
        explain_contains_index_range(&explain.access, UNIQUE_INDEX_RANGE_INDEX_MODELS[0].name, 0),
        "unique asc query should plan an IndexRange access path"
    );

    // Phase 3: assert full-result directional symmetry.
    let asc = load
        .execute(
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("code")
                .plan()
                .expect("unique asc plan should build"),
        )
        .expect("unique asc execution should succeed");
    let desc = load
        .execute(
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate)
                .order_by_desc("code")
                .plan()
                .expect("unique desc plan should build"),
        )
        .expect("unique desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc.0);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc.0),
        "full DESC unique stream should match reversed full ASC stream"
    );
}

#[test]
fn load_single_field_range_limit_matrix_matches_unbounded() {
    setup_pagination_test();

    let rows = [
        (31_001, 30, "t30"),
        (31_002, 10, "t10-a"),
        (31_003, 10, "t10-b"),
        (31_004, 20, "t20"),
        (31_005, 25, "t25"),
        (31_006, 40, "t40"),
        (31_007, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(2)
        .explain()
        .expect("single-field limit matrix explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
        },
        &[0_u32, 1_u32, 2_u32, 4_u32, 16_u32],
        16,
    );
}

#[test]
fn load_composite_range_limit_matrix_matches_unbounded() {
    setup_pagination_test();

    let rows = [
        (32_001, 7, 10, "g7-r10-a"),
        (32_002, 7, 10, "g7-r10-b"),
        (32_003, 7, 20, "g7-r20-a"),
        (32_004, 7, 20, "g7-r20-b"),
        (32_005, 7, 25, "g7-r25"),
        (32_006, 7, 30, "g7-r30"),
        (32_007, 7, 35, "g7-r35"),
        (32_008, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .explain()
        .expect("composite limit matrix explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
        },
        &[0_u32, 1_u32, 2_u32, 3_u32, 16_u32],
        20,
    );
}

#[test]
fn load_single_field_range_limit_exact_size_returns_single_page_without_cursor() {
    setup_pagination_test();

    let rows = [
        (33_001, 10, "t10-a"),
        (33_002, 10, "t10-b"),
        (33_003, 20, "t20"),
        (33_004, 25, "t25"),
        (33_005, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let page_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("tag")
        .limit(4)
        .plan()
        .expect("single-field exact-size page plan should build");
    let planned_cursor = page_plan
        .prepare_cursor(None)
        .expect("single-field exact-size cursor should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, planned_cursor)
        .expect("single-field exact-size page should execute");

    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);
    let expected_ids = indexed_metrics_ids_in_tag_range(&rows, 10, 30);
    assert_eq!(
        page_ids, expected_ids,
        "exact-size single-field range page should return the full bounded result set"
    );
    assert!(
        page.next_cursor.is_none(),
        "exact-size single-field range page should not emit a continuation cursor"
    );
}

#[test]
fn load_composite_range_limit_terminal_page_suppresses_cursor() {
    setup_pagination_test();

    let rows = [
        (34_001, 7, 10, "g7-r10-a"),
        (34_002, 7, 10, "g7-r10-b"),
        (34_003, 7, 20, "g7-r20-a"),
        (34_004, 7, 20, "g7-r20-b"),
        (34_005, 7, 25, "g7-r25"),
        (34_006, 7, 30, "g7-r30"),
        (34_007, 7, 35, "g7-r35"),
        (34_008, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut cursor: Option<Vec<u8>> = None;
    let mut page_sizes = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(pages <= 8, "composite terminal-page test must terminate");

        let page_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .expect("composite terminal-page plan should build");
        let planned_cursor = page_plan
            .prepare_cursor(cursor.as_deref())
            .expect("composite terminal-page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("composite terminal-page execution should succeed");

        page_sizes.push(page.items.0.len());

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(encode_token(
            &next_cursor,
            "continuation cursor should serialize for terminal-page resume",
        ));
    }

    assert_eq!(
        page_sizes,
        vec![3, 3, 1],
        "composite limited pagination should end with one terminal page item"
    );
}

#[test]
fn load_index_range_limit_pushdown_trace_reports_limited_access_rows_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_001, 10, "t10-a"),
        (35_002, 10, "t10-b"),
        (35_003, 20, "t20"),
        (35_004, 25, "t25"),
        (35_005, 28, "t28"),
        (35_006, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("trace limit-pushdown execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned);

    assert_eq!(
        access_rows,
        Some(3),
        "limit=2 index-range pushdown should scan only offset+limit+1 rows in access phase"
    );
}

#[test]
fn load_index_range_limit_pushdown_trace_reports_limited_access_rows_for_desc_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_051, 10, "t10-a"),
        (35_052, 10, "t10-b"),
        (35_053, 20, "t20"),
        (35_054, 25, "t25"),
        (35_055, 28, "t28"),
        (35_056, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("trace descending limit-pushdown execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned);

    assert_eq!(
        access_rows,
        Some(3),
        "descending limit=2 index-range pushdown should scan only offset+limit+1 rows in access phase"
    );
}

#[test]
fn load_index_range_limit_zero_short_circuits_access_scan_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_101, 10, "t10-a"),
        (35_102, 20, "t20"),
        (35_103, 25, "t25"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(0),
        offset: 0,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("limit=0 trace execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned);

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 index-range pushdown should not scan access rows"
    );
    assert_exhausted_continuation_page!(
        page,
        "limit=0 should return an empty page for eligible index-range plans",
        "limit=0 should not emit a continuation cursor",
    );
}

#[test]
fn load_index_range_limit_zero_with_offset_short_circuits_access_scan_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_201, 10, "t10-a"),
        (35_202, 20, "t20"),
        (35_203, 25, "t25"),
        (35_204, 28, "t28"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(0),
        offset: 2,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("limit=0 with offset trace execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned);

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 should short-circuit access scanning even when offset is non-zero"
    );
    assert_exhausted_continuation_page!(
        page,
        "limit=0 with offset should return an empty page for eligible index-range plans",
        "limit=0 with offset should not emit a continuation cursor",
    );
}

#[test]
fn load_index_range_limit_pushdown_with_residual_predicate_reduces_access_rows() {
    setup_pagination_test();

    let rows = [
        (35_301, 10, "keep-t10"),
        (35_302, 12, "keep-t12"),
        (35_303, 14, "drop-t14"),
        (35_304, 16, "keep-t16"),
        (35_305, 18, "keep-t18"),
        (35_306, 20, "drop-t20"),
    ];
    seed_indexed_metrics_rows(&rows);

    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let mut fast_logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(21)),
        ),
        ReadConsistency::MissingOk,
    );
    fast_logical.predicate = Some(label_contains_keep.clone());
    fast_logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fast_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fast_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fast_logical);

    let mut fallback_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    fallback_logical.predicate = Some(Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(21)),
        label_contains_keep,
    ]));
    fallback_logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fallback_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fallback_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fallback_logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(fast_plan, None)
        .expect("fast residual limit execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback residual limit execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items.0),
        ids_from_items(&fallback_page.items.0),
        "residual-filter index-range pushdown must preserve fallback row parity",
    );
    assert!(
        fast_trace.keys_scanned <= 3,
        "residual-filter fast path should remain within the bounded fetch window when it can satisfy the page (fast={fast_trace:?}, fallback={fallback_trace:?})",
    );
    assert_eq!(
        fast_trace.optimization,
        Some(ExecutionOptimization::IndexRangeLimitPushdown),
        "residual-filter fast path should report index-range limit pushdown when no retry is needed",
    );
    assert!(
        fast_trace.keys_scanned < fallback_trace.keys_scanned,
        "residual-filter index-range pushdown should reduce scanned rows when early bounded candidates satisfy the page (fast={fast_trace:?}, fallback={fallback_trace:?})",
    );
}

#[test]
fn load_index_range_limit_pushdown_residual_underfill_retries_without_pushdown() {
    setup_pagination_test();

    let rows = [
        (35_401, 10, "drop-t10"),
        (35_402, 11, "drop-t11"),
        (35_403, 12, "drop-t12"),
        (35_404, 13, "keep-t13"),
        (35_405, 14, "keep-t14"),
        (35_406, 15, "keep-t15"),
    ];
    seed_indexed_metrics_rows(&rows);

    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let mut fast_logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(16)),
        ),
        ReadConsistency::MissingOk,
    );
    fast_logical.predicate = Some(label_contains_keep.clone());
    fast_logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fast_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fast_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fast_logical);

    let mut fallback_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, ReadConsistency::MissingOk);
    fallback_logical.predicate = Some(Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(16)),
        label_contains_keep,
    ]));
    fallback_logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fallback_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fallback_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fallback_logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(fast_plan, None)
        .expect("fast residual underfill execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback residual underfill execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items.0),
        ids_from_items(&fallback_page.items.0),
        "residual underfill retry path must preserve fallback row parity",
    );
    assert_eq!(
        fast_trace.optimization, None,
        "residual underfill should retry without index-range limit pushdown and report fallback optimization outcome",
    );
    assert!(
        fast_trace.keys_scanned > 3,
        "residual underfill should rescan beyond the initial bounded fetch window",
    );
    assert!(
        fast_trace.keys_scanned > fallback_trace.keys_scanned,
        "residual underfill retry should report additional scan work beyond canonical fallback (fast={fast_trace:?}, fallback={fallback_trace:?})",
    );
}

#[test]
fn load_index_range_limit_pushdown_residual_predicate_parity_matches_canonical_fallback_matrix() {
    setup_pagination_test();

    let rows = [
        (35_501, 10, "drop-t10"),
        (35_502, 11, "drop-t11"),
        (35_503, 12, "drop-t12"),
        (35_504, 13, "keep-t13"),
        (35_505, 14, "keep-t14"),
        (35_506, 15, "keep-t15"),
        (35_507, 16, "keep-t16"),
        (35_508, 17, "drop-t17"),
        (35_509, 18, "keep-t18"),
        (35_510, 19, "keep-t19"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let cases = [
        ("bounded-satisfied-without-retry", 13u64, 20u64, 2u32, 0u32),
        ("bounded-underfill-retry-required", 10u64, 16u64, 2u32, 0u32),
    ];

    for (case_name, lower, upper, limit, offset) in cases {
        let label_contains_keep = Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        };
        let mut fast_logical = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEXED_METRICS_INDEX_MODELS[0],
                Vec::new(),
                Bound::Included(Value::Uint(lower)),
                Bound::Excluded(Value::Uint(upper)),
            ),
            ReadConsistency::MissingOk,
        );
        fast_logical.predicate = Some(label_contains_keep.clone());
        fast_logical.order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        fast_logical.page = Some(PageSpec {
            limit: Some(limit),
            offset,
        });
        let fast_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fast_logical);

        let mut fallback_logical =
            AccessPlannedQuery::new(AccessPath::FullScan, ReadConsistency::MissingOk);
        fallback_logical.predicate = Some(Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(lower)),
            strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(upper)),
            label_contains_keep,
        ]));
        fallback_logical.order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        fallback_logical.page = Some(PageSpec {
            limit: Some(limit),
            offset,
        });
        let fallback_plan = ExecutablePlan::<IndexedMetricsEntity>::new(fallback_logical);

        let (fast_page, _fast_trace) = load
            .execute_paged_with_cursor_traced(fast_plan, None)
            .expect("fast residual matrix execution should succeed");
        let (fallback_page, _fallback_trace) = load
            .execute_paged_with_cursor_traced(fallback_plan, None)
            .expect("fallback residual matrix execution should succeed");

        assert_eq!(
            ids_from_items(&fast_page.items.0),
            ids_from_items(&fallback_page.items.0),
            "residual range matrix case should preserve fallback row parity: case={case_name}",
        );
        assert_eq!(
            fast_page.next_cursor.is_some(),
            fallback_page.next_cursor.is_some(),
            "residual range matrix case should preserve continuation presence parity: case={case_name}",
        );
        if let (Some(fast_cursor), Some(fallback_cursor)) =
            (&fast_page.next_cursor, &fallback_page.next_cursor)
        {
            assert_eq!(
                fast_cursor.boundary().clone(),
                fallback_cursor.boundary().clone(),
                "residual range matrix case should preserve continuation boundary parity: case={case_name}",
            );
        }
    }
}

#[test]
fn load_index_only_predicate_reduces_access_rows_vs_fallback() {
    setup_pagination_test();

    let rows = [
        (36_001, 7, 10, "g7-r10"),
        (36_002, 7, 20, "g7-r20-a"),
        (36_003, 7, 20, "g7-r20-b"),
        (36_004, 7, 30, "g7-r30"),
        (36_005, 7, 40, "g7-r40"),
        (36_006, 8, 20, "g8-r20"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_not_20_strict,
                ]))
                .order_by("rank")
                .plan()
                .expect("index-shape plan should build"),
            None,
        )
        .expect("index-shape execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    group_eq_fallback,
                    rank_not_20_fallback,
                ]))
                .order_by("rank")
                .plan()
                .expect("fallback plan should build"),
            None,
        )
        .expect("fallback execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items.0),
        ids_from_items(&fallback_page.items.0),
        "index-only predicate path must preserve fallback result parity",
    );
    assert!(
        fast_trace.index_predicate_applied,
        "index-backed strict predicate should activate index-only evaluation"
    );
    assert!(
        !fallback_trace.index_predicate_applied,
        "by-ids fallback path must not report index-only predicate activation"
    );
    assert!(
        fast_trace.index_predicate_keys_rejected > 0,
        "index-only path should report rejected index keys for non-matching predicate rows",
    );
    assert_eq!(
        fallback_trace.index_predicate_keys_rejected, 0,
        "fallback path must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace.distinct_keys_deduped, 0,
        "non-distinct plans must not report DISTINCT dedup activity",
    );
    assert_eq!(
        fallback_trace.distinct_keys_deduped, 0,
        "non-distinct fallback plans must not report DISTINCT dedup activity",
    );
    assert!(
        fast_trace.keys_scanned < fallback_trace.keys_scanned,
        "index-only predicate activation should reduce scanned rows for this shape",
    );
}

#[test]
fn load_index_only_predicate_distinct_continuation_matches_fallback() {
    setup_pagination_test();

    let rows = [
        (36_101, 7, 10, "g7-r10"),
        (36_102, 7, 20, "g7-r20-a"),
        (36_103, 7, 20, "g7-r20-b"),
        (36_104, 7, 30, "g7-r30"),
        (36_105, 7, 40, "g7-r40"),
        (36_106, 8, 1, "g8-r1"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let build_fast_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(Predicate::And(vec![
                pushdown_group_predicate(7),
                rank_not_20_strict.clone(),
            ]))
            .order_by("rank")
            .distinct()
            .limit(2)
            .plan()
            .expect("fast distinct plan should build")
    };
    let build_fallback_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(Predicate::And(vec![
                group_eq_fallback.clone(),
                rank_not_20_fallback.clone(),
            ]))
            .order_by("rank")
            .distinct()
            .limit(2)
            .plan()
            .expect("fallback distinct plan should build")
    };

    let (fast_page1, fast_trace1) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("fast distinct page1 should execute");
    let fast_trace1 = fast_trace1.expect("debug trace should be present");
    let (fallback_page1, fallback_trace1) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("fallback distinct page1 should execute");
    let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page1.items.0),
        ids_from_items(&fallback_page1.items.0),
        "fast and fallback distinct page1 rows should match",
    );
    assert!(
        fast_trace1.index_predicate_applied && !fast_trace1.continuation_applied,
        "first index-only page should report activation without continuation"
    );
    assert!(
        !fallback_trace1.index_predicate_applied,
        "fallback distinct page1 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace1.optimization, None,
        "fallback distinct page1 should remain non-optimized",
    );
    assert!(
        fast_trace1.index_predicate_keys_rejected > 0,
        "index-only distinct page1 should report rejected index keys",
    );
    assert_eq!(
        fallback_trace1.index_predicate_keys_rejected, 0,
        "fallback distinct page1 must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace1.distinct_keys_deduped, fallback_trace1.distinct_keys_deduped,
        "fast and fallback distinct page1 should report the same DISTINCT dedup count",
    );

    let fast_cursor = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast distinct page1 should emit continuation cursor");
    let fallback_cursor = fallback_page1
        .next_cursor
        .as_ref()
        .expect("fallback distinct page1 should emit continuation cursor");
    let shared_boundary = fast_cursor.boundary().clone();
    assert_eq!(
        fast_cursor.boundary().clone(),
        fallback_cursor.boundary().clone(),
        "fast and fallback distinct page1 cursors should encode the same boundary",
    );

    let (fast_page2, fast_trace2) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
        .expect("fast distinct page2 should execute");
    let fast_trace2 = fast_trace2.expect("debug trace should be present");
    let (fallback_page2, fallback_trace2) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
        .expect("fallback distinct page2 should execute");
    let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page2.items.0),
        ids_from_items(&fallback_page2.items.0),
        "fast and fallback distinct page2 rows should match",
    );
    assert!(
        fast_trace2.index_predicate_applied && fast_trace2.continuation_applied,
        "continued index-only page should report both activation and continuation"
    );
    assert!(
        !fallback_trace2.index_predicate_applied,
        "fallback distinct page2 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace2.optimization, None,
        "fallback distinct page2 should remain non-optimized",
    );
    assert_eq!(
        fallback_trace2.index_predicate_keys_rejected, 0,
        "fallback distinct page2 must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace2.distinct_keys_deduped, fallback_trace2.distinct_keys_deduped,
        "fast and fallback distinct page2 should report the same DISTINCT dedup count",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        fallback_page2.next_cursor.is_some(),
        "fast and fallback distinct page2 continuation presence should match",
    );
}

#[test]
fn load_index_only_predicate_distinct_desc_continuation_matches_fallback() {
    setup_pagination_test();

    let rows = [
        (36_201, 7, 10, "g7-r10"),
        (36_202, 7, 20, "g7-r20-a"),
        (36_203, 7, 20, "g7-r20-b"),
        (36_204, 7, 30, "g7-r30"),
        (36_205, 7, 40, "g7-r40"),
        (36_206, 8, 1, "g8-r1"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let build_fast_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(Predicate::And(vec![
                pushdown_group_predicate(7),
                rank_not_20_strict.clone(),
            ]))
            .order_by_desc("rank")
            .distinct()
            .limit(2)
            .plan()
            .expect("fast descending distinct plan should build")
    };
    let build_fallback_plan = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(Predicate::And(vec![
                group_eq_fallback.clone(),
                rank_not_20_fallback.clone(),
            ]))
            .order_by_desc("rank")
            .distinct()
            .limit(2)
            .plan()
            .expect("fallback descending distinct plan should build")
    };

    let (fast_page1, fast_trace1) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("fast descending distinct page1 should execute");
    let fast_trace1 = fast_trace1.expect("debug trace should be present");
    let (fallback_page1, fallback_trace1) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("fallback descending distinct page1 should execute");
    let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page1.items.0),
        ids_from_items(&fallback_page1.items.0),
        "fast and fallback descending distinct page1 rows should match",
    );
    assert!(
        fast_trace1.index_predicate_applied && !fast_trace1.continuation_applied,
        "first descending index-only page should report activation without continuation"
    );
    assert!(
        !fallback_trace1.index_predicate_applied,
        "fallback descending distinct page1 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace1.optimization, None,
        "fallback descending distinct page1 should remain non-optimized",
    );

    let fast_cursor = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast descending distinct page1 should emit continuation cursor");
    let fallback_cursor = fallback_page1
        .next_cursor
        .as_ref()
        .expect("fallback descending distinct page1 should emit continuation cursor");
    let shared_boundary = fast_cursor.boundary().clone();
    assert_eq!(
        fast_cursor.boundary().clone(),
        fallback_cursor.boundary().clone(),
        "fast and fallback descending distinct page1 cursors should encode the same boundary",
    );

    let (fast_page2, fast_trace2) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
        .expect("fast descending distinct page2 should execute");
    let fast_trace2 = fast_trace2.expect("debug trace should be present");
    let (fallback_page2, fallback_trace2) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
        .expect("fallback descending distinct page2 should execute");
    let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page2.items.0),
        ids_from_items(&fallback_page2.items.0),
        "fast and fallback descending distinct page2 rows should match",
    );
    assert!(
        fast_trace2.index_predicate_applied && fast_trace2.continuation_applied,
        "continued descending index-only page should report both activation and continuation"
    );
    assert!(
        !fallback_trace2.index_predicate_applied,
        "fallback descending distinct page2 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace2.optimization, None,
        "fallback descending distinct page2 should remain non-optimized",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        fallback_page2.next_cursor.is_some(),
        "fast and fallback descending distinct page2 continuation presence should match",
    );
}

#[test]
fn load_index_only_predicate_in_constants_reduces_access_rows_vs_fallback() {
    setup_pagination_test();

    // Phase 1: seed rows where strict IN and residual text filtering reject multiple candidates.
    let rows = [
        (36_301, 7, 10, "keep-g7-r10"),
        (36_302, 7, 20, "drop-g7-r20"),
        (36_303, 7, 20, "keep-g7-r20"),
        (36_304, 7, 30, "keep-g7-r30"),
        (36_305, 7, 40, "keep-g7-r40"),
        (36_306, 7, 50, "keep-g7-r50"),
        (36_307, 7, 60, "drop-g7-r60"),
        (36_308, 8, 20, "keep-g8-r20"),
    ];
    seed_pushdown_rows(&rows);

    let rank_in_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(20), Value::Uint(40), Value::Uint(50)]),
        CoercionId::Strict,
    ));
    let rank_in_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(20), Value::Uint(40), Value::Uint(50)]),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    // Phase 2: execute fast and fallback plans with equivalent row semantics.
    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_in_strict,
                    label_contains_keep.clone(),
                ]))
                .order_by("rank")
                .plan()
                .expect("strict IN fast plan should build"),
            None,
        )
        .expect("strict IN fast execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    group_eq_fallback,
                    rank_in_fallback,
                    label_contains_keep,
                ]))
                .order_by("rank")
                .plan()
                .expect("fallback IN plan should build"),
            None,
        )
        .expect("fallback IN execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    // Phase 3: assert parity and read-reduction behavior.
    assert_eq!(
        ids_from_items(&fast_page.items.0),
        ids_from_items(&fallback_page.items.0),
        "strict IN index-only execution must preserve fallback row parity",
    );
    assert!(
        fast_trace.index_predicate_applied,
        "strict IN predicate should activate index-only filtering"
    );
    assert!(
        !fallback_trace.index_predicate_applied,
        "fallback IN path must keep index-only filtering disabled",
    );
    assert!(
        fast_trace.index_predicate_keys_rejected > 0,
        "strict IN index-only path should reject non-matching index keys",
    );
    assert_eq!(
        fallback_trace.index_predicate_keys_rejected, 0,
        "fallback IN path must not report index-only rejected-key counts",
    );
    assert!(
        fast_trace.keys_scanned < fallback_trace.keys_scanned,
        "strict IN index-only filtering should reduce scanned rows for this shape",
    );
}

#[test]
#[expect(clippy::similar_names)]
fn load_index_only_predicate_bounded_range_distinct_continuation_matches_fallback_for_asc_and_desc()
{
    setup_pagination_test();

    // Phase 1: seed rows that require both range bounds and residual text checks.
    let rows = [
        (36_401, 7, 10, "keep-g7-r10"),
        (36_402, 7, 20, "drop-g7-r20"),
        (36_403, 7, 20, "keep-g7-r20"),
        (36_404, 7, 30, "keep-g7-r30"),
        (36_405, 7, 40, "drop-g7-r40"),
        (36_406, 7, 40, "keep-g7-r40"),
        (36_407, 7, 50, "keep-g7-r50"),
        (36_408, 8, 30, "keep-g8-r30"),
    ];
    seed_pushdown_rows(&rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    for descending in [false, true] {
        let rank_gte_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Gte,
            Value::Uint(20),
            CoercionId::Strict,
        ));
        let rank_lte_40_strict = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Lte,
            Value::Uint(40),
            CoercionId::Strict,
        ));
        let rank_gte_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Gte,
            Value::Uint(20),
            CoercionId::NumericWiden,
        ));
        let rank_lte_40_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Lte,
            Value::Uint(40),
            CoercionId::NumericWiden,
        ));
        let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        ));
        let label_contains_keep = Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        };

        let build_fast_plan = || {
            let base = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_gte_20_strict.clone(),
                    rank_lte_40_strict.clone(),
                    label_contains_keep.clone(),
                ]))
                .distinct()
                .limit(2);

            if descending {
                base.order_by_desc("rank")
                    .plan()
                    .expect("fast bounded-range descending plan should build")
            } else {
                base.order_by("rank")
                    .plan()
                    .expect("fast bounded-range ascending plan should build")
            }
        };
        let build_fallback_plan = || {
            let base = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    group_eq_fallback.clone(),
                    rank_gte_20_fallback.clone(),
                    rank_lte_40_fallback.clone(),
                    label_contains_keep.clone(),
                ]))
                .distinct()
                .limit(2);

            if descending {
                base.order_by_desc("rank")
                    .plan()
                    .expect("fallback bounded-range descending plan should build")
            } else {
                base.order_by("rank")
                    .plan()
                    .expect("fallback bounded-range ascending plan should build")
            }
        };

        // Phase 2: compare first page and continuation boundary parity.
        let (fast_page1, fast_trace1) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), None)
            .expect("fast bounded-range page1 should execute");
        let fast_trace1 = fast_trace1.expect("debug trace should be present");
        let (fallback_page1, fallback_trace1) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), None)
            .expect("fallback bounded-range page1 should execute");
        let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

        assert_eq!(
            ids_from_items(&fast_page1.items.0),
            ids_from_items(&fallback_page1.items.0),
            "fast and fallback bounded-range page1 rows should match for descending={descending}",
        );
        assert!(
            fast_trace1.index_predicate_applied && !fast_trace1.continuation_applied,
            "fast bounded-range page1 should report index-only activation for descending={descending}",
        );
        assert!(
            !fallback_trace1.index_predicate_applied,
            "fallback bounded-range page1 must not report index-only activation for descending={descending}",
        );
        assert_eq!(
            fallback_trace1.optimization, None,
            "fallback bounded-range page1 should remain non-optimized for descending={descending}",
        );
        assert!(
            fast_trace1.index_predicate_keys_rejected > 0,
            "fast bounded-range page1 should reject non-matching index keys for descending={descending}",
        );
        assert_eq!(
            fallback_trace1.index_predicate_keys_rejected, 0,
            "fallback bounded-range page1 must not report index-only rejected-key counts for descending={descending}",
        );

        let fast_cursor = fast_page1
            .next_cursor
            .as_ref()
            .expect("fast bounded-range page1 should emit continuation cursor");
        let fallback_cursor = fallback_page1
            .next_cursor
            .as_ref()
            .expect("fallback bounded-range page1 should emit continuation cursor");
        let shared_boundary = fast_cursor.boundary().clone();
        assert_eq!(
            fast_cursor.boundary().clone(),
            fallback_cursor.boundary().clone(),
            "fast and fallback bounded-range page1 cursors should match for descending={descending}",
        );

        // Phase 3: compare continuation page parity and aggregate scan reduction.
        let (fast_page2, fast_trace2) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
            .expect("fast bounded-range page2 should execute");
        let fast_trace2 = fast_trace2.expect("debug trace should be present");
        let (fallback_page2, fallback_trace2) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
            .expect("fallback bounded-range page2 should execute");
        let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

        assert_eq!(
            ids_from_items(&fast_page2.items.0),
            ids_from_items(&fallback_page2.items.0),
            "fast and fallback bounded-range page2 rows should match for descending={descending}",
        );
        assert!(
            fast_trace2.index_predicate_applied && fast_trace2.continuation_applied,
            "fast bounded-range page2 should report activation with continuation for descending={descending}",
        );
        assert!(
            !fallback_trace2.index_predicate_applied,
            "fallback bounded-range page2 must not report index-only activation for descending={descending}",
        );
        assert_eq!(
            fallback_trace2.optimization, None,
            "fallback bounded-range page2 should remain non-optimized for descending={descending}",
        );
        assert_eq!(
            fallback_trace2.index_predicate_keys_rejected, 0,
            "fallback bounded-range page2 must not report index-only rejected-key counts for descending={descending}",
        );
        assert_eq!(
            fast_trace1.distinct_keys_deduped, fallback_trace1.distinct_keys_deduped,
            "fast and fallback bounded-range page1 distinct counts should match for descending={descending}",
        );
        assert_eq!(
            fast_trace2.distinct_keys_deduped, fallback_trace2.distinct_keys_deduped,
            "fast and fallback bounded-range page2 distinct counts should match for descending={descending}",
        );
        assert_eq!(
            fast_page2.next_cursor.is_some(),
            fallback_page2.next_cursor.is_some(),
            "fast and fallback bounded-range page2 continuation presence should match for descending={descending}",
        );

        let fast_scanned_total = fast_trace1
            .keys_scanned
            .saturating_add(fast_trace2.keys_scanned);
        let fallback_scanned_total = fallback_trace1
            .keys_scanned
            .saturating_add(fallback_trace2.keys_scanned);
        assert!(
            fast_scanned_total < fallback_scanned_total,
            "fast bounded-range index-only filtering should reduce total scanned rows for descending={descending}",
        );
    }
}
