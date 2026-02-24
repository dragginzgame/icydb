use super::*;

#[test]
fn load_single_field_between_equivalent_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = [
        (19_101, 30, "t30"),
        (19_102, 10, "t10-a"),
        (19_103, 10, "t10-b"),
        (19_104, 20, "t20"),
        (19_105, 40, "t40"),
        (19_106, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field between-equivalent explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field between-equivalent predicate should plan an IndexRange access path"
    );

    let fallback_ids = indexed_metrics_ids_in_between_equivalent_range(&rows, 10, 30);
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
fn load_composite_between_equivalent_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(19_200);
    seed_pushdown_rows(&rows);

    let predicate = group_rank_between_equivalent_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite between-equivalent explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite between-equivalent predicate should plan an IndexRange access path"
    );

    let fallback_ids = pushdown_ids_in_group_rank_between_equivalent_range(&rows, 7, 10, 30);
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
fn load_single_field_range_pushdown_handles_min_and_max_tag_edges() {
    const MAX_TAG: u32 = u32::MAX;

    setup_pagination_test();

    let rows = [
        (19_301, 0, "t0"),
        (19_302, 1, "t1"),
        (19_303, 50, "t50"),
        (19_304, MAX_TAG - 1, "tmax-1"),
        (19_305, MAX_TAG, "tmax"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    // Phase 1: exclusive upper bound should exclude the max-value group.
    let exclusive_predicate = tag_range_predicate(0, MAX_TAG);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(exclusive_predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field extreme-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field extreme-edge range should plan an IndexRange access path"
    );

    let fallback_exclusive_ids = indexed_metrics_ids_in_tag_range(&rows, 0, MAX_TAG);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(exclusive_predicate.clone())
                .order_by("tag")
        },
        fallback_exclusive_ids,
        |query| query.order_by("tag"),
    );

    // Phase 2: inclusive upper bound should include the max-value group.
    let inclusive_predicate = tag_between_equivalent_predicate(0, MAX_TAG);
    let fallback_inclusive_ids = indexed_metrics_ids_in_between_equivalent_range(&rows, 0, MAX_TAG);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate.clone())
                .order_by("tag")
        },
        fallback_inclusive_ids.iter().copied(),
        |query| query.order_by("tag"),
    );

    let pushdown_inclusive_has_max = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate)
                .order_by("tag")
                .plan()
                .expect("single-field extreme-edge inclusive plan should build"),
        )
        .expect("single-field extreme-edge inclusive pushdown should execute")
        .0
        .iter()
        .any(|(_, entity)| entity.id == Ulid::from_u128(19_305));
    assert!(
        pushdown_inclusive_has_max,
        "inclusive upper-bound range must include rows at the max field value"
    );
}

#[test]
fn load_composite_range_pushdown_handles_min_and_max_rank_edges() {
    const MAX_RANK: u32 = u32::MAX;

    setup_pagination_test();

    let rows = [
        (19_401, 7, 0, "g7-r0"),
        (19_402, 7, 1, "g7-r1"),
        (19_403, 7, 10, "g7-r10"),
        (19_404, 7, MAX_RANK - 1, "g7-rmax-1"),
        (19_405, 7, MAX_RANK, "g7-rmax"),
        (19_406, 8, MAX_RANK, "g8-rmax"),
    ];
    seed_pushdown_rows(&rows);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: exclusive upper bound should exclude the max-value rank group.
    let exclusive_predicate = group_rank_range_predicate(7, 0, MAX_RANK);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(exclusive_predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite extreme-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite extreme-edge range should plan an IndexRange access path"
    );

    let fallback_exclusive_ids = pushdown_ids_in_group_rank_range(&rows, 7, 0, MAX_RANK);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(exclusive_predicate.clone())
                .order_by("rank")
        },
        fallback_exclusive_ids,
        |query| query.order_by("rank"),
    );

    // Phase 2: inclusive upper bound should include the max-value rank group.
    let inclusive_predicate = group_rank_between_equivalent_predicate(7, 0, MAX_RANK);
    let fallback_inclusive_ids =
        pushdown_ids_in_group_rank_between_equivalent_range(&rows, 7, 0, MAX_RANK);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate.clone())
                .order_by("rank")
        },
        fallback_inclusive_ids.iter().copied(),
        |query| query.order_by("rank"),
    );

    let pushdown_inclusive_has_max = load
        .execute(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate)
                .order_by("rank")
                .plan()
                .expect("composite extreme-edge inclusive plan should build"),
        )
        .expect("composite extreme-edge inclusive pushdown should execute")
        .0
        .iter()
        .any(|(_, entity)| entity.id == Ulid::from_u128(19_405));
    assert!(
        pushdown_inclusive_has_max,
        "inclusive upper-bound range must include rows at the max field value"
    );
}

#[test]
fn load_composite_range_cursor_pagination_matches_fallback_without_duplicates() {
    setup_pagination_test();

    let rows = [
        (20_001, 7, 5, "g7-r5"),
        (20_002, 7, 10, "g7-r10-a"),
        (20_003, 7, 20, "g7-r20-a"),
        (20_004, 7, 20, "g7-r20-b"),
        (20_005, 7, 30, "g7-r30"),
        (20_006, 7, 35, "g7-r35"),
        (20_007, 7, 40, "g7-r40"),
        (20_008, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .explain()
        .expect("composite range pagination explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite range pagination should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (pushdown_ids, _) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(2)
        },
        8,
    );

    let fallback_seed_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 40);
    let (fallback_ids, _) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(fallback_seed_ids.iter().copied())
                .order_by("rank")
                .limit(2)
        },
        8,
    );

    assert_eq!(
        pushdown_ids, fallback_ids,
        "composite range cursor pagination should match fallback across all pages"
    );
    let unique_pushdown_ids: BTreeSet<Ulid> = pushdown_ids.iter().copied().collect();
    assert_eq!(
        unique_pushdown_ids.len(),
        pushdown_ids.len(),
        "composite range cursor pagination must not emit duplicate rows"
    );
}

#[test]
fn load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic() {
    setup_pagination_test();

    let rows = [
        (20_101, 7, 10, "g7-r10-a"),
        (20_102, 7, 10, "g7-r10-b"),
        (20_103, 7, 20, "g7-r20-a"),
        (20_104, 7, 20, "g7-r20-b"),
        (20_105, 7, 25, "g7-r25"),
        (20_106, 7, 30, "g7-r30"),
        (20_107, 7, 35, "g7-r35"),
        (20_108, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(3)
        .explain()
        .expect("composite range monotonicity explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite range monotonicity test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Baseline: one unbounded execution for the exact same predicate + order.
    let unbounded_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .plan()
        .expect("unbounded plan should build");
    let unbounded = load
        .execute(unbounded_plan)
        .expect("unbounded execution should succeed");
    let unbounded_ids: Vec<Ulid> = ids_from_items(&unbounded.0);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .0
        .iter()
        .map(|(_, entity)| serialize(entity).expect("entity serialization should succeed"))
        .collect();

    let (paged_ids, paged_row_bytes) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(3)
        },
        8,
    );

    // Anchor monotonicity: preserve explicit cursor-shape validation across pages.
    let mut cursor: Option<Vec<u8>> = None;
    let mut page_anchors = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(
            pages <= 8,
            "composite range pagination should terminate in bounded pages"
        );

        let page_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .expect("page plan should build");
        let planned_cursor = page_plan
            .plan_cursor(cursor.as_deref())
            .expect("page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("paged execution should succeed");

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let next_cursor_bytes = encode_token(
            &next_cursor,
            "continuation cursor should serialize for anchor checks",
        );

        assert_anchor_monotonic(
            &mut page_anchors,
            next_cursor_bytes.as_slice(),
            "continuation cursor should decode",
            "index-range cursor should include a raw-key anchor",
            "index-range continuation anchors must progress strictly monotonically",
        );
        cursor = Some(next_cursor_bytes);
    }

    assert!(
        page_anchors.len() >= 2,
        "fixture should produce at least two continuation anchors"
    );
    assert_eq!(
        paged_ids, unbounded_ids,
        "concatenated paginated ids must match unbounded ids in the same order"
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "concatenated paginated rows must be byte-for-byte identical to the unbounded result set"
    );
}

#[test]
fn load_unique_index_range_cursor_pagination_matches_unbounded_case_f() {
    setup_pagination_test();

    let rows = [
        (23_001, 5, "c5"),
        (23_002, 10, "c10"),
        (23_003, 20, "c20"),
        (23_004, 30, "c30"),
        (23_005, 40, "c40"),
        (23_006, 55, "c55"),
        (23_007, 70, "c70"),
    ];
    seed_unique_index_range_rows(&rows);

    let predicate = unique_code_range_predicate(10, 60);
    let explain = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("code")
        .limit(2)
        .explain()
        .expect("unique index-range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, UNIQUE_INDEX_RANGE_INDEX_MODELS[0].name, 0),
        "unique index-range continuation should plan an IndexRange access path"
    );

    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let unbounded_plan = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("code")
        .plan()
        .expect("unique unbounded plan should build");
    let unbounded = load
        .execute(unbounded_plan)
        .expect("unique unbounded execution should succeed");
    let unbounded_ids: Vec<Ulid> = ids_from_items(&unbounded.0);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .0
        .iter()
        .map(|(_, entity)| serialize(entity).expect("entity serialization should succeed"))
        .collect();

    let (paged_ids, paged_row_bytes) = collect_all_pages(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("code")
                .limit(2)
        },
        8,
    );

    let mut cursor: Option<Vec<u8>> = None;
    let mut anchors = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(pages <= 8, "unique index-range pagination should terminate");

        let page_plan = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("code")
            .limit(2)
            .plan()
            .expect("unique page plan should build");
        let planned_cursor = page_plan
            .plan_cursor(cursor.as_deref())
            .expect("unique page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("unique paged execution should succeed");

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let next_cursor_bytes = encode_token(
            &next_cursor,
            "continuation cursor should serialize for anchor checks",
        );
        assert_anchor_monotonic(
            &mut anchors,
            next_cursor_bytes.as_slice(),
            "unique continuation cursor should decode",
            "unique index-range cursor should include a raw-key anchor",
            "unique index-range continuation anchors must advance strictly",
        );
        cursor = Some(next_cursor_bytes);
    }

    let unique_paged_ids: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
    assert_eq!(
        unique_paged_ids.len(),
        paged_ids.len(),
        "unique index-range pagination must not emit duplicate rows"
    );
    assert_eq!(
        paged_ids, unbounded_ids,
        "unique index-range paginated ids must match unbounded ids in order"
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "unique index-range paginated rows must match unbounded rows byte-for-byte"
    );
}

#[test]
fn load_single_field_range_cursor_boundaries_respect_lower_and_upper_edges() {
    setup_pagination_test();

    let rows = [
        (21_001, 10, "t10-a"),
        (21_002, 10, "t10-b"),
        (21_003, 20, "t20"),
        (21_004, 25, "t25"),
        (21_005, 30, "t30"),
        (21_006, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(10)
        .explain()
        .expect("single-field range boundary explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field range boundary test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let base_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(10)
        .plan()
        .expect("single-field base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("single-field base page should execute");
    let all_ids: Vec<Ulid> = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids.len(),
        4,
        "single-field range should include only rows in [10, 30)"
    );

    let first_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
                .limit(10)
        },
        first_entity,
        all_ids[1..].to_vec(),
    );

    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(tag_range_predicate(10, 30))
                .order_by("tag")
                .limit(10)
        },
        terminal_entity,
        "cursor boundary at the upper edge row should return an empty continuation page",
        "single-field empty continuation page should not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_resume_from_upper_anchor_returns_remaining_rows() {
    setup_pagination_test();

    let rows = [
        (21_101, 10, "t10"),
        (21_102, 20, "t20"),
        (21_103, 30, "t30"),
        (21_104, 40, "t40"),
        (21_105, 50, "t50"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-field desc upper-anchor page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("single-field desc upper-anchor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("single-field desc upper-anchor page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_105)],
        "descending first page should start at the upper envelope row"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("single-field desc upper-anchor page1 should emit continuation cursor");
    let resume_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc upper-anchor resume plan should build");
    let resume_boundary = resume_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("single-field desc upper-anchor resume boundary should plan");
    let resume = load
        .execute_paged_with_cursor(resume_plan, resume_boundary)
        .expect("single-field desc upper-anchor resume should execute");

    assert_eq!(
        ids_from_items(&resume.items.0),
        vec![
            Ulid::from_u128(21_104),
            Ulid::from_u128(21_103),
            Ulid::from_u128(21_102),
            Ulid::from_u128(21_101),
        ],
        "descending resume from the upper anchor must continue with the remaining lower rows",
    );
}

#[test]
fn load_single_field_desc_range_resume_from_lower_boundary_returns_empty() {
    setup_pagination_test();

    let rows = [
        (21_201, 10, "t10"),
        (21_202, 20, "t20"),
        (21_203, 30, "t30"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(10, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let base_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc lower-boundary base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("single-field desc lower-boundary base page should execute");

    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(tag_between_equivalent_predicate(10, 30))
                .order_by_desc("tag")
                .limit(10)
        },
        terminal_entity,
        "descending resume from the lower boundary row must return an empty page",
        "empty descending continuation page must not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_single_element_resume_returns_empty() {
    setup_pagination_test();

    let rows = [
        (21_301, 20, "t20"),
        (21_302, 30, "t30"),
        (21_303, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(30, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-element desc page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, None)
        .expect("single-element desc page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_302)],
        "single-element descending range should return the only row"
    );
    assert!(
        page1.next_cursor.is_none(),
        "single-element descending first page should not emit a cursor"
    );

    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(tag_between_equivalent_predicate(30, 30))
                .order_by_desc("tag")
                .limit(1)
        },
        &page1.items.0[0].1,
        "resuming a single-element descending range must return an empty page",
        "single-element empty resume must not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_multi_page_has_no_duplicate_or_omission() {
    setup_pagination_test();

    let rows = [
        (21_401, 10, "A"),
        (21_402, 20, "B"),
        (21_403, 30, "C"),
        (21_404, 40, "D"),
        (21_405, 50, "E"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("multi-page desc page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("multi-page desc page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_405), Ulid::from_u128(21_404)],
        "descending page1 should return E, D"
    );

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("multi-page desc page1 should emit continuation cursor");
    let page2_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            page1_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("multi-page desc page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("multi-page desc page2 should execute");
    assert_eq!(
        ids_from_items(&page2.items.0),
        vec![Ulid::from_u128(21_403), Ulid::from_u128(21_402)],
        "descending page2 should return C, B"
    );

    let page2_cursor = page2
        .next_cursor
        .as_ref()
        .expect("multi-page desc page2 should emit continuation cursor");
    let page3_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page3 plan should build");
    let page3_boundary = page3_plan
        .plan_cursor(Some(
            page2_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("multi-page desc page3 boundary should plan");
    let page3 = load
        .execute_paged_with_cursor(page3_plan, page3_boundary)
        .expect("multi-page desc page3 should execute");
    assert_eq!(
        ids_from_items(&page3.items.0),
        vec![Ulid::from_u128(21_401)],
        "descending page3 should return A"
    );
    assert!(
        page3.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor"
    );

    let mut all_ids = ids_from_items(&page1.items.0);
    all_ids.extend(ids_from_items(&page2.items.0));
    all_ids.extend(ids_from_items(&page3.items.0));

    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_405),
            Ulid::from_u128(21_404),
            Ulid::from_u128(21_403),
            Ulid::from_u128(21_402),
            Ulid::from_u128(21_401),
        ],
        "descending pagination must not omit rows and must preserve strict order",
    );
    let unique_ids: BTreeSet<Ulid> = all_ids.iter().copied().collect();
    assert_eq!(
        unique_ids.len(),
        all_ids.len(),
        "descending pagination must not duplicate rows"
    );
}

#[test]
fn load_single_field_desc_range_mixed_edges_resume_inside_duplicate_group() {
    setup_pagination_test();

    let rows = [
        (21_501, 10, "t10-a"),
        (21_502, 10, "t10-b"),
        (21_503, 20, "t20-a"),
        (21_504, 20, "t20-b"),
        (21_505, 30, "t30-a"),
        (21_506, 30, "t30-b"),
        (21_507, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    // Mixed envelope: (10, 30] => includes 20 and 30 groups.
    let predicate = Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gt, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lte, Value::Uint(30)),
    ]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let base_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field mixed-edge desc base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("single-field mixed-edge desc base page should execute");
    let all_ids = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_505),
            Ulid::from_u128(21_506),
            Ulid::from_u128(21_503),
            Ulid::from_u128(21_504),
        ],
        "descending mixed-edge range should preserve duplicate-group order with canonical PK tie-break",
    );

    // Boundary inside the upper duplicate group should resume strictly to the
    // next duplicate row, then continue through lower groups.
    let boundary_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by_desc("tag")
                .limit(10)
        },
        boundary_entity,
        all_ids[1..].to_vec(),
    );

    // Boundary at the terminal row should exhaust the descending range.
    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    strict_compare_predicate("tag", CompareOp::Gt, Value::Uint(10)),
                    strict_compare_predicate("tag", CompareOp::Lte, Value::Uint(30)),
                ]))
                .order_by_desc("tag")
                .limit(10)
        },
        terminal_entity,
        "descending mixed-edge range should be exhausted after the lower-edge terminal boundary",
        "empty descending mixed-edge continuation page must not emit a cursor",
    );
}

#[test]
fn load_composite_desc_range_mixed_edges_resume_inside_duplicate_group() {
    setup_pagination_test();

    let rows = [
        (21_601, 7, 10, "g7-r10-a"),
        (21_602, 7, 10, "g7-r10-b"),
        (21_603, 7, 20, "g7-r20"),
        (21_604, 7, 30, "g7-r30-a"),
        (21_605, 7, 30, "g7-r30-b"),
        (21_606, 7, 40, "g7-r40"),
        (21_607, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);

    // Mixed envelope for the ranged component: (10, 30] under one prefix.
    let predicate = Predicate::And(vec![
        strict_compare_predicate("group", CompareOp::Eq, Value::Uint(7)),
        strict_compare_predicate("rank", CompareOp::Gt, Value::Uint(10)),
        strict_compare_predicate("rank", CompareOp::Lte, Value::Uint(30)),
    ]);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(10)
        .explain()
        .expect("composite mixed-edge desc explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite mixed-edge desc should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let base_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(10)
        .plan()
        .expect("composite mixed-edge desc base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("composite mixed-edge desc base page should execute");
    let all_ids = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_604),
            Ulid::from_u128(21_605),
            Ulid::from_u128(21_603),
        ],
        "composite descending mixed-edge range should preserve duplicate-group order with canonical PK tie-break",
    );

    // Boundary inside duplicate upper group should continue from the sibling row.
    let boundary_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .limit(10)
        },
        boundary_entity,
        all_ids[1..].to_vec(),
    );

    // Boundary at terminal lower row should exhaust the range.
    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(Predicate::And(vec![
                    strict_compare_predicate("group", CompareOp::Eq, Value::Uint(7)),
                    strict_compare_predicate("rank", CompareOp::Gt, Value::Uint(10)),
                    strict_compare_predicate("rank", CompareOp::Lte, Value::Uint(30)),
                ]))
                .order_by_desc("rank")
                .limit(10)
        },
        terminal_entity,
        "composite descending mixed-edge range should be exhausted after the lower-edge terminal boundary",
        "composite empty descending mixed-edge continuation page must not emit a cursor",
    );
}

#[test]
fn load_single_field_range_pushdown_parity_matrix_is_table_driven() {
    const GT_10: RangeBounds = &[(CompareOp::Gt, 10)];
    const GTE_10: RangeBounds = &[(CompareOp::Gte, 10)];
    const LT_30: RangeBounds = &[(CompareOp::Lt, 30)];
    const LTE_30: RangeBounds = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lt, 30)];
    const GT_10_LTE_30: RangeBounds = &[(CompareOp::Gt, 10), (CompareOp::Lte, 30)];
    const BETWEEN_10_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_40_NO_MATCH: RangeBounds = &[(CompareOp::Gt, 40)];
    const LTE_40_ALL: RangeBounds = &[(CompareOp::Lte, 40)];

    let cases = [
        RangeMatrixCase {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_lt_window",
            bounds: GTE_10_LT_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "no_match",
            bounds: GT_40_NO_MATCH,
            descending: false,
        },
        RangeMatrixCase {
            name: "all_rows",
            bounds: LTE_40_ALL,
            descending: false,
        },
    ];

    setup_pagination_test();
    let rows = [
        (23_001, 0, "t0"),
        (23_002, 10, "t10-a"),
        (23_003, 10, "t10-b"),
        (23_004, 20, "t20"),
        (23_005, 30, "t30"),
        (23_006, 40, "t40"),
    ];
    run_range_pushdown_parity_matrix::<IndexedMetricsEntity, IndexedMetricsSeedRow>(
        &rows,
        &cases,
        seed_indexed_metrics_rows,
        |bounds| predicate_from_field_bounds("tag", bounds),
        indexed_metrics_ids_for_bounds,
        "tag",
        INDEXED_METRICS_INDEX_MODELS[0].name,
        0,
        "single-field",
    );
}

#[test]
fn load_composite_range_pushdown_parity_matrix_is_table_driven() {
    const GT_10: RangeBounds = &[(CompareOp::Gt, 10)];
    const GTE_10: RangeBounds = &[(CompareOp::Gte, 10)];
    const LT_30: RangeBounds = &[(CompareOp::Lt, 30)];
    const LTE_30: RangeBounds = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_40: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lt, 40)];
    const GT_10_LTE_40: RangeBounds = &[(CompareOp::Gt, 10), (CompareOp::Lte, 40)];
    const BETWEEN_10_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_50_NO_MATCH: RangeBounds = &[(CompareOp::Gt, 50)];
    const LTE_50_ALL: RangeBounds = &[(CompareOp::Lte, 50)];

    let cases = [
        RangeMatrixCase {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_lt_window",
            bounds: GTE_10_LT_40,
            descending: false,
        },
        RangeMatrixCase {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_40,
            descending: true,
        },
        RangeMatrixCase {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "no_match",
            bounds: GT_50_NO_MATCH,
            descending: false,
        },
        RangeMatrixCase {
            name: "all_rows",
            bounds: LTE_50_ALL,
            descending: false,
        },
    ];

    setup_pagination_test();
    let rows = [
        (24_001, 7, 0, "g7-r0"),
        (24_002, 7, 10, "g7-r10-a"),
        (24_003, 7, 10, "g7-r10-b"),
        (24_004, 7, 20, "g7-r20"),
        (24_005, 7, 30, "g7-r30"),
        (24_006, 7, 40, "g7-r40"),
        (24_007, 8, 15, "g8-r15"),
        (24_008, 7, 50, "g7-r50"),
    ];
    run_range_pushdown_parity_matrix::<PushdownParityEntity, PushdownSeedRow>(
        &rows,
        &cases,
        seed_pushdown_rows,
        |bounds| predicate_from_group_rank_bounds(7, bounds),
        |seed_rows, bounds| pushdown_ids_for_group_rank_bounds(seed_rows, 7, bounds),
        "rank",
        PUSHDOWN_PARITY_INDEX_MODELS[0].name,
        1,
        "composite",
    );
}

#[test]
fn load_composite_between_cursor_boundaries_respect_duplicate_lower_and_upper_edges() {
    setup_pagination_test();

    let rows = [
        (25_001, 7, 10, "g7-r10-a"),
        (25_002, 7, 10, "g7-r10-b"),
        (25_003, 7, 20, "g7-r20"),
        (25_004, 7, 30, "g7-r30-a"),
        (25_005, 7, 30, "g7-r30-b"),
        (25_006, 7, 40, "g7-r40"),
        (25_007, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_between_equivalent_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(10)
        .explain()
        .expect("composite duplicate-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite duplicate-edge boundary test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: collect the full ranged row set and verify expected window size.
    let base_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(10)
        .plan()
        .expect("composite duplicate-edge base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("composite duplicate-edge base page should execute");
    let all_ids: Vec<Ulid> = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids.len(),
        5,
        "composite between range should include duplicate lower and upper edge rows"
    );

    // Phase 2: boundary at the first lower-edge row should skip only that row.
    let lower_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(10)
        },
        lower_entity,
        all_ids[1..].to_vec(),
    );

    // Phase 3: mid-window boundary should resume at the next strict row.
    let mid_entity = &base_page.items.0[2].1;
    assert_resume_after_entity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(10)
        },
        mid_entity,
        all_ids[3..].to_vec(),
    );

    // Phase 4: boundary at the terminal upper-edge row should produce an empty continuation page.
    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    assert_resume_from_terminal_entity_exhausts_range(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_rank_between_equivalent_predicate(7, 10, 30))
                .order_by("rank")
                .limit(10)
        },
        terminal_entity,
        "boundary at upper-edge terminal row should return an empty continuation page",
        "composite empty continuation page should not emit a cursor",
    );
}

#[test]
fn load_trace_marks_secondary_order_pushdown_outcomes() {
    #[derive(Clone, Copy)]
    enum ExpectedDecision {
        Accepted,
        RejectedMixedDirection,
    }

    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        prefix: u128,
        order: [(&'static str, OrderDirection); 2],
        expected: ExpectedDecision,
    }

    let cases = [
        Case {
            name: "accepted_ascending",
            prefix: 16_000,
            order: [("rank", OrderDirection::Asc), ("id", OrderDirection::Asc)],
            expected: ExpectedDecision::Accepted,
        },
        Case {
            name: "rejected_descending",
            prefix: 17_000,
            order: [("rank", OrderDirection::Desc), ("id", OrderDirection::Asc)],
            expected: ExpectedDecision::RejectedMixedDirection,
        },
        Case {
            name: "accepted_descending_with_explicit_pk_desc",
            prefix: 18_000,
            order: [("rank", OrderDirection::Desc), ("id", OrderDirection::Desc)],
            expected: ExpectedDecision::Accepted,
        },
    ];

    setup_pagination_test();

    for case in cases {
        reset_store();
        seed_pushdown_rows(&pushdown_rows_trace(case.prefix));

        let predicate = pushdown_group_predicate(7);
        let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate)
            .limit(1);
        for (field, direction) in case.order {
            query = match direction {
                OrderDirection::Asc => query.order_by(field),
                OrderDirection::Desc => query.order_by_desc(field),
            };
        }

        let plan = query
            .plan()
            .expect("trace outcome test plan should build for case");

        let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
        let (_page, trace) = load
            .execute_paged_with_cursor_traced(plan, None)
            .expect("trace outcome execution should succeed for case");
        let trace = trace.expect("debug trace should be present");

        let matched = match case.expected {
            ExpectedDecision::Accepted => {
                trace.optimization == Some(ExecutionOptimization::SecondaryOrderPushdown)
            }
            ExpectedDecision::RejectedMixedDirection => trace.optimization.is_none(),
        };

        assert!(
            matched,
            "trace should emit expected secondary-order pushdown outcome for case '{}'",
            case.name
        );
    }
}

#[test]
fn load_trace_marks_composite_index_range_pushdown_rejection_outcome() {
    setup_pagination_test();
    seed_pushdown_rows(&pushdown_rows_trace(22_000));

    let logical = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::IndexRange {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                prefix: vec![Value::Uint(7)],
                lower: std::ops::Bound::Included(Value::Uint(10)),
                upper: std::ops::Bound::Excluded(Value::Uint(20)),
            }),
            AccessPlan::path(AccessPath::FullScan),
        ]),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        distinct: false,
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(1),
            offset: 0,
        }),
        consistency: ReadConsistency::MissingOk,
    };
    let plan = ExecutablePlan::<PushdownParityEntity>::new(logical);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite-index-range trace test execution should succeed");
    let trace = trace.expect("debug trace should be present");
    let matched = trace.optimization.is_none();
    assert!(
        matched,
        "composite access with index-range child should not emit secondary-order pushdown traces"
    );
}
