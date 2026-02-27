use super::*;

#[test]
fn load_composite_pk_budget_trace_limits_access_rows_for_safe_shape() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [37_201_u128, 37_202, 37_203, 37_204, 37_205, 37_206] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("composite budget seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_201);
    let id2 = Ulid::from_u128(37_202);
    let id3 = Ulid::from_u128(37_203);
    let id4 = Ulid::from_u128(37_204);
    let id5 = Ulid::from_u128(37_205);
    let id6 = Ulid::from_u128(37_206);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    };
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(4),
        "safe composite PK-order shape should apply offset+limit+1 scan budget"
    );
    assert_eq!(
        ids_from_items(&page.items.0),
        vec![id2, id3],
        "safe composite budget path must preserve canonical offset/limit page rows"
    );
}

#[test]
fn load_composite_pk_budget_disabled_when_cursor_boundary_present() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [37_301_u128, 37_302, 37_303, 37_304, 37_305, 37_306] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("composite cursor-budget seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_301);
    let id2 = Ulid::from_u128(37_302);
    let id3 = Ulid::from_u128(37_303);
    let id4 = Ulid::from_u128(37_304);
    let id5 = Ulid::from_u128(37_305);
    let id6 = Ulid::from_u128(37_306);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 0,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    };
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);
    let cursor = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Ulid(id3))],
    };

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, Some(cursor))
        .expect("composite cursor trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(6),
        "cursor narrowing is post-access for this shape, so scan budgeting must stay disabled"
    );
}

#[test]
fn load_composite_budget_disabled_when_post_access_sort_is_required() {
    setup_pagination_test();

    let rows = [
        (37_401, 7, 30, "r30"),
        (37_402, 7, 10, "r10-a"),
        (37_403, 7, 20, "r20"),
        (37_404, 7, 10, "r10-b"),
        (37_405, 7, 40, "r40"),
        (37_406, 7, 50, "r50"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(37_401);
    let id2 = Ulid::from_u128(37_402);
    let id3 = Ulid::from_u128(37_403);
    let id4 = Ulid::from_u128(37_404);
    let id5 = Ulid::from_u128(37_405);
    let id6 = Ulid::from_u128(37_406);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    };
    let plan = ExecutablePlan::<PushdownParityEntity>::new(logical);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite post-sort trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(6),
        "post-access sort requirement must disable scan budgeting for composite paths"
    );
}

#[test]
fn load_composite_budget_disabled_for_offset_with_residual_filter() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [37_501_u128, 37_502, 37_503, 37_504, 37_505, 37_506] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("residual-filter budget seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_501);
    let id2 = Ulid::from_u128(37_502);
    let id3 = Ulid::from_u128(37_503);
    let id4 = Ulid::from_u128(37_504);
    let id5 = Ulid::from_u128(37_505);
    let id6 = Ulid::from_u128(37_506);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: Some(strict_compare_predicate(
                "id",
                CompareOp::Gte,
                Value::Ulid(id2),
            )),
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    };
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("offset+filter budget-disable trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(6),
        "residual filter must disable scan budgeting and preserve full access scan volume"
    );
    assert_eq!(
        ids_from_items(&page.items.0),
        vec![id3, id4],
        "offset+filter window should remain canonical under fallback path"
    );
    assert!(
        page.next_cursor.is_some(),
        "offset+filter first page should still emit continuation when more rows remain"
    );
}

#[test]
fn load_composite_pk_budget_trace_limits_access_rows_for_safe_desc_shape() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [37_601_u128, 37_602, 37_603, 37_604, 37_605, 37_606] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("composite desc budget seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_601);
    let id2 = Ulid::from_u128(37_602);
    let id3 = Ulid::from_u128(37_603);
    let id4 = Ulid::from_u128(37_604);
    let id5 = Ulid::from_u128(37_605);
    let id6 = Ulid::from_u128(37_606);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Desc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    };
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite desc budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(4),
        "safe DESC composite PK-order shape should apply offset+limit+1 scan budget"
    );
    assert_eq!(
        ids_from_items(&page.items.0),
        vec![id5, id4],
        "safe DESC composite budget path must preserve canonical offset/limit page rows"
    );
}

#[test]
fn load_nested_composite_pk_budget_trace_limits_access_rows_for_safe_shape() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [
        37_701_u128,
        37_702,
        37_703,
        37_704,
        37_705,
        37_706,
        37_707,
        37_708,
    ] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("nested composite budget seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_701);
    let id2 = Ulid::from_u128(37_702);
    let id3 = Ulid::from_u128(37_703);
    let id4 = Ulid::from_u128(37_704);
    let id5 = Ulid::from_u128(37_705);
    let id6 = Ulid::from_u128(37_706);
    let id7 = Ulid::from_u128(37_707);
    let id8 = Ulid::from_u128(37_708);

    let logical = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
            ]),
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id7, id8])),
            ]),
        ]),
    };
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("nested composite budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned),
        Some(4),
        "safe nested composite PK-order shape should apply offset+limit+1 scan budget"
    );
    assert_eq!(
        ids_from_items(&page.items.0),
        vec![id4, id5],
        "safe nested composite budget path must preserve canonical page window"
    );
}

#[test]
fn load_composite_budgeted_and_fallback_paths_emit_equivalent_continuation_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [37_801_u128, 37_802, 37_803, 37_804, 37_805, 37_806] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("budget-parity seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_801);
    let id2 = Ulid::from_u128(37_802);
    let id3 = Ulid::from_u128(37_803);
    let id4 = Ulid::from_u128(37_804);
    let id5 = Ulid::from_u128(37_805);
    let id6 = Ulid::from_u128(37_806);

    let budgeted_plan = ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    });
    let fallback_plan = ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: Some(Predicate::And(vec![
                strict_compare_predicate("id", CompareOp::Gte, Value::Ulid(id1)),
                strict_compare_predicate("id", CompareOp::Lte, Value::Ulid(id6)),
            ])),
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: ReadConsistency::MissingOk,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
            AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
        ]),
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (budgeted_page, budgeted_trace) = load
        .execute_paged_with_cursor_traced(budgeted_plan, None)
        .expect("budgeted trace execution should succeed");
    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback trace execution should succeed");

    assert_eq!(
        budgeted_trace.map(|trace| trace.keys_scanned),
        Some(4),
        "budgeted path should cap keys scanned at offset+limit+1"
    );
    assert_eq!(
        fallback_trace.map(|trace| trace.keys_scanned),
        Some(6),
        "residual-filter fallback path should preserve full access scan volume"
    );
    assert_eq!(
        ids_from_items(&budgeted_page.items.0),
        ids_from_items(&fallback_page.items.0),
        "budgeted and fallback paths must emit identical page rows"
    );

    let budgeted_cursor = budgeted_page
        .next_cursor
        .as_ref()
        .expect("budgeted path should emit continuation cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback path should emit continuation cursor");
    assert_eq!(
        budgeted_cursor.boundary().clone(),
        fallback_cursor.boundary().clone(),
        "budgeted and fallback paths should encode the same continuation boundary"
    );
}

#[test]
fn load_composite_union_mixed_direction_fallback_preserves_order_and_pagination() {
    setup_pagination_test();

    let rows = [
        (39_001, 7, 10, "g7-r10"),
        (39_002, 7, 20, "g7-r20-a"),
        (39_003, 7, 20, "g7-r20-b"),
        (39_004, 7, 30, "g7-r30"),
        (39_005, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(39_001);
    let id2 = Ulid::from_u128(39_002);
    let id3 = Ulid::from_u128(39_003);
    let id4 = Ulid::from_u128(39_004);
    let id5 = Ulid::from_u128(39_005);

    let build_plan = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: ReadConsistency::MissingOk,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id5])),
            ]),
        })
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (ids, boundaries) = collect_all_pages_from_executable_plan(&load, build_plan, 10);

    assert_eq!(
        ids,
        vec![id4, id2, id3, id5, id1],
        "mixed-direction union fallback should preserve rank DESC with PK ASC tie-break across pages",
    );
    assert_eq!(
        boundaries.len(),
        2,
        "limit=2 over five rows should emit exactly two continuation boundaries"
    );
}
