use super::*;

fn seed_simple_entities(ids: &[u128]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in ids {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("seed row save should succeed");
    }
}

fn seed_pushdown_entities(rows: &[(u128, u32, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in rows {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(*id),
            group: *group,
            rank: *rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }
}

fn seed_unique_index_range_entities(rows: &[(u128, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    for (id, code) in rows {
        save.insert(UniqueIndexRangeEntity {
            id: Ulid::from_u128(*id),
            code: *code,
            label: format!("code-{code}"),
        })
        .expect("seed unique-index row save should succeed");
    }
}

fn assert_aggregate_parity_for_query<E>(
    load: &LoadExecutor<E>,
    make_query: impl Fn() -> Query<E>,
    context: &str,
) where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    // Execute canonical materialized baseline once per query shape.
    let expected_response = load
        .execute(
            make_query()
                .plan()
                .expect("baseline materialized plan should build"),
        )
        .expect("baseline materialized execution should succeed");
    let expected_count = expected_response.count();
    let expected_exists = !expected_response.is_empty();
    let expected_min = expected_response.ids().into_iter().min();
    let expected_max = expected_response.ids().into_iter().max();

    // Execute aggregate terminals against the same logical query shape.
    let actual_count = load
        .aggregate_count(
            make_query()
                .plan()
                .expect("aggregate COUNT plan should build"),
        )
        .expect("aggregate COUNT should succeed");
    let actual_exists = load
        .aggregate_exists(
            make_query()
                .plan()
                .expect("aggregate EXISTS plan should build"),
        )
        .expect("aggregate EXISTS should succeed");
    let actual_min = load
        .aggregate_min(
            make_query()
                .plan()
                .expect("aggregate MIN plan should build"),
        )
        .expect("aggregate MIN should succeed");
    let actual_max = load
        .aggregate_max(
            make_query()
                .plan()
                .expect("aggregate MAX plan should build"),
        )
        .expect("aggregate MAX should succeed");

    assert_eq!(
        actual_count, expected_count,
        "{context}: count parity failed"
    );
    assert_eq!(
        actual_exists, expected_exists,
        "{context}: exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "{context}: min parity failed");
    assert_eq!(actual_max, expected_max, "{context}: max parity failed");
}

fn id_in_predicate(ids: &[u128]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(
            ids.iter()
                .copied()
                .map(|id| Value::Ulid(Ulid::from_u128(id)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::NumericWiden,
    ))
}

fn u32_range_predicate(field: &str, lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
            CoercionId::NumericWiden,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
            CoercionId::NumericWiden,
        )),
    ])
}

#[test]
fn aggregate_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(3)
        },
        "ordered ASC page window",
    );
}

#[test]
fn aggregate_parity_ordered_page_window_desc() {
    seed_simple_entities(&[8201, 8202, 8203, 8204, 8205, 8206, 8207, 8208]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        },
        "ordered DESC page window",
    );
}

#[test]
fn aggregate_parity_by_id_and_by_ids_paths() {
    seed_simple_entities(&[8601, 8602, 8603, 8604]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_id(Ulid::from_u128(8602)),
        "by_id path",
    );

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_ids([
                Ulid::from_u128(8604),
                Ulid::from_u128(8601),
                Ulid::from_u128(8604),
            ])
        },
        "by_ids path",
    );
}

#[test]
fn aggregate_parity_distinct_asc() {
    seed_simple_entities(&[8301, 8302, 8303, 8304, 8305, 8306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8301, 8302, 8303, 8304]),
        id_in_predicate(&[8303, 8304, 8305, 8306]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
                .limit(3)
        },
        "distinct ASC",
    );
}

#[test]
fn aggregate_parity_distinct_desc() {
    seed_simple_entities(&[8401, 8402, 8403, 8404, 8405, 8406]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8401, 8402, 8403, 8404]),
        id_in_predicate(&[8403, 8404, 8405, 8406]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "distinct DESC",
    );
}

#[test]
fn aggregate_parity_union_and_intersection_paths() {
    seed_simple_entities(&[8701, 8702, 8703, 8704, 8705, 8706]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let union_predicate = Predicate::Or(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(union_predicate.clone())
                .order_by("id")
                .offset(1)
                .limit(4)
        },
        "union path",
    );

    let intersection_predicate = Predicate::And(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(intersection_predicate.clone())
                .order_by_desc("id")
                .offset(0)
                .limit(2)
        },
        "intersection path",
    );
}

#[test]
fn aggregate_parity_secondary_index_order_shape() {
    seed_pushdown_entities(&[
        (8801, 7, 40),
        (8802, 7, 10),
        (8803, 7, 30),
        (8804, 7, 20),
        (8805, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape",
    );
}

#[test]
fn aggregate_parity_index_range_shape() {
    seed_unique_index_range_entities(&[
        (8901, 100),
        (8902, 101),
        (8903, 102),
        (8904, 103),
        (8905, 104),
        (8906, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 101, 105);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(range_predicate.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(2)
        },
        "index-range shape",
    );
}

#[test]
fn aggregate_parity_strict_consistency() {
    seed_simple_entities(&[9001, 9002, 9003, 9004, 9005]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::Strict)
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "strict consistency",
    );
}

#[test]
fn aggregate_parity_limit_zero_window() {
    seed_simple_entities(&[9101, 9102, 9103, 9104]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(0)
        },
        "limit zero window",
    );
}

#[test]
fn session_load_aggregate_terminals_match_execute() {
    seed_simple_entities(&[8501, 8502, 8503, 8504, 8505]);
    let session = DbSession::new(DB);

    let expected = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .execute()
        .expect("baseline session execute should succeed");
    let expected_count = expected.count();
    let expected_exists = !expected.is_empty();
    let expected_min = expected.ids().into_iter().min();
    let expected_max = expected.ids().into_iter().max();

    let actual_count = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .count()
        .expect("session count should succeed");
    let actual_exists = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .exists()
        .expect("session exists should succeed");
    let actual_min = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .min()
        .expect("session min should succeed");
    let actual_max = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .max()
        .expect("session max should succeed");

    assert_eq!(actual_count, expected_count, "session count parity failed");
    assert_eq!(
        actual_exists, expected_exists,
        "session exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "session min parity failed");
    assert_eq!(actual_max, expected_max, "session max parity failed");
}
