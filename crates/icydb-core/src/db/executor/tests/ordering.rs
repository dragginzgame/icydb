//! Module: db::executor::tests::ordering
//! Responsibility: module-local ownership and contracts for executor ordering behavior.
//! Does not own: pagination or snapshot serializer behavior outside row ordering.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;

#[test]
fn delete_applies_order_and_delete_limit() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [30_u128, 10_u128, 20_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let delete = DeleteExecutor::<SimpleEntity>::new(DB);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .order_by("id")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("delete plan should build");

    let response = delete.execute(plan).expect("delete should succeed");
    assert_eq!(response.len(), 1, "delete limit should remove one row");
    assert_eq!(
        response[0].entity_ref().id,
        Ulid::from_u128(10),
        "delete limit should run after canonical ordering by id"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let remaining_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should succeed");
    let remaining_ids: Vec<Ulid> = remaining
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        remaining_ids,
        vec![Ulid::from_u128(20), Ulid::from_u128(30)],
        "only the first ordered row should have been deleted"
    );
}

#[test]
fn load_ordering_treats_missing_values_consistently_with_direction() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(902),
            opt_rank: None,
            rank: 2,
            tags: vec![2],
            label: "missing-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(901),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "missing-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(903),
            opt_rank: Some(10),
            rank: 3,
            tags: vec![3],
            label: "present-10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(904),
            opt_rank: Some(20),
            rank: 4,
            tags: vec![4],
            label: "present-20".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let asc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("opt_rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("ascending optional-order plan should build");
    let asc = load
        .execute(asc_plan)
        .expect("ascending optional-order query should execute");
    let asc_ids: Vec<Ulid> = asc.into_iter().map(|row| row.entity_ref().id).collect();
    assert_eq!(
        asc_ids,
        vec![
            Ulid::from_u128(901),
            Ulid::from_u128(902),
            Ulid::from_u128(903),
            Ulid::from_u128(904),
        ],
        "ascending order should treat missing as lowest and use PK tie-break within missing rows"
    );

    let desc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("opt_rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("descending optional-order plan should build");
    let desc = load
        .execute(desc_plan)
        .expect("descending optional-order query should execute");
    let desc_ids: Vec<Ulid> = desc.into_iter().map(|row| row.entity_ref().id).collect();
    assert_eq!(
        desc_ids,
        vec![
            Ulid::from_u128(904),
            Ulid::from_u128(903),
            Ulid::from_u128(901),
            Ulid::from_u128(902),
        ],
        "descending order should reverse present/missing groups while preserving PK tie-break"
    );
}
