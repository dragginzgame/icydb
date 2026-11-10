use crate::{SQL_SCHEDULED_SHARD_COUNT, ScenarioShardError, scheduled_sql_scenario_shard};

#[test]
fn scheduled_sharding_matches_shared_golden_assignments() {
    let assignments = ["scenario.a", "scenario.b", "scenario.c"]
        .map(|id| scheduled_sql_scenario_shard(id).expect("scenario should shard"));

    assert_eq!(SQL_SCHEDULED_SHARD_COUNT, 8);
    assert_eq!(assignments, [0, 6, 7]);
}

#[test]
fn scheduled_sharding_rejects_empty_identity() {
    assert_eq!(
        scheduled_sql_scenario_shard(""),
        Err(ScenarioShardError::EmptyScenarioId)
    );
}
