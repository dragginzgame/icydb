use super::TIER_C_MERGED_MAX_ARTIFACT_BYTES;

use crate::{
    ExecutionAccess, ExecutionCovering, ObservedExecutionFacts, SQL_SCHEDULED_SHARD_COUNT,
    TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS, TierCEvidenceError, TierCMergedReport,
    TierCScenarioObservation, TierCScenarioOutcome, TierCShardReport,
    generate_scheduled_select_case, scheduled::TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
    scheduled_select_witnesses, scheduled_sql_scenario_shard,
};

#[test]
fn exact_eight_shard_reports_merge_and_round_trip_canonically() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let reports = reports_for(&declared, |shard_index| {
        if shard_index == 3 {
            TierCScenarioOutcome::ExpectedRejection
        } else {
            TierCScenarioOutcome::Passed
        }
    });
    let merged =
        TierCMergedReport::try_merge(&declared, reports).expect("exact receipts should merge");

    assert_eq!(merged.observed_scenario_count(), 8);
    assert_eq!(merged.passed_scenario_count(), 7);
    assert_eq!(merged.expected_rejection_count(), 1);
    assert_eq!(merged.failed_scenario_count(), 0);
    assert!(merged.is_clean());
    merged.require_clean().expect("clean evidence should pass");

    let encoded = merged
        .to_canonical_json(&declared)
        .expect("merged report should encode");
    let decoded = TierCMergedReport::from_canonical_json(encoded.as_slice(), &declared)
        .expect("merged report should decode");
    assert_eq!(decoded, merged);

    for report in merged.shard_reports() {
        let encoded = report
            .to_canonical_json(&declared)
            .expect("shard report should encode");
        let decoded = TierCShardReport::from_canonical_json(encoded.as_slice(), &declared)
            .expect("shard report should decode");
        assert_eq!(&decoded, report);
    }
}

#[test]
fn scenario_declaration_order_does_not_change_receipt_identity() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let reversed = declared.iter().copied().rev().collect::<Vec<_>>();
    let observations = observations_for_shard(0, &declared, TierCScenarioOutcome::Passed);

    let first = TierCShardReport::try_new(0, &declared, observations.clone())
        .expect("forward declaration should validate");
    let second = TierCShardReport::try_new(0, &reversed, observations)
        .expect("reversed declaration should validate");

    assert_eq!(first, second);
}

#[test]
fn tier_c_scenario_set_identity_has_a_fixed_golden_vector() {
    let forward = super::scenario_set_hash(&["scenario.a", "scenario.b"])
        .expect("distinct scenario IDs should hash");
    let reversed = super::scenario_set_hash(&["scenario.b", "scenario.a"])
        .expect("scenario declaration order should not change identity");

    assert_eq!(forward, reversed);
    assert_eq!(
        forward,
        "eb8df215669ca3f5f36225a8a54910f26e3e1c44590e4b0a5ac65c98365bdc23",
    );
}

#[test]
fn shard_report_rejects_missing_duplicate_and_misassigned_observations() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(matches!(
        TierCShardReport::try_new(0, &declared, Vec::new()),
        Err(TierCEvidenceError::ScenarioMembershipMismatch { .. })
    ));

    let mut duplicate = observations_for_shard(0, &declared, TierCScenarioOutcome::Passed);
    duplicate.push(duplicate[0].clone());
    assert!(matches!(
        TierCShardReport::try_new(0, &declared, duplicate),
        Err(TierCEvidenceError::DuplicateScenarioId(_))
    ));

    let misassigned = vec![
        TierCScenarioObservation::try_new(
            scenario_ids[1].clone(),
            test_signature(),
            test_execution_facts(),
            TierCScenarioOutcome::Passed,
        )
        .expect("observation should construct"),
    ];
    assert!(matches!(
        TierCShardReport::try_new(0, &declared, misassigned),
        Err(TierCEvidenceError::ScenarioAssignedToDifferentShard { .. })
    ));
}

#[test]
fn failed_evidence_remains_reportable_but_cannot_pass() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(matches!(
        TierCScenarioObservation::try_new(
            scenario_ids[5].clone(),
            test_signature(),
            test_execution_facts(),
            TierCScenarioOutcome::Failed("not-content-addressed".to_string()),
        ),
        Err(TierCEvidenceError::InvalidFailureArtifactId)
    ));
    let reports = reports_for(&declared, |shard_index| {
        if shard_index == 5 {
            TierCScenarioOutcome::Failed(format!("failure.{}", "5".repeat(64)))
        } else {
            TierCScenarioOutcome::Passed
        }
    });
    let merged = TierCMergedReport::try_merge(&declared, reports)
        .expect("complete failed evidence should still merge for reporting");

    assert_eq!(merged.failed_scenario_count(), 1);
    assert!(!merged.is_clean());
    assert!(matches!(
        merged.require_clean(),
        Err(TierCEvidenceError::FailedEvidence {
            failed_scenario_count: 1
        })
    ));
}

#[test]
fn strict_decode_rejects_unknown_tampered_and_oversized_artifacts() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let report = TierCShardReport::try_new(
        0,
        &declared,
        observations_for_shard(0, &declared, TierCScenarioOutcome::Passed),
    )
    .expect("report should construct");
    let encoded = report
        .to_canonical_json(&declared)
        .expect("report should encode");

    let mut value = serde_json::from_slice::<serde_json::Value>(&encoded)
        .expect("canonical report should be JSON");
    value
        .as_object_mut()
        .expect("report should be an object")
        .insert("unknown".to_string(), serde_json::Value::Bool(true));
    let unknown = serde_json::to_vec(&value).expect("tampered report should encode");
    assert!(matches!(
        TierCShardReport::from_canonical_json(&unknown, &declared),
        Err(TierCEvidenceError::Decode { .. })
    ));

    value
        .as_object_mut()
        .expect("report should be an object")
        .remove("unknown");
    value
        .as_object_mut()
        .expect("report should be an object")
        .insert(
            "passed_scenario_count".to_string(),
            serde_json::Value::from(99_u32),
        );
    let tampered =
        crate::replay::canonical_json_bytes(&value).expect("tampered report should canonicalize");
    assert!(matches!(
        TierCShardReport::from_canonical_json(&tampered, &declared),
        Err(TierCEvidenceError::ShardReportDrift(0))
    ));

    let oversized = vec![b' '; TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES + 1];
    assert!(matches!(
        TierCShardReport::from_canonical_json(&oversized, &declared),
        Err(TierCEvidenceError::ArtifactTooLarge { .. })
    ));
}

#[test]
fn merge_rejects_missing_and_duplicate_shard_reports() {
    let scenario_ids = one_scenario_per_shard();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let mut reports = reports_for(&declared, |_| TierCScenarioOutcome::Passed);
    reports.pop();
    assert!(matches!(
        TierCMergedReport::try_merge(&declared, reports),
        Err(TierCEvidenceError::ShardReportCountMismatch {
            expected: 8,
            actual: 7
        })
    ));

    let mut reports = reports_for(&declared, |_| TierCScenarioOutcome::Passed);
    reports[7] = reports[0].clone();
    assert!(matches!(
        TierCMergedReport::try_merge(&declared, reports),
        Err(TierCEvidenceError::DuplicateShardReport(0))
    ));
}

#[test]
fn merged_artifacts_reject_oversized_encoding_and_input() {
    let scenario_ids = (0..32)
        .map(|index| format!("scenario.{index}.{}", "x".repeat(65_536)))
        .collect::<Vec<_>>();
    let declared = scenario_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let merged = TierCMergedReport::try_merge(
        &declared,
        reports_for(&declared, |_| TierCScenarioOutcome::Passed),
    )
    .expect("complete synthetic evidence should merge before byte admission");
    assert!(matches!(
        merged.to_canonical_json(&declared),
        Err(TierCEvidenceError::ArtifactTooLarge { observed_bytes, maximum_bytes })
            if observed_bytes > TIER_C_MERGED_MAX_ARTIFACT_BYTES
                && maximum_bytes == TIER_C_MERGED_MAX_ARTIFACT_BYTES
    ));

    let mut input = vec![b' '; TIER_C_MERGED_MAX_ARTIFACT_BYTES];
    assert!(matches!(
        TierCMergedReport::from_canonical_json(&input, &declared),
        Err(TierCEvidenceError::Decode { .. })
    ));
    input.push(b' ');
    assert!(matches!(
        TierCMergedReport::from_canonical_json(&input, &declared),
        Err(TierCEvidenceError::ArtifactTooLarge { observed_bytes, maximum_bytes })
            if observed_bytes == TIER_C_MERGED_MAX_ARTIFACT_BYTES + 1
                && maximum_bytes == TIER_C_MERGED_MAX_ARTIFACT_BYTES
    ));
}

fn reports_for(
    declared: &[&str],
    outcome: impl Fn(u8) -> TierCScenarioOutcome,
) -> Vec<TierCShardReport> {
    (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            TierCShardReport::try_new(
                shard_index,
                declared,
                observations_for_shard(shard_index, declared, outcome(shard_index)),
            )
            .expect("complete shard report should construct")
        })
        .collect()
}

fn observations_for_shard(
    shard_index: u8,
    declared: &[&str],
    outcome: TierCScenarioOutcome,
) -> Vec<TierCScenarioObservation> {
    declared
        .iter()
        .filter(|scenario_id| {
            scheduled_sql_scenario_shard(scenario_id).expect("test scenario ID should shard")
                == shard_index
        })
        .map(|scenario_id| {
            TierCScenarioObservation::try_new(
                (*scenario_id).to_string(),
                test_signature(),
                test_execution_facts(),
                outcome.clone(),
            )
            .expect("test observation should construct")
        })
        .collect()
}

fn one_scenario_per_shard() -> Vec<String> {
    (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            (0_u32..10_000)
                .map(|candidate| format!("scenario.shard_{shard_index}.{candidate}"))
                .find(|scenario_id| {
                    scheduled_sql_scenario_shard(scenario_id)
                        .expect("test scenario ID should shard")
                        == shard_index
                })
                .expect("each fixed shard should receive a finite test identity")
        })
        .collect()
}

fn test_signature() -> crate::StructuralSignature {
    let witness = scheduled_select_witnesses()
        .expect("checked-in witnesses should decode")
        .into_iter()
        .next()
        .expect("catalog should contain a generated SELECT witness");
    generate_scheduled_select_case(&witness, TIER_C_ROOT_SEEDS[0], 0, TIER_C_SELECT_BUDGETS)
        .expect("test signature should derive")
        .structural_signature()
        .expect("test structural signature should derive")
}

const fn test_execution_facts() -> ObservedExecutionFacts {
    ObservedExecutionFacts::new(ExecutionAccess::FullScan, ExecutionCovering::NonCovering)
}
