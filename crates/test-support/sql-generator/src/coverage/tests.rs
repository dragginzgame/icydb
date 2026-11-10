use super::TierCGeneratedProfile;
use crate::{
    EligibleProvider, EvidenceStrength, ExecutionAccess, ExecutionCovering,
    GeneratedExpressionDepth, MutationKind, NullabilityClass, ObservedExecutionFacts,
    PredicateFamily, QueryShape, RequiredExecutionFacts, RouteFamily, SQL_SCHEDULED_SHARD_COUNT,
    StatementFamily, TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS, TierCCoverageDistributionReport,
    TierCCoverageLabels, TierCDistributionError, TierCExpectedAcceptance, TierCMergedReport,
    TierCScenarioDeclaration, TierCScenarioObservation, TierCScenarioOutcome, TierCShardReport,
    ValueTypeFamily, WindowBehavior, generate_scheduled_select_case,
    generated_select_tier_c_declaration, scheduled_select_witnesses, scheduled_sql_scenario_shard,
};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn multi_label_declarations_project_each_exercised_mutation_family() {
    let scenario_ids = one_scenario_per_shard();
    let declarations = scenario_ids
        .iter()
        .map(|scenario_id| mutation_declaration(scenario_id))
        .collect::<Vec<_>>();
    let merged = merged_receipts(&declarations);
    let report = TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged)
        .expect("exact clean declarations should project");

    assert_eq!(report.scenario_count(), 8);
    assert_eq!(report.statement_count(StatementFamily::Insert), 8);
    assert_eq!(report.statement_count(StatementFamily::Update), 8);
    assert_eq!(report.statement_count(StatementFamily::Delete), 8);
    assert_eq!(report.mutation_count(MutationKind::Insert), 8);
    assert_eq!(report.mutation_count(MutationKind::Update), 8);
    assert_eq!(report.mutation_count(MutationKind::Delete), 8);
    assert_eq!(
        report.generated_expression_depth_count(GeneratedExpressionDepth::NotApplicable),
        8,
    );
    assert_eq!(report.generated_mutation_fixture_row_count(4), 8);
    assert_eq!(report.generated_mutation_statement_count(8), 8);
    assert_eq!(
        report.generated_schema_fixture_family_count("test-mutation-snapshot-v1"),
        8,
    );

    let encoded = report
        .to_canonical_json(&declarations, &merged)
        .expect("distribution should encode canonically");
    let decoded = TierCCoverageDistributionReport::from_canonical_json(
        encoded.as_slice(),
        &declarations,
        &merged,
    )
    .expect("distribution should decode canonically");
    assert_eq!(decoded, report);
}

#[test]
fn distribution_rejects_inconsistent_operation_labels() {
    let labels = TierCCoverageLabels::try_new(
        BTreeSet::from([EvidenceStrength::ReferenceOracle]),
        BTreeSet::from([GeneratedExpressionDepth::NotApplicable]),
        BTreeSet::from([MutationKind::Insert]),
        BTreeSet::from([NullabilityClass::NonNullable]),
        BTreeSet::from([PredicateFamily::None]),
        BTreeSet::from([EligibleProvider::StateModelReference]),
        BTreeSet::from([RouteFamily::NotContractual]),
        BTreeSet::from([QueryShape::Mutation]),
        BTreeSet::from([StatementFamily::Delete]),
        BTreeSet::from([ValueTypeFamily::Mixed]),
        BTreeSet::from([WindowBehavior::None]),
    );

    assert!(matches!(
        labels,
        Err(TierCDistributionError::InconsistentMutationLabels)
    ));
}

#[test]
fn unexpected_observed_signature_cannot_satisfy_a_required_declaration() {
    let witnesses = scheduled_select_witnesses().expect("schedule should derive");
    let witness = &witnesses[0];
    let different_signature = generate_scheduled_select_case(
        &witnesses[1],
        TIER_C_ROOT_SEEDS[0],
        0,
        TIER_C_SELECT_BUDGETS,
    )
    .expect("different scheduled signature should derive")
    .structural_signature()
    .expect("different structural signature should derive");
    let generated =
        generate_scheduled_select_case(witness, TIER_C_ROOT_SEEDS[0], 0, TIER_C_SELECT_BUDGETS)
            .expect("scheduled SELECT should generate");
    let declaration = generated_select_tier_c_declaration(witness.witness_id(), &generated)
        .expect("generated declaration should derive");
    let declarations = vec![declaration];
    let declared = vec![witness.witness_id()];
    let assigned_shard =
        scheduled_sql_scenario_shard(witness.witness_id()).expect("scheduled witness should shard");
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = if shard_index == assigned_shard {
                vec![
                    TierCScenarioObservation::try_new(
                        witness.witness_id(),
                        different_signature.clone(),
                        witness.required_execution_facts().into(),
                        TierCScenarioOutcome::Passed,
                    )
                    .expect("different full signature should remain valid discovery evidence"),
                ]
            } else {
                Vec::new()
            };
            TierCShardReport::try_new(shard_index, &declared, observations)
                .expect("shard membership should remain exact")
        })
        .collect();
    let merged = TierCMergedReport::try_merge(&declared, reports).expect("receipts should merge");

    assert!(matches!(
        TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged),
        Err(TierCDistributionError::StructuralSignatureMismatch(scenario_id))
            if scenario_id == witness.witness_id()
    ));
}

fn mutation_declaration(scenario_id: &str) -> TierCScenarioDeclaration {
    let labels = TierCCoverageLabels::try_new(
        BTreeSet::from([EvidenceStrength::ReferenceOracle]),
        BTreeSet::from([GeneratedExpressionDepth::NotApplicable]),
        BTreeSet::from([
            MutationKind::Delete,
            MutationKind::Insert,
            MutationKind::Update,
        ]),
        BTreeSet::from([NullabilityClass::NonNullable]),
        BTreeSet::from([PredicateFamily::PrimaryKey, PredicateFamily::Range]),
        BTreeSet::from([
            EligibleProvider::SqliteReference,
            EligibleProvider::StateModelReference,
        ]),
        BTreeSet::from([RouteFamily::NotContractual]),
        BTreeSet::from([QueryShape::Mutation]),
        BTreeSet::from([
            StatementFamily::Delete,
            StatementFamily::Insert,
            StatementFamily::Update,
        ]),
        BTreeSet::from([ValueTypeFamily::Mixed]),
        BTreeSet::from([WindowBehavior::None, WindowBehavior::OrderedLimit]),
    )
    .expect("test labels should align");
    TierCScenarioDeclaration::try_new_generated(
        scenario_id,
        BTreeSet::from([
            "mutation.delete".to_string(),
            "mutation.insert".to_string(),
            "mutation.update".to_string(),
        ]),
        BTreeSet::from([
            "sqlite.generated_mutation".to_string(),
            "state_model.generated_mutation".to_string(),
        ]),
        TierCExpectedAcceptance::Accepted,
        labels,
        TierCGeneratedProfile::Mutation {
            fixture_row_count: 4,
            ingress: "sql_and_typed".to_string(),
            intent_class: "authored".to_string(),
            intent_counts: BTreeMap::from([("authored".to_string(), 3)]),
            schema_field_count: 3,
            schema_fixture_family: "test-mutation-snapshot-v1".to_string(),
            statement_count: 8,
            structural_signature_digest: "test-mutation-signature".to_string(),
        },
        Some(test_signature()),
        RequiredExecutionFacts::new(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
    )
    .expect("test declaration should validate")
}

fn merged_receipts(declarations: &[TierCScenarioDeclaration]) -> TierCMergedReport {
    let declared = declarations
        .iter()
        .map(TierCScenarioDeclaration::scenario_id)
        .collect::<Vec<_>>();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = declared
                .iter()
                .filter(|scenario_id| {
                    scheduled_sql_scenario_shard(scenario_id).expect("test scenario should shard")
                        == shard_index
                })
                .map(|scenario_id| {
                    TierCScenarioObservation::try_new(
                        *scenario_id,
                        test_signature(),
                        test_execution_facts(),
                        TierCScenarioOutcome::Passed,
                    )
                    .expect("test observation should validate")
                })
                .collect();
            TierCShardReport::try_new(shard_index, &declared, observations)
                .expect("test shard should validate")
        })
        .collect();
    TierCMergedReport::try_merge(&declared, reports).expect("test merge should validate")
}

fn one_scenario_per_shard() -> Vec<String> {
    let mut by_shard = vec![None; usize::from(SQL_SCHEDULED_SHARD_COUNT)];
    for candidate in 0_u32..100_000 {
        let scenario_id = format!("distribution.scenario.{candidate}");
        let shard =
            scheduled_sql_scenario_shard(scenario_id.as_str()).expect("test scenario should shard");
        by_shard[usize::from(shard)].get_or_insert(scenario_id);
        if by_shard.iter().all(Option::is_some) {
            break;
        }
    }

    by_shard
        .into_iter()
        .map(|scenario_id| scenario_id.expect("search should populate every shard"))
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
