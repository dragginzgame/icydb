//! Module: sql_generator::tests
//! Responsibility: structural generation, replay, shrinking, and receipt contracts.
//! Does not own: IcyDB execution or bundled SQLite semantics.
//! Boundary: proves the obligation-driven generator is stable before adapters consume it.

use crate::{
    ALL_SELECT_VIOLATIONS, GeneratedFixtureProperty, REGRESSION_CORPUS_FORMAT_VERSION,
    RegressionCorpusCase, RegressionCorpusEntry, SELECT_GENERATOR_VERSION,
    SELECT_REPLAY_FORMAT_VERSION, SQL_SCHEDULED_SHARD_COUNT, ScheduledSelectWitness,
    SelectComparisonProvider, SelectExecutionPhase, SelectFeature, SelectMismatchCategory,
    SelectMismatchSignature, SelectObservedOutcome, SelectProvider, SelectSchemaProfile,
    StructuralSignature, TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS, TIER_C_INVALID_REPETITIONS,
    TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_REPETITIONS, TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS,
    TIER_C_SELECT_REPETITIONS, TierCCoverageDistributionReport, TierCFailureArtifact,
    TierCMergedReport, TierCScenarioObservation, TierCScenarioOutcome, TierCShardReport,
    generate_invalid_select_case, generate_scheduled_mutation_sequence,
    generate_scheduled_select_case, generated_mutation_tier_c_declaration,
    generated_select_tier_c_declaration,
    rng::{SplitMix64, derive_select_witness_sub_seed},
    scheduled_mutation_witnesses, scheduled_select_witnesses, scheduled_sql_scenario_shard,
    shrink_select_failure, structural_obligation_catalog_hash,
};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn splitmix64_state_transition_has_fixed_golden_vector() {
    let mut rng = SplitMix64::new(0);
    let actual = (0..5).map(|_| rng.next_u64()).collect::<Vec<_>>();

    assert_eq!(
        actual,
        vec![
            0xe220_a839_7b1d_cdaf,
            0x6e78_9e6a_a1b9_65f4,
            0x06c4_5d18_8009_454f,
            0xf88b_b8a8_724c_81ec,
            0x1b39_896a_51a8_749b,
        ],
    );
}

#[test]
fn witness_sub_seed_is_stable_and_independent_of_catalog_order() {
    let witnesses = scheduled_select_witnesses().expect("catalog should decode");
    let first = &witnesses[0];
    let sub_seed = derive_select_witness_sub_seed(
        SELECT_GENERATOR_VERSION,
        TIER_A_ROOT_SEEDS[0],
        first.witness_id(),
        0,
    )
    .expect("fixed witness identity should derive");

    assert_eq!(sub_seed, 0x30eb_dc72_dc79_7307);
    let mut reverse = witnesses.clone();
    reverse.reverse();
    let reversed = reverse
        .iter()
        .find(|candidate| candidate.witness_id() == first.witness_id())
        .expect("reversed catalog should retain the witness");
    assert_eq!(
        derive_select_witness_sub_seed(
            SELECT_GENERATOR_VERSION,
            TIER_A_ROOT_SEEDS[0],
            reversed.witness_id(),
            0,
        )
        .expect("reordered witness should derive"),
        sub_seed,
    );
}

#[test]
fn frozen_catalog_exposes_exact_select_witnesses_and_hash() {
    let witnesses = scheduled_select_witnesses().expect("catalog should decode");
    assert_eq!(witnesses.len(), 17);
    assert_eq!(
        structural_obligation_catalog_hash().expect("catalog hash should decode"),
        "c273d1ce46eda26a1e664ceb47794c21c444d1d5ab90a9c19cc7b6185c92d74a",
    );
    assert_eq!(
        witnesses
            .iter()
            .map(ScheduledSelectWitness::witness_id)
            .collect::<BTreeSet<_>>()
            .len(),
        witnesses.len(),
    );
    assert!(
        witnesses
            .iter()
            .all(|witness| witness.provider_id().starts_with("generated.select."))
    );
}

#[test]
fn structural_signature_round_trip_equality_and_order_are_lossless() {
    let mut signatures = scheduled_select_witnesses()
        .expect("catalog should decode")
        .into_iter()
        .map(|witness| {
            generate_scheduled_select_case(&witness, TIER_C_ROOT_SEEDS[0], 0, TIER_C_SELECT_BUDGETS)
                .expect("scheduled signature should derive")
                .structural_signature()
                .expect("structural signature should derive")
        })
        .collect::<Vec<_>>();
    for signature in &signatures {
        let encoded = signature
            .to_canonical_json()
            .expect("signature should encode canonically");
        assert_eq!(
            StructuralSignature::from_canonical_json(encoded.as_slice())
                .expect("current signature should decode"),
            *signature,
        );
        assert_eq!(signature.digest().expect("digest should derive").len(), 64);
    }
    let mut reversed = signatures.iter().rev().cloned().collect::<Vec<_>>();
    signatures.sort();
    reversed.sort();
    assert_eq!(signatures, reversed);
}

#[test]
fn every_required_select_structure_generates_deterministically() {
    let witnesses = scheduled_select_witnesses().expect("catalog should decode");
    let mut identities = BTreeSet::new();
    let mut signature_counts = BTreeMap::new();
    let mut profile_counts = BTreeMap::new();
    let mut fixture_classes = BTreeSet::new();
    let mut expression_depths = BTreeSet::new();
    for root_seed in TIER_C_ROOT_SEEDS {
        for witness in &witnesses {
            for repetition in 0..TIER_C_SELECT_REPETITIONS {
                let first = generate_scheduled_select_case(
                    witness,
                    *root_seed,
                    repetition,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("required SELECT witness should generate");
                let second = generate_scheduled_select_case(
                    witness,
                    *root_seed,
                    repetition,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("same witness repetition should reproduce");
                assert_eq!(first, second);
                assert!(identities.insert(first.identity().id().to_string()));
                first.validate().expect("generated case should revalidate");
                let structural_signature = first
                    .structural_signature()
                    .expect("structural signature should derive");
                assert_eq!(&structural_signature, witness.signature());
                *signature_counts
                    .entry(
                        structural_signature
                            .digest()
                            .expect("signature digest should derive"),
                    )
                    .or_insert(0_u32) += 1;
                *profile_counts
                    .entry(structural_signature.schema_profile().to_string())
                    .or_insert(0_u32) += 1;
                fixture_classes.insert(
                    crate::generator::fixture_class_for_identity(first.identity().witness_id())
                        .to_string(),
                );
                expression_depths.insert(first.query().max_expression_depth());
            }
        }
    }

    assert_eq!(
        identities.len(),
        TIER_C_ROOT_SEEDS.len()
            * witnesses.len()
            * usize::try_from(TIER_C_SELECT_REPETITIONS).expect("repetitions fit usize"),
    );
    assert_eq!(signature_counts.len(), witnesses.len() - 1);
    assert_eq!(
        signature_counts
            .values()
            .filter(|count| **count == 32)
            .count(),
        1,
        "two separately scheduled entry-path obligations share one typed SELECT structure",
    );
    assert!(
        signature_counts
            .values()
            .all(|count| matches!(*count, 16 | 32))
    );
    assert_eq!(
        profile_counts,
        BTreeMap::from([
            ("indexed_nullable_reference".to_string(), 112),
            ("reference_scalar".to_string(), 160),
        ]),
    );
    assert_eq!(
        fixture_classes,
        BTreeSet::from([
            "computed_null_and_nonnull".to_string(),
            "computed_null_order_ties".to_string(),
            "duplicate_computed_null".to_string(),
            "duplicate_computed_stored_null".to_string(),
            "duplicate_rich".to_string(),
            "duplicate_rich_indexed".to_string(),
            "empty".to_string(),
            "multiple_duplicate_rich_indexed_groups".to_string(),
            "multiple_groups".to_string(),
            "order_ties".to_string(),
            "order_ties_more_than_window".to_string(),
            "small_duplicate_rich".to_string(),
            "stored_null_duplicate_rich".to_string(),
            "stored_null_duplicate_rich_indexed".to_string(),
            "stored_null_order_ties".to_string(),
            "valid_base".to_string(),
        ]),
    );
    assert_eq!(expression_depths, BTreeSet::from([1, 2, 3]));
}

#[test]
fn every_invalid_kind_is_singly_invalid_and_deterministic() {
    let mut identities = BTreeSet::new();
    for root_seed in TIER_C_ROOT_SEEDS {
        for violation in ALL_SELECT_VIOLATIONS {
            for repetition in 0..TIER_C_INVALID_REPETITIONS {
                let generated = generate_invalid_select_case(
                    SelectSchemaProfile::ReferenceScalar,
                    *root_seed,
                    *violation,
                    repetition,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("typed invalid proposal should generate");
                assert_eq!(generated.violation(), Some(*violation));
                assert_eq!(generated.provider(), SelectProvider::RejectionInvariant);
                assert!(
                    generated
                        .structural_signature()
                        .expect("invalid structural signature should derive")
                        .is_singly_invalid()
                );
                assert_eq!(
                    generated
                        .structural_signature()
                        .expect("invalid structural signature should derive")
                        .expected_violation(),
                    violation.code(),
                );
                assert!(identities.insert(generated.identity().id().to_string()));
                generated
                    .validate()
                    .expect("singly invalid generated case should revalidate");
            }
        }
    }
    assert_eq!(
        identities.len(),
        TIER_C_ROOT_SEEDS.len()
            * ALL_SELECT_VIOLATIONS.len()
            * usize::try_from(TIER_C_INVALID_REPETITIONS)
                .expect("invalid repetition count should fit usize"),
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end receipt test keeps declaration, shard, merge, and distribution proof together"
)]
fn generated_select_declarations_and_receipts_cover_the_catalog_exactly() {
    let witnesses = scheduled_select_witnesses().expect("catalog should decode");
    let cases = TIER_C_ROOT_SEEDS
        .iter()
        .flat_map(|root_seed| {
            witnesses.iter().flat_map(move |witness| {
                (0..TIER_C_SELECT_REPETITIONS).map(move |repetition| {
                    generate_scheduled_select_case(
                        witness,
                        *root_seed,
                        repetition,
                        TIER_C_SELECT_BUDGETS,
                    )
                    .expect("scheduled witness repetition should generate")
                })
            })
        })
        .collect::<Vec<_>>();
    let declarations = cases
        .iter()
        .map(|case| {
            generated_select_tier_c_declaration(case.identity().id(), case)
                .expect("typed declaration should derive")
        })
        .collect::<Vec<_>>();
    let declared = cases
        .iter()
        .map(|case| case.identity().id())
        .collect::<Vec<_>>();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = cases
                .iter()
                .filter(|case| {
                    scheduled_sql_scenario_shard(case.identity().id())
                        .expect("catalog witness should shard")
                        == shard_index
                })
                .map(|case| {
                    let outcome = if case.violation().is_some() {
                        TierCScenarioOutcome::ExpectedRejection
                    } else {
                        TierCScenarioOutcome::Passed
                    };
                    TierCScenarioObservation::try_new(
                        case.identity().id(),
                        case.structural_signature()
                            .expect("structural signature should derive"),
                        if case.violation().is_some() {
                            crate::ObservedExecutionFacts::new(
                                crate::ExecutionAccess::NotApplicable,
                                crate::ExecutionCovering::NotApplicable,
                            )
                        } else {
                            scheduled_select_witnesses()
                                .expect("catalog should decode")
                                .into_iter()
                                .find(|witness| {
                                    witness.witness_id() == case.identity().witness_id()
                                })
                                .expect("accepted case should have a reviewed witness")
                                .required_execution_facts()
                                .into()
                        },
                        outcome,
                    )
                    .expect("observed signature should validate")
                })
                .collect();
            TierCShardReport::try_new(shard_index, &declared, observations)
                .expect("exact shard receipt should validate")
        })
        .collect();
    let merged =
        TierCMergedReport::try_merge(&declared, reports).expect("exact receipts should merge");
    assert_eq!(
        merged.obligation_catalog_hash(),
        structural_obligation_catalog_hash()
            .expect("catalog hash should decode")
            .as_str(),
    );
    assert!(merged.is_clean());
    let distribution =
        TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged)
            .expect("exact generated receipts should project");
    assert_eq!(
        distribution.generated_select_structural_signature_count(),
        witnesses.len().saturating_sub(1),
        "cold-cache and scalar full-window obligations intentionally share one typed query structure while retaining distinct execution obligations",
    );
    assert_eq!(
        distribution.generated_select_fixture_class_count("empty"),
        u32::try_from(TIER_C_ROOT_SEEDS.len())
            .expect("root count should fit u32")
            .saturating_mul(
                u32::try_from(TIER_C_SELECT_REPETITIONS).expect("repetition count should fit u32"),
            ),
    );
    assert_eq!(
        distribution.generated_select_repetition_count(0),
        u32::try_from(TIER_C_ROOT_SEEDS.len() * witnesses.len())
            .expect("scheduled repetition population should fit u32"),
    );
    assert_eq!(
        distribution.generated_select_repetition_count(1),
        u32::try_from(TIER_C_ROOT_SEEDS.len() * witnesses.len())
            .expect("scheduled repetition population should fit u32"),
    );
    for property in [
        GeneratedFixtureProperty::DuplicateValue,
        GeneratedFixtureProperty::NumericBoundary,
        GeneratedFixtureProperty::OrderingTie,
        GeneratedFixtureProperty::StoredNull,
    ] {
        assert!(
            distribution.generated_select_fixture_property_count(property) > 0,
            "scheduled SELECT profile must execute fixture property {property:?}",
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one receipt test proves the complete mutation schedule and its distributions together"
)]
fn generated_mutation_declarations_and_receipts_cover_the_catalog_exactly() {
    let witnesses = scheduled_mutation_witnesses().expect("mutation catalog should decode");
    let sequences = TIER_C_ROOT_SEEDS
        .iter()
        .flat_map(|root_seed| {
            witnesses.iter().flat_map(move |witness| {
                (0..TIER_C_MUTATION_REPETITIONS).map(move |repetition| {
                    generate_scheduled_mutation_sequence(
                        witness,
                        *root_seed,
                        repetition,
                        TIER_C_MUTATION_BUDGETS,
                    )
                    .expect("scheduled mutation repetition should generate")
                })
            })
        })
        .collect::<Vec<_>>();
    let declarations = sequences
        .iter()
        .map(|sequence| {
            generated_mutation_tier_c_declaration(sequence.identity().id(), sequence)
                .expect("typed mutation declaration should derive")
        })
        .collect::<Vec<_>>();
    let declared = sequences
        .iter()
        .map(|sequence| sequence.identity().id())
        .collect::<Vec<_>>();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = sequences
                .iter()
                .filter(|sequence| {
                    scheduled_sql_scenario_shard(sequence.identity().id())
                        .expect("mutation witness should shard")
                        == shard_index
                })
                .map(|sequence| {
                    let outcome = if sequence
                        .steps()
                        .iter()
                        .any(|step| step.expected().rejection().is_some())
                    {
                        TierCScenarioOutcome::ExpectedRejection
                    } else {
                        TierCScenarioOutcome::Passed
                    };
                    TierCScenarioObservation::try_new(
                        sequence.identity().id(),
                        sequence
                            .structural_signature()
                            .expect("mutation signature should derive"),
                        scheduled_mutation_witnesses()
                            .expect("catalog should decode")
                            .into_iter()
                            .find(|witness| {
                                witness.witness_id() == sequence.identity().witness_id()
                            })
                            .expect("mutation case should have a reviewed witness")
                            .required_execution_facts()
                            .into(),
                        outcome,
                    )
                    .expect("observed mutation signature should validate")
                })
                .collect();
            TierCShardReport::try_new(shard_index, &declared, observations)
                .expect("exact mutation shard receipt should validate")
        })
        .collect();
    let merged =
        TierCMergedReport::try_merge(&declared, reports).expect("mutation receipts should merge");
    assert!(merged.is_clean());
    assert_eq!(merged.expected_rejection_count(), 48);
    assert_eq!(merged.passed_scenario_count(), 192);
    let distribution =
        TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged)
            .expect("exact mutation receipts should project");
    assert_eq!(
        distribution.scenario_count(),
        u32::try_from(sequences.len()).expect("mutation scenario count should fit u32"),
    );
    assert_eq!(distribution.generated_mutation_ingress_count("sql"), 96);
    assert_eq!(
        distribution.generated_mutation_ingress_count("sql_and_typed"),
        144,
    );
    assert_eq!(
        distribution.generated_mutation_intent_class_count("authored"),
        112,
    );
    assert_eq!(
        distribution.generated_mutation_intent_class_count("explicit_default"),
        48,
    );
    assert_eq!(
        distribution.generated_mutation_intent_class_count("mixed_batch"),
        32,
    );
    assert_eq!(
        distribution.generated_mutation_intent_class_count("omitted"),
        32,
    );
    assert_eq!(
        distribution.generated_mutation_intent_class_count("preserve"),
        16,
    );
    let mut expected_intents = BTreeMap::new();
    for sequence in &sequences {
        for (intent, count) in sequence.intent_counts() {
            *expected_intents.entry(intent.id()).or_insert(0_u32) += count;
        }
    }
    for (intent, count) in expected_intents {
        assert_eq!(
            distribution.generated_mutation_intent_occurrence_count(intent),
            count,
        );
    }
    for witness in &witnesses {
        let signature = generate_scheduled_mutation_sequence(
            witness,
            TIER_A_ROOT_SEEDS[0],
            0,
            TIER_C_MUTATION_BUDGETS,
        )
        .expect("mutation signature should derive");
        assert_eq!(
            distribution.generated_mutation_structural_signature_count(
                signature
                    .structural_signature()
                    .expect("mutation signature should derive")
                    .digest()
                    .expect("mutation signature digest should derive")
                    .as_str(),
            ),
            16,
        );
    }
}

#[test]
fn mismatch_shrinks_and_round_trips_in_current_formats() {
    let witness = scheduled_select_witnesses()
        .expect("catalog should decode")
        .into_iter()
        .find(|witness| witness.witness_id() == "tier_c.scalar.reference_full_window")
        .expect("full-window witness should exist");
    let original =
        generate_scheduled_select_case(&witness, TIER_C_ROOT_SEEDS[0], 0, TIER_C_SELECT_BUDGETS)
            .expect("full-window witness should generate");
    let signature = mismatch_signature(&original);
    let report = shrink_select_failure(&original, &signature, |_| Ok(Some(signature.clone())))
        .expect("stable mismatch should shrink");
    assert!(report.minimization_complete());
    let replay = report
        .into_replay_record(
            SelectObservedOutcome::accepted("subject-result", 2),
            SelectObservedOutcome::accepted("reference-result", 2),
        )
        .expect("shrunk failure should form a replay");
    let bytes = replay
        .to_canonical_json()
        .expect("replay should fit its artifact budget");
    let canonical = str::from_utf8(bytes.as_slice()).expect("canonical replay should be UTF-8");
    assert!(!canonical.contains("\"structural_signature\":"));
    let decoded = crate::SelectReplayRecord::from_canonical_json(&bytes)
        .expect("canonical current replay should decode");
    assert_eq!(decoded, replay);
    assert_eq!(decoded.format_version(), SELECT_REPLAY_FORMAT_VERSION);

    let artifact =
        TierCFailureArtifact::try_from_select_replay(original.identity().id(), replay.clone())
            .expect("complete replay should form a failure artifact");
    let artifact_bytes = artifact
        .to_canonical_json()
        .expect("failure artifact should encode");
    assert_eq!(
        TierCFailureArtifact::from_canonical_json(&artifact_bytes)
            .expect("current failure artifact should decode"),
        artifact,
    );

    let corpus =
        RegressionCorpusEntry::try_from_select_replay("select.full-window-regression", &replay)
            .expect("complete replay should form a corpus entry");
    let corpus_bytes = corpus
        .to_canonical_json()
        .expect("corpus entry should encode");
    let decoded_corpus = RegressionCorpusEntry::from_canonical_json(&corpus_bytes)
        .expect("current corpus entry should decode");
    assert_eq!(decoded_corpus, corpus);
    assert_eq!(
        decoded_corpus.format_version(),
        REGRESSION_CORPUS_FORMAT_VERSION,
    );
    assert!(matches!(
        decoded_corpus.regression_case(),
        RegressionCorpusCase::Select(_)
    ));
}

fn mismatch_signature(case: &crate::GeneratedSelectCase) -> SelectMismatchSignature {
    SelectMismatchSignature::try_new(
        BTreeSet::from([SelectFeature::Projection]),
        SelectExecutionPhase::Comparison,
        "icydb",
        SelectComparisonProvider::SqliteReference,
        None,
        SelectMismatchCategory::Value,
        None,
    )
    .unwrap_or_else(|error| {
        panic!(
            "test mismatch for {:?} should be valid: {error}",
            case.identity().id()
        )
    })
}
