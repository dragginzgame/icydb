//! Module: sql_generator::tests
//! Responsibility: deterministic machinery, generation bounds, shrinking, and replay contracts.
//! Does not own: IcyDB or SQLite execution-provider behavior.
//! Boundary: proves the test-owned generator is stable before product adapters consume it.

use crate::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedSelectCase,
    REGRESSION_CORPUS_FORMAT_VERSION, RegressionCorpusCase, RegressionCorpusEntry,
    SELECT_GENERATOR_VERSION, SELECT_REPLAY_FORMAT_VERSION, SelectBudgets,
    SelectComparisonProvider, SelectExecutionPhase, SelectFeature, SelectField, SelectFieldKind,
    SelectGeneratorFamily, SelectIndex, SelectMismatchCategory, SelectMismatchSignature,
    SelectObservedOutcome, SelectProvider, SelectReplayRecord, SelectSnapshot,
    SqlGeneratorErrorKind, TIER_A_INVALID_CASES_PER_VIOLATION, TIER_A_ROOT_SEEDS,
    TIER_A_SELECT_BUDGETS, TIER_A_VALID_CASES_PER_FAMILY, TIER_C_INVALID_CASES_PER_VIOLATION,
    TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS, TIER_C_VALID_CASES_PER_FAMILY, TierCFailureArtifact,
    TierCFailureArtifactError, generate_invalid_select_case, generate_valid_select_case,
    rng::{SplitMix64, derive_sql_sub_seed},
    shrink_select_failure,
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
fn splitmix64_bounded_and_weighted_choices_have_fixed_golden_vectors() {
    let mut bounded_rng = SplitMix64::new(0x1020_3040_5060_7080);
    let bounded = (0..8)
        .map(|_| bounded_rng.bounded(7).expect("non-zero bound should work"))
        .collect::<Vec<_>>();
    let mut weighted_rng = SplitMix64::new(0x1020_3040_5060_7080);
    let weighted = (0..8)
        .map(|_| {
            weighted_rng
                .weighted_index(&[1, 3, 2])
                .expect("checked weights should work")
        })
        .collect::<Vec<_>>();

    assert_eq!(bounded, vec![4, 6, 1, 4, 3, 1, 0, 6]);
    assert_eq!(weighted, vec![1, 2, 1, 2, 1, 1, 1, 0]);
}

#[test]
fn select_sub_seed_has_fixed_blake3_golden_vector() {
    let actual = derive_sql_sub_seed(
        SELECT_GENERATOR_VERSION,
        TIER_A_ROOT_SEEDS[0],
        SelectGeneratorFamily::Expression.id(),
        3,
    )
    .expect("fixed family identity should derive");

    assert_eq!(actual, 0xdab6_477b_1b44_b05c);
}

#[test]
fn accepted_snapshot_order_and_representative_case_are_golden() {
    let snapshot = select_snapshot();
    let case = generate_valid_select_case(
        &snapshot,
        TIER_A_ROOT_SEEDS[0],
        SelectGeneratorFamily::Expression,
        3,
        TIER_A_SELECT_BUDGETS,
    )
    .expect("representative generated case should be valid");
    let canonical = crate::replay::canonical_json_bytes(&case)
        .expect("representative generated case should serialize");

    assert_eq!(
        snapshot
            .fields()
            .iter()
            .map(SelectField::id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
    );
    assert_eq!(
        case.rendered_sql(),
        "SELECT LENGTH(name) AS generated_value FROM GeneratorEntity",
    );
    assert_eq!(
        blake3::hash(&canonical).to_hex().as_str(),
        "105d273bf3f79dff5e09985317e75454723bf2bcc3eaa3ed70065dcbcd486a37",
    );
}

#[test]
fn tier_a_generation_is_deterministic_bounded_and_feature_complete() {
    let snapshot = select_snapshot();
    let mut identities = BTreeSet::new();
    let mut reached = BTreeMap::<SelectGeneratorFamily, BTreeSet<SelectFeature>>::new();

    for root_seed in TIER_A_ROOT_SEEDS {
        for family in ALL_SELECT_GENERATOR_FAMILIES {
            for case_index in 0..TIER_A_VALID_CASES_PER_FAMILY {
                let first = generate_valid_select_case(
                    &snapshot,
                    *root_seed,
                    *family,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A valid case should generate");
                let second = generate_valid_select_case(
                    &snapshot,
                    *root_seed,
                    *family,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("same Tier A valid case should reproduce");
                assert_eq!(first, second);
                assert!(identities.insert(first.identity().id().to_string()));
                assert!(first.fixture().len() <= 16);
                assert!(first.query().projection_count() <= 4);
                assert!(first.query().group_key_count() <= 4);
                assert!(first.query().order_term_count() <= 3);
                first.validate().expect("generated case should revalidate");
                reached
                    .entry(*family)
                    .or_default()
                    .extend(first.features().iter().copied());
            }
        }
    }

    assert_eq!(reached.len(), ALL_SELECT_GENERATOR_FAMILIES.len());
    assert_family_features(&reached);
}

#[test]
fn independent_family_streams_do_not_depend_on_iteration_order() {
    let snapshot = select_snapshot();
    let mut forward = BTreeMap::new();
    let mut reverse = BTreeMap::new();
    for family in ALL_SELECT_GENERATOR_FAMILIES {
        let case = generated_case(&snapshot, *family, 5);
        forward.insert(*family, case);
    }
    for family in ALL_SELECT_GENERATOR_FAMILIES.iter().rev() {
        let case = generated_case(&snapshot, *family, 5);
        reverse.insert(*family, case);
    }

    assert_eq!(forward, reverse);
}

#[test]
fn tier_a_invalid_generation_attaches_one_typed_rejection_before_rendering() {
    let snapshot = select_snapshot();
    let mut identities = BTreeSet::new();
    for root_seed in TIER_A_ROOT_SEEDS {
        for violation in ALL_SELECT_VIOLATIONS {
            for case_index in 0..TIER_A_INVALID_CASES_PER_VIOLATION {
                let generated = generate_invalid_select_case(
                    &snapshot,
                    *root_seed,
                    *violation,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A invalid case should generate from a valid base");
                assert_eq!(generated.violation(), Some(*violation));
                assert_eq!(
                    generated.expected(),
                    crate::SelectExpectedOutcome::Rejected(violation.expected_rejection()),
                );
                assert_eq!(generated.provider(), SelectProvider::RejectionInvariant);
                assert!(identities.insert(generated.identity().id().to_string()));
                generated
                    .validate()
                    .expect("classified invalid case should revalidate structurally");
            }
        }
    }
}

#[test]
fn tier_c_profile_is_exact_bounded_and_fully_generatable() {
    assert_eq!(
        TIER_C_ROOT_SEEDS,
        &[
            0x1cdb_0204_0000_0011,
            0x1cdb_0204_0000_0012,
            0x1cdb_0204_0000_0013,
            0x1cdb_0204_0000_0014,
            0x1cdb_0204_0000_0015,
            0x1cdb_0204_0000_0016,
            0x1cdb_0204_0000_0017,
            0x1cdb_0204_0000_0018,
        ]
    );
    assert_eq!(TIER_C_VALID_CASES_PER_FAMILY, 32);
    assert_eq!(TIER_C_INVALID_CASES_PER_VIOLATION, 8);
    assert_eq!(TIER_C_SELECT_BUDGETS.max_fixture_rows(), 64);
    assert_eq!(TIER_C_SELECT_BUDGETS.max_expression_depth(), 4);
    assert_eq!(TIER_C_SELECT_BUDGETS.max_shrink_candidates(), 4_096);
    assert_eq!(TIER_C_SELECT_BUDGETS.max_evaluations(), 8_192);
    assert_eq!(TIER_C_SELECT_BUDGETS.max_artifact_bytes(), 1_048_576);

    let snapshot = select_snapshot();
    let mut identities = BTreeSet::new();
    for root_seed in TIER_C_ROOT_SEEDS {
        for family in ALL_SELECT_GENERATOR_FAMILIES {
            for case_index in 0..TIER_C_VALID_CASES_PER_FAMILY {
                let generated = generate_valid_select_case(
                    &snapshot,
                    *root_seed,
                    *family,
                    case_index,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("Tier C valid case should generate");
                assert!(identities.insert(generated.identity().id().to_string()));
            }
        }
        for violation in ALL_SELECT_VIOLATIONS {
            for case_index in 0..TIER_C_INVALID_CASES_PER_VIOLATION {
                let generated = generate_invalid_select_case(
                    &snapshot,
                    *root_seed,
                    *violation,
                    case_index,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("Tier C invalid case should generate");
                assert!(identities.insert(generated.identity().id().to_string()));
            }
        }
    }

    let expected_count = TIER_C_ROOT_SEEDS.len()
        * (ALL_SELECT_GENERATOR_FAMILIES.len()
            * usize::try_from(TIER_C_VALID_CASES_PER_FAMILY)
                .expect("Tier C valid count should fit usize")
            + ALL_SELECT_VIOLATIONS.len()
                * usize::try_from(TIER_C_INVALID_CASES_PER_VIOLATION)
                    .expect("Tier C invalid count should fit usize"));
    assert_eq!(identities.len(), expected_count);
}

#[test]
fn injected_failure_shrinks_and_round_trips_as_canonical_replay() {
    let snapshot = select_snapshot();
    let original = generated_case(&snapshot, SelectGeneratorFamily::Expression, 6);
    let signature = mismatch_signature(&original);
    let report = shrink_select_failure(&original, &signature, |_| Ok(Some(signature.clone())))
        .expect("injected stable mismatch should shrink");

    assert!(report.minimization_complete());
    assert!(report.minimized_case().fixture().len() < original.fixture().len());
    assert!(
        report.minimized_case().query().projection_count() <= original.query().projection_count()
    );
    let replay = report
        .into_replay_record(
            SelectObservedOutcome::accepted("subject-result", 2),
            SelectObservedOutcome::accepted("reference-result", 2),
        )
        .expect("shrunk failure should form a replay");
    let bytes = replay
        .to_canonical_json()
        .expect("replay should fit its artifact budget");
    let decoded = crate::SelectReplayRecord::from_canonical_json(&bytes)
        .expect("canonical current replay should decode");

    assert_eq!(decoded, replay);
    assert_eq!(decoded.format_version(), SELECT_REPLAY_FORMAT_VERSION);

    assert_select_failure_artifact_round_trip(&original, &replay);

    let corpus = RegressionCorpusEntry::try_from_select_replay(
        "select.expression-value-regression",
        &replay,
    )
    .expect("complete minimized replay should form a current corpus entry");
    let corpus_bytes = corpus
        .to_canonical_json()
        .expect("corpus entry should fit the replay artifact budget");
    let decoded_corpus = RegressionCorpusEntry::from_canonical_json(&corpus_bytes)
        .expect("canonical current corpus entry should decode");

    assert_eq!(decoded_corpus, corpus);
    assert_eq!(
        decoded_corpus.format_version(),
        REGRESSION_CORPUS_FORMAT_VERSION
    );
    assert_eq!(
        decoded_corpus.regression_case().generated_id(),
        replay.minimized_case().identity().id()
    );
    assert!(matches!(
        decoded_corpus.regression_case(),
        RegressionCorpusCase::Select(_)
    ));

    let mut unknown_field = serde_json::from_slice::<serde_json::Value>(&corpus_bytes)
        .expect("canonical corpus should materialize as JSON");
    unknown_field
        .as_object_mut()
        .expect("corpus root should be an object")
        .insert(
            "unexpected_field".to_string(),
            serde_json::Value::Bool(true),
        );
    let unknown_field_bytes = crate::replay::canonical_json_bytes(&unknown_field)
        .expect("tampered JSON should serialize canonically");
    let unknown_field_error =
        RegressionCorpusEntry::from_canonical_json(unknown_field_bytes.as_slice())
            .expect_err("unknown corpus fields must fail closed");
    assert_eq!(
        unknown_field_error.kind(),
        SqlGeneratorErrorKind::CanonicalCorpus
    );

    let invalid_id_error = RegressionCorpusEntry::try_from_select_replay("Invalid ID", &replay)
        .expect_err("non-canonical regression IDs must reject");
    assert_eq!(
        invalid_id_error.kind(),
        SqlGeneratorErrorKind::CanonicalCorpus
    );
}

fn assert_select_failure_artifact_round_trip(
    original: &GeneratedSelectCase,
    replay: &SelectReplayRecord,
) {
    let artifact =
        TierCFailureArtifact::try_from_select_replay(original.identity().id(), replay.clone())
            .expect("complete SELECT replay should form a Tier C failure artifact");
    let artifact_id = artifact
        .artifact_id()
        .expect("valid failure artifact should have a content identity");
    let bytes = artifact
        .to_canonical_json()
        .expect("failure artifact should fit its byte budget");
    let decoded = TierCFailureArtifact::from_canonical_json(bytes.as_slice())
        .expect("canonical current failure artifact should decode");

    assert!(artifact.minimization_complete());
    assert!(artifact_id.starts_with("failure."));
    assert_eq!(artifact.replay_scenario_id(), original.identity().id());
    assert_eq!(decoded, artifact);
    assert_eq!(
        decoded
            .artifact_id()
            .expect("decoded artifact should retain its content identity"),
        artifact_id,
    );

    let mut unknown_field = serde_json::from_slice::<serde_json::Value>(bytes.as_slice())
        .expect("canonical failure artifact should materialize as JSON");
    unknown_field
        .as_object_mut()
        .expect("failure artifact root should be an object")
        .insert(
            "unexpected_field".to_string(),
            serde_json::Value::Bool(true),
        );
    let unknown_field_bytes = crate::replay::canonical_json_bytes(&unknown_field)
        .expect("tampered failure JSON should serialize canonically");
    assert!(matches!(
        TierCFailureArtifact::from_canonical_json(unknown_field_bytes.as_slice()),
        Err(TierCFailureArtifactError::Decode { .. })
    ));
}

#[test]
fn shrink_budget_exhaustion_remains_an_incomplete_failure() {
    let snapshot = select_snapshot();
    let budgets = SelectBudgets::new(16, 3, 4, 3, 1, 1, 262_144);
    let original = generate_valid_select_case(
        &snapshot,
        TIER_A_ROOT_SEEDS[0],
        SelectGeneratorFamily::ScalarProjection,
        6,
        budgets,
    )
    .expect("bounded shrink test case should generate");
    let signature = mismatch_signature(&original);
    let report = shrink_select_failure(&original, &signature, |_| Ok(Some(signature.clone())))
        .expect("budget exhaustion should produce a report");

    assert!(!report.minimization_complete());
    assert_eq!(report.shrink_candidates_attempted(), 1);
    assert_eq!(report.evaluations(), 1);
    assert!(report.minimized_case().fixture().len() < original.fixture().len());

    let replay = report
        .into_replay_record(
            SelectObservedOutcome::accepted("subject-result", 2),
            SelectObservedOutcome::accepted("reference-result", 2),
        )
        .expect("incomplete shrink report should remain replayable");
    let error = RegressionCorpusEntry::try_from_select_replay("incomplete-select", &replay)
        .expect_err("incomplete minimization must not enter the reviewed corpus");
    assert_eq!(error.kind(), SqlGeneratorErrorKind::CanonicalCorpus);
}

fn select_snapshot() -> SelectSnapshot {
    SelectSnapshot::try_new(
        "tier-a-select-v1",
        "tests::GeneratorEntity",
        "GeneratorEntity",
        1,
        vec![
            SelectField::new(5, "active", SelectFieldKind::Boolean, false, false, false),
            SelectField::new(1, "id", SelectFieldKind::Ulid, false, true, true),
            SelectField::new(4, "score", SelectFieldKind::Integer, false, false, false),
            SelectField::new(2, "name", SelectFieldKind::Text, false, false, false),
            SelectField::new(3, "age", SelectFieldKind::Integer, false, false, false),
        ],
        vec![
            SelectIndex::new(2, "by_score", vec![4]),
            SelectIndex::new(1, "by_name", vec![2]),
        ],
    )
    .expect("test accepted snapshot should be valid")
}

fn generated_case(
    snapshot: &SelectSnapshot,
    family: SelectGeneratorFamily,
    case_index: u64,
) -> GeneratedSelectCase {
    generate_valid_select_case(
        snapshot,
        TIER_A_ROOT_SEEDS[0],
        family,
        case_index,
        TIER_A_SELECT_BUDGETS,
    )
    .expect("fixed generated case should be valid")
}

fn mismatch_signature(case: &GeneratedSelectCase) -> SelectMismatchSignature {
    SelectMismatchSignature::try_new(
        case.features().clone(),
        SelectExecutionPhase::Comparison,
        "icydb",
        SelectComparisonProvider::SqliteReference,
        None,
        SelectMismatchCategory::Value,
        None,
    )
    .expect("test mismatch signature should be valid")
}

fn assert_family_features(reached: &BTreeMap<SelectGeneratorFamily, BTreeSet<SelectFeature>>) {
    let distinct = &reached[&SelectGeneratorFamily::Distinct];
    assert!(distinct.contains(&SelectFeature::Distinct));
    assert!(distinct.contains(&SelectFeature::Projection));

    let scalar = &reached[&SelectGeneratorFamily::ScalarProjection];
    assert!(scalar.contains(&SelectFeature::Alias));
    assert!(scalar.contains(&SelectFeature::Projection));

    let expression = &reached[&SelectGeneratorFamily::Expression];
    for feature in [
        SelectFeature::Arithmetic,
        SelectFeature::Function,
        SelectFeature::Null,
        SelectFeature::SearchedCase,
        SelectFeature::Text,
    ] {
        assert!(expression.contains(&feature), "missing {feature:?}");
    }

    let predicate = &reached[&SelectGeneratorFamily::Predicate];
    for feature in [
        SelectFeature::Boolean,
        SelectFeature::Comparison,
        SelectFeature::Predicate,
        SelectFeature::Text,
    ] {
        assert!(predicate.contains(&feature), "missing {feature:?}");
    }

    let global = &reached[&SelectGeneratorFamily::GlobalAggregate];
    for feature in [SelectFeature::Aggregate, SelectFeature::AggregateDistinct] {
        assert!(global.contains(&feature), "missing {feature:?}");
    }

    let grouped = &reached[&SelectGeneratorFamily::GroupedAggregate];
    for feature in [
        SelectFeature::Aggregate,
        SelectFeature::AggregateDistinct,
        SelectFeature::Grouping,
    ] {
        assert!(grouped.contains(&feature), "missing {feature:?}");
    }

    let having = &reached[&SelectGeneratorFamily::Having];
    for feature in [
        SelectFeature::Aggregate,
        SelectFeature::Grouping,
        SelectFeature::Having,
    ] {
        assert!(having.contains(&feature), "missing {feature:?}");
    }

    let window = &reached[&SelectGeneratorFamily::Window];
    for feature in [
        SelectFeature::Limit,
        SelectFeature::Offset,
        SelectFeature::Ordering,
    ] {
        assert!(window.contains(&feature), "missing {feature:?}");
    }
}
