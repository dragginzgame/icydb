//! Module: sql_generator::mutation::tests
//! Responsibility: catalog-bound mutation generation, model, replay, and shrink contract checks.
//! Does not own: product or SQLite differential execution.
//! Boundary: proves the two frozen profiles and exact intent provenance without production helpers.

use crate::{
    GeneratedMutationSequence, MUTATION_GENERATOR_VERSION, MUTATION_REPLAY_FORMAT_VERSION,
    MutationExecutionPhase, MutationExpectedRejection, MutationFeature, MutationIngress,
    MutationIntentClass, MutationIntentKind, MutationMismatchCategory, MutationMismatchSignature,
    MutationObservedOutcome, MutationOperation, MutationReplayRecord, MutationRow,
    MutationSchemaProfile, MutationValue, RegressionCorpusCase, RegressionCorpusEntry,
    SqlGeneratorErrorKind, TIER_A_MUTATION_BUDGETS, TIER_A_MUTATION_REPETITIONS, TIER_A_ROOT_SEEDS,
    TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_REPETITIONS, TIER_C_ROOT_SEEDS, TierCFailureArtifact,
    generate_scheduled_mutation_sequence, scheduled_mutation_witnesses, shrink_mutation_failure,
};
use std::collections::{BTreeMap, BTreeSet};

const EXPECTED_MUTATION_WITNESSES: &[&str] = &[
    "tier_c.mutation.authored_insert",
    "tier_c.mutation.authored_insert_from_query",
    "tier_c.mutation.authored_windowed",
    "tier_c.mutation.default_delete_returning",
    "tier_c.mutation.default_insert_authored",
    "tier_c.mutation.default_insert_explicit",
    "tier_c.mutation.default_insert_mixed_batch",
    "tier_c.mutation.default_insert_omitted",
    "tier_c.mutation.default_no_match",
    "tier_c.mutation.default_reject_duplicate",
    "tier_c.mutation.default_reject_pk_default",
    "tier_c.mutation.default_reject_required",
    "tier_c.mutation.default_update_authored",
    "tier_c.mutation.default_update_default",
    "tier_c.mutation.default_update_preserve",
];

#[test]
fn scheduled_mutation_witnesses_freeze_the_complete_operation_intent_ingress_matrix() {
    let witnesses = scheduled_mutation_witnesses().expect("mutation witness catalog should load");
    assert_eq!(
        witnesses
            .iter()
            .map(crate::ScheduledMutationWitness::witness_id)
            .collect::<Vec<_>>(),
        EXPECTED_MUTATION_WITNESSES,
    );

    let mut matrix = BTreeSet::new();
    for witness in &witnesses {
        let sequence = generate_scheduled_mutation_sequence(
            witness,
            TIER_A_ROOT_SEEDS[0],
            0,
            TIER_A_MUTATION_BUDGETS,
        )
        .expect("every frozen mutation witness should generate");
        matrix.insert((
            sequence.snapshot().profile(),
            sequence.ingress(),
            sequence.intent_class(),
        ));
        assert_eq!(
            &sequence
                .structural_signature()
                .expect("mutation signature should derive"),
            witness.signature(),
        );
    }

    assert_eq!(
        matrix,
        BTreeSet::from([
            (
                MutationSchemaProfile::AuthoredScalar,
                MutationIngress::Sql,
                MutationIntentClass::Authored,
            ),
            (
                MutationSchemaProfile::AuthoredScalar,
                MutationIngress::SqlAndTyped,
                MutationIntentClass::Authored,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::Sql,
                MutationIntentClass::ExplicitDefault,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::Sql,
                MutationIntentClass::MixedBatch,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::Sql,
                MutationIntentClass::Omitted,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::SqlAndTyped,
                MutationIntentClass::Authored,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::SqlAndTyped,
                MutationIntentClass::MixedBatch,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::SqlAndTyped,
                MutationIntentClass::Omitted,
            ),
            (
                MutationSchemaProfile::AcceptedDefault,
                MutationIngress::SqlAndTyped,
                MutationIntentClass::Preserve,
            ),
        ]),
    );
}

#[test]
fn tier_a_mutation_sequences_are_deterministic_bounded_and_structurally_distinct() {
    assert_eq!(TIER_A_MUTATION_REPETITIONS, 1);
    let witnesses = scheduled_mutation_witnesses().expect("mutation witness catalog should load");
    let mut identities = BTreeSet::new();
    let mut signatures = BTreeSet::new();
    let mut operations = BTreeSet::new();
    for root_seed in TIER_A_ROOT_SEEDS {
        for witness in &witnesses {
            let sequence = generate_scheduled_mutation_sequence(
                witness,
                *root_seed,
                0,
                TIER_A_MUTATION_BUDGETS,
            )
            .expect("Tier A mutation witness should generate");
            sequence
                .validate()
                .expect("Tier A mutation witness should revalidate");
            assert_eq!(
                sequence,
                generate_scheduled_mutation_sequence(
                    witness,
                    *root_seed,
                    0,
                    TIER_A_MUTATION_BUDGETS,
                )
                .expect("same witness identity should regenerate identically"),
            );
            assert!(identities.insert(sequence.identity().id().to_string()));
            signatures.insert(
                sequence
                    .structural_signature()
                    .expect("mutation signature should derive"),
            );
            for step in sequence.steps() {
                operations.insert(match step.statement().operation() {
                    MutationOperation::Delete { .. } => "delete",
                    MutationOperation::Insert { .. } => "insert",
                    MutationOperation::InsertFromQuery { .. } => "insert_from_query",
                    MutationOperation::Update { .. } => "update",
                });
            }
        }
    }

    assert_eq!(identities.len(), witnesses.len() * TIER_A_ROOT_SEEDS.len());
    assert_eq!(signatures.len(), witnesses.len());
    assert_eq!(
        operations,
        BTreeSet::from(["delete", "insert", "insert_from_query", "update"])
    );
}

#[test]
fn tier_c_mutation_profile_generates_every_witness_root_and_repetition() {
    assert_eq!(TIER_C_MUTATION_REPETITIONS, 2);
    assert_eq!(TIER_C_MUTATION_BUDGETS.max_fixture_rows(), 64);
    assert_eq!(TIER_C_MUTATION_BUDGETS.max_statements(), 32);
    assert_eq!(TIER_C_MUTATION_BUDGETS.max_shrink_candidates(), 4_096);
    assert_eq!(TIER_C_MUTATION_BUDGETS.max_evaluations(), 8_192);
    assert_eq!(TIER_C_MUTATION_BUDGETS.max_artifact_bytes(), 1_048_576);

    let witnesses = scheduled_mutation_witnesses().expect("mutation witness catalog should load");
    let mut identities = BTreeSet::new();
    for root_seed in TIER_C_ROOT_SEEDS {
        for witness in &witnesses {
            for repetition in 0..TIER_C_MUTATION_REPETITIONS {
                let sequence = generate_scheduled_mutation_sequence(
                    witness,
                    *root_seed,
                    repetition,
                    TIER_C_MUTATION_BUDGETS,
                )
                .expect("Tier C mutation witness should generate");
                assert!(identities.insert(sequence.identity().id().to_string()));
            }
        }
    }
    assert_eq!(
        identities.len(),
        witnesses.len()
            * TIER_C_ROOT_SEEDS.len()
            * usize::try_from(TIER_C_MUTATION_REPETITIONS)
                .expect("mutation repetition count should fit usize"),
    );

    let error = generate_scheduled_mutation_sequence(
        &witnesses[0],
        TIER_C_ROOT_SEEDS[0],
        TIER_C_MUTATION_REPETITIONS,
        TIER_C_MUTATION_BUDGETS,
    )
    .expect_err("repetitions outside the frozen profile must reject");
    assert_eq!(error.kind(), SqlGeneratorErrorKind::InvalidCase);
}

#[test]
fn insert_from_query_witness_has_distinct_derived_keys_and_commits() {
    let sequence = sequence("tier_c.mutation.authored_insert_from_query");
    assert!(
        sequence
            .steps()
            .iter()
            .all(|step| step.expected().rejection().is_none())
    );
    assert_eq!(sequence.final_state().len(), 6);
}

#[test]
fn accepted_default_profile_resolves_exact_values_and_secondary_index_entries() {
    for witness_id in [
        "tier_c.mutation.default_insert_omitted",
        "tier_c.mutation.default_insert_explicit",
    ] {
        let sequence = sequence(witness_id);
        assert_eq!(
            sequence.final_state(),
            &[MutationRow::accepted_default(
                1,
                if witness_id.ends_with("omitted") {
                    "omitted"
                } else {
                    "explicit"
                },
                "bronze",
                7,
                None,
            )],
        );
        let entries = sequence
            .snapshot()
            .secondary_index_entries(sequence.final_state())
            .expect("default-profile index entries should derive");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].value(),
            &MutationValue::Text("bronze".to_string())
        );
        assert_eq!(entries[0].key(), 1);
    }

    let update = sequence("tier_c.mutation.default_update_default");
    let row = &update.final_state()[0];
    assert_eq!(row.tier(), Some("bronze"));
    assert_eq!(row.score(), Some(7));
    assert_eq!(row.note(), Some(None));
    let returned = update.steps()[0]
        .expected()
        .returned_rows()
        .expect("accepted update should expose RETURNING rows");
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].fields().len(), 3);
}

#[test]
fn mutation_model_preserves_exact_intent_and_atomic_rejection() {
    let mixed = sequence("tier_c.mutation.default_insert_mixed_batch");
    assert_eq!(
        mixed.intent_counts(),
        BTreeMap::from([
            (MutationIntentKind::Authored, 9),
            (MutationIntentKind::InsertDefault, 3),
            (MutationIntentKind::Omitted, 3),
        ]),
    );
    assert_eq!(mixed.final_state().len(), 3);

    let preserve = sequence("tier_c.mutation.default_update_preserve");
    assert!(
        preserve
            .intent_counts()
            .get(&MutationIntentKind::Preserve)
            .copied()
            .unwrap_or_default()
            >= 3,
    );
    assert_eq!(preserve.final_state()[0].tier(), Some("silver"));
    assert_eq!(preserve.final_state()[0].score(), Some(42));
    assert_eq!(preserve.final_state()[0].note(), Some(Some("existing")));

    for (witness_id, rejection) in [
        (
            "tier_c.mutation.default_reject_duplicate",
            MutationExpectedRejection::DuplicatePrimaryKey,
        ),
        (
            "tier_c.mutation.default_reject_pk_default",
            MutationExpectedRejection::DefaultUnavailable,
        ),
        (
            "tier_c.mutation.default_reject_required",
            MutationExpectedRejection::MissingRequiredField,
        ),
    ] {
        let rejected = sequence(witness_id);
        let step = &rejected.steps()[0];
        assert_eq!(step.expected().rejection(), Some(rejection));
        assert_eq!(step.state_before(), step.expected().state_after());
    }
}

#[test]
fn no_match_and_delete_returning_have_exact_state_oracle_outcomes() {
    let no_match = sequence("tier_c.mutation.default_no_match");
    for step in no_match.steps() {
        assert_eq!(step.expected().affected_rows(), Some(0));
        assert_eq!(step.state_before(), step.expected().state_after());
    }

    let deleted = sequence("tier_c.mutation.default_delete_returning");
    assert!(deleted.final_state().is_empty());
    assert_eq!(deleted.steps()[0].expected().affected_rows(), Some(1));
    assert_eq!(
        deleted.steps()[0]
            .expected()
            .returned_rows()
            .expect("delete RETURNING should be modeled")
            .len(),
        1,
    );
}

#[test]
fn mutation_identity_and_replay_use_only_the_current_witness_shape() {
    let sequence = sequence("tier_c.mutation.authored_insert");
    assert_eq!(
        sequence.identity().generator_version(),
        MUTATION_GENERATOR_VERSION
    );
    assert_eq!(
        sequence.identity().witness_id(),
        EXPECTED_MUTATION_WITNESSES[0]
    );
    assert_eq!(sequence.identity().repetition(), 0);
    assert!(sequence.identity().id().starts_with(
        format!(
            "sql-mutation/v{MUTATION_GENERATOR_VERSION}/tier_c.mutation.authored_insert/1cdb020400000001/"
        )
        .as_str(),
    ));

    let bytes = crate::replay::canonical_json_bytes(&sequence)
        .expect("mutation sequence should serialize canonically");
    let canonical = str::from_utf8(bytes.as_slice()).expect("canonical sequence should be UTF-8");
    assert!(canonical.contains("\"witness_id\":"));
    assert!(canonical.contains("\"repetition\":\"u64:"));
    assert!(!canonical.contains("\"structural_signature\":"));
}

#[test]
fn mutation_step_outcomes_project_to_stable_typed_replay_evidence() {
    let sequence = sequence("tier_c.mutation.default_reject_duplicate");
    let outcomes = sequence
        .steps()
        .iter()
        .map(|step| {
            MutationObservedOutcome::try_from_step_outcome(step.expected())
                .expect("modeled step outcome should project to replay evidence")
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        outcomes.as_slice(),
        [MutationObservedOutcome::Rejected { error_class_id, .. }]
            if error_class_id == "duplicate_primary_key"
    ));
}

#[test]
fn injected_mutation_failure_shrinks_and_replays_canonically() {
    let sequence = sequence("tier_c.mutation.authored_insert");
    let signature = MutationMismatchSignature::try_new(
        BTreeSet::from([MutationFeature::Insert]),
        MutationExecutionPhase::Comparison,
        "icydb-native",
        "independent-model",
        None,
        MutationMismatchCategory::Atomicity,
        Some("injected-atomicity".to_string()),
    )
    .expect("injected mutation mismatch signature should validate");
    let report = shrink_mutation_failure(&sequence, &signature, |_candidate| {
        Ok(Some(signature.clone()))
    })
    .expect("injected mutation failure should shrink");

    assert!(report.minimization_complete());
    assert_eq!(report.minimized_sequence().steps().len(), 1);
    let replay = report
        .into_replay_record(
            MutationObservedOutcome::rejected("conflict", "state-a"),
            MutationObservedOutcome::accepted(0, "rows-a", "state-b"),
        )
        .expect("injected mutation failure should form replay");
    let bytes = replay
        .to_canonical_json()
        .expect("mutation replay should serialize canonically");
    let decoded = MutationReplayRecord::from_canonical_json(bytes.as_slice())
        .expect("canonical mutation replay should decode");

    assert_eq!(decoded, replay);
    assert_eq!(decoded.format_version(), MUTATION_REPLAY_FORMAT_VERSION);
    let canonical = str::from_utf8(bytes.as_slice()).expect("canonical replay should be UTF-8");
    assert!(canonical.contains("\"witness_id\":"));
    assert!(canonical.contains("\"repetition\":\"u64:"));

    assert_mutation_failure_artifact_round_trip(&sequence, &replay);

    let corpus =
        RegressionCorpusEntry::try_from_mutation_replay("mutation.atomicity-regression", &replay)
            .expect("complete minimized mutation replay should form a corpus entry");
    let corpus_bytes = corpus
        .to_canonical_json()
        .expect("mutation corpus entry should serialize canonically");
    let decoded_corpus = RegressionCorpusEntry::from_canonical_json(corpus_bytes.as_slice())
        .expect("canonical mutation corpus entry should decode");
    assert_eq!(decoded_corpus, corpus);
    assert!(matches!(
        decoded_corpus.regression_case(),
        RegressionCorpusCase::Mutation(_)
    ));
}

fn sequence(witness_id: &str) -> GeneratedMutationSequence {
    let witness = scheduled_mutation_witnesses()
        .expect("mutation witness catalog should load")
        .into_iter()
        .find(|witness| witness.witness_id() == witness_id)
        .expect("named mutation witness should exist");
    generate_scheduled_mutation_sequence(&witness, TIER_A_ROOT_SEEDS[0], 0, TIER_A_MUTATION_BUDGETS)
        .expect("named mutation witness should generate")
}

fn assert_mutation_failure_artifact_round_trip(
    sequence: &GeneratedMutationSequence,
    replay: &MutationReplayRecord,
) {
    let artifact =
        TierCFailureArtifact::try_from_mutation_replay(sequence.identity().id(), replay.clone())
            .expect("complete mutation replay should form a Tier C failure artifact");
    let artifact_id = artifact
        .artifact_id()
        .expect("valid mutation failure artifact should have a content identity");
    let bytes = artifact
        .to_canonical_json()
        .expect("mutation failure artifact should fit its byte budget");
    let decoded = TierCFailureArtifact::from_canonical_json(bytes.as_slice())
        .expect("canonical current mutation failure artifact should decode");

    assert!(artifact.minimization_complete());
    assert!(artifact_id.starts_with("failure."));
    assert_eq!(artifact.replay_scenario_id(), sequence.identity().id());
    assert_eq!(decoded, artifact);
}
