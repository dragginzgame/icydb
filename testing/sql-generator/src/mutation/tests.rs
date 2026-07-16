//! Module: sql_generator::mutation::tests
//! Responsibility: deterministic mutation generation, model, replay, and shrink contract checks.
//! Does not own: product or SQLite differential execution.
//! Boundary: injects harness-local failures to prove bounded replay without preserving product defects.

use crate::{
    MUTATION_GENERATOR_VERSION, MutationExecutionPhase, MutationFeature, MutationField,
    MutationFieldKind, MutationFieldRole, MutationInsertQueryKeySource, MutationMismatchCategory,
    MutationMismatchSignature, MutationObservedOutcome, MutationOperation, MutationOrder,
    MutationPredicate, MutationReplayRecord, MutationSnapshot, MutationSqliteEligibility,
    MutationStepOutcome, MutationWindow, TIER_A_MUTATION_BUDGETS, TIER_A_MUTATION_CASES_PER_ROOT,
    TIER_A_MUTATION_ROOT_SEEDS, generate_mutation_sequence, shrink_mutation_failure,
};
use std::collections::BTreeSet;

#[test]
fn tier_a_sequences_are_deterministic_bounded_and_cover_current_dml_contract() {
    let snapshot = mutation_snapshot();
    let mut insert = 0_u32;
    let mut insert_from_query = 0_u32;
    let mut update = 0_u32;
    let mut delete = 0_u32;
    let mut returning = 0_u32;
    let mut rejected = 0_u32;
    let mut excluded = 0_u32;
    let mut generated = 0_u32;
    let mut rendered_profiles = BTreeSet::new();

    for root_seed in TIER_A_MUTATION_ROOT_SEEDS {
        for case_index in 0..TIER_A_MUTATION_CASES_PER_ROOT {
            let sequence = generate_mutation_sequence(
                &snapshot,
                *root_seed,
                case_index,
                TIER_A_MUTATION_BUDGETS,
            )
            .expect("Tier A mutation sequence should generate");
            sequence
                .validate()
                .expect("Tier A mutation sequence should revalidate");
            assert_eq!(sequence.steps().len(), 8);
            assert_eq!(
                sequence,
                generate_mutation_sequence(
                    &snapshot,
                    *root_seed,
                    case_index,
                    TIER_A_MUTATION_BUDGETS,
                )
                .expect("same mutation identity should regenerate identically"),
            );
            assert_eq!(
                sequence.identity().generator_version(),
                MUTATION_GENERATOR_VERSION
            );
            assert!(
                rendered_profiles.insert(
                    sequence
                        .steps()
                        .iter()
                        .map(|step| step.rendered_sql().to_string())
                        .collect::<Vec<_>>(),
                ),
                "every required root/case identity must generate a distinct SQL sequence",
            );

            for step in sequence.steps() {
                match step.statement().operation() {
                    MutationOperation::Delete { .. } => delete = delete.saturating_add(1),
                    MutationOperation::Insert { .. } => insert = insert.saturating_add(1),
                    MutationOperation::InsertFromQuery { .. } => {
                        insert_from_query = insert_from_query.saturating_add(1);
                    }
                    MutationOperation::Update { .. } => update = update.saturating_add(1),
                }
                returning = returning.saturating_add(u32::from(step.statement().returning()));
                rejected = rejected.saturating_add(u32::from(matches!(
                    step.expected(),
                    MutationStepOutcome::Rejected { .. }
                )));
                excluded = excluded.saturating_add(u32::from(matches!(
                    step.sqlite_eligibility(),
                    MutationSqliteEligibility::Excluded(_)
                )));
                if matches!(step.expected(), MutationStepOutcome::Rejected { .. }) {
                    assert_eq!(step.state_before(), step.expected().state_after());
                }
            }
            generated = generated.saturating_add(1);
        }
    }

    assert_eq!(generated, 8);
    assert!(insert > 0);
    assert!(insert_from_query > 0);
    assert!(update > 0);
    assert!(delete > 0);
    assert!(returning > 0);
    assert_eq!(rejected, generated);
    assert!(excluded > 0);
    assert_eq!(rendered_profiles.len(), 8);
}

#[test]
fn tier_a_sequences_produce_every_closed_mutation_ast_variant() {
    let snapshot = mutation_snapshot();
    let mut orders = BTreeSet::new();
    let mut key_sources = BTreeSet::new();
    let mut has_all_predicate = false;

    for root_seed in TIER_A_MUTATION_ROOT_SEEDS {
        for case_index in 0..TIER_A_MUTATION_CASES_PER_ROOT {
            let sequence = generate_mutation_sequence(
                &snapshot,
                *root_seed,
                case_index,
                TIER_A_MUTATION_BUDGETS,
            )
            .expect("Tier A mutation sequence should generate");
            for step in sequence.steps() {
                match step.statement().operation() {
                    MutationOperation::Delete { predicate, window } => {
                        has_all_predicate |= matches!(predicate, MutationPredicate::All);
                        orders.extend(window.map(MutationWindow::order));
                    }
                    MutationOperation::InsertFromQuery { key_source, .. } => {
                        key_sources.insert(*key_source);
                    }
                    MutationOperation::Update { window, .. } => {
                        orders.extend(window.map(MutationWindow::order));
                    }
                    MutationOperation::Insert { .. } => {}
                }
            }
        }
    }

    assert_eq!(
        orders,
        BTreeSet::from([MutationOrder::KeyAscending, MutationOrder::KeyDescending])
    );
    assert_eq!(
        key_sources,
        BTreeSet::from([
            MutationInsertQueryKeySource::Key,
            MutationInsertQueryKeySource::Number,
        ])
    );
    assert!(has_all_predicate);
}

#[test]
fn mutation_snapshot_names_are_the_only_rendering_authority() {
    let snapshot = MutationSnapshot::try_new(
        "renamed-fixture",
        "crate::RenamedEntity",
        "RenamedEntity",
        7,
        vec![
            MutationField::new(
                9,
                "entity_key",
                MutationFieldKind::UnsignedInteger,
                MutationFieldRole::Key,
            ),
            MutationField::new(
                11,
                "label",
                MutationFieldKind::Text,
                MutationFieldRole::Text,
            ),
            MutationField::new(
                15,
                "score",
                MutationFieldKind::UnsignedInteger,
                MutationFieldRole::Number,
            ),
        ],
    )
    .expect("renamed accepted mutation snapshot should validate");
    let sequence = generate_mutation_sequence(
        &snapshot,
        TIER_A_MUTATION_ROOT_SEEDS[0],
        0,
        TIER_A_MUTATION_BUDGETS,
    )
    .expect("renamed accepted mutation sequence should generate");

    for step in sequence.steps() {
        assert!(step.rendered_sql().contains("RenamedEntity"));
        assert!(!step.rendered_sql().contains("SessionSqlWriteEntity"));
    }
    assert!(sequence.steps()[0].rendered_sql().contains("entity_key"));
    assert!(sequence.steps()[0].rendered_sql().contains("label"));
    assert!(sequence.steps()[0].rendered_sql().contains("score"));
}

#[test]
fn injected_mutation_failure_shrinks_from_initial_state_and_replays_canonically() {
    let sequence = generate_mutation_sequence(
        &mutation_snapshot(),
        TIER_A_MUTATION_ROOT_SEEDS[0],
        1,
        TIER_A_MUTATION_BUDGETS,
    )
    .expect("injected mutation sequence should generate");
    let signature = MutationMismatchSignature::try_new(
        BTreeSet::from([MutationFeature::InsertFromQuery, MutationFeature::Rejection]),
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
    assert!(report.minimized_sequence().initial_rows().is_empty());
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
}

fn mutation_snapshot() -> MutationSnapshot {
    MutationSnapshot::try_new(
        "test-mutation-snapshot-v1",
        "crate::MutationEntity",
        "MutationEntity",
        1,
        vec![
            MutationField::new(
                1,
                "id",
                MutationFieldKind::UnsignedInteger,
                MutationFieldRole::Key,
            ),
            MutationField::new(2, "name", MutationFieldKind::Text, MutationFieldRole::Text),
            MutationField::new(
                3,
                "age",
                MutationFieldKind::UnsignedInteger,
                MutationFieldRole::Number,
            ),
        ],
    )
    .expect("test mutation snapshot should validate")
}
