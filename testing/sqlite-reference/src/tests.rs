//! Module: sqlite_reference::tests
//! Responsibility: focused bundled environment, profile, and value-contract checks.
//! Does not own: IcyDB differential execution.
//! Boundary: proves the shared adapter fails closed before product runners consume it.

use crate::adapter::execute_value_mapping_probe;
use crate::{
    MutationSqliteEvidence, SqliteAdapterErrorKind, SqliteReferenceFamily, SqliteReferenceResult,
    SqliteReferenceRowOrder, SqliteReferenceValue, current_sqlite_environment_contract,
    execute_generated_mutation_sequence, execute_generated_select_case,
    execute_sqlite_reference_scenario, observe_sqlite_environment,
    required_sqlite_reference_scenarios,
};
use icydb_testing_sql_generator::{
    TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_REPETITIONS, TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS,
    TIER_C_SELECT_REPETITIONS, generate_scheduled_mutation_sequence,
    generate_scheduled_select_case, scheduled_mutation_witnesses, scheduled_select_witnesses,
};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn bundled_sqlite_environment_matches_checked_contract() {
    let observed = observe_sqlite_environment().expect("bundled SQLite identity should resolve");
    let expected = current_sqlite_environment_contract();

    assert_eq!(observed.runtime_version(), expected.version());
    assert_eq!(observed.runtime_version_number(), expected.version_number());
    assert_eq!(observed.compile_version_number(), expected.version_number());
    assert_eq!(observed.source_id(), expected.source_id());
    assert_eq!(
        observed.compile_options(),
        expected
            .compile_options()
            .iter()
            .map(|option| (*option).to_string())
            .collect::<Vec<_>>(),
    );
}

#[test]
fn required_profile_has_stable_unique_identity_and_two_cases_per_family() {
    let scenarios = required_sqlite_reference_scenarios();
    let ids = scenarios
        .iter()
        .map(|scenario| scenario.id())
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), scenarios.len());

    let mut family_counts = BTreeMap::new();
    for scenario in scenarios {
        assert!(!scenario.contract_features().is_empty());
        assert!(!scenario.families().is_empty());
        scenario
            .tier_c_declaration()
            .expect("required SQLite facts should form one typed Tier C declaration");
        for family in scenario.families() {
            *family_counts.entry(*family).or_insert(0usize) += 1;
        }
    }
    for family in [
        SqliteReferenceFamily::Aggregate,
        SqliteReferenceFamily::Expression,
        SqliteReferenceFamily::Grouped,
        SqliteReferenceFamily::Predicate,
        SqliteReferenceFamily::Scalar,
    ] {
        assert!(
            family_counts.get(&family).copied().unwrap_or_default() >= 2,
            "required SQLite family {family:?} needs at least two scenarios",
        );
    }
}

#[test]
fn required_profile_executes_through_checked_adapter() {
    for scenario in required_sqlite_reference_scenarios() {
        execute_sqlite_reference_scenario(*scenario)
            .unwrap_or_else(|error| panic!("scenario {:?} failed: {error}", scenario.id()));
    }
}

#[test]
fn result_shape_and_identifier_validation_fail_closed() {
    let malformed = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![vec![
            SqliteReferenceValue::Integer(1),
            SqliteReferenceValue::Integer(2),
        ]],
        SqliteReferenceRowOrder::Ordered,
    )
    .expect_err("non-rectangular result must reject");
    assert_eq!(malformed.kind(), SqliteAdapterErrorKind::Result);

    let invalid_identifier = required_sqlite_reference_scenarios()[0]
        .render_sql("Entity; DROP TABLE Entity")
        .expect_err("unsafe entity identifier must reject");
    assert_eq!(
        invalid_identifier.kind(),
        SqliteAdapterErrorKind::Identifier
    );
}

#[test]
fn typed_result_fingerprint_is_stable_and_contract_sensitive() {
    let ordered = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![
            vec![SqliteReferenceValue::Integer(1)],
            vec![SqliteReferenceValue::Integer(2)],
        ],
        SqliteReferenceRowOrder::Ordered,
    )
    .expect("ordered fingerprint fixture should validate");
    let reversed = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![
            vec![SqliteReferenceValue::Integer(2)],
            vec![SqliteReferenceValue::Integer(1)],
        ],
        SqliteReferenceRowOrder::Ordered,
    )
    .expect("reversed fingerprint fixture should validate");
    let unordered = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![
            vec![SqliteReferenceValue::Integer(2)],
            vec![SqliteReferenceValue::Integer(1)],
        ],
        SqliteReferenceRowOrder::Unordered,
    )
    .expect("unordered fingerprint fixture should validate");

    assert_eq!(
        ordered.fingerprint().expect("fingerprint should derive"),
        "blake3.125c04aa64c9fed11fc92f149a0755d3e018ca84a82e9a5ee64e14e0be6f9974",
    );
    assert_ne!(
        ordered.fingerprint().expect("fingerprint should derive"),
        reversed.fingerprint().expect("fingerprint should derive"),
    );
    assert_ne!(
        ordered.fingerprint().expect("fingerprint should derive"),
        unordered.fingerprint().expect("fingerprint should derive"),
    );
}

#[test]
fn declared_common_value_families_map_losslessly() {
    let values = execute_value_mapping_probe().expect("value-mapping probe should execute");
    assert_eq!(
        values,
        vec![
            SqliteReferenceValue::Blob(vec![0, 1, 255]),
            SqliteReferenceValue::Boolean(true),
            SqliteReferenceValue::Integer(7),
            SqliteReferenceValue::Null,
            SqliteReferenceValue::Text("text".to_string()),
        ],
    );
}

#[test]
fn reviewed_generated_select_witnesses_execute_without_silent_exclusions() {
    let witnesses = scheduled_select_witnesses().expect("reviewed witness catalog should decode");
    let mut executed = 0_u32;
    for root_seed in TIER_C_ROOT_SEEDS {
        for witness in &witnesses {
            for repetition in 0..TIER_C_SELECT_REPETITIONS {
                let generated = generate_scheduled_select_case(
                    witness,
                    *root_seed,
                    repetition,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("reviewed SQLite witness should generate");
                if generated.violation().is_some() {
                    continue;
                }
                execute_generated_select_case(&generated).unwrap_or_else(|error| {
                    panic!(
                        "generated SQLite case {:?} should execute: {error}",
                        generated.identity().id(),
                    )
                });
                executed = executed.saturating_add(1);
            }
        }
    }

    assert_eq!(
        executed,
        u32::try_from(
            TIER_C_ROOT_SEEDS.len()
                * witnesses
                    .iter()
                    .filter(|witness| !witness.signature().is_singly_invalid())
                    .count()
                * usize::try_from(TIER_C_SELECT_REPETITIONS)
                    .expect("repetition count should fit usize"),
        )
        .expect("reviewed generated profile should fit u32"),
    );
}

#[test]
fn tier_c_generated_mutation_overlap_matches_independent_model() {
    let witnesses = scheduled_mutation_witnesses().expect("mutation witness catalog should load");
    let mut compared = 0_u32;
    let mut excluded = 0_u32;
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
                let evidence = execute_generated_mutation_sequence(&sequence)
                    .expect("eligible Tier C mutation steps should execute in bundled SQLite");
                assert_eq!(evidence.len(), sequence.steps().len());
                for (step, observed) in sequence.steps().iter().zip(evidence) {
                    match observed {
                        MutationSqliteEvidence::Compared(outcome) => {
                            assert_eq!(&outcome, step.expected());
                            compared = compared.saturating_add(1);
                        }
                        MutationSqliteEvidence::Excluded(reason) => {
                            assert_eq!(
                                step.sqlite_eligibility(),
                                icydb_testing_sql_generator::MutationSqliteEligibility::Excluded(
                                    reason
                                )
                            );
                            excluded = excluded.saturating_add(1);
                        }
                    }
                }
            }
        }
    }

    assert!(compared > 0);
    assert!(excluded > 0);
}
