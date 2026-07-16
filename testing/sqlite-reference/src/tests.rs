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
    ALL_SELECT_GENERATOR_FAMILIES, MutationField, MutationFieldKind, MutationFieldRole,
    MutationSnapshot, SelectField, SelectFieldKind, SelectIndex, SelectSnapshot,
    TIER_A_MUTATION_BUDGETS, TIER_A_MUTATION_CASES_PER_ROOT, TIER_A_MUTATION_ROOT_SEEDS,
    TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS, TIER_A_VALID_CASES_PER_FAMILY,
    generate_mutation_sequence, generate_valid_select_case,
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
fn tier_a_generated_select_profile_executes_without_silent_exclusions() {
    let snapshot = generated_select_snapshot();
    let mut executed = 0_u32;
    for root_seed in TIER_A_ROOT_SEEDS {
        for family in ALL_SELECT_GENERATOR_FAMILIES {
            for case_index in 0..TIER_A_VALID_CASES_PER_FAMILY {
                let generated = generate_valid_select_case(
                    &snapshot,
                    *root_seed,
                    *family,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A SQLite case should generate");
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
            TIER_A_ROOT_SEEDS.len()
                * ALL_SELECT_GENERATOR_FAMILIES.len()
                * usize::try_from(TIER_A_VALID_CASES_PER_FAMILY)
                    .expect("Tier A case count should fit usize"),
        )
        .expect("Tier A generated profile should fit u32"),
    );
}

#[test]
fn tier_a_generated_mutation_overlap_matches_independent_model() {
    let snapshot = generated_mutation_snapshot();
    let mut compared = 0_u32;
    let mut excluded = 0_u32;
    for root_seed in TIER_A_MUTATION_ROOT_SEEDS {
        for case_index in 0..TIER_A_MUTATION_CASES_PER_ROOT {
            let sequence = generate_mutation_sequence(
                &snapshot,
                *root_seed,
                case_index,
                TIER_A_MUTATION_BUDGETS,
            )
            .expect("Tier A mutation sequence should generate");
            let evidence = execute_generated_mutation_sequence(&sequence)
                .expect("eligible Tier A mutation steps should execute in bundled SQLite");
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

    assert!(compared > 0);
    assert!(excluded > 0);
}

fn generated_select_snapshot() -> SelectSnapshot {
    SelectSnapshot::try_new(
        "sqlite-tier-a-v1",
        "sqlite_reference::GeneratorEntity",
        "GeneratorEntity",
        1,
        vec![
            SelectField::new(1, "id", SelectFieldKind::Ulid, false, true, true),
            SelectField::new(2, "name", SelectFieldKind::Text, false, false, false),
            SelectField::new(3, "age", SelectFieldKind::Integer, false, false, false),
            SelectField::new(4, "score", SelectFieldKind::Integer, false, false, false),
            SelectField::new(5, "active", SelectFieldKind::Boolean, false, false, false),
        ],
        vec![
            SelectIndex::new(1, "by_name", vec![2]),
            SelectIndex::new(2, "by_score", vec![4]),
        ],
    )
    .expect("generated SQLite snapshot should be valid")
}

fn generated_mutation_snapshot() -> MutationSnapshot {
    MutationSnapshot::try_new(
        "sqlite-mutation-v1",
        "sqlite_reference::MutationEntity",
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
    .expect("generated SQLite mutation snapshot should be valid")
}
