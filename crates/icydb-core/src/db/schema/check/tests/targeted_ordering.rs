//! Targeted-rule ordering must consume the compiler caller's budget authority.

use super::*;
use crate::db::{
    QueryError, query::preparation::PreparationWork,
    schema::check::targeted::CompiledAcceptedTargetedRules, tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

const GROUPED_RULES: usize = 14;

fn grouped_fixture() -> TargetedRuleFixture {
    let mut fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let operation = snapshot
        .constraints()
        .iter()
        .find_map(|constraint| {
            let AcceptedConstraintKind::TargetedRule { operation, .. } = constraint.kind() else {
                return None;
            };
            Some(operation.as_ref().clone())
        })
        .unwrap();
    let mut catalog = snapshot.constraint_catalog().clone();
    for root in [2, 3] {
        catalog = catalog
            .with_added_targeted_rule(
                format!("extra_degree_{root}"),
                ConstraintOrigin::Generated,
                AcceptedRuleTarget::new(
                    FieldId::new(root),
                    AcceptedNamedTypeIdentity::Composite(fixture.degree_type),
                ),
                operation.clone(),
            )
            .unwrap();
    }
    fixture.accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog)).unwrap();
    fixture.program = CompiledAcceptedRowConstraints::compile(
        &fixture.accepted,
        &fixture.value_catalog,
        FINGERPRINT,
        &MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    assert_eq!(fixture.accepted_targeted_sources().len(), GROUPED_RULES);
    fixture
}

fn compile_with_limit(
    fixture: &TargetedRuleFixture,
    resource: Resource,
    limit: u64,
    lane: Lane,
    source_count: usize,
) -> (u64, Result<CompiledAcceptedTargetedRules, QueryError>) {
    let mut sources = fixture.accepted_targeted_sources();
    sources.truncate(source_count);
    let root = request_with_limit(resource, limit);
    let result = PreparationWork::run(&root.scope(), lane, |work| {
        CompiledAcceptedTargetedRules::compile_sources(
            &fixture.accepted,
            &fixture.value_catalog,
            sources,
            work,
        )
        .map_err(QueryError::execute)
    });
    (root.observed(resource), result)
}

#[test]
fn targeted_ordering_uses_the_existing_caller_allowance() {
    let fixture = targeted_rule_fixture();
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        let (_, result) = compile_with_limit(&fixture, Resource::SortEntries, 0, lane, usize::MAX);
        assert!(
            result.is_err(),
            "targeted ordering must not bypass the caller's zero sort allowance"
        );
    }
}

fn assert_budget(error: QueryError, resource: Resource, lane: Lane, limit: u64, actual: u64) {
    for fact in [
        (DiagnosticFactTag::BudgetResource, resource.raw()),
        (DiagnosticFactTag::ExecutionLane, lane.raw()),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
    ] {
        assert!(error.diagnostic_facts().contains(&fact), "missing {fact:?}");
    }
}

#[test]
fn targeted_ordering_admits_exact_short_and_cumulative_resources_without_changing_output() {
    let fixture = grouped_fixture();
    // Fourteen rules span ten roots, including groups of two and three. Seven
    // composite definitions include one record with two oppositely ordered IDs.
    let scratch = icydb_schema::compact_sort_scratch_bytes(GROUPED_RULES)
        + icydb_schema::compact_sort_scratch_bytes(2);
    let resources = [
        (Resource::SortEntries, (GROUPED_RULES + 2) as u64),
        (
            Resource::SortComparisons,
            crate::db::resource::budget::sort_comparison_allowance(GROUPED_RULES)
                + crate::db::resource::budget::sort_comparison_allowance(2),
        ),
        (Resource::SortTemporaryBytes, scratch),
        (
            Resource::TemporaryBytes,
            scratch + 2 * size_of::<usize>() as u64,
        ),
        (
            Resource::PredicateExpressionSteps,
            14 + GROUPED_RULES as u64 * (11 + 1 + 8),
        ),
    ];
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        for (resource, expected) in resources {
            let (observed, baseline) =
                compile_with_limit(&fixture, resource, u64::MAX, lane, usize::MAX);
            assert_eq!(observed, expected);
            let baseline = baseline.unwrap();
            assert!(
                baseline
                    .evaluate(&borrowed_values(&fixture.values))
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                compile_with_limit(&fixture, resource, expected, lane, usize::MAX)
                    .1
                    .unwrap(),
                baseline
            );
            assert_eq!(
                compile_with_limit(&fixture, resource, expected + 1, lane, usize::MAX)
                    .1
                    .unwrap(),
                baseline
            );
            let (_, short) = compile_with_limit(&fixture, resource, expected - 1, lane, usize::MAX);
            assert_budget(short.unwrap_err(), resource, lane, expected - 1, expected);
            // Independent preparation segments must share the existing request.
            let root = request_with_limit(resource, expected * 2 - 1);
            for repetition in 0..2 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    CompiledAcceptedTargetedRules::compile_sources(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        fixture.accepted_targeted_sources(),
                        work,
                    )
                    .map_err(QueryError::execute)
                });
                if repetition == 0 {
                    assert_eq!(result.unwrap(), baseline);
                } else {
                    assert_budget(
                        result.unwrap_err(),
                        resource,
                        lane,
                        expected * 2 - 1,
                        expected * 2,
                    );
                }
            }
        }
    }
}

#[test]
fn targeted_ordering_retains_stable_programs_and_first_violation_under_reversed_input() {
    let fixture = grouped_fixture();
    let compile = |reverse| {
        let mut sources = fixture.accepted_targeted_sources();
        if reverse {
            sources.reverse();
        }
        CompiledAcceptedTargetedRules::compile_sources(
            &fixture.accepted,
            &fixture.value_catalog,
            sources,
            &MaintenanceConstructionBudget::new(),
        )
        .unwrap()
    };
    let forward = compile(false);
    let reverse = compile(true);
    assert_eq!(forward, reverse);
    let mut invalid = fixture.values.clone();
    invalid[1] = Some(TargetedRuleFixture::node(
        12,
        vec![TargetedRuleFixture::node(13, Vec::new())],
    ));
    let violation = forward
        .evaluate(&borrowed_values(&invalid))
        .unwrap()
        .unwrap();
    assert_eq!(
        reverse.evaluate(&borrowed_values(&invalid)).unwrap(),
        Some(violation.clone())
    );
    assert_eq!(
        violation.path.components(),
        &[
            AcceptedTargetPathComponent::RootField(FieldId::new(2)),
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: fixture.node_type,
                member_id: CompositeFieldId::new(10).unwrap()
            },
        ]
    );
}

#[test]
fn empty_sources_skip_catalog_ordering_and_single_sources_still_admit_record_order() {
    let fixture = grouped_fixture();
    for resource in [
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::SortTemporaryBytes,
        Resource::TemporaryBytes,
        Resource::PredicateExpressionSteps,
    ] {
        let (observed, empty) = compile_with_limit(&fixture, resource, 0, Lane::Mutation, 0);
        assert_eq!(observed, 0);
        assert!(empty.unwrap().is_empty());
    }
    let (entries, single) =
        compile_with_limit(&fixture, Resource::SortEntries, 2, Lane::Mutation, 1);
    assert_eq!(entries, 2);
    assert!(!single.unwrap().is_empty());
    let (_, short) = compile_with_limit(&fixture, Resource::SortEntries, 1, Lane::Mutation, 1);
    assert_budget(
        short.unwrap_err(),
        Resource::SortEntries,
        Lane::Mutation,
        1,
        2,
    );
}

#[test]
fn whole_row_compilation_preserves_targeted_ordering_refusal_facts_before_other_sorts() {
    let fixture = grouped_fixture();
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        let root = request_with_limit(Resource::SortEntries, 0);
        let result = PreparationWork::run(&root.scope(), lane, |work| {
            CompiledAcceptedRowConstraints::compile(
                &fixture.accepted,
                &fixture.value_catalog,
                FINGERPRINT,
                work,
            )
            .map_err(QueryError::execute)
        });
        assert_budget(
            result.unwrap_err(),
            Resource::SortEntries,
            lane,
            0,
            GROUPED_RULES as u64,
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
    }
}
