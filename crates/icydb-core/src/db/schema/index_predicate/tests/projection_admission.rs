//! Accepted projection shares caller authority before shell/name construction.

use super::{K, P, catalog, snapshot};
use crate::{
    db::{
        QueryError,
        data::AcceptedStructuralRowAuthority,
        predicate::{CoercionId, CompareOp, Predicate},
        query::preparation::PreparationWork,
        schema::{AcceptedSchemaSnapshot, FieldId},
        tests::support::request_with_limit,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::{cell::Cell, rc::Rc};

#[test]
fn projection_shells_names_and_lookups_share_exact_cumulative_limits() {
    let base = snapshot(K::Nat64);
    let names = base
        .fields()
        .iter()
        .map(|field| (field.id(), field.name().to_owned()))
        .collect::<Vec<_>>();
    let contract = AcceptedStructuralRowAuthority::from_candidate_snapshot(
        "tests::Filtered",
        Rc::new(AcceptedSchemaSnapshot::try_new(base.clone()).unwrap()),
        catalog(),
    )
    .unwrap()
    .into_row_contract();
    let comparison = |op, values| P::Compare {
        field: FieldId::new(2),
        op,
        coercion: CoercionId::Strict,
        values,
    };
    let shell = size_of::<Predicate>() as u64;
    // Counts are independent of the projection implementation: visits, scanned
    // field IDs, copied name visits/bytes, and known destination cardinalities.
    let cases = [
        (P::True, 1, 0),
        (P::False, 1, 0),
        (P::IsNull(FieldId::new(2)), 8, 4),
        (P::IsNotNull(FieldId::new(3)), 10, 5),
        (
            P::CompareFields {
                left: FieldId::new(2),
                op: CompareOp::Eq,
                right: FieldId::new(3),
                coercion: CoercionId::Strict,
            },
            17,
            9,
        ),
        (P::Not(Box::new(P::True)), 2, shell),
        (P::And(vec![P::True, P::False]), 3, 2 * shell),
        (P::Or(vec![P::True, P::False]), 3, 2 * shell),
        (comparison(CompareOp::Eq, vec![None]), 8, 4),
        (
            comparison(CompareOp::In, vec![None; 2]),
            8,
            4 + 2 * size_of::<Value>() as u64,
        ),
        (comparison(CompareOp::NotIn, vec![]), 8, 4),
    ];
    for (bound, steps, bytes) in cases {
        let expected = bound.to_predicate(base.fields(), &catalog(), None).unwrap();
        for route in 0..3 {
            for lane in [
                Lane::PublicRead,
                Lane::TrustedRead,
                Lane::Diagnostic,
                Lane::Mutation,
            ] {
                for (resource, required) in [
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::TemporaryBytes, bytes),
                ] {
                    if required == 0 {
                        continue;
                    }
                    for limit in [required - 1, required, 2 * required] {
                        let root = request_with_limit(resource, limit);
                        for attempt in 1..=3 {
                            let result = PreparationWork::run(&root.scope(), lane, |work| {
                                let result = match route {
                                    0 => bound.to_predicate(base.fields(), &catalog(), Some(work)),
                                    1 => bound.to_predicate_with_names(
                                        &names,
                                        &catalog(),
                                        Some(work),
                                    ),
                                    _ => bound.for_row_contract(&contract, Some(work)),
                                };
                                result.map_err(QueryError::execute)
                            });
                            if attempt * required <= limit {
                                assert_eq!(result.unwrap(), expected);
                                assert_eq!(root.observed(resource), attempt * required);
                            } else {
                                let facts = result.unwrap_err().diagnostic_facts();
                                assert!(root.observed(resource) > limit);
                                for fact in [
                                    (DiagnosticFactTag::BudgetResource, resource.raw()),
                                    (DiagnosticFactTag::ExecutionLane, lane.raw()),
                                    (DiagnosticFactTag::Limit, limit),
                                    (DiagnosticFactTag::Actual, root.observed(resource)),
                                ] {
                                    assert!(facts.contains(&fact));
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn refused_projection_backing_stops_before_children_or_name_resolution() {
    let leaf = P::IsNull(FieldId::new(2));
    for bound in [
        P::Not(Box::new(leaf.clone())),
        P::And(vec![leaf.clone()]),
        P::Or(vec![leaf]),
    ] {
        let root = request_with_limit(Resource::TemporaryBytes, 0);
        let visits = Cell::new(0);
        let error = PreparationWork::run(&root.scope(), Lane::Mutation, |work| {
            bound
                .project(
                    &|_| {
                        visits.set(visits.get() + 1);
                        Err(InternalError::store_corruption())
                    },
                    &catalog(),
                    Some(work),
                )
                .map_err(QueryError::execute)
        })
        .unwrap_err();
        assert_eq!(visits.get(), 0);
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
    }
}
