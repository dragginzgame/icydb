//! Literal payload admission shares the projection caller's decode authority.

use super::{K, P, catalog, snapshot};
use crate::{
    db::{
        QueryError,
        data::AcceptedStructuralRowAuthority,
        predicate::{CoercionId, CompareOp},
        query::preparation::PreparationWork,
        schema::{
            AcceptedCheckLiteralV1, AcceptedSchemaSnapshot, FieldId,
            check::bind_index_predicate_literal,
        },
        tests::support::request_with_limit,
    },
    error::InternalError,
    value::InputValue,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::rc::Rc;

#[test]
fn literal_payload_bytes_obey_exact_cumulative_projection_limits() {
    for (kind, input) in [
        (K::Nat64, InputValue::nat64(42)),
        (K::Text { max_len: None }, InputValue::from("é☃ payload")),
    ] {
        let base = snapshot(kind.clone());
        let catalog = catalog();
        let literal = bind_index_predicate_literal(
            input,
            kind,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        )
        .unwrap();
        let bytes = literal.payload().len() as u64;
        assert!(bytes > 0);
        let names = base
            .fields()
            .iter()
            .map(|f| (f.id(), f.name().to_owned()))
            .collect::<Vec<_>>();
        let contract = AcceptedStructuralRowAuthority::from_candidate_snapshot(
            "tests::Filtered",
            Rc::new(AcceptedSchemaSnapshot::try_new(base.clone()).unwrap()),
            catalog.clone(),
        )
        .unwrap()
        .into_row_contract();
        for (op, values, required) in [
            (CompareOp::Eq, vec![Some(literal.clone())], bytes),
            (
                CompareOp::In,
                vec![Some(literal.clone()), None, Some(literal.clone())],
                2 * bytes,
            ),
            (CompareOp::NotIn, vec![None, Some(literal)], bytes),
            (CompareOp::Eq, vec![None], 0),
            (CompareOp::In, vec![], 0),
        ] {
            let bound = P::Compare {
                field: FieldId::new(2),
                op,
                coercion: CoercionId::Strict,
                values,
            };
            let expected = bound.to_predicate(base.fields(), &catalog, None).unwrap();
            for route in 0..3 {
                for lane in [
                    Lane::PublicRead,
                    Lane::TrustedRead,
                    Lane::Diagnostic,
                    Lane::Mutation,
                ] {
                    for limit in [required.saturating_sub(1), required, 2 * required] {
                        let root = request_with_limit(Resource::DecodedBytes, limit);
                        for attempt in 1..=3 {
                            let result = PreparationWork::run(&root.scope(), lane, |work| {
                                match route {
                                    0 => bound.to_predicate(base.fields(), &catalog, Some(work)),
                                    1 => {
                                        bound.to_predicate_with_names(&names, &catalog, Some(work))
                                    }
                                    _ => bound.for_row_contract(&contract, Some(work)),
                                }
                                .map_err(QueryError::execute)
                            });
                            if attempt * required <= limit {
                                assert_eq!(result.unwrap(), expected);
                                assert_eq!(
                                    root.observed(Resource::DecodedBytes),
                                    attempt * required
                                );
                            } else {
                                let facts = result.unwrap_err().diagnostic_facts();
                                for fact in [
                                    (
                                        DiagnosticFactTag::BudgetResource,
                                        Resource::DecodedBytes.raw(),
                                    ),
                                    (DiagnosticFactTag::ExecutionLane, lane.raw()),
                                    (DiagnosticFactTag::Limit, limit),
                                    (
                                        DiagnosticFactTag::Actual,
                                        root.observed(Resource::DecodedBytes),
                                    ),
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
fn literal_decode_refusal_precedes_corruption_and_name_lookup() {
    let catalog = catalog();
    let valid = bind_index_predicate_literal(
        InputValue::nat64(42),
        K::Nat64,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap();
    let corrupt = AcceptedCheckLiteralV1::from_accepted_parts(
        valid.kind().clone(),
        valid.storage_decode(),
        valid.leaf_codec(),
        vec![255],
    );
    let bound = P::Compare {
        field: FieldId::new(2),
        op: CompareOp::Eq,
        coercion: CoercionId::Strict,
        values: vec![Some(corrupt)],
    };
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        let root = request_with_limit(Resource::DecodedBytes, 0);
        let error = PreparationWork::run(&root.scope(), lane, |work| {
            bound
                .project(
                    &|_| panic!("decode refusal must precede name lookup"),
                    &catalog,
                    Some(work),
                )
                .map_err(QueryError::execute)
        })
        .unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::DecodedBytes.raw()
        )));
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        let admitted = request_with_limit(Resource::DecodedBytes, 1);
        PreparationWork::run(&admitted.scope(), lane, |work| {
            let error = bound
                .project(
                    &|_| panic!("corruption precedes names"),
                    &catalog,
                    Some(work),
                )
                .unwrap_err();
            assert_eq!(
                error.diagnostic_code(),
                InternalError::store_corruption().diagnostic_code()
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(admitted.observed(Resource::DecodedBytes), 1);
    }
    let error = bound
        .to_predicate(snapshot(K::Nat64).fields(), &catalog, None)
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::store_corruption().diagnostic_code()
    );
}

#[test]
fn retained_literal_projection_does_not_borrow_ambient_decode_limits() {
    let base = snapshot(K::Nat64);
    let catalog = catalog();
    let bound = P::bind_test_sql("left = 42", base.fields());
    let expected = bound.to_predicate(base.fields(), &catalog, None).unwrap();
    let root = request_with_limit(Resource::DecodedBytes, 0);
    let result = PreparationWork::run(&root.scope(), Lane::Mutation, |_work| {
        bound
            .to_predicate(base.fields(), &catalog, None)
            .map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(result, expected);
    assert_eq!(root.observed(Resource::DecodedBytes), 0);
}

#[test]
fn later_literal_refusal_stops_membership_and_nested_projection() {
    let catalog = catalog();
    let valid = bind_index_predicate_literal(
        InputValue::nat64(42),
        K::Nat64,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap();
    let bytes = valid.payload().len() as u64;
    let corrupt = AcceptedCheckLiteralV1::from_accepted_parts(
        valid.kind().clone(),
        valid.storage_decode(),
        valid.leaf_codec(),
        vec![255],
    );
    let compare = |op, values| P::Compare {
        field: FieldId::new(2),
        op,
        coercion: CoercionId::Strict,
        values,
    };
    let scalar = || compare(CompareOp::Eq, vec![Some(valid.clone())]);
    let bad = || compare(CompareOp::Eq, vec![Some(corrupt.clone())]);
    let cases = [
        (
            compare(
                CompareOp::In,
                vec![
                    Some(valid.clone()),
                    None,
                    Some(corrupt.clone()),
                    Some(valid.clone()),
                ],
            ),
            0,
        ),
        (
            compare(
                CompareOp::NotIn,
                vec![Some(valid.clone()), Some(corrupt.clone())],
            ),
            0,
        ),
        (P::And(vec![scalar(), bad(), scalar()]), 1),
        (P::Not(Box::new(P::Or(vec![scalar(), bad(), scalar()]))), 1),
    ];
    for (bound, resolved_prefix) in cases {
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let root = request_with_limit(Resource::DecodedBytes, bytes);
            let names = std::cell::Cell::new(0);
            let error = PreparationWork::run(&root.scope(), lane, |work| {
                bound
                    .project(
                        &|_| {
                            names.set(names.get() + 1);
                            Ok("left")
                        },
                        &catalog,
                        Some(work),
                    )
                    .map_err(QueryError::execute)
            })
            .unwrap_err();
            assert_eq!(names.get(), resolved_prefix);
            assert_eq!(root.observed(Resource::DecodedBytes), bytes + 1);
            for fact in [
                (
                    DiagnosticFactTag::BudgetResource,
                    Resource::DecodedBytes.raw(),
                ),
                (DiagnosticFactTag::ExecutionLane, lane.raw()),
                (DiagnosticFactTag::Limit, bytes),
                (DiagnosticFactTag::Actual, bytes + 1),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
            let error = bound.project(&|_| Ok("left"), &catalog, None).unwrap_err();
            assert_eq!(
                error.diagnostic_code(),
                InternalError::store_corruption().diagnostic_code()
            );
        }
    }
}
