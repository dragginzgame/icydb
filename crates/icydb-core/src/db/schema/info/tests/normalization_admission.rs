//! Schema normalization uses the current request before retaining operands.

use super::{
    enum_newtype_query_schema, enum_newtype_query_schema_with_variants, newtype_query_schema,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate,
            normalize_enum_literals,
        },
        query::preparation::PreparationWork,
        schema::{AcceptedFieldKind, FieldStorageDecode, SchemaInfo, TestEnumVariant},
        tests::support::request_with_limit,
    },
    value::{CanonicalEnumBody, Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn run(
    schema: &SchemaInfo,
    predicate: &Predicate,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<Predicate, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        normalize_enum_literals(schema, predicate, work)
    })
}

fn schema_normalization_input() -> Predicate {
    let leaves = vec![
        Predicate::True,
        Predicate::False,
        Predicate::eq("id".into(), Value::Int64(7)),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![Value::Int64(7), Value::Nat64(7)]),
            CoercionId::Strict,
        )),
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "id",
            CompareOp::Eq,
            "id",
            CoercionId::Strict,
        )),
        Predicate::IsNull {
            field: "name".into(),
        },
        Predicate::IsNotNull {
            field: "name".into(),
        },
        Predicate::IsMissing {
            field: "name".into(),
        },
        Predicate::IsEmpty {
            field: "aliases".into(),
        },
        Predicate::IsNotEmpty {
            field: "aliases".into(),
        },
        Predicate::TextContains {
            field: "name".into(),
            value: Value::Text("x".repeat(128)),
        },
        Predicate::TextContainsCi {
            field: "name".into(),
            value: Value::Text("İ".repeat(128)),
        },
        Predicate::eq(
            "unknown".into(),
            Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 512]))]),
        ),
    ];
    Predicate::And(vec![Predicate::Not(Box::new(Predicate::Or(leaves)))])
}

#[test]
fn schema_normalization_admits_shapes_before_copying_and_shares_request_limits() {
    let schema = newtype_query_schema();
    let input = schema_normalization_input();
    let mut expected = input.clone();
    let Predicate::And(and) = &mut expected else {
        panic!("AND")
    };
    let Predicate::Not(not) = &mut and[0] else {
        panic!("NOT")
    };
    let Predicate::Or(children) = not.as_mut() else {
        panic!("OR")
    };
    children[2] = Predicate::eq("id".into(), Value::Nat64(7));
    children[3] = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(vec![Value::Nat64(7)]),
        CoercionId::Strict,
    ));
    children[4] = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        "id",
        CompareOp::Eq,
        "id",
        CoercionId::NumericWiden,
    ));

    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for resource in [
            Resource::SortEntries,
            Resource::SortComparisons,
            Resource::TemporaryBytes,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
        ] {
            let measured = request_with_limit(resource, 16_000_000);
            assert_eq!(run(&schema, &input, &measured, lane).unwrap(), expected);
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request_with_limit(resource, limit);
                for _ in 0..limit / exact {
                    assert_eq!(run(&schema, &input, &root, lane).unwrap(), expected);
                }
                let error = run(&schema, &input, &root, lane).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                );
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn enum_normalization_admits_loose_canonical_and_collection_operands() {
    for collection in [false, true] {
        let schema = enum_newtype_query_schema(collection);
        let op = if collection {
            CompareOp::Contains
        } else {
            CompareOp::Eq
        };
        let loose = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            op,
            Value::Text("Active".into()),
            CoercionId::Strict,
        ));
        let root = request_with_limit(Resource::TemporaryBytes, 16_000_000);
        let canonical = run(&schema, &loose, &root, Lane::Diagnostic).unwrap();
        assert!(
            matches!(&canonical, Predicate::Compare(cmp) if matches!(cmp.value(), Value::Enum(_)))
        );
        for input in [&loose, &canonical] {
            for resource in [
                Resource::TemporaryBytes,
                Resource::NestedValueSteps,
                Resource::PredicateExpressionSteps,
            ] {
                let measured = request_with_limit(resource, 16_000_000);
                assert_eq!(
                    run(&schema, input, &measured, Lane::Diagnostic).unwrap(),
                    canonical
                );
                let exact = measured.observed(resource);
                let root = request_with_limit(resource, exact - 1);
                let error = run(&schema, input, &root, Lane::Diagnostic).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
        }
    }
}

#[test]
fn discarded_boolean_branches_still_validate_and_admit() {
    let schema = enum_newtype_query_schema(false);
    for first in [Predicate::True, Predicate::False] {
        let input = Predicate::Or(vec![
            first,
            Predicate::eq("stage".into(), Value::Text("not_a_variant".into())),
        ]);
        let root = request_with_limit(Resource::TemporaryBytes, 16_000_000);
        assert!(matches!(
            run(&schema, &input, &root, Lane::PublicRead),
            Err(QueryError::Validate(_))
        ));
        let root = request_with_limit(Resource::TemporaryBytes, 0);
        let error = run(&schema, &input, &root, Lane::PublicRead).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
    }
}

#[test]
fn enum_membership_ordering_uses_existing_request_admission() {
    let schema = enum_newtype_query_schema(false);
    for op in [CompareOp::In, CompareOp::NotIn] {
        let input = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            op,
            Value::List(vec![Value::Text("Active".into()); 3]),
            CoercionId::Strict,
        ));
        for resource in [Resource::SortEntries, Resource::SortComparisons] {
            let generous = request_with_limit(resource, u64::MAX);
            let expected = run(&schema, &input, &generous, Lane::PublicRead).unwrap();
            let amount = generous.observed(resource);
            assert!(amount > 0);
            let exact = request_with_limit(resource, amount);
            assert_eq!(
                run(&schema, &input, &exact, Lane::PublicRead).unwrap(),
                expected
            );
            for root in [&exact, &request_with_limit(resource, amount - 1)] {
                let error = run(&schema, &input, root, Lane::PublicRead).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
        }
    }
}

#[test]
fn enum_set_ordering_refuses_before_catalog_duplicate_validation() {
    let schema = enum_newtype_query_schema(true);
    let loose = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Contains,
        Value::Text("Active".into()),
        CoercionId::Strict,
    ));
    let root = request_with_limit(Resource::TemporaryBytes, u64::MAX);
    let Predicate::Compare(member) = run(&schema, &loose, &root, Lane::PublicRead).unwrap() else {
        panic!("canonical enum member")
    };
    let input = Predicate::eq("stage".into(), Value::List(vec![member.value().clone(); 2]));
    let root = request_with_limit(Resource::SortComparisons, 0);
    let error = run(&schema, &input, &root, Lane::PublicRead).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::SortComparisons.raw()
    )));
    let root = request_with_limit(Resource::SortComparisons, u64::MAX);
    assert!(matches!(
        run(&schema, &input, &root, Lane::PublicRead),
        Err(QueryError::Validate(_))
    ));
}

fn canonical_enum_text(schema: &SchemaInfo, text: &str) -> Predicate {
    let catalog = schema.enum_catalog();
    let type_id = catalog.type_id("tests::Stage").unwrap();
    let variant_id = catalog
        .enum_type(type_id)
        .unwrap()
        .variant_id("Text")
        .unwrap();
    Predicate::eq(
        "stage".into(),
        Value::Enum(ValueEnum::new(
            type_id,
            variant_id,
            CanonicalEnumBody::Payload(Box::new(Value::Text(text.into()))),
        )),
    )
}

fn enum_text_query_schema() -> SchemaInfo {
    enum_newtype_query_schema_with_variants(
        false,
        vec![TestEnumVariant::payload(
            "Text",
            AcceptedFieldKind::Text { max_len: Some(4) },
            FieldStorageDecode::CatalogValue,
        )],
    )
}

#[test]
fn canonical_enum_text_scan_refuses_before_schema_length_validation() {
    let schema = enum_text_query_schema();
    for text in ["x".repeat(1024), "界".repeat(1024)] {
        let input = canonical_enum_text(&schema, &text);
        // Admit the predicate dispatch, but none of the later text scan.
        let root = request_with_limit(Resource::PredicateExpressionSteps, 1);
        let error = run(&schema, &input, &root, Lane::PublicRead).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        )));
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        let root = request_with_limit(Resource::PredicateExpressionSteps, u64::MAX);
        assert!(matches!(
            run(&schema, &input, &root, Lane::PublicRead),
            Err(QueryError::Validate(_))
        ));
    }
}

#[test]
fn canonical_enum_text_validation_and_copy_share_cumulative_byte_admission() {
    let schema = enum_text_query_schema();
    for text in ["", "a", "界", "界界界界"] {
        let input = canonical_enum_text(&schema, text);
        // Dispatch (1), validation bytes, copy bytes, and copied "stage" (6).
        // Schema length remains a character count; admission reserves UTF-8 bytes.
        let amount = 7 + 2 * text.len() as u64;
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            for limit in [amount - 1, amount, amount * 2] {
                let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
                for attempt in 1..=3 {
                    let result = run(&schema, &input, &root, lane);
                    if attempt * amount <= limit {
                        assert_eq!(result.unwrap(), input);
                        assert_eq!(
                            root.observed(Resource::PredicateExpressionSteps),
                            attempt * amount
                        );
                    } else {
                        let error = result.unwrap_err();
                        let actual = root.observed(Resource::PredicateExpressionSteps);
                        assert!(actual > limit);
                        for fact in [
                            (
                                DiagnosticFactTag::BudgetResource,
                                Resource::PredicateExpressionSteps.raw(),
                            ),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                            (DiagnosticFactTag::Actual, actual),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn enum_set_validation_shares_exact_and_cumulative_request_allowances() {
    let schema = enum_newtype_query_schema(true);
    let root = request_with_limit(Resource::TemporaryBytes, u64::MAX);
    let members = ["Active", "Paused"].map(|name| {
        let input = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::Contains,
            Value::Text(name.into()),
            CoercionId::Strict,
        ));
        let Predicate::Compare(member) = run(&schema, &input, &root, Lane::PublicRead).unwrap()
        else {
            panic!("canonical enum member")
        };
        member.value().clone()
    });
    let set = Value::List(members.into());
    for op in [CompareOp::Eq, CompareOp::In, CompareOp::NotIn] {
        let value = if op == CompareOp::Eq {
            set.clone()
        } else {
            Value::List(vec![set.clone(); 2])
        };
        let expected = if op == CompareOp::Eq {
            set.clone()
        } else {
            Value::List(vec![set.clone()])
        };
        let predicate = |value| {
            Predicate::Compare(ComparePredicate::with_coercion(
                "stage",
                op,
                value,
                CoercionId::Strict,
            ))
        };
        assert_enum_collection_boundary(&schema, &predicate(value), &predicate(expected));
    }
    // Canonical validation must still reject disorder rather than silently
    // sorting an already-canonical value under the new admission preflight.
    let Value::List(mut reversed) = set else {
        unreachable!()
    };
    reversed.reverse();
    let input = Predicate::eq("stage".into(), Value::List(reversed));
    assert!(matches!(
        run(&schema, &input, &root, Lane::PublicRead),
        Err(QueryError::Validate(_))
    ));
}

fn assert_enum_collection_boundary(schema: &SchemaInfo, input: &Predicate, expected: &Predicate) {
    for resource in [
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::TemporaryBytes,
        Resource::PredicateExpressionSteps,
        Resource::NestedValueSteps,
    ] {
        let baseline = request_with_limit(resource, u64::MAX);
        assert_eq!(
            run(schema, input, &baseline, Lane::PublicRead).unwrap(),
            *expected
        );
        let amount = baseline.observed(resource);
        assert!(amount > 0);
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            for limit in [amount - 1, amount, amount * 2] {
                let root = request_with_limit(resource, limit);
                for attempt in 1..=3 {
                    let result = run(schema, input, &root, lane);
                    if attempt * amount <= limit {
                        assert_eq!(result.unwrap(), *expected);
                        assert_eq!(root.observed(resource), attempt * amount);
                    } else {
                        let error = result.unwrap_err();
                        let actual = root.observed(resource);
                        assert!(actual > limit);
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                            (DiagnosticFactTag::Actual, actual),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}
