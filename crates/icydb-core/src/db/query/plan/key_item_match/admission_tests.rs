//! Selected lookup conversion reuses compiler semantics and request admission.

use super::{copy_lookup_value_for_key_item, eq_lookup_value_for_key_item};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{SemanticIndexExpression, SemanticIndexKeyItemRef},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::CoercionId,
        query::preparation::PreparationWork,
        schema::PersistedIndexExpressionOp,
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn selected_lookup_conversion_obeys_exact_and_cumulative_admission() {
    let lower = SemanticIndexExpression::new(PersistedIndexExpressionOp::Lower, "name".into());
    for text in [
        String::new(),
        "ASCII".into(),
        "İΣ\0 🦀".into(),
        "İΣ".repeat(2048),
    ] {
        let value = Value::Text(text.clone());
        let (lower_bytes, lower_work) = lower_text_construction_allowance(text.len());
        for (key, coercion, bytes, steps, visits) in [
            (
                SemanticIndexKeyItemRef::Field("name"),
                CoercionId::Strict,
                text.len() as u64,
                text.len() as u64,
                1,
            ),
            (
                SemanticIndexKeyItemRef::AcceptedExpression(&lower),
                CoercionId::TextCasefold,
                lower_bytes,
                lower_work,
                0,
            ),
        ] {
            let expected =
                eq_lookup_value_for_key_item(key, "name", &value, coercion, true).unwrap();
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::NestedValueSteps, visits),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 0..3 {
                                let result = copy_lookup_value_for_key_item(
                                    key, "name", &value, coercion, true, work,
                                );
                                if exact == 0 || limit >= exact && attempt < 2 {
                                    assert_eq!(result.unwrap(), Some(expected.clone()));
                                    assert_eq!(root.observed(resource), exact * (attempt + 1));
                                } else {
                                    let error = QueryError::execute(result.unwrap_err());
                                    assert!(error.diagnostic_facts().contains(&(
                                        DiagnosticFactTag::BudgetResource,
                                        resource.raw(),
                                    )));
                                    break;
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(value, Value::Text(text.clone()));
                        assert_eq!(root.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn unsupported_selected_lookup_pairs_do_not_start_conversion() {
    let lower = SemanticIndexExpression::new(PersistedIndexExpressionOp::Lower, "name".into());
    let upper = SemanticIndexExpression::new(PersistedIndexExpressionOp::Upper, "name".into());
    let text = Value::Text("İΣ".into());
    let number = Value::Nat64(7);
    for (key, field, value, coercion, compatible) in [
        (
            SemanticIndexKeyItemRef::Field("name"),
            "other",
            &text,
            CoercionId::Strict,
            true,
        ),
        (
            SemanticIndexKeyItemRef::Field("name"),
            "name",
            &text,
            CoercionId::Strict,
            false,
        ),
        (
            SemanticIndexKeyItemRef::Field("name"),
            "name",
            &text,
            CoercionId::TextCasefold,
            true,
        ),
        (
            SemanticIndexKeyItemRef::AcceptedExpression(&lower),
            "name",
            &text,
            CoercionId::Strict,
            true,
        ),
        (
            SemanticIndexKeyItemRef::AcceptedExpression(&lower),
            "name",
            &number,
            CoercionId::TextCasefold,
            true,
        ),
        (
            SemanticIndexKeyItemRef::AcceptedExpression(&upper),
            "name",
            &text,
            CoercionId::TextCasefold,
            true,
        ),
    ] {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            assert!(
                copy_lookup_value_for_key_item(key, field, value, coercion, compatible, work)
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
        for resource in [
            Resource::TemporaryBytes,
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            assert_eq!(root.observed(resource), 0);
        }
    }
}
