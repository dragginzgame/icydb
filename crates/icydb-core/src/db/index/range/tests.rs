//! Raw bounds share byte-capacity admission across prefix and range lowering.

use super::{
    IndexRangeBoundEncodeError, TextPrefixBoundMode, admit_text_prefix_bounds,
    build_index_component_range_with_encoded_prefix,
    build_index_prefix_bounds_for_encoded_components, starts_with_component_bounds,
};
use crate::{
    MAX_INDEX_FIELDS,
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::{EncodedValue, IndexId, IndexKeyKind, RawIndexStoreKey},
        query::preparation::PreparationWork,
    },
    types::EntityTag,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::ops::Bound;

#[test]
fn semantic_prefix_admission_covers_output_and_rejects_before_construction() {
    for prefix in ["", "abc", "\u{7f}", "\u{d7ff}", "é\u{10ffff}", "\u{10ffff}"] {
        for mode in [TextPrefixBoundMode::Strict, TextPrefixBoundMode::LowerOnly] {
            let len = prefix.len() as u64;
            let (bytes, steps) = if prefix.is_empty() {
                (0, 0)
            } else if mode == TextPrefixBoundMode::Strict {
                (2 * len + 1, 3 * len + 1)
            } else {
                (len, len)
            };
            let expected = starts_with_component_bounds(prefix, mode);
            let build = |work: &PreparationWork<'_>| {
                admit_text_prefix_bounds(prefix, mode, work).map_err(QueryError::execute)?;
                Ok(starts_with_component_bounds(prefix, mode))
            };
            for (resource, allowance) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
            ] {
                let root = |limit| {
                    RequestExecutionRoot::new_for_tests(
                        HardExecutionBudget::uniform_for_tests(
                            16_000_000,
                            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                        )
                        .with_limit_for_tests(resource, limit),
                    )
                };
                if allowance > 0 {
                    let rejected = root(allowance - 1);
                    let error = PreparationWork::run(&rejected.scope(), Lane::Diagnostic, build)
                        .unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
                let request = root(allowance);
                let actual =
                    PreparationWork::run(&request.scope(), Lane::Diagnostic, build).unwrap();
                assert_eq!(actual, expected);
                assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
                assert_eq!(request.observed(Resource::PredicateExpressionSteps), steps);
                if let Some(bounds) = actual {
                    let retained: usize = <[Bound<Value>; 2]>::from(bounds)
                        .into_iter()
                        .map(|bound| match bound {
                            Bound::Included(Value::Text(text))
                            | Bound::Excluded(Value::Text(text)) => text.capacity(),
                            Bound::Unbounded => 0,
                            _ => unreachable!(),
                        })
                        .sum();
                    assert!(retained as u64 <= bytes);
                    assert!(
                        PreparationWork::run(&request.scope(), Lane::Diagnostic, build).is_err()
                    );
                }
                assert_eq!(request.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn text_prefix_intervals_preserve_unicode_successors_and_lower_only_bounds() {
    let cases = [
        ("\0", Some("\u{1}")),
        ("a\0b", Some("a\0c")),
        ("\u{7f}", Some("\u{80}")),
        ("\u{7ff}", Some("\u{800}")),
        ("\u{d7ff}", Some("\u{e000}")),
        ("\u{ffff}", Some("\u{10000}")),
        ("\u{10fffe}", Some("\u{10ffff}")),
        ("é\u{10ffff}\u{10ffff}", Some("ê")),
        ("\u{10ffff}\u{7f}\u{10ffff}", Some("\u{10ffff}\u{80}")),
        ("\u{10ffff}\u{10ffff}", None),
    ];
    for mode in [TextPrefixBoundMode::Strict, TextPrefixBoundMode::LowerOnly] {
        assert!(starts_with_component_bounds("", mode).is_none());
        for (prefix, successor) in cases {
            let expected_upper = match mode {
                TextPrefixBoundMode::Strict => successor.map_or(Bound::Unbounded, |next| {
                    Bound::Excluded(Value::Text(next.into()))
                }),
                TextPrefixBoundMode::LowerOnly => Bound::Unbounded,
            };
            assert_eq!(
                starts_with_component_bounds(prefix, mode),
                Some((Bound::Included(Value::Text(prefix.into())), expected_upper)),
            );
        }
    }
}

#[test]
fn text_prefix_successor_preserves_long_leading_bytes_and_drops_terminal_suffix() {
    let leading = "a\0é".repeat(2048);
    let prefix = format!("{leading}\u{7ff}{}", "\u{10ffff}".repeat(2048));
    assert_eq!(
        starts_with_component_bounds(&prefix, TextPrefixBoundMode::Strict),
        Some((
            Bound::Included(Value::Text(prefix.clone())),
            Bound::Excluded(Value::Text(format!("{leading}\u{800}"))),
        )),
    );
}

#[test]
fn raw_bounds_charge_exact_backing_and_preserve_cumulative_exhaustion() {
    let id = IndexId::new(EntityTag::new(254), 1);
    let cases = [
        (
            Bound::Included(Value::Text("a\0b".into())),
            Bound::Excluded(Value::Text("z".into())),
        ),
        (
            Bound::Excluded(Value::Text("a".into())),
            Bound::Included(Value::Text("z".into())),
        ),
        (Bound::Unbounded, Bound::Unbounded),
    ];
    for arity in [1, 2, MAX_INDEX_FIELDS] {
        for (lower, upper) in &cases {
            for prefix_only in [false, true] {
                let build = |work: &PreparationWork<'_>| {
                    let prefix =
                        vec![EncodedValue::try_from_ref(&Value::Nat64(42)).unwrap(); arity - 1];
                    if prefix_only {
                        build_index_prefix_bounds_for_encoded_components(
                            &id,
                            IndexKeyKind::User,
                            arity,
                            &prefix,
                            work,
                        )
                    } else {
                        build_index_component_range_with_encoded_prefix(
                            &id, arity, prefix, lower, upper, work,
                        )
                        .map(|result| {
                            let (lower, upper, _) = result.into_bounds_and_prefix_components();
                            (lower, upper)
                        })
                    }
                    .map_err(|error| QueryError::execute(error.into_internal_error()))
                };
                let root = |resource, limit| {
                    RequestExecutionRoot::new_for_tests(
                        HardExecutionBudget::uniform_for_tests(
                            16_000_000,
                            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                        )
                        .with_limit_for_tests(resource, limit),
                    )
                };
                let measured = root(Resource::TemporaryBytes, 16_000_000);
                let expected =
                    PreparationWork::run(&measured.scope(), Lane::Diagnostic, build).unwrap();
                let scalar_bytes: u64 = if prefix_only {
                    0
                } else {
                    [lower, upper]
                        .into_iter()
                        .map(|bound| match bound {
                            Bound::Unbounded => 0,
                            Bound::Included(value) | Bound::Excluded(value) => {
                                EncodedValue::try_from_ref(value)
                                    .unwrap()
                                    .into_bytes()
                                    .capacity() as u64
                            }
                        })
                        .sum()
                };
                let bytes = (RawIndexStoreKey::bound_backing_bytes(&expected.0)
                    + RawIndexStoreKey::bound_backing_bytes(&expected.1))
                    as u64
                    + scalar_bytes;
                assert_eq!(measured.observed(Resource::TemporaryBytes), bytes);
                assert_eq!(measured.observed(Resource::PredicateExpressionSteps), bytes);
                for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                    let request = root(resource, bytes * 2 - 1);
                    let actual =
                        PreparationWork::run(&request.scope(), Lane::Diagnostic, build).unwrap();
                    assert_eq!(actual, expected);
                    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, build)
                        .unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                    assert_eq!(request.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn range_scalar_exhaustion_precedes_raw_bound_construction() {
    let id = IndexId::new(EntityTag::new(254), 1);
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        for lower_bounded in [false, true] {
            let bounded = Bound::Included(Value::Text("a\0b".into()));
            let (lower, upper) = if lower_bounded {
                (bounded, Bound::Unbounded)
            } else {
                (Bound::Unbounded, bounded)
            };
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, 8),
            );
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                let error = build_index_component_range_with_encoded_prefix(
                    &id,
                    1,
                    Vec::new(),
                    &lower,
                    &upper,
                    work,
                )
                .err()
                .unwrap();
                let IndexRangeBoundEncodeError::Construction(error) = error else {
                    panic!("typed construction failure")
                };
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(Resource::TemporaryBytes), 9);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}
