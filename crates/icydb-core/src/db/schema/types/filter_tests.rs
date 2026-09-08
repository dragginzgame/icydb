//! Focused ownership and request-accounting boundaries for filter conversion.

use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
        schema::{
            AcceptedFieldKind, canonicalize_filter_literal_for_persisted_kind,
            materialize_filter_literal,
        },
    },
    types::{IntBig, NatBig},
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};
use std::borrow::Cow;

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn exact_big_filter_literals_borrow_storage_and_check_encoded_limits() {
    let cases = [
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            Value::IntBig(IntBig::from(-64)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 1 },
            Value::NatBig(NatBig::from(127_u64)),
        ),
    ];
    for (kind, input) in cases {
        let root = root(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            let result = canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)?;
            assert!(matches!(result, Some(Cow::Borrowed(value)) if std::ptr::eq(value, &raw const input)));
            Ok(())
        }).expect("an exact admitted big atom needs no allocation");
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
    for (kind, input) in [
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            Value::IntBig(IntBig::from(64)),
        ),
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            Value::IntBig(IntBig::from(-65)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 1 },
            Value::NatBig(NatBig::from(128_u64)),
        ),
    ] {
        let root = root(Resource::TemporaryBytes, 0);
        assert!(
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)
            })
            .expect("size rejection does not allocate")
            .is_none()
        );
    }
}

#[test]
fn big_filter_text_conversion_preserves_its_narrow_admission_family() {
    for (kind, valid, invalid, expected) in [
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            "-64",
            "64",
            Value::IntBig(IntBig::from(-64)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 1 },
            "127",
            "128",
            Value::NatBig(NatBig::from(127_u64)),
        ),
    ] {
        let root = root(Resource::PredicateExpressionSteps, 1_000);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            assert_eq!(
                canonicalize_filter_literal_for_persisted_kind(
                    &kind,
                    &Value::Text(valid.into()),
                    work
                )?
                .as_deref(),
                Some(&expected)
            );
            for input in [
                Value::Text(invalid.into()),
                Value::Text("invalid".into()),
                Value::Int64(1),
                Value::Nat64(1),
                Value::Null,
            ] {
                assert!(
                    canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)?.is_none()
                );
            }
            Ok(())
        })
        .expect("type misses are not budget failures");
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            (valid.len() + invalid.len() + "invalid".len()) as u64
        );
    }
}

#[test]
fn necessary_big_literal_copies_charge_before_materialization() {
    for input in [
        Value::IntBig(IntBig::from(-64)),
        Value::NatBig(NatBig::from(127_u64)),
    ] {
        for limit in [7, 8] {
            let root = root(Resource::TemporaryBytes, limit);
            let result =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    materialize_filter_literal(Cow::Borrowed(&input), work)
                });
            if limit == 8 {
                assert_eq!(result.expect("one conservative limb word fits"), input);
            } else {
                let error = result.expect_err("reject before cloning a magnitude");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 8);
        }
    }
}

#[test]
fn unchanged_filter_storage_is_borrowed_without_allocating() {
    let cases = [
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("Copper".into()),
        ),
        (
            AcceptedFieldKind::Blob { max_len: Some(2) },
            Value::Blob(vec![0, 1]),
        ),
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: None })),
            Value::List(vec![
                Value::Text("Copper".into()),
                Value::Text("Silver".into()),
            ]),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::List(Box::new(
                AcceptedFieldKind::Blob { max_len: None },
            )))),
            Value::List(vec![Value::List(vec![Value::Blob(vec![1, 2])])]),
        ),
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
            Value::List(vec![]),
        ),
    ];
    for (kind, input) in cases {
        let root = root(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            let canonical = canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)?;
            assert!(matches!(canonical, Some(Cow::Borrowed(value)) if std::ptr::eq(value, &raw const input)));
            Ok(())
        })
        .expect("unchanged storage needs no allocation");
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        assert!(root.observed(Resource::NestedValueSteps) > 0);
    }
}

#[test]
fn converted_lists_charge_storage_and_preserve_the_source_on_failure() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: None }));
    let input = Value::List(vec![
        Value::Blob(vec![1, 2]),
        Value::Text("abcd".into()),
        Value::Blob(vec![3]),
    ]);
    let expected = Value::List(vec![
        Value::Blob(vec![1, 2]),
        Value::Blob(vec![0xab, 0xcd]),
        Value::Blob(vec![3]),
    ]);
    let bytes = 3 * size_of::<Value>() as u64 + 2 + 2 + 1;
    for limit in [bytes, bytes - 1] {
        let root = root(Resource::TemporaryBytes, limit);
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)
            });
        if limit == bytes {
            assert_eq!(
                result.expect("exact allocation allowance").as_deref(),
                Some(&expected)
            );
        } else {
            let error = result.expect_err("one byte short rejects");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
        }
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(
            input,
            Value::List(vec![
                Value::Blob(vec![1, 2]),
                Value::Text("abcd".into()),
                Value::Blob(vec![3])
            ])
        );
    }
    let invalid = Value::List(vec![Value::Text("abcd".into()), Value::Bool(true)]);
    let root = root(Resource::TemporaryBytes, 1_000);
    let result = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        canonicalize_filter_literal_for_persisted_kind(&kind, &invalid, work)
    })
    .expect("invalid value is not resource exhaustion");
    assert!(result.is_none());
    assert_eq!(
        invalid,
        Value::List(vec![Value::Text("abcd".into()), Value::Bool(true)])
    );
    assert_eq!(
        root.observed(Resource::TemporaryBytes),
        2 + 2 * size_of::<Value>() as u64
    );
}

#[test]
fn nested_borrowed_prefixes_are_copied_only_after_a_conversion() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::List(Box::new(
        AcceptedFieldKind::Blob { max_len: None },
    ))));
    let input = Value::List(vec![
        Value::List(vec![Value::Blob(vec![1])]),
        Value::List(vec![Value::Text("02".into())]),
    ]);
    let root = root(Resource::TemporaryBytes, 4 * size_of::<Value>() as u64 + 2);
    let result = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)
    })
    .expect("exact nested output allowance");
    assert_eq!(
        result.as_deref(),
        Some(&Value::List(vec![
            Value::List(vec![Value::Blob(vec![1])]),
            Value::List(vec![Value::Blob(vec![2])]),
        ]))
    );
    assert_eq!(
        root.observed(Resource::TemporaryBytes),
        4 * size_of::<Value>() as u64 + 2
    );
}

#[test]
fn hex_conversion_charges_even_when_digits_are_rejected() {
    for (text, bytes, accepted) in [
        ("abcd", 2, true),
        ("abxz", 2, false),
        ("abc", 0, false),
        ("abcdef", 0, false),
    ] {
        let kind = AcceptedFieldKind::Blob { max_len: Some(2) };
        let input = Value::Text(text.into());
        let root = root(Resource::TemporaryBytes, bytes);
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                canonicalize_filter_literal_for_persisted_kind(&kind, &input, work)
            })
            .expect("decode fits its exact output buffer");
        assert_eq!(result.is_some(), accepted);
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            text.len() as u64
        );
    }
}

#[test]
fn scalar_parse_work_rejects_before_conversion_and_accumulates_retries() {
    let input = Value::Text("18446744073709551615".into());
    let root = root(Resource::PredicateExpressionSteps, 19);
    for attempt in 1..=2 {
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                canonicalize_filter_literal_for_persisted_kind(
                    &AcceptedFieldKind::Nat64,
                    &input,
                    work,
                )
            })
            .expect_err("parse allowance is charged before parsing");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw()
        )));
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            attempt * 20
        );
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}
