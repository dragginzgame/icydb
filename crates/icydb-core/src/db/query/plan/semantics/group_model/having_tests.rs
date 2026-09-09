//! Owned grouped-literal conversion preserves storage and partial conversions.

use super::canonicalize_grouped_having_numeric_literal_for_accepted_kind as normalize;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
        schema::AcceptedFieldKind,
    },
    types::{IntBig, NatBig},
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

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
fn unchanged_nested_lists_keep_every_backing_allocation() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Set(Box::new(
        AcceptedFieldKind::Text { max_len: None },
    ))));
    let mut value = Value::List(vec![Value::List(vec![Value::Text("kept".into())])]);
    let backing = |value: &Value| {
        let Value::List(outer) = value else {
            panic!("outer list")
        };
        let Value::List(inner) = &outer[0] else {
            panic!("inner list")
        };
        let Value::Text(text) = &inner[0] else {
            panic!("text")
        };
        (outer.as_ptr(), inner.as_ptr(), text.as_ptr())
    };
    let before = backing(&value);
    let root = root(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        normalize(&kind, &mut value, work)
    })
    .expect("no replacement containers or text copies");
    assert_eq!(backing(&value), before);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(root.observed(Resource::NestedValueSteps), 4);
}

#[test]
fn partial_numeric_conversion_keeps_unconverted_recursive_payloads() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64));
    let mut value = Value::List(vec![
        Value::Nat64(3),
        Value::Map(vec![(Value::Text("key".into()), Value::Blob(vec![7; 128]))]),
        Value::Text("not-an-integer".into()),
        Value::Null,
    ]);
    let backing = |value: &Value| {
        let Value::List(items) = value else {
            panic!("list")
        };
        let Value::Map(entries) = &items[1] else {
            panic!("map")
        };
        let (Value::Text(key), Value::Blob(bytes)) = &entries[0] else {
            panic!("entry")
        };
        let Value::Text(text) = &items[2] else {
            panic!("text")
        };
        (
            items.as_ptr(),
            entries.as_ptr(),
            key.as_ptr(),
            bytes.as_ptr(),
            text.as_ptr(),
        )
    };
    let before = backing(&value);
    let root = root(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        normalize(&kind, &mut value, work)
    })
    .expect("fallback values remain in their existing slots");
    assert_eq!(backing(&value), before);
    let Value::List(items) = value else {
        panic!("list")
    };
    assert_eq!(items[0], Value::Int64(3));
    assert_eq!(items[2], Value::Text("not-an-integer".into()));
    assert_eq!(items[3], Value::Null);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn exact_big_atoms_and_nonconverting_kinds_need_no_copy_budget() {
    for (kind, mut value) in [
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            Value::IntBig(IntBig::from(-64)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 1 },
            Value::NatBig(NatBig::from(127_u64)),
        ),
        (
            AcceptedFieldKind::IntBig { max_bytes: 1 },
            Value::IntBig(IntBig::from(64)),
        ),
        (
            AcceptedFieldKind::Ulid,
            Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
        ),
    ] {
        let before = value.clone();
        let root = root(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            normalize(&kind, &mut value, work)
        })
        .expect("unchanged atoms need no allocation");
        assert_eq!(value, before);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn introduced_decode_storage_still_charges_before_conversion() {
    for limit in [1, 2] {
        let mut value = Value::Text("abcd".into());
        let root = root(Resource::TemporaryBytes, limit);
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                normalize(
                    &AcceptedFieldKind::Blob { max_len: Some(2) },
                    &mut value,
                    work,
                )
            });
        if limit == 2 {
            result.expect("exact decode allocation fits");
            assert_eq!(value, Value::Blob(vec![0xab, 0xcd]));
        } else {
            let error = result.expect_err("reject before decoder allocation");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
            assert_eq!(value, Value::Text("abcd".into()));
        }
        assert_eq!(root.observed(Resource::TemporaryBytes), 2);
    }
}

#[test]
fn failed_traversal_charges_accumulate_on_the_current_request() {
    let mut value = Value::List(vec![Value::Null]);
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Ulid));
    let root = root(Resource::NestedValueSteps, 1);
    for attempt in 0..2 {
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                normalize(&kind, &mut value, work)
            })
            .expect_err("traversal must not reset the budget or become a conversion miss");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw()
        )));
        assert_eq!(root.observed(Resource::NestedValueSteps), attempt + 2);
    }
    assert_eq!(value, Value::List(vec![Value::Null]));
}
