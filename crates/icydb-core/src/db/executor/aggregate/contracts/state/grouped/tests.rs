use super::*;
use crate::{
    db::{
        executor::aggregate::{
            ExecutionConfig, field::resolve_aggregate_target_slot_from_planner_slot,
        },
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        query::plan::FieldSlot,
        schema::AcceptedFieldKind,
    },
    types::{Account, Decimal, EntityTag, Principal, Subaccount, Timestamp, U256, Ulid},
};

#[test]
fn grouped_identity_extrema_preserve_all_key_kinds_duplicates_and_empty_input() {
    use PrimaryKeyComponent as Component;

    let owner = Principal::from_slice(&[1]);
    let pairs = [
        (Component::Unit, Component::Unit),
        (Component::Nat64(0), Component::Nat64(u64::MAX)),
        (Component::Int64(i64::MIN), Component::Int64(i64::MAX)),
        (Component::Nat128(1 << 127), Component::Nat128(u128::MAX)),
        (Component::Int128(i128::MIN), Component::Int128(i128::MAX)),
        (
            Component::Timestamp(Timestamp::from_millis(-1)),
            Component::Timestamp(Timestamp::from_millis(1)),
        ),
        (
            Component::Ulid(Ulid::from_u128(1)),
            Component::Ulid(Ulid::from_u128(2)),
        ),
        (
            Component::Principal(owner),
            Component::Principal(Principal::from_slice(&[0, 0])),
        ),
        (
            Component::Subaccount(Subaccount::MIN),
            Component::Subaccount(Subaccount::MAX),
        ),
        (
            Component::Account(Account::from_owner_and_subaccount(owner, None)),
            Component::Account(Account::from_owner_and_subaccount(
                owner,
                Some(Subaccount::MIN),
            )),
        ),
        (
            Component::U256(U256::from(1_u64)),
            Component::U256(U256::MAX),
        ),
    ];
    let scalar_pairs = pairs.map(|(left, right)| (left.into(), right.into()));
    let composite = |first, second| {
        PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                Component::Nat64(first),
                Component::Int64(second),
            ])
            .unwrap(),
        )
    };
    for (left, right) in scalar_pairs.into_iter().chain([
        (composite(1, 9), composite(2, -9)),
        (composite(1, -9), composite(1, 9)),
    ]) {
        for kind in [AggregateKind::First, AggregateKind::Last] {
            for keys in [[left, right, left], [right, left, right]] {
                let mut state = GroupedTerminalAggregateState::new(
                    kind,
                    GroupedDistinctExecutionMode::new(false, false),
                    None,
                    None,
                    None,
                    u64::MAX,
                );
                let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
                for key in keys {
                    let key = DecodedDataStoreKey::new(EntityTag::new(1), &key);
                    assert!(matches!(
                        state.apply_with_row_view(&key, None, &mut context).unwrap(),
                        FoldControl::Continue
                    ));
                }
                let expected = if matches!(kind, AggregateKind::First) {
                    left.min(right)
                } else {
                    left.max(right)
                };
                assert_eq!(state.finalize().unwrap(), expected.as_runtime_value());
            }
            for filtered in [false, true] {
                let mut state = GroupedTerminalAggregateState::new(
                    kind,
                    GroupedDistinctExecutionMode::new(false, false),
                    None,
                    None,
                    filtered.then(|| Rc::new(CompiledExpr::Literal(Value::Bool(false)))),
                    u64::MAX,
                );
                if filtered {
                    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
                    let key = DecodedDataStoreKey::new(EntityTag::new(1), &left);
                    state
                        .apply_with_row_view(&key, Some(&RowView::new(vec![])), &mut context)
                        .unwrap();
                }
                assert_eq!(state.finalize().unwrap(), Value::Null);
            }
        }
    }
}

#[test]
fn grouped_expression_extrema_preserve_borrowed_and_owned_selection() {
    let key = DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(1).into());
    for (kind, extremum, winner, loser) in [
        (AggregateKind::Min, ExtremumKind::Min, "a", "z"),
        (AggregateKind::Max, ExtremumKind::Max, "z", "a"),
    ] {
        let mut reducer = GroupedAggregateReducerState::for_kind(kind);
        let selected = |reducer: &GroupedAggregateReducerState| {
            let value = match kind {
                AggregateKind::Min => reducer.min_value().unwrap(),
                AggregateKind::Max => reducer.max_value().unwrap(),
                _ => unreachable!(),
            };
            let Some(Value::Text(value)) = value else {
                panic!("text extremum expected")
            };
            (value.as_ptr(), value.clone())
        };
        let initial = Value::Text("m".repeat(64));
        GroupedTerminalAggregateState::apply_extremum(
            &mut reducer,
            extremum,
            &key,
            None,
            true,
            Some(Cow::Borrowed(&initial)),
        )
        .unwrap();
        let (initial_ptr, initial_text) = selected(&reducer);
        if let Value::Text(value) = &initial {
            assert_ne!(initial_ptr, value.as_ptr());
        }
        for value in [Value::Null, Value::Text(loser.repeat(64)), initial.clone()] {
            GroupedTerminalAggregateState::apply_extremum(
                &mut reducer,
                extremum,
                &key,
                None,
                true,
                Some(Cow::Borrowed(&value)),
            )
            .unwrap();
            assert_eq!(selected(&reducer), (initial_ptr, initial_text.clone()));
        }
        let owned = winner.repeat(64);
        let owned_ptr = owned.as_ptr();
        GroupedTerminalAggregateState::apply_extremum(
            &mut reducer,
            extremum,
            &key,
            None,
            true,
            Some(Cow::Owned(Value::Text(owned))),
        )
        .unwrap();
        assert_eq!(selected(&reducer), (owned_ptr, winner.repeat(64)));
        let error = GroupedTerminalAggregateState::apply_extremum(
            &mut reducer,
            extremum,
            &key,
            None,
            true,
            Some(Cow::Borrowed(&Value::Bool(true))),
        )
        .unwrap_err();
        assert_eq!(
            error.diagnostic().code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
        );
        assert_eq!(selected(&reducer), (owned_ptr, winner.repeat(64)));
    }
}

#[test]
fn grouped_distinct_reducers_share_borrowed_field_and_expression_inputs() {
    for kind in [AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg] {
        for expression in [false, true] {
            let field = FieldSlot::from_test_accepted_kind(
                0,
                "amount",
                AcceptedFieldKind::Decimal { scale: 2 },
            );
            let target = resolve_aggregate_target_slot_from_planner_slot(kind, &field).unwrap();
            let input_expr = expression.then(|| Rc::new(CompiledExpr::Slot { slot: 0 }));
            let mut state = GroupedTerminalAggregateState::new(
                kind,
                GroupedDistinctExecutionMode::new(true, true),
                (!expression).then_some(target),
                input_expr,
                None,
                2,
            );
            let key =
                DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(1).into());
            let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
            for value in [
                Value::Decimal(Decimal::new(100, 2)),
                Value::Decimal(Decimal::new(1, 0)),
                Value::Null,
                Value::Decimal(Decimal::new(3, 0)),
            ] {
                let row = RowView::new(vec![Some(value)]);
                let input = GroupedTerminalAggregateState::resolve_input_value(
                    state.grouped_input_expr.as_deref(),
                    state.target_field,
                    Some(&row),
                )
                .unwrap();
                if let Some(value) = input {
                    assert!(matches!(value, Cow::Borrowed(_)));
                    assert!(std::ptr::eq(
                        value.as_ref(),
                        row.require_slot_value(0).unwrap()
                    ));
                }
                state
                    .apply_with_row_view(&key, Some(&row), &mut context)
                    .unwrap();
            }
            assert_eq!(context.budget().distinct_values(), 2);
            assert_eq!(
                state.finalize().unwrap(),
                match kind {
                    AggregateKind::Count => Value::Nat64(2),
                    AggregateKind::Sum => Value::Decimal(Decimal::new(4, 0)),
                    AggregateKind::Avg => Value::Decimal(Decimal::new(2, 0)),
                    _ => unreachable!(),
                }
            );
        }
    }
}
