//! Module: executor::aggregate::contracts::state::grouped
//! Responsibility: grouped aggregate terminal state transitions.
//! Does not own: aggregate route planning or grouped row-shaping output.
//! Boundary: applies prepared grouped aggregate contracts to row/key inputs.

use crate::{
    db::{
        data::DecodedDataStoreKey,
        executor::{
            aggregate::{
                contracts::{
                    AggregateKind,
                    error::GroupError,
                    grouped::ExecutionContext,
                    plan::{CompiledExpr, collapse_true_only_boolean_admission},
                    state::{
                        ExtremumKind, FoldControl, GroupedAggregateReducerState,
                        GroupedDistinctExecutionMode,
                    },
                },
                field::{
                    AggregateFieldValueError, FieldSlot as AggregateFieldSlot,
                    compare_orderable_field_values_with_slot,
                },
            },
            group::GroupKeySet,
            pipeline::runtime::RowView,
            projection::ProjectionEvalError,
        },
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    value::Value,
};
use std::{borrow::Cow, rc::Rc};

///
/// SumLikeKind
///
/// SumLikeKind identifies the grouped SUM/AVG reducer family inside the state
/// module.
/// Keeping this local avoids attaching grouped executor behavior to
/// `AggregateKind` while preserving the shared SUM/AVG numeric path.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SumLikeKind {
    Sum,
    Avg,
}

impl SumLikeKind {
    // Convert the planner aggregate kind into the local SUM/AVG grouped reducer
    // family, or return `None` when the aggregate does not use this path.
    const fn from_aggregate_kind(kind: AggregateKind) -> Option<Self> {
        match kind {
            AggregateKind::Sum => Some(Self::Sum),
            AggregateKind::Avg => Some(Self::Avg),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => None,
        }
    }

    // Apply one grouped value through the SUM/AVG reducer family while keeping
    // the fixed-width U256 SUM lane distinct from decimal AVG.
    fn apply_value(
        self,
        reducer: &mut GroupedAggregateReducerState,
        value: &Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum => reducer.add_sum_value(value),
            Self::Avg => reducer.add_average_value(value),
        }
    }
}

///
/// GroupedTerminalAggregateState
///
/// GroupedTerminalAggregateState binds one grouped aggregate kind
/// to one structural reducer state machine so grouped execution no longer
/// depends on entity-typed terminal identity state.
///

pub(in crate::db::executor) struct GroupedTerminalAggregateState {
    kind: AggregateKind,
    distinct_mode: GroupedDistinctExecutionMode,
    max_distinct_values_per_group: u64,
    distinct_keys: Option<GroupKeySet>,
    target_field: Option<AggregateFieldSlot>,
    grouped_input_expr: Option<Rc<CompiledExpr>>,
    grouped_filter_expr: Option<Rc<CompiledExpr>>,
    reducer: GroupedAggregateReducerState,
}

impl GroupedTerminalAggregateState {
    /// Build one grouped terminal state from prepared aggregate contracts.
    #[must_use]
    pub(in crate::db::executor) fn new(
        kind: AggregateKind,
        distinct_mode: GroupedDistinctExecutionMode,
        target_field: Option<AggregateFieldSlot>,
        grouped_input_expr: Option<Rc<CompiledExpr>>,
        grouped_filter_expr: Option<Rc<CompiledExpr>>,
        max_distinct_values_per_group: u64,
    ) -> Self {
        Self {
            kind,
            distinct_mode,
            max_distinct_values_per_group,
            distinct_keys: if distinct_mode.enabled() {
                Some(GroupKeySet::new())
            } else {
                None
            },
            target_field,
            grouped_input_expr,
            grouped_filter_expr,
            reducer: GroupedAggregateReducerState::for_kind(kind),
        }
    }

    // Preserve numeric failures and classify invalid grouped input or filter
    // expressions through one shared evaluator boundary.
    fn expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        if let ProjectionEvalError::Numeric(err) = err {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan()
    }

    // Evaluate one row-backed grouped expression through the shared
    // slot-indexed evaluator while preserving its error mapping.
    fn evaluate_row_expression_value<'a>(
        row_view: Option<&'a RowView>,
        expression: &'a CompiledExpr,
    ) -> Result<Cow<'a, Value>, InternalError> {
        let Some(row_view) = row_view else {
            return Err(InternalError::query_executor_invariant());
        };

        expression
            .evaluate(row_view)
            .map_err(Self::expression_evaluation_failed)
    }

    // Read one direct field-target input when the aggregate only needs to
    // inspect the row value.
    fn target_field_value(
        target_field: Option<AggregateFieldSlot>,
        row_view: Option<&RowView>,
    ) -> Result<&Value, InternalError> {
        let Some(target_field) = target_field else {
            return Err(InternalError::query_executor_invariant());
        };
        let Some(row_view) = row_view else {
            return Err(InternalError::query_executor_invariant());
        };

        row_view.require_slot_value(target_field.index)
    }

    // Resolve the one canonical grouped aggregate input value for COUNT/SUM/AVG
    // and field/expression MIN/MAX reducers. Key-only reducers deliberately stay
    // outside this helper because their input is a primary-key value, not a row slot.
    fn resolve_input_value<'a>(
        grouped_input_expr: Option<&'a CompiledExpr>,
        target_field: Option<AggregateFieldSlot>,
        row_view: Option<&'a RowView>,
    ) -> Result<Option<Cow<'a, Value>>, InternalError> {
        let value = if let Some(expression) = grouped_input_expr {
            Self::evaluate_row_expression_value(row_view, expression)?
        } else {
            Cow::Borrowed(Self::target_field_value(target_field, row_view)?)
        };

        Ok((!matches!(value.as_ref(), Value::Null)).then_some(value))
    }

    // Evaluate one grouped aggregate filter expression through the same compiled
    // grouped expression boundary used by aggregate inputs.
    fn admits_filter_row(&self, row_view: Option<&RowView>) -> Result<bool, InternalError> {
        let Some(grouped_filter_expr) = self.grouped_filter_expr.as_deref() else {
            return Ok(true);
        };

        let value = Self::evaluate_row_expression_value(row_view, grouped_filter_expr)?;

        collapse_true_only_boolean_admission(value.into_owned(), |_found| {
            InternalError::query_invalid_logical_plan()
        })
    }

    /// Apply one grouped candidate data key plus one structural row view when
    /// grouped field-target semantics need slot access.
    pub(in crate::db::executor) fn apply_with_row_view(
        &mut self,
        key: &DecodedDataStoreKey,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        if !self.admits_filter_row(row_view).map_err(GroupError::from)? {
            return Ok(FoldControl::Continue);
        }

        let has_input = self.grouped_input_expr.is_some() || self.target_field.is_some();
        let value_distinct =
            self.distinct_mode.enabled() && self.distinct_mode.uses_value_dedup() && has_input;
        let input = if value_distinct {
            Self::resolve_input_value(
                self.grouped_input_expr.as_deref(),
                self.target_field,
                row_view,
            )
            .map_err(GroupError::from)?
        } else {
            None
        };

        if self.distinct_mode.enabled() {
            let primary_value;
            let value = if value_distinct {
                let Some(value) = input.as_deref() else {
                    return Ok(FoldControl::Continue);
                };
                value
            } else {
                primary_value = key.primary_key_runtime_value();
                &primary_value
            };
            if let Some(distinct_keys) = self.distinct_keys.as_mut()
                && !execution_context.admit_distinct_value(
                    distinct_keys,
                    self.max_distinct_values_per_group,
                    value,
                )?
            {
                return Ok(FoldControl::Continue);
            }
        }

        // Key-based duplicate rejection precedes input evaluation, as before.
        // Value-based admission and the reducer share exactly one input read.
        let needs_input = matches!(self.kind, AggregateKind::Sum | AggregateKind::Avg)
            || (has_input
                && matches!(
                    self.kind,
                    AggregateKind::Count | AggregateKind::Min | AggregateKind::Max
                ));
        let input = if value_distinct || !needs_input {
            input
        } else {
            Self::resolve_input_value(
                self.grouped_input_expr.as_deref(),
                self.target_field,
                row_view,
            )
            .map_err(GroupError::from)?
        };
        Self::apply_terminal_update(
            self.kind,
            self.target_field,
            self.grouped_input_expr.is_some(),
            &mut self.reducer,
            key,
            input,
        )
        .map_err(GroupError::from)
    }

    /// Finalize this grouped aggregate state into one structural output value.
    pub(in crate::db::executor) fn finalize(self) -> Result<Value, InternalError> {
        self.reducer.into_value()
    }

    // Dispatch one grouped terminal update using the input already checked by
    // DISTINCT admission. Only retained extrema require an owned input value.
    fn apply_terminal_update(
        kind: AggregateKind,
        target_field: Option<AggregateFieldSlot>,
        has_expression: bool,
        reducer: &mut GroupedAggregateReducerState,
        key: &DecodedDataStoreKey,
        input: Option<Cow<'_, Value>>,
    ) -> Result<FoldControl, InternalError> {
        match kind {
            AggregateKind::Count => Self::apply_count(
                reducer,
                has_expression || target_field.is_some(),
                input.as_deref(),
            ),
            AggregateKind::Sum | AggregateKind::Avg => {
                Self::apply_sum_like(reducer, kind, input.as_deref())
            }
            AggregateKind::Exists => Self::apply_exists(reducer),
            AggregateKind::Min => Self::apply_extremum(
                reducer,
                ExtremumKind::Min,
                key,
                target_field,
                has_expression,
                input,
            ),
            AggregateKind::Max => Self::apply_extremum(
                reducer,
                ExtremumKind::Max,
                key,
                target_field,
                has_expression,
                input,
            ),
            AggregateKind::First => Self::apply_first(reducer, &key.primary_key_value()),
            AggregateKind::Last => Self::apply_last(reducer, &key.primary_key_value()),
        }
    }

    // COUNT's admitted input has already passed NULL filtering; COUNT(*) has
    // no row input and always increments.
    fn apply_count(
        reducer: &mut GroupedAggregateReducerState,
        has_input: bool,
        input: Option<&Value>,
    ) -> Result<FoldControl, InternalError> {
        if !has_input || input.is_some() {
            reducer.increment_count()?;
        }
        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS grouped terminal update.
    fn apply_exists(
        reducer: &mut GroupedAggregateReducerState,
    ) -> Result<FoldControl, InternalError> {
        reducer.set_exists_true()?;
        Ok(FoldControl::Break)
    }

    // SUM/AVG borrow the same value used for DISTINCT admission.
    fn apply_sum_like(
        reducer: &mut GroupedAggregateReducerState,
        kind: AggregateKind,
        input: Option<&Value>,
    ) -> Result<FoldControl, InternalError> {
        let Some(sum_like_kind) = SumLikeKind::from_aggregate_kind(kind) else {
            return Err(InternalError::query_executor_invariant());
        };
        if let Some(value) = input {
            sum_like_kind.apply_value(reducer, value)?;
        }
        Ok(FoldControl::Continue)
    }

    // Keep field-aware extrema comparison and primary-key ordering unchanged.
    fn apply_extremum(
        reducer: &mut GroupedAggregateReducerState,
        kind: ExtremumKind,
        key: &DecodedDataStoreKey,
        target_field: Option<AggregateFieldSlot>,
        has_expression: bool,
        input: Option<Cow<'_, Value>>,
    ) -> Result<FoldControl, InternalError> {
        if has_expression {
            let Some(value) = input else {
                return Ok(FoldControl::Continue);
            };
            match kind {
                ExtremumKind::Min => reducer.ingest_min_value(value.into_owned())?,
                ExtremumKind::Max => reducer.ingest_max_value(value.into_owned())?,
            }
        } else if let Some(target_field) = target_field {
            let Some(value) = input else {
                return Ok(FoldControl::Continue);
            };
            let current = match kind {
                ExtremumKind::Min => reducer.min_value()?,
                ExtremumKind::Max => reducer.max_value()?,
            };
            let replace = match current {
                Some(current) => {
                    let ordering =
                        compare_orderable_field_values_with_slot(target_field, &value, current)
                            .map_err(AggregateFieldValueError::into_internal_error)?;
                    match kind {
                        ExtremumKind::Min => ordering.is_lt(),
                        ExtremumKind::Max => ordering.is_gt(),
                    }
                }
                None => true,
            };
            if replace {
                match kind {
                    ExtremumKind::Min => reducer.replace_min_value(value.into_owned())?,
                    ExtremumKind::Max => reducer.replace_max_value(value.into_owned())?,
                }
            }
        } else {
            let value = key.primary_key_runtime_value();
            match kind {
                ExtremumKind::Min => reducer.update_min_value(value)?,
                ExtremumKind::Max => reducer.update_max_value(value)?,
            }
        }
        Ok(FoldControl::Continue)
    }

    // Apply one FIRST grouped terminal update.
    fn apply_first(
        reducer: &mut GroupedAggregateReducerState,
        key: &PrimaryKeyValue,
    ) -> Result<FoldControl, InternalError> {
        reducer.set_first(key)?;
        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(
        reducer: &mut GroupedAggregateReducerState,
        key: &PrimaryKeyValue,
    ) -> Result<FoldControl, InternalError> {
        reducer.set_last(key)?;
        Ok(FoldControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            executor::aggregate::{
                ExecutionConfig, field::resolve_aggregate_target_slot_from_planner_slot,
            },
            key_taxonomy::PrimaryKeyComponent,
            query::plan::FieldSlot,
            schema::AcceptedFieldKind,
        },
        types::{Decimal, EntityTag},
    };

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
                let key = DecodedDataStoreKey::new(
                    EntityTag::new(1),
                    &PrimaryKeyComponent::Nat64(1).into(),
                );
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
}
