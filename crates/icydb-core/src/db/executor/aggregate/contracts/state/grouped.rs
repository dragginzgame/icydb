//! Module: executor::aggregate::contracts::state::grouped
//! Responsibility: grouped aggregate terminal state transitions.
//! Does not own: aggregate route planning or grouped row-shaping output.
//! Boundary: applies prepared grouped aggregate contracts to row/key inputs.

use crate::{
    db::{
        data::DecodedDataStoreKey,
        direction::Direction,
        executor::{
            aggregate::{
                contracts::{
                    AggregateKind,
                    error::GroupError,
                    grouped::ExecutionContext,
                    plan::{CompiledExpr, collapse_true_only_boolean_admission},
                    state::{
                        ExtremumKind, FoldControl, GroupedAggregateReducerState,
                        GroupedDistinctExecutionMode, canonical_key_from_data_key,
                    },
                },
                field::{
                    AggregateFieldValueError, FieldSlot as AggregateFieldSlot,
                    compare_orderable_field_values_with_slot,
                },
            },
            group::{CanonicalKey, GroupKeySet},
            pipeline::runtime::RowView,
            projection::ProjectionEvalError,
        },
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    value::Value,
};
use std::rc::Rc;

///
/// AggregateInputValue
///
/// AggregateInputValue normalizes grouped aggregate input reads before reducer
/// admission.
/// It keeps SQL NULL filtering explicit without repeating expression-vs-field
/// resolution at every grouped terminal update site.
///

enum AggregateInputValue {
    Null,
    Value(Value),
}

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
/// GroupedTerminalAggregateState binds one grouped aggregate kind + direction
/// to one structural reducer state machine so grouped execution no longer
/// depends on entity-typed terminal identity state.
///

pub(in crate::db::executor) struct GroupedTerminalAggregateState {
    pub(in crate::db::executor::aggregate::contracts::state) kind: AggregateKind,
    pub(in crate::db::executor::aggregate::contracts::state) direction: Direction,
    pub(in crate::db::executor::aggregate::contracts::state) distinct_mode:
        GroupedDistinctExecutionMode,
    pub(in crate::db::executor::aggregate::contracts::state) max_distinct_values_per_group: u64,
    pub(in crate::db::executor::aggregate::contracts::state) distinct_keys: Option<GroupKeySet>,
    pub(in crate::db::executor::aggregate::contracts::state) target_field:
        Option<AggregateFieldSlot>,
    pub(in crate::db::executor::aggregate::contracts::state) grouped_input_expr:
        Option<Rc<CompiledExpr>>,
    pub(in crate::db::executor::aggregate::contracts::state) grouped_filter_expr:
        Option<Rc<CompiledExpr>>,
    pub(in crate::db::executor::aggregate::contracts::state) requires_primary_key_value: bool,
    pub(in crate::db::executor::aggregate::contracts::state) reducer: GroupedAggregateReducerState,
}

impl GroupedTerminalAggregateState {
    // Build the canonical grouped terminal invariant for aggregate-input
    // expressions that drift outside the grouped compiled evaluator.
    fn input_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        if let ProjectionEvalError::Numeric(err) = err {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan()
    }

    // Build the canonical grouped terminal invariant for aggregate filters
    // that drift outside the grouped compiled evaluator.
    fn filter_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        if let ProjectionEvalError::Numeric(err) = err {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan()
    }

    // Evaluate one row-backed grouped expression through the shared
    // slot-indexed evaluator while preserving its error mapping.
    fn evaluate_row_expression_value(
        row_view: Option<&RowView>,
        expression: &CompiledExpr,
        map_eval_error: fn(ProjectionEvalError) -> InternalError,
    ) -> Result<Value, InternalError> {
        let Some(row_view) = row_view else {
            return Err(InternalError::query_executor_invariant());
        };

        let value = expression
            .evaluate(row_view)
            .map(std::borrow::Cow::into_owned);

        value.map_err(map_eval_error)
    }

    // Evaluate the compiled grouped aggregate input expression against one row
    // view. Direct field-target reads stay in `target_field_value` so this
    // helper has one responsibility.
    fn evaluate_compiled_input_value(
        &self,
        row_view: Option<&RowView>,
    ) -> Result<Value, InternalError> {
        let Some(grouped_input_expr) = self.grouped_input_expr.as_deref() else {
            return Err(InternalError::query_executor_invariant());
        };

        Self::evaluate_row_expression_value(
            row_view,
            grouped_input_expr,
            Self::input_expression_evaluation_failed,
        )
    }

    // Read one direct field-target input when the aggregate only needs to
    // inspect the row value.
    fn target_field_value<'a>(
        &self,
        row_view: Option<&'a RowView>,
    ) -> Result<&'a Value, InternalError> {
        let Some(target_field) = self.target_field.as_ref() else {
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
    fn resolve_input_value(
        &self,
        row_view: Option<&RowView>,
    ) -> Result<AggregateInputValue, InternalError> {
        let value = if self.grouped_input_expr.is_some() {
            self.evaluate_compiled_input_value(row_view)?
        } else if self.target_field.is_some() {
            self.target_field_value(row_view)?.clone()
        } else {
            return Err(InternalError::query_executor_invariant());
        };

        Ok(if matches!(value, Value::Null) {
            AggregateInputValue::Null
        } else {
            AggregateInputValue::Value(value)
        })
    }

    // Evaluate one grouped aggregate filter expression through the same compiled
    // grouped expression boundary used by aggregate inputs.
    fn admits_filter_row(&self, row_view: Option<&RowView>) -> Result<bool, InternalError> {
        let Some(grouped_filter_expr) = self.grouped_filter_expr.as_deref() else {
            return Ok(true);
        };

        let value = Self::evaluate_row_expression_value(
            row_view,
            grouped_filter_expr,
            Self::filter_expression_evaluation_failed,
        )?;

        collapse_true_only_boolean_admission(value, |_found| {
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

        if !self.admit_distinct(key, row_view, execution_context)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update(key, row_view)
            .map_err(GroupError::from)
    }

    /// Finalize this grouped aggregate state into one structural output value.
    pub(in crate::db::executor) fn finalize(self) -> Result<Value, InternalError> {
        self.reducer.into_value()
    }

    // Dispatch one grouped terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(
        &mut self,
        key: &DecodedDataStoreKey,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let primary_key_value = self
            .requires_primary_key_value
            .then(|| key.primary_key_value());
        match self.kind {
            AggregateKind::Count => self.apply_count(primary_key_value.as_ref(), row_view),
            AggregateKind::Sum | AggregateKind::Avg => {
                self.apply_sum_like(primary_key_value.as_ref(), row_view)
            }
            AggregateKind::Exists => self.apply_exists(primary_key_value.as_ref(), row_view),
            AggregateKind::Min => {
                self.apply_extremum(ExtremumKind::Min, primary_key_value.as_ref(), row_view)
            }
            AggregateKind::Max => {
                self.apply_extremum(ExtremumKind::Max, primary_key_value.as_ref(), row_view)
            }
            AggregateKind::First => self.apply_first(primary_key_value.as_ref(), row_view),
            AggregateKind::Last => self.apply_last(primary_key_value.as_ref(), row_view),
        }
    }

    // Admit one grouped DISTINCT candidate at the reducer boundary. Value-based
    // DISTINCT uses the same canonical input resolver as the aggregate update,
    // while key-based DISTINCT keeps the existing primary-key identity surface.
    fn admit_distinct(
        &mut self,
        key: &DecodedDataStoreKey,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<bool, GroupError> {
        if !self.distinct_mode.enabled() {
            return Ok(true);
        }

        let uses_value_dedup = self.distinct_mode.uses_value_dedup()
            && (self.grouped_input_expr.is_some() || self.target_field.is_some());
        let canonical_key = if uses_value_dedup {
            let input_value = self
                .resolve_input_value(row_view)
                .map_err(GroupError::from)?;
            let AggregateInputValue::Value(value) = input_value else {
                return Ok(false);
            };

            value.canonical_key().map_err(GroupError::from)?
        } else {
            canonical_key_from_data_key(key).map_err(GroupError::from)?
        };

        let Some(distinct_keys) = self.distinct_keys.as_mut() else {
            return Ok(true);
        };

        execution_context.admit_distinct_key(
            distinct_keys,
            self.max_distinct_values_per_group,
            canonical_key,
        )
    }

    // Apply one COUNT grouped terminal update.
    fn apply_count(
        &mut self,
        _key: Option<&PrimaryKeyValue>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if (self.grouped_input_expr.is_some() || self.target_field.is_some())
            && matches!(
                self.resolve_input_value(row_view)?,
                AggregateInputValue::Null
            )
        {
            return Ok(FoldControl::Continue);
        }
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS grouped terminal update.
    fn apply_exists(
        &mut self,
        _key: Option<&PrimaryKeyValue>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Apply grouped SUM/AVG field-target reducers through one shared numeric
    // row-view boundary.
    fn apply_sum_like(
        &mut self,
        _key: Option<&PrimaryKeyValue>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(sum_like_kind) = SumLikeKind::from_aggregate_kind(self.kind) else {
            return Err(InternalError::query_executor_invariant());
        };

        let AggregateInputValue::Value(value) = self.resolve_input_value(row_view)? else {
            return Ok(FoldControl::Continue);
        };
        sum_like_kind.apply_value(&mut self.reducer, &value)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN/MAX grouped terminal update. Field-target extrema keep the
    // slot-aware comparison path, expression extrema use value reducers, and
    // key-only extrema preserve primary-key ordering.
    fn apply_extremum(
        &mut self,
        kind: ExtremumKind,
        key: Option<&PrimaryKeyValue>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if self.grouped_input_expr.is_some() {
            let AggregateInputValue::Value(value) = self.resolve_input_value(row_view)? else {
                return Ok(FoldControl::Continue);
            };
            match kind {
                ExtremumKind::Min => self.reducer.ingest_min_value(value)?,
                ExtremumKind::Max => self.reducer.ingest_max_value(value)?,
            }
        } else if let Some(target_field) = self.target_field.as_ref() {
            let AggregateInputValue::Value(value) = self.resolve_input_value(row_view)? else {
                return Ok(FoldControl::Continue);
            };
            let current = match kind {
                ExtremumKind::Min => self.reducer.min_value()?,
                ExtremumKind::Max => self.reducer.max_value()?,
            };
            let replace = match current {
                Some(current) => {
                    let ordering =
                        compare_orderable_field_values_with_slot(*target_field, &value, current)
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
                    ExtremumKind::Min => self.reducer.replace_min_value(value)?,
                    ExtremumKind::Max => self.reducer.replace_max_value(value)?,
                }
            }
        } else {
            let Some(key) = key else {
                return Err(InternalError::query_executor_invariant());
            };
            let value = key.as_runtime_value();
            match kind {
                ExtremumKind::Min => self.reducer.update_min_value(value)?,
                ExtremumKind::Max => self.reducer.update_max_value(value)?,
            }
        }

        Ok(kind.fold_control_for_direction(self.direction))
    }

    // Apply one FIRST grouped terminal update.
    fn apply_first(
        &mut self,
        key: Option<&PrimaryKeyValue>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(InternalError::query_executor_invariant());
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(
        &mut self,
        key: Option<&PrimaryKeyValue>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(InternalError::query_executor_invariant());
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }
}
