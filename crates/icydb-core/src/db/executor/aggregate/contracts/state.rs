//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and grouped structural terminal reducers.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::{
                contracts::{
                    error::GroupError,
                    grouped::ExecutionContext,
                    spec::{AggregateKind, ScalarAggregateOutput},
                },
                field::{
                    FieldSlot as AggregateFieldSlot, compare_orderable_field_values_with_slot,
                },
            },
            group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::RowView,
            projection::{
                ProjectionEvalError, ScalarProjectionExpr,
                eval_scalar_projection_expr_with_value_ref_reader,
            },
        },
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::plan::FieldSlot,
        query::plan::expr::collapse_true_only_boolean_admission,
    },
    error::InternalError,
    types::Decimal,
    value::{StorageKey, Value},
};

///
/// FoldControl
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// AggregateReducerClass
///
/// Owner-local grouped classification for aggregate reducer state and terminal
/// update dispatch. This keeps `AggregateKind` shock radius out of the reducer
/// implementations themselves.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateReducerClass {
    Count,
    SumLike,
    Exists,
    Min,
    Max,
    First,
    Last,
}

impl AggregateKind {
    // Classify one aggregate kind for reducer-state initialization and terminal dispatch.
    const fn reducer_class(self) -> AggregateReducerClass {
        match self {
            Self::Count => AggregateReducerClass::Count,
            Self::Sum | Self::Avg => AggregateReducerClass::SumLike,
            Self::Exists => AggregateReducerClass::Exists,
            Self::Min => AggregateReducerClass::Min,
            Self::Max => AggregateReducerClass::Max,
            Self::First => AggregateReducerClass::First,
            Self::Last => AggregateReducerClass::Last,
        }
    }

    // Return the executor-facing SUM/AVG input label used by grouped numeric
    // field-target reducers, or `None` when this kind is not in that family.
    const fn sum_like_input_label(self) -> Option<&'static str> {
        match self {
            Self::Sum => Some("SUM(input)"),
            Self::Avg => Some("AVG(input)"),
            Self::Count | Self::Exists | Self::Min | Self::Max | Self::First | Self::Last => None,
        }
    }

    // Apply one grouped numeric field-target decimal payload through the
    // SUM/AVG reducer family, or report that this kind does not admit the
    // shared numeric reducer path.
    fn apply_sum_like_decimal(
        self,
        reducer: &mut GroupedAggregateReducerState,
        decimal: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum => reducer.add_sum_value(decimal),
            Self::Avg => reducer.add_average_value(decimal),
            Self::Count | Self::Exists | Self::Min | Self::Max | Self::First | Self::Last => Err(
                GroupedTerminalAggregateState::field_target_execution_required("SUM/AVG(input)"),
            ),
        }
    }
}

///
/// ScalarAggregateReducerState
///
/// Shared scalar aggregate terminal reducer state used by streaming and
/// fast-path aggregate execution so scalar terminal update semantics stay
/// centralized.
///

pub(in crate::db::executor) enum ScalarAggregateReducerState {
    Count(u32),
    Sum(Option<Decimal>),
    Exists(bool),
    Min(Option<StorageKey>),
    Max(Option<StorageKey>),
    First(Option<StorageKey>),
    Last(Option<StorageKey>),
}

impl ScalarAggregateReducerState {
    // Build the canonical scalar reducer-state mismatch for one aggregate kind.
    fn state_mismatch(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!("aggregate reducer {kind} state mismatch"))
    }

    /// Build the initial scalar reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        match kind.reducer_class() {
            AggregateReducerClass::Count => Self::Count(0),
            AggregateReducerClass::SumLike => Self::Sum(None),
            AggregateReducerClass::Exists => Self::Exists(false),
            AggregateReducerClass::Min => Self::Min(None),
            AggregateReducerClass::Max => Self::Max(None),
            AggregateReducerClass::First => Self::First(None),
            AggregateReducerClass::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    fn increment_count(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    fn update_min_value(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Min(min_key) => {
                let replace = match min_key.as_ref() {
                    Some(current) => key < *current,
                    None => true,
                };
                if replace {
                    *min_key = Some(key);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one MAX reducer update.
    fn update_max_value(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Max(max_key) => {
                let replace = match max_key.as_ref() {
                    Some(current) => key > *current,
                    None => true,
                };
                if replace {
                    *max_key = Some(key);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one FIRST reducer update.
    fn set_first(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key);
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key);
                Ok(())
            }
            _ => Err(Self::state_mismatch("LAST")),
        }
    }

    /// Convert reducer state into the structural scalar aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> ScalarAggregateOutput {
        match self {
            Self::Count(value) => ScalarAggregateOutput::Count(value),
            Self::Sum(value) => ScalarAggregateOutput::Sum(value),
            Self::Exists(value) => ScalarAggregateOutput::Exists(value),
            Self::Min(value) => ScalarAggregateOutput::Min(value),
            Self::Max(value) => ScalarAggregateOutput::Max(value),
            Self::First(value) => ScalarAggregateOutput::First(value),
            Self::Last(value) => ScalarAggregateOutput::Last(value),
        }
    }
}

///
/// GroupedAggregateReducerState
///
/// GroupedAggregateReducerState stores grouped terminal reducer payloads as
/// structural values so grouped execution can return either row identities or
/// resolved field-target extrema without reopening typed decode.
///

enum GroupedAggregateReducerState {
    Count(u32),
    Sum(Option<Decimal>),
    Avg { sum: Decimal, row_count: u64 },
    Exists(bool),
    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
}

impl GroupedAggregateReducerState {
    // Build the canonical grouped reducer-state mismatch for one aggregate kind.
    fn state_mismatch(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} state mismatch"
        ))
    }

    /// Build the initial grouped reducer state for one aggregate terminal.
    #[must_use]
    const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum => Self::Sum(None),
            AggregateKind::Avg => Self::Avg {
                sum: Decimal::ZERO,
                row_count: 0,
            },
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    fn increment_count(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one SUM reducer update.
    fn add_sum_value(&mut self, value: Decimal) -> Result<(), InternalError> {
        match self {
            Self::Sum(sum) => {
                *sum = Some(sum.map_or(value, |current| add_decimal_terms(current, value)));
                Ok(())
            }
            _ => Err(Self::state_mismatch("SUM")),
        }
    }

    // Apply one AVG reducer update.
    fn add_average_value(&mut self, value: Decimal) -> Result<(), InternalError> {
        match self {
            Self::Avg { sum, row_count } => {
                *sum = add_decimal_terms(*sum, value);
                *row_count = row_count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("AVG")),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    fn update_min_value(&mut self, value: Value) -> Result<(), InternalError> {
        match self {
            Self::Min(min_value) => {
                let replace = match min_value.as_ref() {
                    Some(current) => canonical_value_compare(&value, current).is_lt(),
                    None => true,
                };
                if replace {
                    *min_value = Some(value);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one MAX reducer update.
    fn update_max_value(&mut self, value: Value) -> Result<(), InternalError> {
        match self {
            Self::Max(max_value) => {
                let replace = match max_value.as_ref() {
                    Some(current) => canonical_value_compare(&value, current).is_gt(),
                    None => true,
                };
                if replace {
                    *max_value = Some(value);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one FIRST reducer update.
    fn set_first(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key.as_value());
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key.as_value());
                Ok(())
            }
            _ => Err(Self::state_mismatch("LAST")),
        }
    }

    /// Convert reducer state into the grouped aggregate terminal output value.
    #[must_use]
    fn into_value(self) -> Value {
        match self {
            Self::Count(value) => Value::Uint(u64::from(value)),
            Self::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            Self::Avg { sum, row_count } => {
                average_decimal_terms(sum, row_count).map_or(Value::Null, Value::Decimal)
            }
            Self::Exists(value) => Value::Bool(value),
            Self::Min(value) | Self::Max(value) | Self::First(value) | Self::Last(value) => {
                value.unwrap_or(Value::Null)
            }
        }
    }
}

///
/// ScalarAggregateState
///
/// Canonical scalar aggregate state-machine contract consumed by kernel
/// reducer orchestration. Implementations must keep transitions deterministic
/// and emit scalar terminal outputs using the shared aggregate output taxonomy.
///

pub(in crate::db::executor) trait ScalarAggregateState {
    /// Apply one candidate data key to this aggregate state machine.
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError>;

    /// Finalize this aggregate state into one terminal output payload.
    fn finalize(self) -> ScalarAggregateOutput;
}

///
/// ScalarTerminalAggregateState
///
/// ScalarTerminalAggregateState binds one scalar aggregate kind + direction to one
/// reducer state machine so key-stream execution can use a single canonical
/// update pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct ScalarTerminalAggregateState {
    reducer_class: AggregateReducerClass,
    direction: Direction,
    distinct: bool,
    distinct_keys: Option<GroupKeySet>,
    requires_storage_key: bool,
    reducer: ScalarAggregateReducerState,
}

impl ScalarAggregateState for ScalarTerminalAggregateState {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        if self.distinct && !record_distinct_key(self.distinct_keys.as_mut(), key)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update(key)
    }

    fn finalize(self) -> ScalarAggregateOutput {
        self.reducer.into_output()
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
    kind: AggregateKind,
    reducer_class: AggregateReducerClass,
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    distinct_keys: Option<GroupKeySet>,
    target_field: Option<FieldSlot>,
    compiled_input_expr: Option<ScalarProjectionExpr>,
    compiled_filter_expr: Option<ScalarProjectionExpr>,
    requires_storage_key: bool,
    reducer: GroupedAggregateReducerState,
}

impl GroupedTerminalAggregateState {
    // Build the canonical grouped terminal invariant for field-target-only kinds.
    fn field_target_execution_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} requires field-target execution path"
        ))
    }

    // Build the canonical grouped terminal invariant for storage-key-required updates.
    fn storage_key_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} update requires storage key"
        ))
    }

    // Build the canonical grouped terminal invariant for one non-numeric
    // SUM(field) payload that planner semantics should already have rejected.
    fn sum_field_requires_numeric_value(field: &str, value: &Value) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer SUM(field) requires numeric field '{field}', found value {value:?}"
        ))
    }

    // Build the canonical grouped terminal invariant for aggregate-input
    // expressions that drift outside the shared scalar evaluation seam.
    fn input_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        InternalError::query_invalid_logical_plan(format!(
            "grouped aggregate input expression evaluation failed: {err}",
        ))
    }

    // Build the canonical grouped terminal invariant for aggregate filters
    // that drift outside the shared scalar evaluation seam.
    fn filter_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        InternalError::query_invalid_logical_plan(format!(
            "grouped aggregate filter expression evaluation failed: {err}",
        ))
    }

    // Evaluate the canonical grouped aggregate input against one row view.
    fn evaluate_input_value(
        &self,
        row_view: Option<&RowView>,
    ) -> Result<Option<Value>, InternalError> {
        let Some(row_view) = row_view else {
            return Err(Self::field_target_execution_required(
                "grouped aggregate input expression",
            ));
        };

        if let Some(compiled_input_expr) = self.compiled_input_expr.as_ref() {
            let value = eval_scalar_projection_expr_with_value_ref_reader(
                compiled_input_expr,
                &mut |slot| row_view.borrow_slot(slot),
            )
            .map_err(Self::input_expression_evaluation_failed)?;

            return Ok(Some(value));
        }

        let Some(target_field) = self.target_field.as_ref() else {
            return Ok(None);
        };

        Ok(Some(
            row_view.require_slot_ref(target_field.index())?.clone(),
        ))
    }

    // Evaluate one grouped aggregate filter expression through the same shared
    // scalar projection boundary used by aggregate inputs.
    fn admits_filter_row(&self, row_view: Option<&RowView>) -> Result<bool, InternalError> {
        let Some(compiled_filter_expr) = self.compiled_filter_expr.as_ref() else {
            return Ok(true);
        };
        let Some(row_view) = row_view else {
            return Err(Self::field_target_execution_required(
                "grouped aggregate filter expression",
            ));
        };

        let value =
            eval_scalar_projection_expr_with_value_ref_reader(compiled_filter_expr, &mut |slot| {
                row_view.borrow_slot(slot)
            })
            .map_err(Self::filter_expression_evaluation_failed)?;

        collapse_true_only_boolean_admission(value, |found| {
            InternalError::query_invalid_logical_plan(format!(
                "grouped aggregate filter expression produced non-boolean value: {:?}",
                found.as_ref(),
            ))
        })
    }

    /// Apply one grouped candidate data key with grouped DISTINCT budget enforcement.
    #[cfg(test)]
    #[expect(
        dead_code,
        reason = "grouped contract tests still exercise the compatibility apply boundary"
    )]
    pub(in crate::db::executor) fn apply(
        &mut self,
        key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        self.apply_with_row_view(key, None, execution_context)
    }

    /// Apply one grouped candidate data key plus one structural row view when
    /// grouped field-target semantics need slot access.
    pub(in crate::db::executor) fn apply_with_row_view(
        &mut self,
        key: &DataKey,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        if !self.admits_filter_row(row_view).map_err(GroupError::from)? {
            return Ok(FoldControl::Continue);
        }

        if self.distinct {
            let admitted = if (self.compiled_input_expr.is_some() || self.target_field.is_some())
                && self.kind.uses_grouped_distinct_value_dedup_v1()
            {
                self.record_grouped_distinct_input_value(row_view, execution_context)?
            } else {
                record_grouped_distinct_key(
                    self.distinct_keys.as_mut(),
                    key,
                    execution_context,
                    self.max_distinct_values_per_group,
                )?
            };
            if !admitted {
                return Ok(FoldControl::Continue);
            }
        }

        self.apply_terminal_update(key, row_view)
            .map_err(GroupError::from)
    }

    /// Finalize this grouped aggregate state into one structural output value.
    #[must_use]
    pub(in crate::db::executor) fn finalize(self) -> Value {
        self.reducer.into_value()
    }

    // Dispatch one grouped terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(
        &mut self,
        key: &DataKey,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.reducer_class {
            AggregateReducerClass::Count => self.apply_count(storage_key, row_view),
            AggregateReducerClass::SumLike => self.apply_sum_like(storage_key, row_view),
            AggregateReducerClass::Exists => self.apply_exists(storage_key, row_view),
            AggregateReducerClass::Min => self.apply_min(storage_key, row_view),
            AggregateReducerClass::Max => self.apply_max(storage_key, row_view),
            AggregateReducerClass::First => self.apply_first(storage_key, row_view),
            AggregateReducerClass::Last => self.apply_last(storage_key, row_view),
        }
    }

    // Admit one grouped DISTINCT field-target value before grouped COUNT/SUM/AVG
    // reducers consume it so grouped DISTINCT deduplicates on the projected
    // field value instead of row identity.
    fn record_grouped_distinct_input_value(
        &mut self,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<bool, GroupError> {
        let Some(value) = self
            .evaluate_input_value(row_view)
            .map_err(GroupError::from)?
        else {
            return Err(GroupError::from(Self::field_target_execution_required(
                "COUNT/SUM/AVG(DISTINCT input)",
            )));
        };
        if matches!(value, Value::Null) {
            return Ok(false);
        }
        let canonical_key = value
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)
            .map_err(GroupError::from)?;

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
        _key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if self.compiled_input_expr.is_some() || self.target_field.is_some() {
            let value = self
                .evaluate_input_value(row_view)?
                .ok_or_else(|| Self::field_target_execution_required("COUNT(input)"))?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
        }
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS grouped terminal update.
    fn apply_exists(
        &mut self,
        _key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Apply grouped SUM/AVG field-target reducers through one shared numeric
    // row-view boundary.
    fn apply_sum_like(
        &mut self,
        _key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(kind_label) = self.kind.sum_like_input_label() else {
            return Err(Self::field_target_execution_required("SUM/AVG(input)"));
        };

        let Some(value) = self.evaluate_input_value(row_view)? else {
            return Err(Self::field_target_execution_required(kind_label));
        };
        if matches!(value, Value::Null) {
            return Ok(FoldControl::Continue);
        }
        let Some(decimal) = coerce_numeric_decimal(&value) else {
            return Err(match self.target_field.as_ref() {
                Some(target_field) => {
                    Self::sum_field_requires_numeric_value(target_field.field(), &value)
                }
                None => InternalError::query_executor_invariant(format!(
                    "grouped aggregate reducer {kind_label} requires numeric expression input, found value {value:?}",
                )),
            });
        };
        self.kind
            .apply_sum_like_decimal(&mut self.reducer, decimal)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MAX grouped terminal update.
    fn apply_max(
        &mut self,
        key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if let Some(target_field) = self.target_field.as_ref() {
            let Some(target_kind) = target_field.kind() else {
                return Err(Self::field_target_execution_required("MAX(field)"));
            };
            let Some(row_view) = row_view else {
                return Err(Self::field_target_execution_required("MAX(field)"));
            };
            let value = row_view.require_slot_ref(target_field.index())?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let aggregate_field_slot = AggregateFieldSlot {
                index: target_field.index(),
                kind: target_kind,
            };
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Max(Some(current)) => {
                    compare_orderable_field_values_with_slot(
                        target_field.field(),
                        aggregate_field_slot,
                        value,
                        current,
                    )
                    .map_err(super::super::field::AggregateFieldValueError::into_internal_error)?
                    .is_gt()
                }
                GroupedAggregateReducerState::Max(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MAX")),
            };
            if replace {
                self.reducer.update_max_value(value.clone())?;
            }
        } else if self.compiled_input_expr.is_some() {
            let Some(value) = self.evaluate_input_value(row_view)? else {
                return Err(Self::field_target_execution_required("MAX(expr)"));
            };
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Max(Some(current)) => {
                    compare_numeric_or_strict_order(&value, current)
                        .ok_or_else(|| {
                            InternalError::query_executor_invariant(
                                "grouped MAX(expr) encountered incomparable expression values",
                            )
                        })?
                        .is_gt()
                }
                GroupedAggregateReducerState::Max(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MAX")),
            };
            if replace {
                self.reducer.update_max_value(value)?;
            }
        } else {
            let Some(key) = key else {
                return Err(Self::storage_key_required("MAX"));
            };
            self.reducer.update_max_value(key.as_value())?;
        }

        Ok(if self.direction == Direction::Desc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one FIRST grouped terminal update.
    fn apply_first(
        &mut self,
        key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("FIRST"));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(
        &mut self,
        key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("LAST"));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN grouped terminal update.
    fn apply_min(
        &mut self,
        key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if let Some(target_field) = self.target_field.as_ref() {
            let Some(target_kind) = target_field.kind() else {
                return Err(Self::field_target_execution_required("MIN(field)"));
            };
            let Some(row_view) = row_view else {
                return Err(Self::field_target_execution_required("MIN(field)"));
            };
            let value = row_view.require_slot_ref(target_field.index())?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let aggregate_field_slot = AggregateFieldSlot {
                index: target_field.index(),
                kind: target_kind,
            };
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Min(Some(current)) => {
                    compare_orderable_field_values_with_slot(
                        target_field.field(),
                        aggregate_field_slot,
                        value,
                        current,
                    )
                    .map_err(super::super::field::AggregateFieldValueError::into_internal_error)?
                    .is_lt()
                }
                GroupedAggregateReducerState::Min(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MIN")),
            };
            if replace {
                self.reducer.update_min_value(value.clone())?;
            }
        } else if self.compiled_input_expr.is_some() {
            let Some(value) = self.evaluate_input_value(row_view)? else {
                return Err(Self::field_target_execution_required("MIN(expr)"));
            };
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Min(Some(current)) => {
                    compare_numeric_or_strict_order(&value, current)
                        .ok_or_else(|| {
                            InternalError::query_executor_invariant(
                                "grouped MIN(expr) encountered incomparable expression values",
                            )
                        })?
                        .is_lt()
                }
                GroupedAggregateReducerState::Min(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MIN")),
            };
            if replace {
                self.reducer.update_min_value(value)?;
            }
        } else {
            let Some(key) = key else {
                return Err(Self::storage_key_required("MIN"));
            };
            self.reducer.update_min_value(key.as_value())?;
        }

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }
}

///
/// AggregateStateFactory
///
/// AggregateStateFactory builds canonical scalar and grouped terminal state
/// machines from route-owned kind/direction decisions.
/// This keeps state initialization centralized at one boundary.
///

pub(in crate::db::executor) struct AggregateStateFactory;

impl AggregateStateFactory {
    /// Build one scalar terminal aggregate state machine for kernel reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_scalar_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> ScalarTerminalAggregateState {
        ScalarTerminalAggregateState {
            reducer_class: kind.reducer_class(),
            direction,
            distinct,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_storage_key: kind.requires_decoded_id(),
            reducer: ScalarAggregateReducerState::for_kind(kind),
        }
    }

    /// Build one grouped terminal aggregate state machine for grouped reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
        compiled_input_expr: Option<ScalarProjectionExpr>,
        compiled_filter_expr: Option<ScalarProjectionExpr>,
        max_distinct_values_per_group: u64,
    ) -> GroupedTerminalAggregateState {
        GroupedTerminalAggregateState {
            kind,
            reducer_class: kind.reducer_class(),
            direction,
            distinct,
            max_distinct_values_per_group,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            target_field,
            compiled_input_expr,
            compiled_filter_expr,
            requires_storage_key: kind.requires_decoded_id(),
            reducer: GroupedAggregateReducerState::for_kind(kind),
        }
    }
}

impl ScalarTerminalAggregateState {
    // Build the canonical scalar terminal invariant for field-target-only kinds.
    fn field_target_execution_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "aggregate reducer {kind} requires field-target execution path"
        ))
    }

    // Build the canonical scalar terminal invariant for storage-key-required updates.
    fn storage_key_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "aggregate reducer {kind} update requires storage key"
        ))
    }

    // Dispatch one scalar terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.reducer_class {
            AggregateReducerClass::Count => self.apply_count(storage_key),
            AggregateReducerClass::SumLike => Self::apply_sum_like_unsupported(storage_key),
            AggregateReducerClass::Exists => self.apply_exists(storage_key),
            AggregateReducerClass::Min => self.apply_min(storage_key),
            AggregateReducerClass::Max => self.apply_max(storage_key),
            AggregateReducerClass::First => self.apply_first(storage_key),
            AggregateReducerClass::Last => self.apply_last(storage_key),
        }
    }

    // Apply one COUNT scalar terminal update.
    fn apply_count(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS scalar terminal update.
    fn apply_exists(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Reject SUM/AVG through scalar key-based reducer paths.
    fn apply_sum_like_unsupported(_key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        Err(Self::field_target_execution_required("SUM/AVG"))
    }

    // Apply one MAX scalar terminal update.
    fn apply_max(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("MAX"));
        };
        self.reducer.update_max_value(key)?;

        Ok(if self.direction == Direction::Desc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one FIRST scalar terminal update.
    fn apply_first(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("FIRST"));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST scalar terminal update.
    fn apply_last(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("LAST"));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN scalar terminal update.
    fn apply_min(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("MIN"));
        };
        self.reducer.update_min_value(key)?;

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }
}

// Record one distinct data-key marker for one aggregate state.
fn record_distinct_key(
    distinct_keys: Option<&mut GroupKeySet>,
    key: &DataKey,
) -> Result<bool, InternalError> {
    let Some(distinct_keys) = distinct_keys else {
        return Ok(true);
    };
    let canonical_key = canonical_key_from_data_key(key)?;

    Ok(distinct_keys.insert_key(canonical_key))
}

// Record one grouped distinct data-key marker and enforce grouped distinct budgets.
fn record_grouped_distinct_key(
    distinct_keys: Option<&mut GroupKeySet>,
    key: &DataKey,
    execution_context: &mut ExecutionContext,
    max_distinct_values_per_group: u64,
) -> Result<bool, GroupError> {
    let Some(distinct_keys) = distinct_keys else {
        return Ok(true);
    };
    let canonical_key = canonical_key_from_data_key(key).map_err(GroupError::from)?;

    execution_context.admit_distinct_key(
        distinct_keys,
        max_distinct_values_per_group,
        canonical_key,
    )
}

// Convert one data key into the canonical grouped DISTINCT key surface.
fn canonical_key_from_data_key(key: &DataKey) -> Result<GroupKey, InternalError> {
    key.storage_key()
        .as_value()
        .canonical_key()
        .map_err(KeyCanonicalError::into_internal_error)
}

///
/// AggregateFoldMode
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}
