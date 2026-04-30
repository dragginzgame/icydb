use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::{
                contracts::{
                    error::GroupError,
                    grouped::ExecutionContext,
                    spec::AggregateKind,
                    state::{
                        CompiledExpr, ExtremumKind, FoldControl, GroupedAggregateReducerState,
                        GroupedDistinctExecutionMode, canonical_key_from_data_key,
                    },
                },
                field::{
                    AggregateFieldValueError, FieldSlot as AggregateFieldSlot,
                    compare_orderable_field_values_with_slot,
                },
            },
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::runtime::RowView,
            projection::ProjectionEvalError,
        },
        query::plan::FieldSlot,
        query::plan::expr::collapse_true_only_boolean_admission,
    },
    error::InternalError,
    types::Decimal,
    value::{
        StorageKey, Value, ops::numeric::to_numeric_decimal, semantics::supports_numeric_coercion,
        storage_key_as_runtime_value,
    },
};
use std::borrow::Cow;

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

    // Return the executor-facing SUM/AVG input label used by grouped numeric
    // reducers.
    const fn input_label(self) -> &'static str {
        match self {
            Self::Sum => "SUM(input)",
            Self::Avg => "AVG(input)",
        }
    }

    // Apply one grouped numeric decimal payload through the SUM/AVG reducer
    // family.
    fn apply_decimal(
        self,
        reducer: &mut GroupedAggregateReducerState,
        decimal: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum => reducer.add_sum_value(decimal),
            Self::Avg => reducer.add_average_value(decimal),
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
    pub(in crate::db::executor::aggregate::contracts::state) target_field: Option<FieldSlot>,
    pub(in crate::db::executor::aggregate::contracts::state) grouped_input_expr:
        Option<CompiledExpr>,
    pub(in crate::db::executor::aggregate::contracts::state) grouped_filter_expr:
        Option<CompiledExpr>,
    pub(in crate::db::executor::aggregate::contracts::state) requires_storage_key: bool,
    pub(in crate::db::executor::aggregate::contracts::state) reducer: GroupedAggregateReducerState,
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
    // SUM/AVG(field) payload that planner semantics should already have
    // rejected.
    fn sum_like_field_requires_numeric_value(
        label: &'static str,
        field: &str,
        value: &Value,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {label} requires numeric field '{field}', found value {value:?}"
        ))
    }

    // Build the canonical grouped terminal invariant for aggregate-input
    // expressions that drift outside the grouped compiled evaluator.
    fn input_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        if let ProjectionEvalError::Numeric(err) = err {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan(format!(
            "grouped aggregate input expression evaluation failed: {err}",
        ))
    }

    // Build the canonical grouped terminal invariant for aggregate filters
    // that drift outside the grouped compiled evaluator.
    fn filter_expression_evaluation_failed(err: ProjectionEvalError) -> InternalError {
        if let ProjectionEvalError::Numeric(err) = err {
            return err.into_internal_error();
        }

        InternalError::query_invalid_logical_plan(format!(
            "grouped aggregate filter expression evaluation failed: {err}",
        ))
    }

    // Evaluate one row-backed grouped expression for aggregate execution. Input
    // and FILTER expressions use different error labels but share the same
    // slot-indexed evaluator.
    fn evaluate_row_expression_value(
        row_view: Option<&RowView>,
        expression: &CompiledExpr,
        missing_row_label: &'static str,
        map_eval_error: fn(ProjectionEvalError) -> InternalError,
    ) -> Result<Value, InternalError> {
        let Some(row_view) = row_view else {
            return Err(Self::field_target_execution_required(missing_row_label));
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
        let Some(grouped_input_expr) = self.grouped_input_expr.as_ref() else {
            return Err(Self::field_target_execution_required(
                "grouped aggregate input expression",
            ));
        };

        Self::evaluate_row_expression_value(
            row_view,
            grouped_input_expr,
            "grouped aggregate input expression",
            Self::input_expression_evaluation_failed,
        )
    }

    // Read one direct field-target input when the aggregate only needs to
    // inspect the row value. The returned cow keeps single-slot views borrowed
    // while raw-row-backed views avoid retained-slot vector construction.
    fn target_field_value<'a>(
        &self,
        row_view: Option<&'a RowView>,
        label: &'static str,
    ) -> Result<Cow<'a, Value>, InternalError> {
        let Some(target_field) = self.target_field.as_ref() else {
            return Err(Self::field_target_execution_required(label));
        };
        let Some(row_view) = row_view else {
            return Err(Self::field_target_execution_required(label));
        };

        row_view.require_slot_value(target_field.index())
    }

    // Resolve the one canonical grouped aggregate input value for COUNT/SUM/AVG
    // and field/expression MIN/MAX reducers. Key-only reducers deliberately stay
    // outside this helper because their input is the storage key, not a row slot.
    fn resolve_input_value(
        &self,
        row_view: Option<&RowView>,
        label: &'static str,
    ) -> Result<AggregateInputValue, InternalError> {
        let value = if self.grouped_input_expr.is_some() {
            self.evaluate_compiled_input_value(row_view)?
        } else if self.target_field.is_some() {
            self.target_field_value(row_view, label)?.into_owned()
        } else {
            return Err(Self::field_target_execution_required(label));
        };

        Ok(if matches!(value, Value::Null) {
            AggregateInputValue::Null
        } else {
            AggregateInputValue::Value(value)
        })
    }

    // Coerce one already-resolved SUM/AVG input through the shared Value
    // numeric operation boundary. This keeps grouped reducers from reopening
    // Value-level arithmetic while preserving the global numeric admission
    // contract for values that cannot be represented as Decimal.
    fn coerce_sum_like_decimal(value: &Value) -> Option<Decimal> {
        if supports_numeric_coercion(value) {
            return to_numeric_decimal(value);
        }

        None
    }

    // Resolve one SUM/AVG input as an optional Decimal without cloning direct
    // field-target slots. Compiled expression inputs still own their temporary
    // result because the expression evaluator may synthesize a value.
    fn resolve_sum_like_decimal_input(
        &self,
        row_view: Option<&RowView>,
        label: &'static str,
    ) -> Result<Option<Decimal>, InternalError> {
        if self.grouped_input_expr.is_some() {
            let value = self.evaluate_compiled_input_value(row_view)?;
            if matches!(value, Value::Null) {
                return Ok(None);
            }

            return Self::coerce_sum_like_decimal(&value)
                .map(Some)
                .ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "grouped aggregate reducer {label} requires numeric expression input, found value {value:?}",
                    ))
                });
        }

        let Some(target_field) = self.target_field.as_ref() else {
            return Err(Self::field_target_execution_required("SUM/AVG(input)"));
        };
        let value = self.target_field_value(row_view, label)?;
        if matches!(value.as_ref(), Value::Null) {
            return Ok(None);
        }

        Self::coerce_sum_like_decimal(value.as_ref())
            .map(Some)
            .ok_or_else(|| {
                Self::sum_like_field_requires_numeric_value(
                    label,
                    target_field.field(),
                    value.as_ref(),
                )
            })
    }

    // Evaluate one grouped aggregate filter expression through the same compiled
    // grouped expression boundary used by aggregate inputs.
    fn admits_filter_row(&self, row_view: Option<&RowView>) -> Result<bool, InternalError> {
        let Some(grouped_filter_expr) = self.grouped_filter_expr.as_ref() else {
            return Ok(true);
        };

        let value = Self::evaluate_row_expression_value(
            row_view,
            grouped_filter_expr,
            "grouped aggregate filter expression",
            Self::filter_expression_evaluation_failed,
        )?;

        collapse_true_only_boolean_admission(value, |found| {
            InternalError::query_invalid_logical_plan(format!(
                "grouped aggregate filter expression produced non-boolean value: {:?}",
                found.as_ref(),
            ))
        })
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
        key: &DataKey,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.kind {
            AggregateKind::Count => self.apply_count(storage_key, row_view),
            AggregateKind::Sum | AggregateKind::Avg => self.apply_sum_like(storage_key, row_view),
            AggregateKind::Exists => self.apply_exists(storage_key, row_view),
            AggregateKind::Min => self.apply_extremum(ExtremumKind::Min, storage_key, row_view),
            AggregateKind::Max => self.apply_extremum(ExtremumKind::Max, storage_key, row_view),
            AggregateKind::First => self.apply_first(storage_key, row_view),
            AggregateKind::Last => self.apply_last(storage_key, row_view),
        }
    }

    // Admit one grouped DISTINCT candidate at the reducer boundary. Value-based
    // DISTINCT uses the same canonical input resolver as the aggregate update,
    // while key-based DISTINCT keeps the existing storage-key identity surface.
    fn admit_distinct(
        &mut self,
        key: &DataKey,
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
                .resolve_input_value(row_view, "COUNT/SUM/AVG(DISTINCT input)")
                .map_err(GroupError::from)?;
            let AggregateInputValue::Value(value) = input_value else {
                return Ok(false);
            };

            value
                .canonical_key()
                .map_err(KeyCanonicalError::into_internal_error)
                .map_err(GroupError::from)?
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
        _key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if (self.grouped_input_expr.is_some() || self.target_field.is_some())
            && matches!(
                self.resolve_input_value(row_view, "COUNT(input)")?,
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
        let Some(sum_like_kind) = SumLikeKind::from_aggregate_kind(self.kind) else {
            return Err(Self::field_target_execution_required("SUM/AVG(input)"));
        };
        let kind_label = sum_like_kind.input_label();

        let Some(decimal) = self.resolve_sum_like_decimal_input(row_view, kind_label)? else {
            return Ok(FoldControl::Continue);
        };
        sum_like_kind.apply_decimal(&mut self.reducer, decimal)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN/MAX grouped terminal update. Field-target extrema keep the
    // slot-aware comparison path, expression extrema use value reducers, and
    // key-only extrema preserve storage-key ordering.
    fn apply_extremum(
        &mut self,
        kind: ExtremumKind,
        key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if self.grouped_input_expr.is_some() {
            let AggregateInputValue::Value(value) =
                self.resolve_input_value(row_view, kind.expression_label())?
            else {
                return Ok(FoldControl::Continue);
            };
            match kind {
                ExtremumKind::Min => self.reducer.ingest_min_value(value)?,
                ExtremumKind::Max => self.reducer.ingest_max_value(value)?,
            }
        } else if let Some(target_field) = self.target_field.as_ref() {
            let Some(target_kind) = target_field.kind() else {
                return Err(Self::field_target_execution_required(kind.field_label()));
            };
            let AggregateInputValue::Value(value) =
                self.resolve_input_value(row_view, kind.field_label())?
            else {
                return Ok(FoldControl::Continue);
            };
            let aggregate_field_slot = AggregateFieldSlot {
                index: target_field.index(),
                kind: target_kind,
            };
            let current = match kind {
                ExtremumKind::Min => self.reducer.min_value()?,
                ExtremumKind::Max => self.reducer.max_value()?,
            };
            let replace = match current {
                Some(current) => {
                    let ordering = compare_orderable_field_values_with_slot(
                        target_field.field(),
                        aggregate_field_slot,
                        &value,
                        current,
                    )
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
                return Err(Self::storage_key_required(kind.storage_key_label()));
            };
            let value = storage_key_as_runtime_value(&key);
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
}
