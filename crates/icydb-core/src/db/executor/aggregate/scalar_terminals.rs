//! Module: executor::aggregate::scalar_terminals
//! Responsibility: scalar-window aggregate terminals over retained-slot rows.
//! Does not own: SQL lowering, grouped DISTINCT policy, or post-aggregate output shaping.
//! Boundary: consumes prepared scalar access/window plans plus uncached terminal metadata.

use crate::{
    db::{
        executor::{
            PreparedExecutionPlan,
            pipeline::{
                contracts::{CursorEmissionMode, LoadExecutor, ProjectionMaterializationMode},
                entrypoints::execute_prepared_scalar_retained_slot_page_for_canister,
                runtime::compile_retained_slot_layout_for_mode_with_extra_slots,
            },
            projection::{
                ProjectionEvalError, ScalarProjectionExpr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
            },
            terminal::{RetainedSlotLayout, RetainedSlotRow},
        },
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::plan::{AccessPlannedQuery, expr::collapse_true_only_boolean_admission},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

///
/// PreparedScalarAggregateTerminalSet
///
/// PreparedScalarAggregateTerminalSet carries the uncached scalar aggregate
/// terminals that one caller wants to reduce over a prepared scalar access
/// and window plan. It exists so SQL can keep aggregate-specific metadata out
/// of `SharedPreparedExecutionPlan` while executor code owns row reduction.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PreparedScalarAggregateTerminalSet {
    terminals: Vec<PreparedScalarAggregateTerminal>,
}

impl PreparedScalarAggregateTerminalSet {
    /// Build one terminal set from caller-prepared scalar aggregate terminals.
    #[must_use]
    pub(in crate::db) const fn new(terminals: Vec<PreparedScalarAggregateTerminal>) -> Self {
        Self { terminals }
    }

    const fn is_empty(&self) -> bool {
        self.terminals.is_empty()
    }

    fn retained_slot_layout(
        &self,
        model: &EntityModel,
        plan: &AccessPlannedQuery,
    ) -> Result<RetainedSlotLayout, InternalError> {
        let mut extra_slots = Vec::new();

        // Phase 1: collect only terminal-local slot requirements. The scalar
        // runtime helper will add access, residual filter, ordering, and other
        // plan-owned requirements from the prepared scalar plan.
        for terminal in &self.terminals {
            terminal.extend_referenced_slots(&mut extra_slots);
        }

        // Phase 2: compile a retained-slot layout for this execution only.
        // RetainSlotRows keeps even empty-slot COUNT(*) filters row-shaped so
        // the reducer still sees one retained row per scalar input row.
        compile_retained_slot_layout_for_mode_with_extra_slots(
            model,
            plan,
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
            extra_slots.as_slice(),
        )
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar aggregate terminal execution requires a retained-slot layout",
            )
        })
    }
}

///
/// PreparedScalarAggregateTerminal
///
/// PreparedScalarAggregateTerminal describes one executor-owned scalar
/// aggregate reducer. The input and optional filter are already compiled to
/// scalar projection programs so execution reads retained slots without
/// reopening SQL or planner expression trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PreparedScalarAggregateTerminal {
    kind: ScalarAggregateTerminalKind,
    input: ScalarAggregateInput,
    filter: Option<ScalarProjectionExpr>,
    distinct: bool,
    empty_behavior: AggregateEmptyBehavior,
}

impl PreparedScalarAggregateTerminal {
    /// Build one prepared scalar aggregate terminal from validated parts.
    #[must_use]
    pub(in crate::db) const fn from_validated_parts(
        kind: ScalarAggregateTerminalKind,
        input: ScalarAggregateInput,
        filter: Option<ScalarProjectionExpr>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            input,
            filter,
            distinct,
            empty_behavior: kind.empty_behavior(),
        }
    }

    fn extend_referenced_slots(&self, slots: &mut Vec<usize>) {
        self.input.extend_referenced_slots(slots);
        if let Some(filter) = self.filter.as_ref() {
            filter.extend_referenced_slots(slots);
        }
    }
}

///
/// ScalarAggregateTerminalKind
///
/// ScalarAggregateTerminalKind selects the reducer family used for one
/// scalar-window aggregate terminal. These variants intentionally model only
/// row-count, value-count, numeric, and extrema reducers, not grouped folds.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarAggregateTerminalKind {
    CountRows,
    CountValues,
    Sum,
    Avg,
    Min,
    Max,
}

impl ScalarAggregateTerminalKind {
    // Keep empty-window behavior attached to the executor-owned terminal kind
    // so SQL callers cannot choose a reducer/finalizer combination that drifts
    // from aggregate semantics.
    const fn empty_behavior(self) -> AggregateEmptyBehavior {
        match self {
            Self::CountRows | Self::CountValues => AggregateEmptyBehavior::Zero,
            Self::Sum | Self::Avg | Self::Min | Self::Max => AggregateEmptyBehavior::Null,
        }
    }
}

///
/// ScalarAggregateInput
///
/// ScalarAggregateInput identifies how one terminal obtains its per-row input
/// value from the retained-slot scalar row. `Rows` consumes only row presence,
/// while field and expression inputs materialize one value per admitted row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarAggregateInput {
    Rows,
    Field { slot: usize, field: String },
    Expr(ScalarProjectionExpr),
}

impl ScalarAggregateInput {
    fn extend_referenced_slots(&self, slots: &mut Vec<usize>) {
        match self {
            Self::Rows => {}
            Self::Field { slot, .. } => {
                if !slots.contains(slot) {
                    slots.push(*slot);
                }
            }
            Self::Expr(expr) => expr.extend_referenced_slots(slots),
        }
    }
}

///
/// AggregateEmptyBehavior
///
/// AggregateEmptyBehavior preserves the SQL scalar aggregate finalization
/// contract for empty or all-NULL input windows. COUNT terminals finalize to
/// zero, while numeric and extrema terminals finalize to NULL.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateEmptyBehavior {
    Zero,
    Null,
}

///
/// ScalarAggregateReducerState
///
/// ScalarAggregateReducerState stores the in-progress fold for one prepared
/// scalar aggregate terminal. It keeps DISTINCT admission adjacent to reducer
/// state so callers never materialize one `Vec<Value>` per aggregate.
///

struct ScalarAggregateReducerState {
    terminal: PreparedScalarAggregateTerminal,
    distinct_values: Vec<Value>,
    count: u64,
    sum: Option<Decimal>,
    selected: Option<Value>,
}

impl ScalarAggregateReducerState {
    const fn new(terminal: PreparedScalarAggregateTerminal) -> Self {
        Self {
            terminal,
            distinct_values: Vec::new(),
            count: 0,
            sum: None,
            selected: None,
        }
    }

    fn admit_distinct_value(&mut self, value: &Value) -> bool {
        if !self.terminal.distinct {
            return true;
        }
        if self.distinct_values.iter().any(|current| current == value) {
            return false;
        }
        self.distinct_values.push(value.clone());

        true
    }

    fn ingest_row(&mut self) -> Result<(), InternalError> {
        if self.terminal.distinct {
            return Err(InternalError::query_executor_invariant(
                "COUNT(*) scalar aggregate terminal cannot be DISTINCT",
            ));
        }

        self.count = self.count.saturating_add(1);

        Ok(())
    }

    fn ingest_value(&mut self, value: Value) -> Result<(), InternalError> {
        if !self.admit_distinct_value(&value) || matches!(value, Value::Null) {
            return Ok(());
        }

        match self.terminal.kind {
            ScalarAggregateTerminalKind::CountValues => {
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            ScalarAggregateTerminalKind::Sum | ScalarAggregateTerminalKind::Avg => {
                let decimal = coerce_numeric_decimal(&value).ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "scalar aggregate numeric terminal encountered non-numeric value: {value:?}",
                    ))
                })?;
                self.sum = Some(
                    self.sum
                        .map_or(decimal, |current| add_decimal_terms(current, decimal)),
                );
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            ScalarAggregateTerminalKind::Min | ScalarAggregateTerminalKind::Max => {
                let replace = match self.selected.as_ref() {
                    None => true,
                    Some(current) => {
                        let ordering = compare_numeric_or_strict_order(&value, current)
                            .ok_or_else(|| {
                                InternalError::query_executor_invariant(format!(
                                    "scalar aggregate extrema terminal encountered incomparable values: left={value:?} right={current:?}",
                                ))
                            })?;

                        match self.terminal.kind {
                            ScalarAggregateTerminalKind::Min => ordering.is_lt(),
                            ScalarAggregateTerminalKind::Max => ordering.is_gt(),
                            ScalarAggregateTerminalKind::CountRows
                            | ScalarAggregateTerminalKind::CountValues
                            | ScalarAggregateTerminalKind::Sum
                            | ScalarAggregateTerminalKind::Avg => {
                                return Err(InternalError::query_executor_invariant(
                                    "scalar aggregate extrema terminal kind mismatch",
                                ));
                            }
                        }
                    }
                };

                if replace {
                    self.selected = Some(value);
                }
                Ok(())
            }
            ScalarAggregateTerminalKind::CountRows => Err(InternalError::query_executor_invariant(
                "COUNT(*) scalar aggregate terminal cannot consume projected values",
            )),
        }
    }

    fn finalize(self) -> Value {
        match self.terminal.kind {
            ScalarAggregateTerminalKind::CountRows | ScalarAggregateTerminalKind::CountValues => {
                Value::Uint(self.count)
            }
            ScalarAggregateTerminalKind::Sum => {
                self.sum.map_or_else(|| self.empty_value(), Value::Decimal)
            }
            ScalarAggregateTerminalKind::Avg => self
                .sum
                .and_then(|sum| average_decimal_terms(sum, self.count))
                .map_or_else(|| self.empty_value(), Value::Decimal),
            ScalarAggregateTerminalKind::Min | ScalarAggregateTerminalKind::Max => {
                let empty_value = self.empty_value();

                self.selected.unwrap_or(empty_value)
            }
        }
    }

    const fn empty_value(&self) -> Value {
        match self.terminal.empty_behavior {
            AggregateEmptyBehavior::Zero => Value::Uint(0),
            AggregateEmptyBehavior::Null => Value::Null,
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute scalar aggregate terminals over one prepared scalar access/window plan.
    pub(in crate::db) fn execute_scalar_aggregate_terminals(
        &self,
        plan: PreparedExecutionPlan<E>,
        terminals: PreparedScalarAggregateTerminalSet,
    ) -> Result<Vec<Value>, InternalError> {
        if terminals.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: run the scalar plan with an execution-local retained-slot
        // layout that includes aggregate input and filter slots.
        let plan = plan.into_prepared_load_plan();
        let retained_slot_layout =
            terminals.retained_slot_layout(plan.authority().model(), plan.logical_plan())?;
        let page = execute_prepared_scalar_retained_slot_page_for_canister(
            &self.db,
            self.debug,
            plan,
            retained_slot_layout,
        )?;
        let rows = match page.into_payload() {
            crate::db::executor::pipeline::contracts::StructuralCursorPagePayload::SlotRows(
                rows,
            ) => rows,
            crate::db::executor::pipeline::contracts::StructuralCursorPagePayload::DataRows(_) => {
                return Err(InternalError::query_executor_invariant(
                    "scalar aggregate terminal execution requires retained slot rows",
                ));
            }
        };

        // Phase 2: reduce every terminal over the scalar-window row set once,
        // preserving SQL projection/filter semantics while avoiding per-terminal
        // projected-value vectors.
        // A later optimization can intern shared terminal expressions here; this
        // first boundary keeps expression reuse out of the semantic extraction.
        let mut reducers = terminals
            .terminals
            .into_iter()
            .map(ScalarAggregateReducerState::new)
            .collect::<Vec<_>>();

        for row in &rows {
            for reducer in &mut reducers {
                if !terminal_filter_matches(&reducer.terminal, row)? {
                    continue;
                }
                match &reducer.terminal.input {
                    ScalarAggregateInput::Rows => reducer.ingest_row()?,
                    ScalarAggregateInput::Field { slot, field } => {
                        let value = row.slot_ref(*slot).cloned().ok_or_else(|| {
                            ProjectionEvalError::MissingFieldValue {
                                field: field.clone(),
                                index: *slot,
                            }
                            .into_invalid_logical_plan_internal_error()
                        })?;
                        reducer.ingest_value(value)?;
                    }
                    ScalarAggregateInput::Expr(expr) => {
                        let value = evaluate_scalar_terminal_expr(expr, row)?;
                        reducer.ingest_value(value)?;
                    }
                }
            }
        }

        Ok(reducers
            .into_iter()
            .map(ScalarAggregateReducerState::finalize)
            .collect())
    }
}

fn terminal_filter_matches(
    terminal: &PreparedScalarAggregateTerminal,
    row: &RetainedSlotRow,
) -> Result<bool, InternalError> {
    let Some(filter) = terminal.filter.as_ref() else {
        return Ok(true);
    };
    let value = evaluate_scalar_terminal_expr(filter, row)?;

    collapse_true_only_boolean_admission(value, |found| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal filter expression produced non-boolean value: {:?}",
            found.as_ref(),
        ))
    })
}

fn evaluate_scalar_terminal_expr(
    expr: &ScalarProjectionExpr,
    row: &RetainedSlotRow,
) -> Result<Value, InternalError> {
    let mut read_slot = |slot: usize| {
        row.slot_ref(slot)
            .map(std::borrow::Cow::Borrowed)
            .ok_or_else(|| {
                ProjectionEvalError::MissingFieldValue {
                    field: format!("slot[{slot}]"),
                    index: slot,
                }
                .into_invalid_logical_plan_internal_error()
            })
    };

    eval_canonical_scalar_projection_expr_with_required_value_reader_cow(expr, &mut read_slot)
        .map(std::borrow::Cow::into_owned)
}
