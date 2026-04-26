//! Module: executor::aggregate::scalar_terminals
//! Responsibility: scalar-window aggregate terminals over retained-slot rows.
//! Does not own: SQL lowering, grouped DISTINCT policy, or post-aggregate output shaping.
//! Boundary: consumes prepared scalar access/window plans plus uncached terminal metadata.

#[cfg(feature = "diagnostics")]
use std::cell::Cell;

use crate::{
    db::{
        executor::{
            PreparedExecutionPlan,
            pipeline::{
                contracts::{CursorEmissionMode, LoadExecutor, ProjectionMaterializationMode},
                entrypoints::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister,
                runtime::compile_retained_slot_layout_for_mode_with_extra_slots,
            },
            projection::{
                ProjectionEvalError, ScalarProjectionExpr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
            },
            terminal::{KernelRow, RetainedSlotLayout},
        },
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

#[cfg(feature = "diagnostics")]
std::thread_local! {
    static SCALAR_AGGREGATE_TERMINAL_ATTRIBUTION: Cell<ScalarAggregateTerminalAttribution> =
        const { Cell::new(ScalarAggregateTerminalAttribution::none()) };
}

///
/// ScalarAggregateSinkMode
///
/// ScalarAggregateSinkMode records which executor-owned scalar aggregate sink
/// strategy reduced one terminal set. It exists for diagnostics so the future
/// streaming sink can be compared against today's buffered kernel-row boundary.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) enum ScalarAggregateSinkMode {
    #[default]
    None,
    Buffered,
}

#[cfg(feature = "diagnostics")]
impl ScalarAggregateSinkMode {
    pub(in crate::db) const fn label(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Buffered => Some("Buffered"),
        }
    }
}

///
/// ScalarAggregateTerminalAttribution
///
/// ScalarAggregateTerminalAttribution is the diagnostics-only executor snapshot
/// for one scalar aggregate terminal execution. It keeps base-row materialization,
/// reducer fold work, expression reuse counts, and terminal shape metrics together.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct ScalarAggregateTerminalAttribution {
    pub(in crate::db) base_row_local_instructions: u64,
    pub(in crate::db) reducer_fold_local_instructions: u64,
    pub(in crate::db) expression_evaluations: u64,
    pub(in crate::db) filter_evaluations: u64,
    pub(in crate::db) rows_ingested: u64,
    pub(in crate::db) terminal_count: u64,
    pub(in crate::db) unique_input_expr_count: u64,
    pub(in crate::db) unique_filter_expr_count: u64,
    pub(in crate::db) sink_mode: ScalarAggregateSinkMode,
}

#[cfg(feature = "diagnostics")]
impl ScalarAggregateTerminalAttribution {
    pub(in crate::db) const fn none() -> Self {
        Self {
            base_row_local_instructions: 0,
            reducer_fold_local_instructions: 0,
            expression_evaluations: 0,
            filter_evaluations: 0,
            rows_ingested: 0,
            terminal_count: 0,
            unique_input_expr_count: 0,
            unique_filter_expr_count: 0,
            sink_mode: ScalarAggregateSinkMode::None,
        }
    }

    fn from_terminal_set(terminals: &PreparedScalarAggregateTerminalSet) -> Self {
        Self {
            terminal_count: usize_to_u64(terminals.terminals.len()),
            unique_input_expr_count: usize_to_u64(terminals.input_exprs.len()),
            unique_filter_expr_count: usize_to_u64(terminals.filter_exprs.len()),
            sink_mode: ScalarAggregateSinkMode::Buffered,
            ..Self::none()
        }
    }

    const fn merge_runtime(&mut self, runtime: Self) {
        self.reducer_fold_local_instructions = self
            .reducer_fold_local_instructions
            .saturating_add(runtime.reducer_fold_local_instructions);
        self.expression_evaluations = self
            .expression_evaluations
            .saturating_add(runtime.expression_evaluations);
        self.filter_evaluations = self
            .filter_evaluations
            .saturating_add(runtime.filter_evaluations);
        self.rows_ingested = self.rows_ingested.saturating_add(runtime.rows_ingested);
    }

    fn merge_recorded(&mut self, other: Self) {
        self.base_row_local_instructions = self
            .base_row_local_instructions
            .saturating_add(other.base_row_local_instructions);
        self.reducer_fold_local_instructions = self
            .reducer_fold_local_instructions
            .saturating_add(other.reducer_fold_local_instructions);
        self.expression_evaluations = self
            .expression_evaluations
            .saturating_add(other.expression_evaluations);
        self.filter_evaluations = self
            .filter_evaluations
            .saturating_add(other.filter_evaluations);
        self.rows_ingested = self.rows_ingested.saturating_add(other.rows_ingested);
        self.terminal_count = self.terminal_count.saturating_add(other.terminal_count);
        self.unique_input_expr_count = self
            .unique_input_expr_count
            .saturating_add(other.unique_input_expr_count);
        self.unique_filter_expr_count = self
            .unique_filter_expr_count
            .saturating_add(other.unique_filter_expr_count);
        if other.sink_mode != ScalarAggregateSinkMode::None {
            self.sink_mode = other.sink_mode;
        }
    }
}

/// Run one closure while collecting scalar aggregate terminal diagnostics.
#[cfg(feature = "diagnostics")]
pub(in crate::db) fn with_scalar_aggregate_terminal_attribution<T>(
    run: impl FnOnce() -> T,
) -> (ScalarAggregateTerminalAttribution, T) {
    let previous = SCALAR_AGGREGATE_TERMINAL_ATTRIBUTION.with(|attribution| {
        let previous = attribution.get();
        attribution.set(ScalarAggregateTerminalAttribution::none());
        previous
    });
    let output = run();
    let captured = SCALAR_AGGREGATE_TERMINAL_ATTRIBUTION.with(|attribution| {
        let captured = attribution.get();
        attribution.set(previous);
        captured
    });

    (captured, output)
}

#[cfg(feature = "diagnostics")]
fn record_scalar_aggregate_terminal_attribution(recorded: ScalarAggregateTerminalAttribution) {
    SCALAR_AGGREGATE_TERMINAL_ATTRIBUTION.with(|attribution| {
        let mut current = attribution.get();
        current.merge_recorded(recorded);
        attribution.set(current);
    });
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_scalar_aggregate_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_scalar_aggregate_terminal_phase<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    let start = read_scalar_aggregate_local_instruction_counter();
    let result = run();
    let delta = read_scalar_aggregate_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

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
    terminals: Vec<InternedPreparedScalarAggregateTerminal>,
    input_exprs: Vec<ScalarProjectionExpr>,
    filter_exprs: Vec<ScalarProjectionExpr>,
}

impl PreparedScalarAggregateTerminalSet {
    /// Build one terminal set from caller-prepared scalar aggregate terminals.
    #[must_use]
    pub(in crate::db) fn new(terminals: Vec<PreparedScalarAggregateTerminal>) -> Self {
        let mut input_exprs = Vec::new();
        let mut filter_exprs = Vec::new();
        let terminals = terminals
            .into_iter()
            .map(|terminal| terminal.into_interned(&mut input_exprs, &mut filter_exprs))
            .collect();

        Self {
            terminals,
            input_exprs,
            filter_exprs,
        }
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
            terminal.input.extend_referenced_slots(&mut extra_slots);
        }
        for expr in &self.input_exprs {
            expr.extend_referenced_slots(&mut extra_slots);
        }
        for expr in &self.filter_exprs {
            expr.extend_referenced_slots(&mut extra_slots);
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

    fn into_interned(
        self,
        input_exprs: &mut Vec<ScalarProjectionExpr>,
        filter_exprs: &mut Vec<ScalarProjectionExpr>,
    ) -> InternedPreparedScalarAggregateTerminal {
        let input = match self.input {
            ScalarAggregateInput::Rows => InternedScalarAggregateInput::Rows,
            ScalarAggregateInput::Field { slot, field } => {
                InternedScalarAggregateInput::Field { slot, field }
            }
            ScalarAggregateInput::Expr(expr) => {
                InternedScalarAggregateInput::Expr(intern_scalar_terminal_expr(input_exprs, expr))
            }
        };
        let filter = self
            .filter
            .map(|expr| intern_scalar_terminal_expr(filter_exprs, expr));

        InternedPreparedScalarAggregateTerminal {
            kind: self.kind,
            input,
            filter,
            distinct: self.distinct,
            empty_behavior: self.empty_behavior,
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

///
/// InternedPreparedScalarAggregateTerminal
///
/// InternedPreparedScalarAggregateTerminal is the executor-local runtime form of
/// one scalar aggregate terminal after the containing terminal set has assigned
/// repeated input and filter expressions to shared expression tables.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct InternedPreparedScalarAggregateTerminal {
    kind: ScalarAggregateTerminalKind,
    input: InternedScalarAggregateInput,
    filter: Option<usize>,
    distinct: bool,
    empty_behavior: AggregateEmptyBehavior,
}

///
/// InternedScalarAggregateInput
///
/// InternedScalarAggregateInput keeps direct row and field inputs inline while
/// expression-backed inputs refer to the terminal set's shared input-expression
/// table, allowing execution to evaluate each unique expression once per row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum InternedScalarAggregateInput {
    Rows,
    Field { slot: usize, field: String },
    Expr(usize),
}

impl InternedScalarAggregateInput {
    fn extend_referenced_slots(&self, slots: &mut Vec<usize>) {
        match self {
            Self::Rows | Self::Expr(_) => {}
            Self::Field { slot, .. } => {
                if !slots.contains(slot) {
                    slots.push(*slot);
                }
            }
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
    terminal: InternedPreparedScalarAggregateTerminal,
    distinct_values: Vec<Value>,
    count: u64,
    sum: Option<Decimal>,
    selected: Option<Value>,
}

impl ScalarAggregateReducerState {
    const fn new(terminal: InternedPreparedScalarAggregateTerminal) -> Self {
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

///
/// ScalarAggregateReducerRuntime
///
/// ScalarAggregateReducerRuntime owns one scalar aggregate sink invocation.
/// It keeps reducer states and per-row interned-expression buffers together so
/// the scalar runtime can feed final window rows without returning a row page.
///

struct ScalarAggregateReducerRuntime {
    reducers: Vec<ScalarAggregateReducerState>,
    input_exprs: Vec<ScalarProjectionExpr>,
    filter_exprs: Vec<ScalarProjectionExpr>,
    input_expr_values: Vec<Option<Value>>,
    filter_expr_values: Vec<Option<Value>>,
    #[cfg(feature = "diagnostics")]
    attribution: ScalarAggregateTerminalAttribution,
}

impl ScalarAggregateReducerRuntime {
    // Build a reducer sink from one prepared terminal set, preserving the
    // expression-interning tables created during terminal preparation.
    fn new(terminals: PreparedScalarAggregateTerminalSet) -> Self {
        let reducers = terminals
            .terminals
            .into_iter()
            .map(ScalarAggregateReducerState::new)
            .collect();
        let input_expr_values = Vec::with_capacity(terminals.input_exprs.len());
        let filter_expr_values = Vec::with_capacity(terminals.filter_exprs.len());

        Self {
            reducers,
            input_exprs: terminals.input_exprs,
            filter_exprs: terminals.filter_exprs,
            input_expr_values,
            filter_expr_values,
            #[cfg(feature = "diagnostics")]
            attribution: ScalarAggregateTerminalAttribution::none(),
        }
    }

    // Ingest one scalar-window row into every aggregate reducer. Filters are
    // evaluated before input expressions so filtered-out rows still avoid input
    // work, while expression tables keep shared expressions to once per row.
    fn ingest_row(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        #[cfg(feature = "diagnostics")]
        {
            self.attribution.rows_ingested = self.attribution.rows_ingested.saturating_add(1);
            let (local_instructions, result) =
                measure_scalar_aggregate_terminal_phase(|| self.ingest_row_inner(row));
            self.attribution.reducer_fold_local_instructions = self
                .attribution
                .reducer_fold_local_instructions
                .saturating_add(local_instructions);

            result
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            self.ingest_row_inner(row)
        }
    }

    // Keep the reducer fold body separate so diagnostics can wrap exactly the
    // per-row terminal work without changing the non-diagnostics control flow.
    fn ingest_row_inner(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        reset_scalar_terminal_expr_values(&mut self.input_expr_values, self.input_exprs.len());
        reset_scalar_terminal_expr_values(&mut self.filter_expr_values, self.filter_exprs.len());

        for reducer in &mut self.reducers {
            if !terminal_filter_matches(
                &reducer.terminal,
                self.filter_exprs.as_slice(),
                row,
                &mut self.filter_expr_values,
                #[cfg(feature = "diagnostics")]
                &mut self.attribution.filter_evaluations,
            )? {
                continue;
            }
            match &reducer.terminal.input {
                InternedScalarAggregateInput::Rows => reducer.ingest_row()?,
                InternedScalarAggregateInput::Field { slot, field } => {
                    let value = row.slot_ref(*slot).cloned().ok_or_else(|| {
                        ProjectionEvalError::MissingFieldValue {
                            field: field.clone(),
                            index: *slot,
                        }
                        .into_invalid_logical_plan_internal_error()
                    })?;
                    reducer.ingest_value(value)?;
                }
                InternedScalarAggregateInput::Expr(expr_index) => {
                    let value = cached_scalar_terminal_expr_value(
                        self.input_exprs.as_slice(),
                        row,
                        &mut self.input_expr_values,
                        *expr_index,
                        "input",
                        #[cfg(feature = "diagnostics")]
                        &mut self.attribution.expression_evaluations,
                    )?
                    .clone();
                    reducer.ingest_value(value)?;
                }
            }
        }

        Ok(())
    }

    // Finalize reducer states in terminal order.
    fn finalize(self) -> Vec<Value> {
        self.reducers
            .into_iter()
            .map(ScalarAggregateReducerState::finalize)
            .collect()
    }

    #[cfg(feature = "diagnostics")]
    const fn attribution(&self) -> ScalarAggregateTerminalAttribution {
        self.attribution
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
        #[cfg(feature = "diagnostics")]
        let mut terminal_attribution =
            ScalarAggregateTerminalAttribution::from_terminal_set(&terminals);

        // Phase 1: prepare the scalar plan with an execution-local retained-slot
        // layout that includes aggregate input and filter slots.
        let plan = plan.into_prepared_load_plan();
        let retained_slot_layout =
            terminals.retained_slot_layout(plan.authority().model(), plan.logical_plan())?;

        // Phase 2: reduce every terminal as the scalar runtime emits its final
        // post-access/windowed row boundary, without constructing a retained-slot
        // response page for SQL-owned aggregate code to consume.
        let mut reducer_runtime = ScalarAggregateReducerRuntime::new(terminals);
        #[cfg(feature = "diagnostics")]
        {
            let (total_local_instructions, execution) =
                measure_scalar_aggregate_terminal_phase(|| {
                    execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
                        &self.db,
                        self.debug,
                        plan,
                        retained_slot_layout,
                        |row| reducer_runtime.ingest_row(row),
                    )
                });
            execution?;
            let runtime_attribution = reducer_runtime.attribution();
            terminal_attribution.merge_runtime(runtime_attribution);
            terminal_attribution.base_row_local_instructions = total_local_instructions
                .saturating_sub(terminal_attribution.reducer_fold_local_instructions);
            record_scalar_aggregate_terminal_attribution(terminal_attribution);
        }
        #[cfg(not(feature = "diagnostics"))]
        execute_prepared_scalar_aggregate_kernel_row_sink_for_canister(
            &self.db,
            self.debug,
            plan,
            retained_slot_layout,
            |row| reducer_runtime.ingest_row(row),
        )?;

        Ok(reducer_runtime.finalize())
    }
}

fn intern_scalar_terminal_expr(
    exprs: &mut Vec<ScalarProjectionExpr>,
    expr: ScalarProjectionExpr,
) -> usize {
    if let Some(index) = exprs.iter().position(|candidate| candidate == &expr) {
        return index;
    }

    let index = exprs.len();
    exprs.push(expr);

    index
}

fn reset_scalar_terminal_expr_values(values: &mut Vec<Option<Value>>, len: usize) {
    values.clear();
    values.resize_with(len, || None);
}

fn cached_scalar_terminal_expr_value<'a>(
    exprs: &[ScalarProjectionExpr],
    row: &KernelRow,
    values: &'a mut [Option<Value>],
    index: usize,
    label: &str,
    #[cfg(feature = "diagnostics")] evaluation_count: &mut u64,
) -> Result<&'a Value, InternalError> {
    let expr = exprs.get(index).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression index missing from expression table",
        ))
    })?;
    let value = values.get_mut(index).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression index missing from row buffer",
        ))
    })?;
    if value.is_none() {
        #[cfg(feature = "diagnostics")]
        {
            *evaluation_count = evaluation_count.saturating_add(1);
        }
        *value = Some(evaluate_scalar_terminal_expr(expr, row)?);
    }

    value.as_ref().ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression evaluation produced no row value",
        ))
    })
}

fn terminal_filter_matches(
    terminal: &InternedPreparedScalarAggregateTerminal,
    filter_exprs: &[ScalarProjectionExpr],
    row: &KernelRow,
    filter_expr_values: &mut [Option<Value>],
    #[cfg(feature = "diagnostics")] filter_evaluation_count: &mut u64,
) -> Result<bool, InternalError> {
    let Some(filter_index) = terminal.filter else {
        return Ok(true);
    };
    let value = cached_scalar_terminal_expr_value(
        filter_exprs,
        row,
        filter_expr_values,
        filter_index,
        "filter",
        #[cfg(feature = "diagnostics")]
        filter_evaluation_count,
    )?;

    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        found => Err(InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal filter expression produced non-boolean value: {found:?}",
        ))),
    }
}

#[cfg(feature = "diagnostics")]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn evaluate_scalar_terminal_expr(
    expr: &ScalarProjectionExpr,
    row: &KernelRow,
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{db::query::plan::expr::BinaryOp, value::Value};

    use super::*;

    fn literal_uint(value: u64) -> ScalarProjectionExpr {
        ScalarProjectionExpr::Literal(Value::Uint(value))
    }

    fn repeated_input_expr() -> ScalarProjectionExpr {
        ScalarProjectionExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(literal_uint(41)),
            right: Box::new(literal_uint(1)),
        }
    }

    fn repeated_filter_expr() -> ScalarProjectionExpr {
        ScalarProjectionExpr::Binary {
            op: BinaryOp::Gte,
            left: Box::new(literal_uint(42)),
            right: Box::new(literal_uint(1)),
        }
    }

    #[test]
    fn scalar_aggregate_terminal_set_interns_duplicate_input_and_filter_exprs() {
        let input = repeated_input_expr();
        let filter = repeated_filter_expr();
        let terminals = PreparedScalarAggregateTerminalSet::new(vec![
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Sum,
                ScalarAggregateInput::Expr(input.clone()),
                Some(filter.clone()),
                false,
            ),
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Avg,
                ScalarAggregateInput::Expr(input),
                Some(filter),
                false,
            ),
        ]);

        assert_eq!(
            terminals.input_exprs.len(),
            1,
            "duplicate SUM/AVG input expressions should share one interned input expression",
        );
        assert_eq!(
            terminals.filter_exprs.len(),
            1,
            "duplicate aggregate FILTER expressions should share one interned filter expression",
        );
        assert!(
            terminals
                .terminals
                .iter()
                .all(|terminal| matches!(terminal.input, InternedScalarAggregateInput::Expr(0))),
            "every expression-backed terminal should point at the shared input expression",
        );
        assert!(
            terminals
                .terminals
                .iter()
                .all(|terminal| terminal.filter == Some(0)),
            "every filtered terminal should point at the shared filter expression",
        );
    }

    #[test]
    fn scalar_aggregate_terminal_set_keeps_field_inputs_out_of_expr_table() {
        let terminals = PreparedScalarAggregateTerminalSet::new(vec![
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::CountValues,
                ScalarAggregateInput::Field {
                    slot: 2,
                    field: "age".to_string(),
                },
                None,
                false,
            ),
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Sum,
                ScalarAggregateInput::Expr(repeated_input_expr()),
                None,
                false,
            ),
        ]);

        assert_eq!(
            terminals.input_exprs.len(),
            1,
            "only expression-backed aggregate inputs should enter the input expression table",
        );
        assert!(
            matches!(
                terminals.terminals[0].input,
                InternedScalarAggregateInput::Field { slot: 2, .. }
            ),
            "field-backed aggregate inputs should remain direct retained-slot reads",
        );
        assert!(
            matches!(
                terminals.terminals[1].input,
                InternedScalarAggregateInput::Expr(0)
            ),
            "the expression-backed aggregate input should point at its interned expression",
        );
    }
}
