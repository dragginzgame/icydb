//! Module: executor::aggregate::scalar_terminals
//! Responsibility: scalar-window aggregate terminals over retained-slot rows.
//! Does not own: adapter lowering, grouped DISTINCT policy, or response DTO shaping.
//! Boundary: consumes prepared scalar access/window plans plus uncached terminal metadata.

#[cfg(feature = "diagnostics")]
use std::cell::Cell;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta as measure_scalar_aggregate_terminal_phase;
use crate::{
    db::{
        executor::{
            PreparedExecutionPlan, SharedPreparedExecutionPlan,
            aggregate::terminals::ScalarTerminalBoundaryRequest,
            pipeline::{
                contracts::{CursorEmissionMode, LoadExecutor, ProjectionMaterializationMode},
                entrypoints::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister,
                runtime::compile_retained_slot_layout_for_mode_with_extra_slots,
            },
            projection::{
                GroupedProjectionExpr, GroupedRowView, ProjectionEvalError, ScalarProjectionExpr,
                compile_grouped_projection_expr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
                eval_grouped_projection_expr, evaluate_grouped_having_expr,
            },
            terminal::{KernelRow, RetainedSlotLayout},
        },
        numeric::{
            add_decimal_terms, average_decimal_terms, coerce_numeric_decimal,
            compare_numeric_or_strict_order,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupedAggregateExecutionSpec,
            expr::{Expr, ProjectionField, ProjectionSpec, compile_scalar_projection_expr},
        },
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

///
/// StructuralAggregateResult
///
/// StructuralAggregateResult is the executor-owned transport wrapper for a
/// fully reduced aggregate result. It intentionally exposes only a consumptive
/// row handoff so adapter layers shape DTOs without owning aggregate execution.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateResult(Vec<Vec<Value>>);

impl StructuralAggregateResult {
    /// Construct one structural aggregate result from executor-owned rows.
    #[must_use]
    const fn new(rows: Vec<Vec<Value>>) -> Self {
        Self(rows)
    }

    /// Consume this structural wrapper into value rows for adapter shaping.
    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.0
    }
}

///
/// StructuralAggregateRequest
///
/// StructuralAggregateRequest carries the canonical aggregate execution intent
/// needed after adapter or fluent lowering has finished. The executor compiles
/// and executes these semantic expressions against a prepared scalar plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateRequest {
    terminals: Vec<StructuralAggregateTerminal>,
    projection: ProjectionSpec,
    having: Option<Expr>,
}

impl StructuralAggregateRequest {
    /// Build one structural aggregate request from lowered semantic parts.
    #[must_use]
    pub(in crate::db) const fn new(
        terminals: Vec<StructuralAggregateTerminal>,
        projection: ProjectionSpec,
        having: Option<Expr>,
    ) -> Self {
        Self {
            terminals,
            projection,
            having,
        }
    }
}

///
/// StructuralAggregateTerminal
///
/// StructuralAggregateTerminal describes one scalar aggregate terminal before
/// executor-local projection programs are compiled. It keeps canonical
/// expression and resolved field-slot inputs together for terminal preparation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateTerminal {
    kind: StructuralAggregateTerminalKind,
    target_slot: Option<FieldSlot>,
    input_expr: Option<Expr>,
    filter_expr: Option<Expr>,
    distinct: bool,
}

impl StructuralAggregateTerminal {
    /// Build one structural scalar aggregate terminal request.
    #[must_use]
    pub(in crate::db) const fn new(
        kind: StructuralAggregateTerminalKind,
        target_slot: Option<FieldSlot>,
        input_expr: Option<Expr>,
        filter_expr: Option<Expr>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_slot,
            input_expr,
            filter_expr,
            distinct,
        }
    }

    const fn aggregate_kind(&self) -> AggregateKind {
        match self.kind {
            StructuralAggregateTerminalKind::CountRows
            | StructuralAggregateTerminalKind::CountValues => AggregateKind::Count,
            StructuralAggregateTerminalKind::Sum => AggregateKind::Sum,
            StructuralAggregateTerminalKind::Avg => AggregateKind::Avg,
            StructuralAggregateTerminalKind::Min => AggregateKind::Min,
            StructuralAggregateTerminalKind::Max => AggregateKind::Max,
        }
    }

    fn projected_field(&self) -> Option<&str> {
        self.target_slot.as_ref().map(FieldSlot::field)
    }

    fn uses_shared_count_terminal(&self, model: &EntityModel) -> bool {
        match self.kind {
            StructuralAggregateTerminalKind::CountRows => {
                self.filter_expr.is_none() && !self.distinct
            }
            StructuralAggregateTerminalKind::CountValues => {
                if self.filter_expr.is_some() || self.input_expr.is_some() || self.distinct {
                    return false;
                }
                let Some(target_slot) = self.target_slot.as_ref() else {
                    return false;
                };
                let Some(field) = model.fields().get(target_slot.index()) else {
                    return false;
                };

                !field.nullable()
            }
            StructuralAggregateTerminalKind::Sum
            | StructuralAggregateTerminalKind::Avg
            | StructuralAggregateTerminalKind::Min
            | StructuralAggregateTerminalKind::Max => false,
        }
    }
}

///
/// StructuralAggregateTerminalKind
///
/// StructuralAggregateTerminalKind selects the reducer family requested by one
/// structural global aggregate terminal. Count-row and count-value variants
/// stay separate because they have different input and fast-count eligibility.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum StructuralAggregateTerminalKind {
    CountRows,
    CountValues,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// CompiledStructuralAggregateRequest
///
/// CompiledStructuralAggregateRequest keeps post-reduction projection and
/// HAVING programs beside the aggregate identity specs needed to evaluate them
/// against the implicit single-row aggregate output.
///

struct CompiledStructuralAggregateRequest {
    aggregate_execution_specs: Vec<GroupedAggregateExecutionSpec>,
    projection: Vec<GroupedProjectionExpr>,
    having: Option<GroupedProjectionExpr>,
}

impl CompiledStructuralAggregateRequest {
    fn compile(request: &StructuralAggregateRequest) -> Result<Self, InternalError> {
        let aggregate_execution_specs = request
            .terminals
            .iter()
            .map(|terminal| {
                GroupedAggregateExecutionSpec::from_uncompiled_parts(
                    terminal.aggregate_kind(),
                    terminal.target_slot.clone(),
                    terminal.input_expr.clone().or_else(|| {
                        terminal.projected_field().map(|field| {
                            Expr::Field(crate::db::query::plan::expr::FieldId::new(field))
                        })
                    }),
                    terminal.filter_expr.clone(),
                    terminal.distinct,
                )
            })
            .collect::<Vec<_>>();

        let mut projection = Vec::with_capacity(request.projection.len());
        for field in request.projection.fields() {
            let ProjectionField::Scalar { expr, .. } = field;
            projection.push(
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        InternalError::query_executor_invariant(format!(
                            "structural aggregate output projection must compile against aggregate row: {err}",
                        ))
                    })?,
            );
        }

        let having = request
            .having
            .as_ref()
            .map(|expr| {
                compile_grouped_projection_expr(expr, &[], aggregate_execution_specs.as_slice())
                    .map_err(|err| {
                        InternalError::query_executor_invariant(format!(
                            "structural aggregate HAVING must compile against aggregate row: {err}",
                        ))
                    })
            })
            .transpose()?;

        Ok(Self {
            aggregate_execution_specs,
            projection,
            having,
        })
    }
}

///
/// PreparedScalarAggregateTerminalSet
///
/// PreparedScalarAggregateTerminalSet carries the uncached scalar aggregate
/// terminals that one caller wants to reduce over a prepared scalar access
/// and window plan. It exists so callers can keep aggregate-specific metadata
/// out of `SharedPreparedExecutionPlan` while executor code owns row reduction.
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
/// reopening adapter or planner expression trees.
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
    // so callers cannot choose a reducer/finalizer combination that drifts
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
/// AggregateEmptyBehavior preserves the scalar aggregate finalization
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

fn compile_structural_scalar_aggregate_terminal(
    model: &EntityModel,
    terminal: &StructuralAggregateTerminal,
) -> Result<PreparedScalarAggregateTerminal, InternalError> {
    let kind = match terminal.kind {
        StructuralAggregateTerminalKind::CountRows => ScalarAggregateTerminalKind::CountRows,
        StructuralAggregateTerminalKind::CountValues => ScalarAggregateTerminalKind::CountValues,
        StructuralAggregateTerminalKind::Sum => ScalarAggregateTerminalKind::Sum,
        StructuralAggregateTerminalKind::Avg => ScalarAggregateTerminalKind::Avg,
        StructuralAggregateTerminalKind::Min => ScalarAggregateTerminalKind::Min,
        StructuralAggregateTerminalKind::Max => ScalarAggregateTerminalKind::Max,
    };
    let input = match terminal.kind {
        StructuralAggregateTerminalKind::CountRows => ScalarAggregateInput::Rows,
        StructuralAggregateTerminalKind::CountValues
        | StructuralAggregateTerminalKind::Sum
        | StructuralAggregateTerminalKind::Avg
        | StructuralAggregateTerminalKind::Min
        | StructuralAggregateTerminalKind::Max => {
            if let Some(input_expr) = terminal.input_expr.as_ref() {
                ScalarAggregateInput::Expr(compile_structural_aggregate_expr(
                    model, input_expr, "input",
                )?)
            } else {
                let Some(target_slot) = terminal.target_slot.as_ref() else {
                    return Err(InternalError::query_executor_invariant(
                        "field-target structural aggregate terminal requires a resolved field slot",
                    ));
                };

                ScalarAggregateInput::Field {
                    slot: target_slot.index(),
                    field: target_slot.field().to_string(),
                }
            }
        }
    };
    let filter = terminal
        .filter_expr
        .as_ref()
        .map(|expr| compile_structural_aggregate_expr(model, expr, "filter"))
        .transpose()?;

    Ok(PreparedScalarAggregateTerminal::from_validated_parts(
        kind,
        input,
        filter,
        terminal.distinct,
    ))
}

fn compile_structural_aggregate_expr(
    model: &EntityModel,
    expr: &Expr,
    label: &str,
) -> Result<ScalarProjectionExpr, InternalError> {
    if let Some(field) = first_unknown_structural_aggregate_expr_field(model, expr) {
        return Err(InternalError::query_executor_invariant(format!(
            "unknown expression field '{field}'",
        )));
    }

    compile_scalar_projection_expr(model, expr).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "structural aggregate {label} expression must compile on the scalar seam",
        ))
    })
}

fn first_unknown_structural_aggregate_expr_field(
    model: &EntityModel,
    expr: &Expr,
) -> Option<String> {
    let mut first_unknown = None;
    let _ = expr.try_for_each_tree_expr(&mut |node| {
        if first_unknown.is_some() {
            return Ok(());
        }
        if let Expr::Field(field) = node
            && model.resolve_field_slot(field.as_str()).is_none()
        {
            first_unknown = Some(field.as_str().to_string());
        }

        Ok::<(), ()>(())
    });

    first_unknown
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one structural global aggregate request over a shared prepared scalar plan.
    pub(in crate::db) fn execute_structural_aggregate_result(
        &self,
        shared_plan: &SharedPreparedExecutionPlan,
        request: StructuralAggregateRequest,
    ) -> Result<StructuralAggregateResult, InternalError> {
        let compiled = CompiledStructuralAggregateRequest::compile(&request)?;
        let mut unique_values = vec![None; request.terminals.len()];
        let mut scalar_aggregate_terminals = Vec::new();
        let mut scalar_aggregate_terminal_positions = Vec::new();

        // Phase 1: route count-equivalent terminals through the shared scalar
        // count boundary and stage all remaining terminals for the aggregate
        // reducer sink. Both paths stay under executor ownership.
        for (terminal_index, terminal) in request.terminals.iter().enumerate() {
            if terminal.uses_shared_count_terminal(E::MODEL) {
                let count = self
                    .execute_scalar_terminal_request(
                        shared_plan.typed_clone::<E>(),
                        ScalarTerminalBoundaryRequest::Count,
                    )?
                    .into_count()?;
                unique_values[terminal_index] = Some(Value::Uint(u64::from(count)));
            } else {
                scalar_aggregate_terminals.push(compile_structural_scalar_aggregate_terminal(
                    E::MODEL,
                    terminal,
                )?);
                scalar_aggregate_terminal_positions.push(terminal_index);
            }
        }

        // Phase 2: reduce every non-count-equivalent terminal through the
        // scalar aggregate terminal sink so row decoding, filter evaluation,
        // expression evaluation, DISTINCT, and reducer finalization remain
        // executor-owned.
        if !scalar_aggregate_terminals.is_empty() {
            let terminal_values = self.execute_scalar_aggregate_terminals(
                shared_plan.typed_clone::<E>(),
                PreparedScalarAggregateTerminalSet::new(scalar_aggregate_terminals),
            )?;
            if terminal_values.len() != scalar_aggregate_terminal_positions.len() {
                return Err(InternalError::query_executor_invariant(
                    "structural aggregate terminal output count must match staged terminals",
                ));
            }

            for (terminal_index, value) in scalar_aggregate_terminal_positions
                .into_iter()
                .zip(terminal_values)
            {
                unique_values[terminal_index] = Some(value);
            }
        }
        let unique_values = unique_values
            .into_iter()
            .map(|value| {
                value.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "structural aggregate terminal did not produce a reduced value",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Phase 3: evaluate global aggregate HAVING and final projection
        // against the implicit single aggregate row. Adapter layers only see
        // the completed structural row payload.
        let grouped_row = GroupedRowView::new(
            &[],
            unique_values.as_slice(),
            &[],
            compiled.aggregate_execution_specs.as_slice(),
        );
        if let Some(expr) = compiled.having.as_ref()
            && !evaluate_grouped_having_expr(expr, &grouped_row).map_err(|err| {
                InternalError::query_executor_invariant(format!(
                    "structural aggregate HAVING evaluation failed: {err}",
                ))
            })?
        {
            return Ok(StructuralAggregateResult::new(Vec::new()));
        }

        let mut row = Vec::with_capacity(compiled.projection.len());
        for expr in &compiled.projection {
            row.push(
                eval_grouped_projection_expr(expr, &grouped_row).map_err(|err| {
                    InternalError::query_executor_invariant(format!(
                        "structural aggregate projection evaluation failed: {err}",
                    ))
                })?,
            );
        }

        Ok(StructuralAggregateResult::new(vec![row]))
    }

    /// Execute scalar aggregate terminals over one prepared scalar access/window plan.
    fn execute_scalar_aggregate_terminals(
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
        // response page for adapter-owned aggregate code to consume.
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
