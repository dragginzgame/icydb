//! Module: executor::aggregate::scalar_terminals::reducer
//! Responsibility: scalar aggregate reducer state and row ingestion runtime.
//! Boundary: owns row-loop execution over pre-classified reducer paths.

use crate::{
    db::executor::{
        aggregate::{
            reducer_core::ValueReducerState,
            scalar_terminals::{
                expr_cache::ScalarTerminalExprCache,
                terminal::{
                    InternedPreparedScalarAggregateTerminal, InternedScalarAggregateInput,
                    PreparedScalarAggregateTerminalSet, ScalarAggregateTerminalKind,
                },
            },
        },
        projection::ProjectionEvalError,
        terminal::KernelRow,
    },
    error::InternalError,
    value::Value,
};

#[cfg(feature = "diagnostics")]
use crate::db::executor::aggregate::scalar_terminals::diagnostics::ScalarAggregateTerminalAttribution;

///
/// ScalarAggregateReducerState
///
/// ScalarAggregateReducerState stores the in-progress fold for one prepared
/// scalar aggregate terminal. It keeps DISTINCT admission adjacent to reducer
/// state so callers never materialize one `Vec<Value>` per aggregate.
///

struct ScalarAggregateReducerState {
    output_index: usize,
    kind: ScalarAggregateTerminalKind,
    distinct: bool,
    distinct_values: Vec<Value>,
    reducer: ValueReducerState,
}

impl ScalarAggregateReducerState {
    const fn new(output_index: usize, terminal: &InternedPreparedScalarAggregateTerminal) -> Self {
        Self {
            output_index,
            kind: terminal.kind,
            distinct: terminal.distinct,
            distinct_values: Vec::new(),
            reducer: reducer_for_terminal_kind(terminal.kind),
        }
    }

    fn ingest_row(&mut self) -> Result<(), InternalError> {
        if self.distinct {
            return Err(InternalError::query_executor_invariant(
                "COUNT(*) scalar aggregate terminal cannot be DISTINCT",
            ));
        }

        self.reducer.increment_count()?;

        Ok(())
    }

    // Ingest one borrowed field or expression value when the source row/cache
    // already owns the payload. Non-DISTINCT reducers inspect the value without
    // cloning; extrema clone only if the value becomes the selected candidate.
    fn ingest_borrowed_value(&mut self, value: &Value) -> Result<(), InternalError> {
        if self.distinct {
            return self.ingest_distinct_borrowed_value(value);
        }
        if matches!(value, Value::Null) {
            return Ok(());
        }

        match self.kind {
            ScalarAggregateTerminalKind::CountValues
            | ScalarAggregateTerminalKind::Sum
            | ScalarAggregateTerminalKind::Avg
            | ScalarAggregateTerminalKind::Min
            | ScalarAggregateTerminalKind::Max => self.reducer.ingest(value),
            ScalarAggregateTerminalKind::CountRows => Err(InternalError::query_executor_invariant(
                "COUNT(*) scalar aggregate terminal cannot consume projected values",
            )),
        }
    }

    // Admit one borrowed DISTINCT value. Accepted values are cloned only at the
    // ownership boundary where the retained DISTINCT admission set must store
    // them beyond the source row/cache lifetime.
    fn ingest_distinct_borrowed_value(&mut self, value: &Value) -> Result<(), InternalError> {
        if self.distinct_values.iter().any(|current| current == value) {
            return Ok(());
        }
        if matches!(value, Value::Null) {
            self.distinct_values.push(Value::Null);
            return Ok(());
        }

        match self.kind {
            ScalarAggregateTerminalKind::CountValues
            | ScalarAggregateTerminalKind::Sum
            | ScalarAggregateTerminalKind::Avg
            | ScalarAggregateTerminalKind::Min
            | ScalarAggregateTerminalKind::Max => {
                self.reducer.ingest(value)?;
                self.distinct_values.push(value.clone());

                Ok(())
            }
            ScalarAggregateTerminalKind::CountRows => Err(InternalError::query_executor_invariant(
                "COUNT(*) scalar aggregate terminal cannot consume projected values",
            )),
        }
    }

    fn finalize(self) -> Result<(usize, Value), InternalError> {
        Ok((self.output_index, self.reducer.into_final_value()?))
    }
}

// Map one prepared terminal kind to the shared semantic reducer. Input routing
// remains in this module; only value reducer payload semantics move to core.
const fn reducer_for_terminal_kind(kind: ScalarAggregateTerminalKind) -> ValueReducerState {
    match kind {
        ScalarAggregateTerminalKind::CountRows | ScalarAggregateTerminalKind::CountValues => {
            ValueReducerState::count()
        }
        ScalarAggregateTerminalKind::Sum => ValueReducerState::sum(),
        ScalarAggregateTerminalKind::Avg => ValueReducerState::avg(),
        ScalarAggregateTerminalKind::Min => ValueReducerState::min(),
        ScalarAggregateTerminalKind::Max => ValueReducerState::max(),
    }
}

///
/// RowAggregateReducer
///
/// RowAggregateReducer stores a pre-classified COUNT(*) reducer.
/// Runtime construction separates these reducers from field and expression
/// reducers so the per-row loop never matches on aggregate input kind.
///

struct RowAggregateReducer {
    filter: Option<usize>,
    state: ScalarAggregateReducerState,
}

///
/// FieldAggregateReducer
///
/// FieldAggregateReducer stores a pre-classified retained-slot reducer.
/// The slot and field label are copied out of the interned terminal once so
/// per-row execution performs only filter evaluation and direct slot loading.
///

struct FieldAggregateReducer {
    filter: Option<usize>,
    state: ScalarAggregateReducerState,
    slot: usize,
    field: String,
}

///
/// ExprAggregateReducer
///
/// ExprAggregateReducer stores a pre-classified expression-backed reducer.
/// The expression index points into `ScalarTerminalExprCache`, preserving
/// shared per-row expression evaluation without input-kind branching.
///

struct ExprAggregateReducer {
    filter: Option<usize>,
    state: ScalarAggregateReducerState,
    expr_index: usize,
}

///
/// ScalarAggregateReducerRuntime
///
/// ScalarAggregateReducerRuntime owns one scalar aggregate sink invocation.
/// It keeps reducer states in row, field, and expression lists so terminal input
/// strategy is resolved once before source rows enter the hot reducer loop.
///

pub(super) struct ScalarAggregateReducerRuntime {
    row_reducers: Vec<RowAggregateReducer>,
    field_reducers: Vec<FieldAggregateReducer>,
    expr_reducers: Vec<ExprAggregateReducer>,
    terminal_count: usize,
    expr_cache: ScalarTerminalExprCache,
    #[cfg(feature = "diagnostics")]
    attribution: ScalarAggregateTerminalAttribution,
}

impl ScalarAggregateReducerRuntime {
    // Build a reducer sink from one prepared terminal set, preserving the
    // expression-interning tables created during terminal preparation.
    pub(super) fn new(terminals: PreparedScalarAggregateTerminalSet) -> Self {
        let (terminals, input_exprs, filter_exprs) = terminals.into_runtime_parts();
        let terminal_count = terminals.len();
        // Count reducer buckets before consuming the terminal vector so each
        // hot-loop list reserves only its own input class, not the full
        // terminal set size three times.
        let mut row_reducer_capacity = 0;
        let mut field_reducer_capacity = 0;
        let mut expr_reducer_capacity = 0;
        for terminal in &terminals {
            match &terminal.input {
                InternedScalarAggregateInput::Rows => {
                    row_reducer_capacity += 1;
                }
                InternedScalarAggregateInput::Field { .. } => {
                    field_reducer_capacity += 1;
                }
                InternedScalarAggregateInput::Expr(_) => {
                    expr_reducer_capacity += 1;
                }
            }
        }
        let mut row_reducers = Vec::with_capacity(row_reducer_capacity);
        let mut field_reducers = Vec::with_capacity(field_reducer_capacity);
        let mut expr_reducers = Vec::with_capacity(expr_reducer_capacity);

        // Classify terminal input strategy once, before row ingestion. The row
        // loop then runs three concrete reducer lists instead of matching on
        // input kind for every reducer on every row.
        for (output_index, terminal) in terminals.into_iter().enumerate() {
            let state = ScalarAggregateReducerState::new(output_index, &terminal);
            let filter = terminal.filter;
            match terminal.input {
                InternedScalarAggregateInput::Rows => {
                    row_reducers.push(RowAggregateReducer { filter, state });
                }
                InternedScalarAggregateInput::Field { slot, field } => {
                    field_reducers.push(FieldAggregateReducer {
                        filter,
                        state,
                        slot,
                        field,
                    });
                }
                InternedScalarAggregateInput::Expr(expr_index) => {
                    expr_reducers.push(ExprAggregateReducer {
                        filter,
                        state,
                        expr_index,
                    });
                }
            }
        }

        Self {
            row_reducers,
            field_reducers,
            expr_reducers,
            terminal_count,
            expr_cache: ScalarTerminalExprCache::new(input_exprs, filter_exprs),
            #[cfg(feature = "diagnostics")]
            attribution: ScalarAggregateTerminalAttribution::none(),
        }
    }

    // Ingest one scalar-window row into every aggregate reducer. Filters are
    // evaluated before input expressions so filtered-out rows still avoid input
    // work, while expression tables keep shared expressions to once per row.
    pub(super) fn ingest_row(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        #[cfg(feature = "diagnostics")]
        {
            self.attribution.rows_ingested = self.attribution.rows_ingested.saturating_add(1);
            let (local_instructions, result) =
                crate::db::executor::aggregate::scalar_terminals::diagnostics::measure_phase(
                    || self.ingest_row_inner(row),
                );
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
        self.expr_cache.reset_for_row();
        self.ingest_row_reducers(row)?;
        self.ingest_field_reducers(row)?;
        self.ingest_expr_reducers(row)?;

        Ok(())
    }

    fn ingest_row_reducers(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        for reducer in &mut self.row_reducers {
            if !self.expr_cache.filter_matches(
                reducer.filter,
                row,
                #[cfg(feature = "diagnostics")]
                &mut self.attribution.filter_evaluations,
            )? {
                continue;
            }
            reducer.state.ingest_row()?;
        }

        Ok(())
    }

    fn ingest_field_reducers(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        for reducer in &mut self.field_reducers {
            if !self.expr_cache.filter_matches(
                reducer.filter,
                row,
                #[cfg(feature = "diagnostics")]
                &mut self.attribution.filter_evaluations,
            )? {
                continue;
            }
            let value = row.slot_ref(reducer.slot).ok_or_else(|| {
                ProjectionEvalError::MissingFieldValue {
                    field: reducer.field.clone(),
                    index: reducer.slot,
                }
                .into_invalid_logical_plan_internal_error()
            })?;
            reducer.state.ingest_borrowed_value(value)?;
        }

        Ok(())
    }

    fn ingest_expr_reducers(&mut self, row: &KernelRow) -> Result<(), InternalError> {
        for reducer in &mut self.expr_reducers {
            if !self.expr_cache.filter_matches(
                reducer.filter,
                row,
                #[cfg(feature = "diagnostics")]
                &mut self.attribution.filter_evaluations,
            )? {
                continue;
            }
            let value = self.expr_cache.input_value(
                row,
                reducer.expr_index,
                #[cfg(feature = "diagnostics")]
                &mut self.attribution.expression_evaluations,
            )?;
            reducer.state.ingest_borrowed_value(value)?;
        }

        Ok(())
    }

    // Finalize reducer states in original terminal order.
    pub(super) fn finalize(self) -> Result<Vec<Value>, InternalError> {
        let mut values = vec![None; self.terminal_count];
        let finalized = self
            .row_reducers
            .into_iter()
            .map(|reducer| reducer.state.finalize())
            .chain(
                self.field_reducers
                    .into_iter()
                    .map(|reducer| reducer.state.finalize()),
            )
            .chain(
                self.expr_reducers
                    .into_iter()
                    .map(|reducer| reducer.state.finalize()),
            );
        for finalized in finalized {
            let (index, value) = finalized?;
            values[index] = Some(value);
        }

        let mut ordered_values = Vec::with_capacity(values.len());
        for value in values {
            let value = value.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "scalar aggregate reducer did not finalize every terminal",
                )
            })?;
            ordered_values.push(value);
        }

        Ok(ordered_values)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn attribution(&self) -> ScalarAggregateTerminalAttribution {
        self.attribution
    }
}
