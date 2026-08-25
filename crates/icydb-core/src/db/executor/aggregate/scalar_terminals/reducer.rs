//! Module: executor::aggregate::scalar_terminals::reducer
//! Responsibility: scalar aggregate reducer state and row ingestion runtime.
//! Boundary: owns row-loop execution over pre-classified reducer paths.

#[cfg(feature = "diagnostics")]
use crate::db::executor::aggregate::terminal_attribution::{
    ScalarAggregateTerminalAttribution, measure_phase,
};
use crate::{
    db::executor::{
        aggregate::{
            scalar_terminals::{
                expr_cache::ScalarTerminalExprCache,
                terminal::{
                    InternedPreparedScalarAggregateTerminal, InternedScalarAggregateInput,
                    PreparedScalarAggregateTerminalSet, ScalarAggregateTerminalKind,
                },
            },
            value_reducer::ValueReducerState,
        },
        budget::{charge_current_execution_budget, runtime_value_work},
        group::{
            StableHash, StableHashBuildHasher, StableHashMap, retained_hash_entry_backing_bytes,
            retained_vec_element_backing_bytes, stable_hash_value, try_reserve_hash_entry,
            try_reserve_vec_elements,
        },
        projection::ProjectionEvalError,
        terminal::KernelRow,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

///
/// ScalarDistinctValueBucket
///
/// ScalarDistinctValueBucket keeps the overwhelmingly common one-value hash
/// bucket inline. A heap allocation is introduced only for a genuine stable
/// hash collision, while equality remains the existing exact `Value`
/// equality contract.
///

enum ScalarDistinctValueBucket {
    Single(Value),
    Colliding(Vec<Value>),
}

/// Return the conservative retained-state and nested-value work for one fixed
/// scalar DISTINCT value. The collision transition is deliberately included:
/// exact metadata routes cannot know whether two canonical values share a
/// stable hash, so their admission proof must cover either bucket shape.
pub(in crate::db::executor::aggregate) fn scalar_distinct_conservative_unit_work(
    value: &Value,
) -> (u64, u64) {
    let (value_bytes, nested_steps) = runtime_value_work(value);
    let hash_entry_bytes =
        retained_hash_entry_backing_bytes::<StableHash, ScalarDistinctValueBucket>();
    let collision_transition_bytes =
        retained_vec_element_backing_bytes::<Value>().saturating_mul(2);
    let structural_bytes = hash_entry_bytes.max(collision_transition_bytes);

    (value_bytes.saturating_add(structural_bytes), nested_steps)
}

impl ScalarDistinctValueBucket {
    fn contains(&self, value: &Value) -> bool {
        match self {
            Self::Single(current) => current == value,
            Self::Colliding(values) => values.iter().any(|current| current == value),
        }
    }

    fn retained_backing_reservation_bytes(&self) -> u64 {
        let retained_elements = match self {
            Self::Single(_) => 2,
            Self::Colliding(_) => 1,
        };
        retained_vec_element_backing_bytes::<Value>().saturating_mul(retained_elements)
    }

    fn insert(&mut self, value: Value) -> Result<(), InternalError> {
        match self {
            Self::Single(current) => {
                let mut values = Vec::new();
                try_reserve_vec_elements(&mut values, 2)?;
                values.push(std::mem::replace(current, Value::Null));
                values.push(value);
                *self = Self::Colliding(values);
            }
            Self::Colliding(values) => {
                try_reserve_vec_elements(values, 1)?;
                values.push(value);
            }
        }

        Ok(())
    }
}

///
/// ScalarDistinctValueSet
///
/// ScalarDistinctValueSet gives scalar aggregate DISTINCT admission bounded
/// hash-bucket lookup instead of rescanning every previously retained value
/// for every input row. Values remain owned only after admission.
///

struct ScalarDistinctValueSet {
    buckets: StableHashMap<ScalarDistinctValueBucket>,
}

impl ScalarDistinctValueSet {
    const fn new() -> Self {
        Self {
            buckets: StableHashMap::with_hasher(StableHashBuildHasher),
        }
    }

    fn contains(&self, hash: StableHash, value: &Value) -> bool {
        self.buckets
            .get(&hash)
            .is_some_and(|bucket| bucket.contains(value))
    }

    fn insert(&mut self, hash: StableHash, value: Value) -> Result<(), InternalError> {
        if let Some(bucket) = self.buckets.get_mut(&hash) {
            let structural_bytes = bucket.retained_backing_reservation_bytes();
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
                structural_bytes,
            )?;
            return bucket.insert(value);
        }

        let structural_bytes =
            retained_hash_entry_backing_bytes::<StableHash, ScalarDistinctValueBucket>();
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            structural_bytes,
        )?;
        try_reserve_hash_entry(&mut self.buckets)?;
        self.buckets
            .insert(hash, ScalarDistinctValueBucket::Single(value));

        Ok(())
    }
}

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
    distinct_values: Option<ScalarDistinctValueSet>,
    reducer: ValueReducerState,
}

impl ScalarAggregateReducerState {
    fn new(output_index: usize, terminal: &InternedPreparedScalarAggregateTerminal) -> Self {
        Self {
            output_index,
            kind: terminal.kind,
            distinct_values: terminal.distinct.then(ScalarDistinctValueSet::new),
            reducer: reducer_for_terminal_kind(terminal.kind),
        }
    }

    fn ingest_row(&mut self) -> Result<(), InternalError> {
        if self.distinct_values.is_some() {
            return Err(InternalError::query_executor_invariant());
        }

        self.reducer.increment_count()?;

        Ok(())
    }

    // Ingest one borrowed field or expression value when the source row/cache
    // already owns the payload. Non-DISTINCT reducers inspect the value without
    // cloning; extrema clone only if the value becomes the selected candidate.
    fn ingest_borrowed_value(&mut self, value: &Value) -> Result<(), InternalError> {
        if self.distinct_values.is_some() {
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
            ScalarAggregateTerminalKind::CountRows => {
                Err(InternalError::query_executor_invariant())
            }
        }
    }

    // Admit one borrowed DISTINCT value. Accepted values are cloned only at the
    // ownership boundary where the retained DISTINCT admission set must store
    // them beyond the source row/cache lifetime.
    fn ingest_distinct_borrowed_value(&mut self, value: &Value) -> Result<(), InternalError> {
        let value_hash = stable_hash_value(value)?;
        let distinct_values = self
            .distinct_values
            .as_mut()
            .ok_or_else(InternalError::query_executor_invariant)?;
        if distinct_values.contains(value_hash, value) {
            return Ok(());
        }
        let value_work = runtime_value_work(value);
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::GroupDistinctEntries,
            1,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            value_work.0,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            value_work.1,
        )?;
        if matches!(value, Value::Null) {
            distinct_values.insert(value_hash, Value::Null)?;
            return Ok(());
        }

        match self.kind {
            ScalarAggregateTerminalKind::CountValues
            | ScalarAggregateTerminalKind::Sum
            | ScalarAggregateTerminalKind::Avg
            | ScalarAggregateTerminalKind::Min
            | ScalarAggregateTerminalKind::Max => {
                self.reducer.ingest(value)?;
                distinct_values.insert(value_hash, value.clone())?;

                Ok(())
            }
            ScalarAggregateTerminalKind::CountRows => {
                Err(InternalError::query_executor_invariant())
            }
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
/// The slot is copied out of the interned terminal once so per-row execution
/// performs only filter evaluation and direct slot loading.
///

struct FieldAggregateReducer {
    filter: Option<usize>,
    state: ScalarAggregateReducerState,
    slot: usize,
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
        let (terminals, input_exprs, filter_exprs) = terminals.into_runtime_inputs();
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
                InternedScalarAggregateInput::Field { slot, field: _ } => {
                    field_reducers.push(FieldAggregateReducer {
                        filter,
                        state,
                        slot,
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
            let (local_instructions, result) = measure_phase(|| self.ingest_row_inner(row));
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
                ProjectionEvalError::missing_slot_value(reducer.slot)
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
            let value = value.ok_or_else(InternalError::query_executor_invariant)?;
            ordered_values.push(value);
        }

        Ok(ordered_values)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn attribution(&self) -> ScalarAggregateTerminalAttribution {
        self.attribution
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ScalarDistinctValueBucket, ScalarDistinctValueSet, scalar_distinct_conservative_unit_work,
    };
    use crate::{
        db::{
            QueryError,
            executor::{
                budget::{
                    HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
                    charge_current_execution_budget, runtime_value_work,
                    with_query_execution_budget_for_tests,
                },
                group::stable_hash_value,
            },
        },
        value::Value,
    };
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
        DiagnosticExecutionLane, RuntimeBoundaryCode,
    };

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x2220_0008_0000_0001,
    );

    #[test]
    fn scalar_distinct_value_set_uses_hash_buckets_without_changing_value_equality() {
        let mut values = ScalarDistinctValueSet::new();
        let first = Value::Text("first".to_string());
        let second = Value::Text("second".to_string());
        let first_hash = stable_hash_value(&first).expect("first hash");
        let second_hash = stable_hash_value(&second).expect("second hash");

        assert!(!values.contains(first_hash, &first));
        values
            .insert(first_hash, first.clone())
            .expect("first retained value");
        assert!(values.contains(first_hash, &first));
        assert!(!values.contains(second_hash, &second));
        values
            .insert(second_hash, second.clone())
            .expect("second retained value");
        assert!(values.contains(second_hash, &second));
    }

    #[test]
    fn scalar_distinct_collision_bucket_compares_exact_values() {
        let mut bucket = ScalarDistinctValueBucket::Single(Value::Nat64(1));
        bucket
            .insert(Value::Nat64(2))
            .expect("collision bucket promotion");

        assert!(bucket.contains(&Value::Nat64(1)));
        assert!(bucket.contains(&Value::Nat64(2)));
        assert!(!bucket.contains(&Value::Nat64(3)));
    }

    #[test]
    fn exact_distinct_budget_covers_hash_entries_and_collision_promotion() {
        let value = Value::Int64(0);
        let (state_bytes, nested_steps) = scalar_distinct_conservative_unit_work(&value);
        let (value_bytes, expected_nested_steps) = runtime_value_work(&value);
        let hash_entry_bytes = crate::db::executor::group::retained_hash_entry_backing_bytes::<
            crate::db::executor::group::StableHash,
            ScalarDistinctValueBucket,
        >();
        let collision_transition_bytes =
            crate::db::executor::group::retained_vec_element_backing_bytes::<Value>()
                .saturating_mul(2);

        assert_eq!(
            std::mem::size_of::<ScalarDistinctValueBucket>(),
            std::mem::size_of::<Value>(),
            "the inline scalar DISTINCT bucket must retain the canonical value layout",
        );
        assert_eq!(nested_steps, expected_nested_steps);
        assert!(state_bytes >= value_bytes.saturating_add(hash_entry_bytes));
        assert!(state_bytes >= value_bytes.saturating_add(collision_transition_bytes));
    }

    #[test]
    fn scalar_distinct_hash_capacity_is_part_of_the_typed_state_budget() {
        let value = Value::Text("retained".to_string());
        let hash = stable_hash_value(&value).expect("stable hash");
        let value_bytes = runtime_value_work(&value).0;
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
                value_bytes,
            );
        let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
            let mut values = ScalarDistinctValueSet::new();
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
                value_bytes,
            )
            .map_err(QueryError::execute)?;
            values.insert(hash, value).map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("value bytes alone must not admit hidden scalar hash capacity")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }
}
