//! Module: executor::aggregate::scalar_terminals::diagnostics
//! Responsibility: diagnostics-only scalar aggregate terminal attribution.
//! Boundary: keeps counters and instruction measurement out of reducer logic.

use std::cell::Cell;

use crate::db::{
    diagnostics::measure_local_instruction_delta as measure_scalar_aggregate_terminal_phase,
    executor::aggregate::scalar_terminals::terminal::PreparedScalarAggregateTerminalSet,
};

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) enum ScalarAggregateSinkMode {
    #[default]
    None,
    Buffered,
}

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

    pub(super) fn from_terminal_set(terminals: &PreparedScalarAggregateTerminalSet) -> Self {
        Self {
            terminal_count: usize_to_u64(terminals.terminal_count()),
            unique_input_expr_count: usize_to_u64(terminals.input_expr_count()),
            unique_filter_expr_count: usize_to_u64(terminals.filter_expr_count()),
            sink_mode: ScalarAggregateSinkMode::Buffered,
            ..Self::none()
        }
    }

    pub(super) const fn merge_runtime(&mut self, runtime: Self) {
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

pub(super) fn record_scalar_aggregate_terminal_attribution(
    recorded: ScalarAggregateTerminalAttribution,
) {
    SCALAR_AGGREGATE_TERMINAL_ATTRIBUTION.with(|attribution| {
        let mut current = attribution.get();
        current.merge_recorded(recorded);
        attribution.set(current);
    });
}

pub(super) fn measure_phase<T>(run: impl FnOnce() -> T) -> (u64, T) {
    measure_scalar_aggregate_terminal_phase(run)
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
