//! Module: shell perf rendering.
//! Responsibility: parse and render SQL shell performance attribution.
//! Does not own: SQL execution, Candid endpoint payload shape, or table output.
//! Boundary: exposes shell-local attribution values and compact footer renderers.

mod format;
#[cfg(test)]
mod parse;

pub(crate) use format::{
    render_executor_residual_suffix, render_perf_suffix, render_pure_covering_suffix,
    render_shell_render_suffix,
};
#[cfg(test)]
pub(crate) use parse::{normalize_grouped_next_cursor_json, parse_perf_result};

///
/// ShellPerfAttribution
///
/// ShellPerfAttribution carries the hard-cut dev-shell perf footer payload.
/// The shell keeps this formatting-only shape local so the canister payload can
/// evolve independently from the rendered footer string.
///

pub(crate) struct ShellPerfAttribution {
    total: u64,
    planner: u64,
    store: u64,
    executor: u64,
    pure_covering_decode: u64,
    pure_covering_row_assembly: u64,
    decode: u64,
    compiler: u64,
}

impl ShellPerfAttribution {
    pub(crate) const fn new(input: ShellPerfAttributionInput) -> Self {
        Self {
            total: input.total,
            planner: input.planner,
            store: input.store,
            executor: input.executor,
            pure_covering_decode: input.pure_covering_decode,
            pure_covering_row_assembly: input.pure_covering_row_assembly,
            decode: input.decode,
            compiler: input.compiler,
        }
    }

    // Sum the current top-level SQL query perf contract exactly as emitted by
    // __icydb_query: compiler, planner, store, executor, then public decode.
    const fn attributed_total(&self) -> u64 {
        self.compiler
            .saturating_add(self.planner)
            .saturating_add(self.store)
            .saturating_add(self.executor)
            .saturating_add(self.decode)
    }

    // Preserve one visible fallback bucket for payloads whose
    // total exceeds the current top-level query perf contract.
    const fn residual_total(&self) -> u64 {
        self.total.saturating_sub(self.attributed_total())
    }

    const fn pure_covering_executor_residual(&self) -> u64 {
        self.executor
            .saturating_sub(self.pure_covering_decode)
            .saturating_sub(self.pure_covering_row_assembly)
    }
}

pub(crate) struct ShellPerfAttributionInput {
    pub(crate) total: u64,
    pub(crate) planner: u64,
    pub(crate) store: u64,
    pub(crate) executor: u64,
    pub(crate) pure_covering_decode: u64,
    pub(crate) pure_covering_row_assembly: u64,
    pub(crate) decode: u64,
    pub(crate) compiler: u64,
}

///
/// ShellLocalRenderAttribution
///
/// ShellLocalRenderAttribution records the CLI-only render time spent turning
/// decoded SQL result payloads into the local table/footer text shown in the
/// shell. This stays separate from the canister-side `c/p/s/e/d` instruction
/// contract because it measures native shell formatting work rather than query
/// engine execution.
///

pub(crate) struct ShellLocalRenderAttribution {
    render_micros: u128,
}

impl ShellLocalRenderAttribution {
    pub(crate) const fn new(render_micros: u128) -> Self {
        Self { render_micros }
    }
}
