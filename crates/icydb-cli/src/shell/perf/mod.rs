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
    pub(crate) total: u64,
    pub(crate) planner: u64,
    pub(crate) store: u64,
    pub(crate) executor: u64,
    pub(crate) pure_covering_decode: u64,
    pub(crate) pure_covering_row_assembly: u64,
    pub(crate) decode: u64,
    pub(crate) compiler: u64,
}

impl ShellPerfAttribution {
    // Sum the current top-level SQL query perf contract exactly as emitted by
    // icydb_admin_sql_query: compiler, planner, store, executor, then public decode.
    pub(crate) const fn attributed_total(&self) -> u64 {
        self.compiler
            .saturating_add(self.planner)
            .saturating_add(self.store)
            .saturating_add(self.executor)
            .saturating_add(self.decode)
    }

    // Preserve one visible fallback bucket for legacy or future payloads whose
    // total exceeds the current top-level query perf contract.
    pub(crate) const fn residual_total(&self) -> u64 {
        self.total.saturating_sub(self.attributed_total())
    }

    pub(crate) const fn pure_covering_executor_residual(&self) -> u64 {
        self.executor
            .saturating_sub(self.pure_covering_decode)
            .saturating_sub(self.pure_covering_row_assembly)
    }
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
    pub(crate) render_micros: u128,
}
