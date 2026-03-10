use crate::db::executor::{ExecutionPlan, ResolvedScalarContinuationContext};

///
/// UnpagedLoadHintStrategy
///
/// Strategy selected once for unpaged scalar execution hinting so the route-plan
/// mutation phase applies one mechanical outcome.
///

enum UnpagedLoadHintStrategy {
    None,
    TopNSeekWindow { fetch: usize },
    PreserveSecondaryOrder,
}

impl UnpagedLoadHintStrategy {
    const fn resolve(
        resolved_continuation: &ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
        top_n_seek_requires_lookahead: bool,
        route_plan: &ExecutionPlan,
    ) -> Self {
        if !unpaged_rows_mode || resolved_continuation.cursor_boundary().is_some() {
            return Self::None;
        }

        if let Some(top_n_seek_spec) = route_plan.top_n_seek_spec() {
            if !route_plan.shape().is_streaming() || !route_plan.stream_order_contract_safe() {
                return Self::None;
            }

            let fetch = if top_n_seek_spec.fetch() == 0 {
                0
            } else if !top_n_seek_requires_lookahead {
                let Some(fetch) = route_plan.continuation().keep_access_window().fetch_limit()
                else {
                    return Self::None;
                };

                fetch
            } else {
                // Deduplicating lookup shapes need one extra lookahead row to
                // preserve parity after key normalization before windowing.
                top_n_seek_spec.fetch()
            };

            return Self::TopNSeekWindow { fetch };
        }

        if route_plan.secondary_fast_path_eligible()
            && route_plan.scan_hints.physical_fetch_hint.is_none()
        {
            return Self::PreserveSecondaryOrder;
        }

        Self::None
    }

    const fn apply(self, route_plan: &mut ExecutionPlan) {
        match self {
            Self::None => {}
            Self::TopNSeekWindow { fetch } => {
                route_plan.scan_hints.physical_fetch_hint = Some(fetch);
                route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
            }
            Self::PreserveSecondaryOrder => {
                route_plan.scan_hints.physical_fetch_hint = Some(usize::MAX);
            }
        }
    }
}

// Unpaged `execute()` does not need continuation lookahead rows. For
// route-eligible top-N seek windows, constrain both access probe and load
// scan-budget hints to the keep-count window (without continuation +1).
pub(super) const fn apply_unpaged_top_n_seek_hints(
    resolved_continuation: &ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
    top_n_seek_requires_lookahead: bool,
    route_plan: &mut ExecutionPlan,
) {
    let strategy = UnpagedLoadHintStrategy::resolve(
        resolved_continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        route_plan,
    );

    strategy.apply(route_plan);
}
