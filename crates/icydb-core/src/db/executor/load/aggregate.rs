use crate::{
    db::{
        Context,
        data::DataKey,
        executor::{
            OrderedKeyStream,
            load::{LoadExecutor, execute::ExecutionInputs},
            plan::{record_plan_metrics, record_rows_scanned},
        },
        query::{
            ReadConsistency,
            plan::{
                AccessPath, AccessPlan, Direction, ExecutablePlan, LogicalPlan,
                validate::validate_executor_plan,
            },
        },
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

// Internal aggregate operation selector for load-query terminals.
#[derive(Clone, Copy)]
enum AggregateKind {
    Count,
    Exists,
    Min,
    Max,
}

// Internal aggregate output carrier. This stays executor-private.
enum AggregateOutput<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
}

#[derive(Clone, Copy)]
enum FoldControl {
    Continue,
    Break,
}

#[derive(Clone, Copy)]
enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}

///
/// AggregateWindowState
///
/// AggregateWindowState
///
/// Tracks effective offset/limit progression for aggregate terminals.
/// Windowing is applied after missing-row consistency handling so
/// aggregate cardinality matches normal load materialization semantics.
///

struct AggregateWindowState {
    offset_remaining: usize,
    limit_remaining: Option<usize>,
}

impl AggregateWindowState {
    fn from_plan(plan: &LogicalPlan<impl Copy>) -> Self {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        Self {
            offset_remaining: offset,
            limit_remaining: limit,
        }
    }

    const fn exhausted(&self) -> bool {
        matches!(self.limit_remaining, Some(0))
    }

    // Advance the window by one existing row and return whether the row
    // is part of the effective output window.
    const fn accept_existing_row(&mut self) -> bool {
        if self.offset_remaining > 0 {
            self.offset_remaining = self.offset_remaining.saturating_sub(1);
            return false;
        }

        if let Some(remaining) = self.limit_remaining.as_mut() {
            if *remaining == 0 {
                return false;
            }

            *remaining = remaining.saturating_sub(1);
        }

        true
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Count)? {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate COUNT result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Exists)? {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate EXISTS result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Min)? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Max)? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX result kind mismatch",
            )),
        }
    }

    // Execute one aggregate terminal. Use streaming fold for conservative-safe
    // plan shapes, otherwise fall back to canonical materialized execution.
    //
    // IMPORTANT:
    // - Streaming eligibility must remain aligned with load fast-path routing.
    // - COUNT pushdown (0.22.1+) must remain a strict subset of streaming safety.
    // - This function must reuse the same key-stream construction path as `execute()`
    //   to preserve ordering, DISTINCT, and pagination semantics.
    fn execute_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
    ) -> Result<AggregateOutput<E>, InternalError> {
        // COUNT pushdown remains a strict subset of streaming eligibility.
        // Route it through key-only fold mode, not a separate streaming engine.
        let count_pushdown_eligible = matches!(kind, AggregateKind::Count)
            && Self::is_count_pushdown_shape_supported(plan.as_inner());

        // If the logical plan requires post-access filtering, sorting,
        // or any non-stream-safe phase, fall back to canonical execution.
        // This preserves exact parity with materialized load semantics.
        if !count_pushdown_eligible
            && !Self::is_streaming_aggregate_shape_supported(plan.as_inner())
        {
            let response = self.execute(plan)?;
            return Ok(Self::aggregate_from_materialized(response, kind));
        }

        // Direction must be captured before consuming the ExecutablePlan.
        // After `into_inner()`, we operate purely on LogicalPlan.
        let direction = plan.direction();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        // EXISTS/MIN/MAX may provide bounded probe hints so eligible fast paths
        // can avoid over-producing keys. Directional hints preserve
        // early-stop symmetry for `min ASC` and `max DESC`.
        let aggregate_probe_fetch_hint =
            Self::aggregate_probe_fetch_hint(plan.as_inner(), kind, direction);
        // COUNT pushdown uses the same streaming fold entry with key-only inclusion.
        // Other terminals use aggregate probe hints.
        let physical_fetch_hint = if count_pushdown_eligible {
            Self::count_pushdown_fetch_hint(plan.as_inner())
        } else {
            aggregate_probe_fetch_hint
        };

        // Move into the underlying logical plan.
        // After this point, `plan` is consumed.
        let logical_plan = plan.into_inner();

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Obtain recovered execution context (read-consistency aware).
        let ctx = self.db.recovered_context::<E>()?;

        // Record plan-level metrics before execution begins.
        // This mirrors the load execution path.
        record_plan_metrics(&logical_plan.access);

        // Fast exit: effective limit == 0 has an empty aggregate window and can
        // return terminal defaults without constructing or scanning key streams.
        if physical_fetch_hint == Some(0) {
            record_rows_scanned::<E>(0);
            return Ok(Self::aggregate_zero_window_result(kind));
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            index_range_anchor: None,
            direction,
        };

        // Fast-path planning must be identical to load execution so aggregate
        // folding sees the exact same ordered key stream.
        let fast_path_plan =
            Self::build_fast_path_plan(&logical_plan, None, None, physical_fetch_hint)?;

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, &fast_path_plan)?;

        // Fold via one streaming engine. COUNT pushdown uses key-only mode;
        // other terminals use row-existence mode.
        let fold_mode = if count_pushdown_eligible {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            &ctx,
            &logical_plan,
            logical_plan.consistency,
            direction,
            resolved.key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Derive bounded probe hints for aggregate terminals where first-kept-row
    // semantics allow early termination under canonical stream order.
    fn aggregate_probe_fetch_hint(
        plan: &LogicalPlan<E::Key>,
        kind: AggregateKind,
        direction: Direction,
    ) -> Option<usize> {
        if !matches!(
            kind,
            AggregateKind::Exists | AggregateKind::Min | AggregateKind::Max
        ) {
            return None;
        }
        if plan.page.as_ref().is_some_and(|page| page.limit == Some(0)) {
            return Some(0);
        }

        // Keep bounded probe hints behind one shared safety gate.
        // DISTINCT + offset must stay unbounded so canonical windowing runs
        // after deduplication and cannot under-produce aggregate results.
        if !Self::bounded_probe_hint_is_safe(plan) {
            return None;
        }
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);

        match kind {
            AggregateKind::Exists => Some(offset.saturating_add(1)),
            AggregateKind::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            AggregateKind::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            _ => None,
        }
    }

    // Return the aggregate terminal value for an empty effective output window.
    const fn aggregate_zero_window_result(kind: AggregateKind) -> AggregateOutput<E> {
        match kind {
            AggregateKind::Count => AggregateOutput::Count(0),
            AggregateKind::Exists => AggregateOutput::Exists(false),
            AggregateKind::Min => AggregateOutput::Min(None),
            AggregateKind::Max => AggregateOutput::Max(None),
        }
    }

    // Conservative streaming gate that allows shapes where post-access phases
    // are limited to missing-row handling plus optional pagination.
    fn is_streaming_aggregate_shape_supported(plan: &LogicalPlan<E::Key>) -> bool {
        plan.is_streaming_access_shape_safe::<E>()
    }

    // Pushdown safety must be narrower than general streaming safety.
    // Any additional COUNT pushdown constraints belong here.
    fn is_count_pushdown_shape_supported(plan: &LogicalPlan<E::Key>) -> bool {
        if !Self::is_streaming_aggregate_shape_supported(plan) {
            return false;
        }

        Self::count_pushdown_access_shape_supported(&plan.access)
    }

    // COUNT pushdown requires key streams backed by rows in the primary data
    // store. Keep this intentionally narrow.
    fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::count_pushdown_path_shape_supported(path),
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => children
                .iter()
                .all(Self::count_pushdown_access_shape_supported),
        }
    }

    // Single-path safety rule for COUNT pushdown.
    const fn count_pushdown_path_shape_supported(path: &AccessPath<E::Key>) -> bool {
        matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
    }

    // Optional bounded fetch hint for COUNT windowing.
    // When limit exists, we only need (offset + limit) keys.
    fn count_pushdown_fetch_hint(plan: &LogicalPlan<E::Key>) -> Option<usize> {
        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if !Self::bounded_probe_hint_is_safe(plan) {
            return None;
        }
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        Some(offset.saturating_add(limit))
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // DISTINCT + offset must remain unbounded so deduplication happens before
    // offset consumption without risking short windows.
    fn bounded_probe_hint_is_safe(plan: &LogicalPlan<E::Key>) -> bool {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        !(plan.distinct && offset > 0)
    }

    fn aggregate_from_materialized(
        response: Response<E>,
        kind: AggregateKind,
    ) -> AggregateOutput<E> {
        match kind {
            AggregateKind::Count => AggregateOutput::Count(response.count()),
            AggregateKind::Exists => AggregateOutput::Exists(!response.is_empty()),
            AggregateKind::Min => {
                AggregateOutput::Min(response.into_iter().map(|(id, _)| id).min())
            }
            AggregateKind::Max => {
                AggregateOutput::Max(response.into_iter().map(|(id, _)| id).max())
            }
        }
    }

    // Single streaming fold entry for all aggregate terminals.
    // Key-only COUNT pushdown and row-aware terminals share this engine.
    fn fold_streaming_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        consistency: ReadConsistency,
        direction: Direction,
        key_stream: &mut dyn OrderedKeyStream,
        kind: AggregateKind,
        mode: AggregateFoldMode,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let window = AggregateWindowState::from_plan(plan);

        match kind {
            AggregateKind::Count => {
                let (count, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    0u32,
                    |count, _key| {
                        *count = count.saturating_add(1);
                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Count(count), keys_scanned))
            }
            AggregateKind::Exists => {
                let (exists, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    false,
                    |exists, _key| {
                        *exists = true;
                        Ok(FoldControl::Break)
                    },
                )?;

                Ok((AggregateOutput::Exists(exists), keys_scanned))
            }
            AggregateKind::Min => {
                let (min_id, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    None::<Id<E>>,
                    |min_id, key| {
                        *min_id = Some(Id::from_key(key.try_key::<E>()?));
                        if direction == Direction::Asc {
                            return Ok(FoldControl::Break);
                        }

                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Min(min_id), keys_scanned))
            }
            AggregateKind::Max => {
                let (max_id, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    None::<Id<E>>,
                    |max_id, key| {
                        *max_id = Some(Id::from_key(key.try_key::<E>()?));
                        if direction == Direction::Desc {
                            return Ok(FoldControl::Break);
                        }

                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Max(max_id), keys_scanned))
            }
        }
    }

    // Generic streaming fold loop used by all aggregate terminal reducers.
    // `mode` controls whether keys require row-existence validation.
    fn fold_streaming<S, F>(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn OrderedKeyStream,
        window: AggregateWindowState,
        mode: AggregateFoldMode,
        mut state: S,
        mut apply: F,
    ) -> Result<(S, usize), InternalError>
    where
        F: FnMut(&mut S, &DataKey) -> Result<FoldControl, InternalError>,
    {
        let mut window = window;
        let mut keys_scanned = 0usize;

        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };

            keys_scanned = keys_scanned.saturating_add(1);
            if !Self::key_qualifies_for_fold(ctx, consistency, mode, &key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }
            if matches!(apply(&mut state, &key)?, FoldControl::Break) {
                break;
            }
        }

        Ok((state, keys_scanned))
    }

    // Determine whether a key is eligible for aggregate folding in the selected mode.
    // Key-only mode is used by COUNT pushdown and intentionally skips row reads.
    fn key_qualifies_for_fold(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        mode: AggregateFoldMode,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        match mode {
            AggregateFoldMode::KeysOnly => Ok(true),
            AggregateFoldMode::ExistingRows => Self::row_exists_for_key(ctx, consistency, key),
        }
    }

    // Keep read-consistency behavior aligned with row materialization paths.
    fn row_exists_for_key(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        match consistency {
            ReadConsistency::Strict => {
                let _ = ctx.read_strict(key)?;

                Ok(true)
            }
            ReadConsistency::MissingOk => match ctx.read(key) {
                Ok(_) => Ok(true),
                Err(err) if err.is_not_found() => Ok(false),
                Err(err) => Err(err),
            },
        }
    }
}
