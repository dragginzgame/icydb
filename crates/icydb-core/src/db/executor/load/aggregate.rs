use crate::{
    db::{
        Context,
        data::DataKey,
        executor::{
            DistinctOrderedKeyStream, OrderedKeyStream,
            load::{LoadExecutor, execute::ExecutionInputs},
            plan::{record_plan_metrics, record_rows_scanned},
        },
        query::{
            ReadConsistency,
            plan::{
                AccessPath, AccessPlan, Direction, ExecutablePlan, IndexPrefixSpec, IndexRangeSpec,
                LogicalPlan,
                validate::{
                    assess_secondary_order_pushdown_if_applicable_validated, validate_executor_plan,
                },
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

// Guard secondary aggregate fast-path assumptions so index-prefix
// spec consumption cannot silently drift if planner shapes evolve.
fn ensure_secondary_aggregate_fast_path_arity(
    secondary_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
) -> Result<(), InternalError> {
    if secondary_pushdown_eligible && index_prefix_spec_count > 1 {
        return Err(InternalError::query_executor_invariant(
            "secondary aggregate fast-path expects at most one index-prefix spec",
        ));
    }

    Ok(())
}

// Guard index-range aggregate fast-path assumptions so planner/executor
// spec boundaries remain explicit and drift-resistant.
fn ensure_index_range_aggregate_fast_path_specs(
    index_range_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    if !index_range_pushdown_eligible {
        return Ok(());
    }
    if index_prefix_spec_count > 0 {
        return Err(InternalError::query_executor_invariant(
            "index-range aggregate fast-path must not consume index-prefix specs",
        ));
    }
    if index_range_spec_count != 1 {
        return Err(InternalError::query_executor_invariant(
            "index-range aggregate fast-path expects exactly one index-range spec",
        ));
    }

    Ok(())
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

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue> {
    ctx: &'exec Context<'ctx, E>,
    logical_plan: &'exec LogicalPlan<E::Key>,
    index_prefix_specs: &'exec [IndexPrefixSpec],
    index_range_specs: &'exec [IndexRangeSpec],
    direction: Direction,
    physical_fetch_hint: Option<usize>,
    kind: AggregateKind,
    fold_mode: AggregateFoldMode,
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
        let fold_mode = if count_pushdown_eligible {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };

        // If the logical plan requires post-access filtering, sorting,
        // or any non-stream-safe phase, fall back to canonical execution.
        // Secondary index-prefix pushdown remains an explicit exception.
        // This preserves exact parity with materialized load semantics.
        let secondary_pushdown_eligible =
            assess_secondary_order_pushdown_if_applicable_validated(E::MODEL, plan.as_inner())
                .is_eligible();
        let index_range_pushdown_eligible =
            Self::is_index_range_limit_pushdown_shape_eligible(plan.as_inner());
        if !count_pushdown_eligible
            && !Self::is_streaming_aggregate_shape_supported(plan.as_inner())
            && !secondary_pushdown_eligible
            && !index_range_pushdown_eligible
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

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &ctx,
            logical_plan: &logical_plan,
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            direction,
            physical_fetch_hint,
            kind,
            fold_mode,
        };
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_fast_path_aggregate(&fast_path_inputs)?
        {
            record_rows_scanned::<E>(rows_scanned);
            return Ok(aggregate_output);
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

    // Attempt aggregate fast-path execution in canonical priority order.
    // Returns `Some` when one branch fully resolves the terminal.
    fn try_fast_path_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        // Aggregate-aware fast path for primary-key point/batch access shapes.
        // This keeps semantics identical while avoiding generic stream setup.
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_execute_primary_key_access_aggregate(
                inputs.ctx,
                inputs.logical_plan,
                inputs.direction,
                inputs.kind,
            )?
        {
            return Ok(Some((aggregate_output, rows_scanned)));
        }

        // Aggregate-aware fast path for secondary index-prefix plans that are
        // eligible for canonical order pushdown.
        if let Some((aggregate_output, rows_scanned)) = Self::try_execute_index_prefix_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs,
            inputs.direction,
            inputs.kind,
            inputs.fold_mode,
        )? {
            return Ok(Some((aggregate_output, rows_scanned)));
        }

        // Aggregate-aware fast path for primary-data range/full scans.
        // This reuses canonical fold logic while skipping generic stream routing.
        if inputs.physical_fetch_hint.is_some()
            && let Some((aggregate_output, rows_scanned)) =
                Self::try_execute_primary_scan_aggregate(
                    inputs.ctx,
                    inputs.logical_plan,
                    inputs.direction,
                    inputs.physical_fetch_hint,
                    inputs.kind,
                    inputs.fold_mode,
                )?
        {
            return Ok(Some((aggregate_output, rows_scanned)));
        }

        // Aggregate-aware fast path for index-range plans using lowered
        // byte-level range specs and shared fold semantics.
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_execute_index_range_aggregate(inputs)?
        {
            return Ok(Some((aggregate_output, rows_scanned)));
        }

        // Fast exit: effective limit == 0 has an empty aggregate window and can
        // return terminal defaults without constructing or scanning key streams.
        if inputs.physical_fetch_hint == Some(0) {
            return Ok(Some((Self::aggregate_zero_window_result(inputs.kind), 0)));
        }

        Ok(None)
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

    // Resolve aggregate terminals directly for primary-key point/batch plans.
    // This preserves consistency + window semantics without building streams.
    fn try_execute_primary_key_access_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        kind: AggregateKind,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        let ordered_keys = match path {
            AccessPath::ByKey(key) => vec![*key],
            AccessPath::ByKeys(keys) => {
                let mut deduped = Context::<E>::dedup_keys(keys.clone());
                if direction == Direction::Desc {
                    deduped.reverse();
                }

                deduped
            }
            _ => return Ok(None),
        };
        if ordered_keys.is_empty() {
            return Ok(None);
        }
        if plan.predicate.is_some() {
            return Ok(None);
        }

        // Phase 1: apply window exhaustion before touching storage.
        let mut window = AggregateWindowState::from_plan(plan);
        if window.exhausted() {
            return Ok(Some((Self::aggregate_zero_window_result(kind), 0)));
        }

        // Phase 2: iterate canonical candidate keys and enforce the same
        // consistency + window semantics used by streaming aggregation.
        let mut keys_scanned = 0usize;
        let mut count = 0u32;
        let mut exists = false;
        let mut min_id = None::<Id<E>>;
        let mut max_id = None::<Id<E>>;
        for key in ordered_keys {
            if window.exhausted() {
                break;
            }

            keys_scanned = keys_scanned.saturating_add(1);
            let data_key = Context::<E>::data_key_from_key(key)?;
            if !Self::key_qualifies_for_fold(
                ctx,
                plan.consistency,
                AggregateFoldMode::ExistingRows,
                &data_key,
            )? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }

            let id = Id::from_key(key);
            match kind {
                AggregateKind::Count => {
                    count = count.saturating_add(1);
                }
                AggregateKind::Exists => {
                    exists = true;
                    break;
                }
                AggregateKind::Min => {
                    min_id = Some(id);
                    if direction == Direction::Asc {
                        break;
                    }
                }
                AggregateKind::Max => {
                    max_id = Some(id);
                    if direction == Direction::Desc {
                        break;
                    }
                }
            }
        }

        // Phase 3: project one terminal output from the reducer state.
        let aggregate_output = match kind {
            AggregateKind::Count => AggregateOutput::Count(count),
            AggregateKind::Exists => AggregateOutput::Exists(exists),
            AggregateKind::Min => AggregateOutput::Min(min_id),
            AggregateKind::Max => AggregateOutput::Max(max_id),
        };

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-prefix access plans when
    // canonical secondary ordering is pushdown-eligible.
    fn try_execute_index_prefix_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        index_prefix_specs: &[IndexPrefixSpec],
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let secondary_pushdown_applicability =
            assess_secondary_order_pushdown_if_applicable_validated(E::MODEL, plan);
        ensure_secondary_aggregate_fast_path_arity(
            secondary_pushdown_applicability.is_eligible(),
            index_prefix_specs.len(),
        )?;
        let Some(mut fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            plan,
            index_prefix_specs.first(),
            &secondary_pushdown_applicability,
            // Keep secondary aggregate traversal unbounded. MissingOk can skip
            // stale index entries, so bounded key production may under-fetch.
            None,
        )?
        else {
            return Ok(None);
        };
        if plan.distinct {
            fast.ordered_key_stream =
                Box::new(DistinctOrderedKeyStream::new(fast.ordered_key_stream));
        }

        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            plan,
            plan.consistency,
            direction,
            fast.ordered_key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Resolve aggregate terminals directly for full-scan/key-range access plans.
    // This keeps canonical stream semantics while avoiding generic route assembly.
    fn try_execute_primary_scan_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        if !matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. }) {
            return Ok(None);
        }

        let mut key_stream = ctx.ordered_key_stream_from_access_with_index_range_anchor(
            path,
            None,
            None,
            None,
            direction,
            physical_fetch_hint,
        )?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            plan,
            plan.consistency,
            direction,
            key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-range access plans.
    // This reuses canonical range traversal while preserving one fold engine.
    fn try_execute_index_range_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let index_range_pushdown_eligible =
            Self::is_index_range_limit_pushdown_shape_eligible(inputs.logical_plan);
        ensure_index_range_aggregate_fast_path_specs(
            index_range_pushdown_eligible,
            inputs.index_prefix_specs.len(),
            inputs.index_range_specs.len(),
        )?;
        if !index_range_pushdown_eligible {
            return Ok(None);
        }
        let effective_fetch = inputs.physical_fetch_hint.unwrap_or(usize::MAX);

        let Some(mut fast) = Self::try_execute_index_range_limit_pushdown_stream(
            inputs.ctx,
            inputs.logical_plan,
            inputs.index_range_specs.first(),
            None,
            inputs.direction,
            effective_fetch,
        )?
        else {
            return Ok(None);
        };
        if inputs.logical_plan.distinct {
            fast.ordered_key_stream =
                Box::new(DistinctOrderedKeyStream::new(fast.ordered_key_stream));
        }

        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            fast.ordered_key_stream.as_mut(),
            inputs.kind,
            inputs.fold_mode,
        )?;

        Ok(Some((aggregate_output, rows_scanned)))
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::error::ErrorClass;

    #[test]
    fn secondary_aggregate_fast_path_arity_accepts_single_prefix_spec() {
        let result = super::ensure_secondary_aggregate_fast_path_arity(true, 1);

        assert!(
            result.is_ok(),
            "single secondary prefix spec should be accepted"
        );
    }

    #[test]
    fn secondary_aggregate_fast_path_arity_rejects_multiple_prefix_specs() {
        let err = super::ensure_secondary_aggregate_fast_path_arity(true, 2)
            .expect_err("secondary aggregate fast-path must reject multiple prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("secondary aggregate fast-path expects at most one index-prefix spec"),
            "arity violation must return a clear invariant message"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_accept_exact_arity() {
        let result = super::ensure_index_range_aggregate_fast_path_specs(true, 0, 1);

        assert!(
            result.is_ok(),
            "index-range aggregate fast-path should accept one range spec and no prefix specs"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_reject_prefix_spec_presence() {
        let err = super::ensure_index_range_aggregate_fast_path_specs(true, 1, 1)
            .expect_err("index-range aggregate fast-path must reject prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "prefix-spec violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("index-range aggregate fast-path must not consume index-prefix specs"),
            "prefix-spec violation must return a clear invariant message"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_reject_non_exact_range_arity() {
        let err = super::ensure_index_range_aggregate_fast_path_specs(true, 0, 2)
            .expect_err("index-range aggregate fast-path must reject non-exact range arity");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "range-arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("index-range aggregate fast-path expects exactly one index-range spec"),
            "range-arity violation must return a clear invariant message"
        );
    }
}
