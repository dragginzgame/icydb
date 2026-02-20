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
        // COUNT pushdown must remain a strict subset of streaming eligibility.
        // Keep this gate centralized so future optimizations do not fork safety logic.
        if matches!(kind, AggregateKind::Count)
            && Self::is_count_pushdown_shape_supported(plan.as_inner())
        {
            return self.execute_count_pushdown(plan);
        }

        // If the logical plan requires post-access filtering, sorting,
        // or any non-stream-safe phase, fall back to canonical execution.
        // This preserves exact parity with materialized load semantics.
        if !Self::is_streaming_aggregate_shape_supported(plan.as_inner()) {
            let response = self.execute(plan)?;
            return Ok(Self::aggregate_from_materialized(response, kind));
        }

        // EXISTS may provide a bounded probe hint (offset + 1) so eligible
        // fast-paths can avoid over-producing candidates.
        // This is only valid under streaming-safe shapes.
        let exists_probe_fetch_hint = Self::exists_probe_fetch_hint(plan.as_inner(), kind);

        // Direction must be captured before consuming the ExecutablePlan.
        // After `into_inner()`, we operate purely on LogicalPlan.
        let direction = plan.direction();

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

        // Fast exit: EXISTS with effective limit == 0 can return false
        // without constructing or scanning any key stream.
        if exists_probe_fetch_hint == Some(0) {
            record_rows_scanned::<E>(0);
            return Ok(AggregateOutput::Exists(false));
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            index_range_anchor: None,
            direction,
        };

        // Fast-path planning must be identical to load execution so aggregate
        // folding sees the exact same ordered key stream.
        let fast_path_plan =
            Self::build_fast_path_plan(&logical_plan, None, None, exists_probe_fetch_hint)?;

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, &fast_path_plan)?;

        // Fold only over rows that actually exist (respecting read consistency)
        // and apply pagination window semantics during folding.
        let (aggregate_output, keys_scanned) = Self::fold_existing_rows(
            &ctx,
            &logical_plan,
            logical_plan.consistency,
            direction,
            resolved.key_stream.as_mut(),
            kind,
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Execute COUNT through the canonical key-stream resolver and count emitted
    // keys directly for access shapes that guarantee key->row existence parity.
    fn execute_count_pushdown(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<AggregateOutput<E>, InternalError> {
        // Direction must be captured before consuming the executable wrapper.
        let direction = plan.direction();
        let logical_plan = plan.into_inner();

        // Phase 1: validate and recover context at the same boundary as load.
        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;

        // Phase 2: resolve keys through the same fast-path/fallback router.
        record_plan_metrics(&logical_plan.access);
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            index_range_anchor: None,
            direction,
        };
        let count_fetch_hint = Self::count_pushdown_fetch_hint(&logical_plan);
        let fast_path_plan =
            Self::build_fast_path_plan(&logical_plan, None, None, count_fetch_hint)?;
        let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, &fast_path_plan)?;

        // Phase 3: count keys in the effective window without per-key row reads.
        let (count, keys_scanned) = Self::count_emitted_keys(
            resolved.key_stream.as_mut(),
            AggregateWindowState::from_plan(&logical_plan),
        )?;
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(AggregateOutput::Count(count))
    }

    // EXISTS probe mode can request an internal fetch hint so eligible
    // fast-paths can stop candidate production earlier.
    fn exists_probe_fetch_hint(plan: &LogicalPlan<E::Key>, kind: AggregateKind) -> Option<usize> {
        if !matches!(kind, AggregateKind::Exists) {
            return None;
        }
        if plan.page.as_ref().is_some_and(|page| page.limit == Some(0)) {
            return Some(0);
        }

        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        // Keep probe-hint semantics explicit: DISTINCT with a positive offset
        // can shrink raw-key windows before offset consumption.
        // Disable bounded probing for that shape so future path changes
        // cannot silently under-produce EXISTS windows.
        if plan.distinct && offset > 0 {
            return None;
        }

        Some(offset.saturating_add(1))
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
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        Some(offset.saturating_add(limit))
    }

    // Count keys emitted in the effective page window without row validation.
    fn count_emitted_keys(
        key_stream: &mut dyn OrderedKeyStream,
        mut window: AggregateWindowState,
    ) -> Result<(u32, usize), InternalError> {
        let mut keys_scanned = 0usize;
        let mut count = 0u32;

        while !window.exhausted() {
            let Some(_key) = key_stream.next_key()? else {
                break;
            };
            keys_scanned = keys_scanned.saturating_add(1);
            if !window.accept_existing_row() {
                continue;
            }

            count = count.saturating_add(1);
        }

        Ok((count, keys_scanned))
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

    // Fold an already-resolved ordered key stream while preserving read-consistency
    // semantics by validating row existence per key.
    fn fold_existing_rows(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        consistency: ReadConsistency,
        direction: Direction,
        key_stream: &mut dyn OrderedKeyStream,
        kind: AggregateKind,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let window = AggregateWindowState::from_plan(plan);

        match kind {
            AggregateKind::Count => Self::fold_count(ctx, consistency, key_stream, window),
            AggregateKind::Exists => Self::fold_exists(ctx, consistency, key_stream, window),
            AggregateKind::Min => Self::fold_min(ctx, consistency, direction, key_stream, window),
            AggregateKind::Max => Self::fold_max(ctx, consistency, direction, key_stream, window),
        }
    }

    fn fold_count(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn OrderedKeyStream,
        mut window: AggregateWindowState,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let mut keys_scanned = 0usize;
        let mut count = 0u32;

        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };

            keys_scanned = keys_scanned.saturating_add(1);
            if Self::row_exists_for_key(ctx, consistency, &key)? {
                if !window.accept_existing_row() {
                    continue;
                }
                count = count.saturating_add(1);
            }
        }

        Ok((AggregateOutput::Count(count), keys_scanned))
    }

    fn fold_exists(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn OrderedKeyStream,
        mut window: AggregateWindowState,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let mut keys_scanned = 0usize;

        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };

            keys_scanned = keys_scanned.saturating_add(1);
            if Self::row_exists_for_key(ctx, consistency, &key)? {
                if !window.accept_existing_row() {
                    continue;
                }

                return Ok((AggregateOutput::Exists(true), keys_scanned));
            }
        }

        Ok((AggregateOutput::Exists(false), keys_scanned))
    }

    fn fold_min(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        direction: Direction,
        key_stream: &mut dyn OrderedKeyStream,
        mut window: AggregateWindowState,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let mut keys_scanned = 0usize;
        let mut last_kept_id: Option<Id<E>> = None;

        // For ASC streams, first kept key is the minimum.
        if direction == Direction::Asc {
            while !window.exhausted() {
                let Some(key) = key_stream.next_key()? else {
                    break;
                };

                keys_scanned = keys_scanned.saturating_add(1);
                if !Self::row_exists_for_key(ctx, consistency, &key)? {
                    continue;
                }
                if !window.accept_existing_row() {
                    continue;
                }

                return Ok((
                    AggregateOutput::Min(Some(Id::from_key(key.try_key::<E>()?))),
                    keys_scanned,
                ));
            }

            return Ok((AggregateOutput::Min(None), keys_scanned));
        }

        // For DESC streams, minimum is the last kept key in the window.
        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };
            keys_scanned = keys_scanned.saturating_add(1);
            if !Self::row_exists_for_key(ctx, consistency, &key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }

            last_kept_id = Some(Id::from_key(key.try_key::<E>()?));
        }

        Ok((AggregateOutput::Min(last_kept_id), keys_scanned))
    }

    fn fold_max(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        direction: Direction,
        key_stream: &mut dyn OrderedKeyStream,
        mut window: AggregateWindowState,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        let mut keys_scanned = 0usize;
        let mut last_kept_id: Option<Id<E>> = None;

        // For DESC streams, first kept key is the maximum.
        if direction == Direction::Desc {
            while !window.exhausted() {
                let Some(key) = key_stream.next_key()? else {
                    break;
                };

                keys_scanned = keys_scanned.saturating_add(1);
                if !Self::row_exists_for_key(ctx, consistency, &key)? {
                    continue;
                }
                if !window.accept_existing_row() {
                    continue;
                }

                return Ok((
                    AggregateOutput::Max(Some(Id::from_key(key.try_key::<E>()?))),
                    keys_scanned,
                ));
            }

            return Ok((AggregateOutput::Max(None), keys_scanned));
        }

        // For ASC streams, maximum is the last kept key in the window.
        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };
            keys_scanned = keys_scanned.saturating_add(1);
            if !Self::row_exists_for_key(ctx, consistency, &key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }

            last_kept_id = Some(Id::from_key(key.try_key::<E>()?));
        }

        Ok((AggregateOutput::Max(last_kept_id), keys_scanned))
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
