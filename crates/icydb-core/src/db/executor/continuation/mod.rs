use crate::{
    db::{
        access::LoweredKey,
        cursor::{
            ContinuationSignature, CursorBoundary, GroupedContinuationToken, GroupedPlannedCursor,
            PlannedCursor, RangeToken, decode_pk_cursor_boundary, range_token_anchor_key,
            range_token_from_validated_cursor_anchor,
        },
        direction::Direction,
        executor::{
            AccessScanContinuationInput, ExecutablePlan, compute_page_keep_and_fetch_counts,
            route::ContinuationMode,
            traversal::{effective_keep_count_for_limit, effective_page_offset_for_window},
        },
        query::plan::{AccessPlannedQuery, ContinuationPolicy, ExecutionOrdering},
    },
    error::InternalError,
    traits::EntityKind,
    value::Value,
};

///
/// ContinuationEngine
///
/// Executor-owned continuation protocol facade.
/// Centralizes scalar cursor runtime bindings and grouped cursor token emission
/// so executor load paths consume one boundary for runtime continuation payloads.
///

pub(in crate::db::executor) struct ContinuationEngine;

impl ContinuationEngine {
    /// Resolve load mode/order compatibility and cursor revalidation contracts.
    pub(in crate::db::executor) fn resolve_load_cursor_context<E: EntityKind>(
        plan: &ExecutablePlan<E>,
        cursor: LoadCursorInput,
        requested_shape: RequestedLoadExecutionShape,
    ) -> Result<ResolvedLoadCursorContext, InternalError> {
        let ordering = plan.execution_ordering()?;
        match (requested_shape, &ordering) {
            (
                RequestedLoadExecutionShape::Scalar,
                ExecutionOrdering::PrimaryKey | ExecutionOrdering::Explicit(_),
            )
            | (RequestedLoadExecutionShape::Grouped, ExecutionOrdering::Grouped(_)) => {}
            (RequestedLoadExecutionShape::Scalar, ExecutionOrdering::Grouped(_)) => {
                return Err(invariant(
                    "grouped plans require grouped load execution mode",
                ));
            }
            (
                RequestedLoadExecutionShape::Grouped,
                ExecutionOrdering::PrimaryKey | ExecutionOrdering::Explicit(_),
            ) => {
                return Err(invariant(
                    "grouped load execution mode requires grouped logical plans",
                ));
            }
        }

        let cursor = match (requested_shape, cursor) {
            (RequestedLoadExecutionShape::Scalar, LoadCursorInput::Scalar(cursor)) => {
                let cursor = plan.revalidate_cursor(*cursor)?;
                let continuation_signature = plan.continuation_signature_for_runtime()?;
                let resolved = Self::resolve_scalar_context(cursor, continuation_signature);
                PreparedLoadCursor::Scalar(Box::new(resolved))
            }
            (RequestedLoadExecutionShape::Grouped, LoadCursorInput::Grouped(cursor)) => {
                PreparedLoadCursor::Grouped(plan.revalidate_grouped_cursor(cursor)?)
            }
            (RequestedLoadExecutionShape::Scalar, LoadCursorInput::Grouped(_)) => {
                return Err(invariant(
                    "scalar load execution mode requires scalar cursor input",
                ));
            }
            (RequestedLoadExecutionShape::Grouped, LoadCursorInput::Scalar(_)) => {
                return Err(invariant(
                    "grouped load execution mode requires grouped cursor input",
                ));
            }
        };

        Ok(ResolvedLoadCursorContext::new(cursor))
    }

    /// Resolve scalar continuation runtime + signature into one contract object.
    #[must_use]
    pub(in crate::db::executor) fn resolve_scalar_context(
        cursor: PlannedCursor,
        continuation_signature: ContinuationSignature,
    ) -> ResolvedScalarContinuationContext {
        ResolvedScalarContinuationContext::new(
            ScalarContinuationContext::new(cursor),
            continuation_signature,
        )
    }

    /// Build one grouped continuation token for grouped page finalization.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_next_cursor_token(
        continuation_signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        resume_initial_offset: u32,
    ) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            continuation_signature,
            last_group_key,
            Direction::Asc,
            resume_initial_offset,
        )
    }
}

///
/// LoadCursorInput
///
/// Load-entrypoint cursor input contract passed into continuation resolver
/// before runtime ordering/shape compatibility checks.
///

pub(in crate::db::executor) enum LoadCursorInput {
    Scalar(Box<PlannedCursor>),
    Grouped(GroupedPlannedCursor),
}

///
/// RequestedLoadExecutionShape
///
/// Requested load execution shape at entrypoint selection time.
/// Used by continuation resolver to validate mode/order compatibility before
/// cursor revalidation occurs.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum RequestedLoadExecutionShape {
    Scalar,
    Grouped,
}

impl LoadCursorInput {
    /// Build scalar load cursor input.
    #[must_use]
    pub(in crate::db::executor) fn scalar(cursor: impl Into<PlannedCursor>) -> Self {
        Self::Scalar(Box::new(cursor.into()))
    }

    /// Build grouped load cursor input.
    #[must_use]
    pub(in crate::db::executor) fn grouped(cursor: impl Into<GroupedPlannedCursor>) -> Self {
        Self::Grouped(cursor.into())
    }
}

///
/// PreparedLoadCursor
///
/// Revalidated load cursor contract returned by continuation resolver.
///

pub(in crate::db::executor) enum PreparedLoadCursor {
    Scalar(Box<ResolvedScalarContinuationContext>),
    Grouped(GroupedPlannedCursor),
}

///
/// ResolvedLoadCursorContext
///
/// Canonical load cursor resolution output.
/// Carries one revalidated cursor payload so load
/// entrypoint orchestration consumes one resolved contract boundary.
///

pub(in crate::db::executor) struct ResolvedLoadCursorContext {
    cursor: PreparedLoadCursor,
}

impl ResolvedLoadCursorContext {
    /// Construct one resolved load cursor context.
    #[must_use]
    const fn new(cursor: PreparedLoadCursor) -> Self {
        Self { cursor }
    }

    /// Consume context and return revalidated cursor payload.
    #[must_use]
    pub(in crate::db::executor) fn into_cursor(self) -> PreparedLoadCursor {
        self.cursor
    }
}

///
/// ContinuationCapabilities
///
/// Immutable continuation capability projection derived once from scalar
/// continuation runtime shape plus planner continuation policy.
/// Route/load consumers read this contract instead of re-deriving policy gates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db::executor) struct ContinuationCapabilities {
    #[cfg_attr(not(test), allow(dead_code))]
    mode: ContinuationMode,
    applied: bool,
    strict_advance_required_when_applied: bool,
    grouped_safe_when_applied: bool,
    index_range_limit_pushdown_allowed: bool,
}

impl ContinuationCapabilities {
    /// Construct one immutable continuation capability projection.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        mode: ContinuationMode,
        continuation_policy: ContinuationPolicy,
    ) -> Self {
        let applied = !matches!(mode, ContinuationMode::Initial);

        Self {
            mode,
            applied,
            strict_advance_required_when_applied: !applied
                || continuation_policy.requires_strict_advance(),
            grouped_safe_when_applied: !applied || continuation_policy.is_grouped_safe(),
            index_range_limit_pushdown_allowed: !continuation_policy.requires_anchor()
                || !matches!(mode, ContinuationMode::CursorBoundary),
        }
    }

    /// Return route continuation mode projected by this capability snapshot.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn mode(self) -> ContinuationMode {
        self.mode
    }

    /// Return whether continuation is applied for this execution.
    #[must_use]
    pub(in crate::db::executor) const fn applied(self) -> bool {
        self.applied
    }

    /// Return whether strict advancement is required under continuation.
    #[must_use]
    pub(in crate::db::executor) const fn strict_advance_required_when_applied(self) -> bool {
        self.strict_advance_required_when_applied
    }

    /// Return whether grouped continuation remains safe under this policy.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_safe_when_applied(self) -> bool {
        self.grouped_safe_when_applied
    }

    /// Return whether index-range limit pushdown can remain enabled.
    #[must_use]
    pub(in crate::db::executor) const fn index_range_limit_pushdown_allowed(self) -> bool {
        self.index_range_limit_pushdown_allowed
    }
}

///
/// ScalarContinuationContext
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarContinuationContext {
    cursor_boundary: Option<CursorBoundary>,
    index_range_token: Option<RangeToken>,
}

impl ScalarContinuationContext {
    /// Construct one empty scalar continuation runtime for initial executions.
    #[must_use]
    pub(in crate::db::executor) const fn initial() -> Self {
        Self {
            cursor_boundary: None,
            index_range_token: None,
        }
    }

    /// Construct one scalar continuation runtime from explicit boundary/token parts.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn from_parts(
        cursor_boundary: Option<CursorBoundary>,
        index_range_token: Option<RangeToken>,
    ) -> Self {
        Self {
            cursor_boundary,
            index_range_token,
        }
    }

    /// Build one scalar runtime cursor binding bundle from one planned cursor.
    #[must_use]
    pub(in crate::db::executor) fn new(cursor: PlannedCursor) -> Self {
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_validated_cursor_anchor);

        Self {
            cursor_boundary,
            index_range_token,
        }
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.cursor_boundary.as_ref()
    }

    /// Return whether this scalar continuation context has one cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn has_cursor_boundary(&self) -> bool {
        self.cursor_boundary.is_some()
    }

    /// Validate scalar cursor-boundary decode for PK fast-path eligibility gates.
    ///
    /// This preserves PK fast-path cursor error classification while keeping
    /// boundary decode authority in continuation runtime.
    pub(in crate::db::executor) fn validate_pk_fast_path_boundary<E: EntityKind>(
        &self,
    ) -> Result<(), InternalError> {
        let _ = decode_pk_cursor_boundary::<E>(self.cursor_boundary())?;

        Ok(())
    }

    /// Borrow optional index-range continuation anchor token.
    #[must_use]
    pub(in crate::db::executor) const fn index_range_token(&self) -> Option<&RangeToken> {
        self.index_range_token.as_ref()
    }

    /// Return whether this scalar continuation context has one index-range anchor.
    #[must_use]
    pub(in crate::db::executor) const fn has_index_range_anchor(&self) -> bool {
        self.index_range_token.is_some()
    }

    /// Derive effective pagination offset under this scalar continuation context.
    #[must_use]
    pub(in crate::db::executor) fn effective_page_offset_for_plan<K>(
        &self,
        plan: &AccessPlannedQuery<K>,
    ) -> u32 {
        effective_page_offset_for_window(plan, self.has_cursor_boundary())
    }

    /// Derive route continuation mode from scalar continuation context shape.
    #[must_use]
    pub(in crate::db::executor) const fn route_continuation_mode(&self) -> ContinuationMode {
        match (self.has_cursor_boundary(), self.has_index_range_anchor()) {
            (_, true) => ContinuationMode::IndexRangeAnchor,
            (true, false) => ContinuationMode::CursorBoundary,
            (false, false) => ContinuationMode::Initial,
        }
    }

    /// Derive immutable continuation capabilities from runtime + planner policy.
    #[must_use]
    pub(in crate::db::executor) const fn continuation_capabilities(
        &self,
        continuation_policy: ContinuationPolicy,
    ) -> ContinuationCapabilities {
        ContinuationCapabilities::new(self.route_continuation_mode(), continuation_policy)
    }

    /// Derive route window projection from scalar continuation context + plan window.
    #[must_use]
    pub(in crate::db::executor) fn route_window_projection_for_plan<K>(
        &self,
        plan: &AccessPlannedQuery<K>,
    ) -> ScalarRouteWindowProjection {
        let effective_offset = self.effective_page_offset_for_plan(plan);
        let limit = plan.scalar_plan().page.as_ref().and_then(|page| page.limit);
        let (keep_count, fetch_count) = match limit {
            Some(limit) => {
                let (keep, fetch) = compute_page_keep_and_fetch_counts(effective_offset, limit);
                (Some(keep), Some(fetch))
            }
            None => (None, None),
        };

        ScalarRouteWindowProjection::new(effective_offset, limit, keep_count, fetch_count)
    }
}

///
/// ScalarRouteWindowProjection
///
/// Continuation-owned route window projection carrying effective offset, limit,
/// and precomputed keep/fetch counts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarRouteWindowProjection {
    effective_offset: u32,
    limit: Option<u32>,
    keep_count: Option<usize>,
    fetch_count: Option<usize>,
}

impl ScalarRouteWindowProjection {
    /// Construct one scalar route-window projection.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        effective_offset: u32,
        limit: Option<u32>,
        keep_count: Option<usize>,
        fetch_count: Option<usize>,
    ) -> Self {
        Self {
            effective_offset,
            limit,
            keep_count,
            fetch_count,
        }
    }

    /// Return projected effective offset.
    #[must_use]
    pub(in crate::db::executor) const fn effective_offset(self) -> u32 {
        self.effective_offset
    }

    /// Return projected page limit.
    #[must_use]
    pub(in crate::db::executor) const fn limit(self) -> Option<u32> {
        self.limit
    }

    /// Return projected keep-count.
    #[must_use]
    pub(in crate::db::executor) const fn keep_count(self) -> Option<usize> {
        self.keep_count
    }

    /// Return projected fetch-count.
    #[must_use]
    pub(in crate::db::executor) const fn fetch_count(self) -> Option<usize> {
        self.fetch_count
    }
}

///
/// ResolvedScalarContinuationContext
///
/// Runtime scalar continuation contract resolved from validated cursor state
/// plus continuation signature. Load entrypoints consume this contract to build
/// routed continuation bindings and access-scan inputs without unpacking cursor
/// token internals (anchor/boundary) directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ResolvedScalarContinuationContext {
    runtime: ScalarContinuationContext,
    continuation_signature: ContinuationSignature,
}

///
/// ScalarRouteContinuationInvariantProjection
///
/// Minimal route-to-continuation invariant projection consumed by scalar
/// continuation runtime assertions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarRouteContinuationInvariantProjection {
    strict_advance_required_when_applied: bool,
    effective_offset: u32,
}

impl ScalarRouteContinuationInvariantProjection {
    /// Construct one scalar continuation invariant projection.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        strict_advance_required_when_applied: bool,
        effective_offset: u32,
    ) -> Self {
        Self {
            strict_advance_required_when_applied,
            effective_offset,
        }
    }

    /// Return whether strict continuation advancement is required.
    #[must_use]
    pub(in crate::db::executor) const fn strict_advance_required_when_applied(self) -> bool {
        self.strict_advance_required_when_applied
    }

    /// Return route-projected effective offset for continuation checks.
    #[must_use]
    pub(in crate::db::executor) const fn effective_offset(self) -> u32 {
        self.effective_offset
    }
}

impl ResolvedScalarContinuationContext {
    /// Construct one resolved scalar continuation context.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        runtime: ScalarContinuationContext,
        continuation_signature: ContinuationSignature,
    ) -> Self {
        Self {
            runtime,
            continuation_signature,
        }
    }

    /// Borrow runtime scalar continuation context for route derivation.
    #[must_use]
    pub(in crate::db::executor) const fn route_context(&self) -> &ScalarContinuationContext {
        &self.runtime
    }

    /// Borrow optional scalar cursor boundary for invariant checks.
    #[must_use]
    pub(in crate::db::executor) const fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.runtime.cursor_boundary()
    }

    /// Build scalar continuation bindings for kernel/load materialization.
    #[must_use]
    pub(in crate::db::executor) fn bindings(
        &self,
        direction: Direction,
    ) -> ScalarContinuationBindings<'_> {
        ScalarContinuationBindings::new(
            self.runtime.cursor_boundary(),
            self.previous_index_range_anchor(),
            direction,
            self.continuation_signature,
        )
    }

    /// Build access-stream continuation input for routed stream resolution.
    #[must_use]
    pub(in crate::db::executor) fn access_scan_input(
        &self,
        direction: Direction,
    ) -> AccessScanContinuationInput<'_> {
        AccessScanContinuationInput::new(self.previous_index_range_anchor(), direction)
    }

    /// Assert scalar route-continuation invariants against this runtime context.
    ///
    /// Keeps scalar continuation protocol sanity checks centralized in
    /// continuation runtime so load entrypoints consume one invariant boundary.
    pub(in crate::db::executor) fn debug_assert_route_continuation_invariants<K>(
        &self,
        plan: &AccessPlannedQuery<K>,
        projection: ScalarRouteContinuationInvariantProjection,
    ) {
        debug_assert!(
            projection.strict_advance_required_when_applied(),
            "route invariant: continuation executions must enforce strict advancement policy",
        );
        debug_assert_eq!(
            projection.effective_offset(),
            effective_page_offset_for_window(plan, self.cursor_boundary().is_some()),
            "route window effective offset must match logical plan offset semantics",
        );
    }

    /// Borrow optional prior index-range anchor lowered key.
    #[must_use]
    fn previous_index_range_anchor(&self) -> Option<&LoweredKey> {
        self.runtime.index_range_token().map(range_token_anchor_key)
    }
}

///
/// ScalarContinuationBindings
///
/// Runtime continuation bindings shared across kernel/load materialization.
/// Bundles scalar continuation boundary and anchor state with routed direction
/// and continuation signature so runtime boundaries stop carrying primitives.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarContinuationBindings<'a> {
    cursor_boundary: Option<&'a CursorBoundary>,
    previous_index_range_anchor: Option<&'a LoweredKey>,
    direction: Direction,
    continuation_signature: ContinuationSignature,
}

impl<'a> ScalarContinuationBindings<'a> {
    /// Construct one scalar continuation runtime binding bundle.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        cursor_boundary: Option<&'a CursorBoundary>,
        previous_index_range_anchor: Option<&'a LoweredKey>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Self {
        Self {
            cursor_boundary,
            previous_index_range_anchor,
            direction,
            continuation_signature,
        }
    }

    /// Borrow optional scalar cursor boundary for post-access cursor semantics.
    #[must_use]
    pub(in crate::db::executor) const fn post_access_cursor_boundary(
        &self,
    ) -> Option<&'a CursorBoundary> {
        self.cursor_boundary
    }

    /// Return whether this continuation context represents a resumed page.
    #[must_use]
    pub(in crate::db::executor) const fn continuation_applied(&self) -> bool {
        self.cursor_boundary.is_some()
    }

    /// Derive effective keep count (`offset + limit`) under this continuation context.
    #[must_use]
    pub(in crate::db::executor) fn effective_keep_count_for_limit<K>(
        &self,
        plan: &AccessPlannedQuery<K>,
        limit: u32,
    ) -> usize {
        effective_keep_count_for_limit(plan, self.continuation_applied(), limit)
    }

    /// Borrow optional previous index-range anchor.
    #[must_use]
    pub(in crate::db::executor) const fn previous_index_range_anchor(
        &self,
    ) -> Option<&'a LoweredKey> {
        self.previous_index_range_anchor
    }

    /// Borrow routed stream direction for this continuation context.
    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Borrow continuation signature for this continuation context.
    #[must_use]
    pub(in crate::db::executor) const fn continuation_signature(&self) -> ContinuationSignature {
        self.continuation_signature
    }

    /// Validate load scan-budget hint preconditions under this continuation context.
    ///
    /// Bounded load scan hints are only valid for non-continuation executions on
    /// streaming-safe access shapes where access order is already final.
    pub(in crate::db::executor) fn validate_load_scan_budget_hint(
        &self,
        scan_budget_hint: Option<usize>,
        streaming_access_shape_safe: bool,
    ) -> Result<(), InternalError> {
        if scan_budget_hint.is_some() {
            if self.continuation_applied() {
                return Err(invariant(
                    "load page scan budget hint requires non-continuation execution",
                ));
            }
            if !streaming_access_shape_safe {
                return Err(invariant(
                    "load page scan budget hint requires streaming-safe access shape",
                ));
            }
        }

        Ok(())
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        executor::{ContinuationCapabilities, route::ContinuationMode},
        query::plan::ContinuationPolicy,
    };

    #[test]
    fn continuation_capabilities_apply_policy_for_initial_mode() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::Initial,
            ContinuationPolicy::new(true, false, false),
        );

        assert!(!capabilities.applied());
        assert!(
            capabilities.strict_advance_required_when_applied(),
            "initial mode must satisfy strict-advance invariants unconditionally",
        );
        assert!(
            capabilities.grouped_safe_when_applied(),
            "initial mode must satisfy grouped safety invariants unconditionally",
        );
        assert!(
            capabilities.index_range_limit_pushdown_allowed(),
            "initial mode must not disable index-range limit pushdown",
        );
    }

    #[test]
    fn continuation_capabilities_disable_index_range_pushdown_for_cursor_boundary_anchor_policy() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::CursorBoundary,
            ContinuationPolicy::new(true, true, true),
        );

        assert!(capabilities.applied());
        assert!(capabilities.strict_advance_required_when_applied());
        assert!(capabilities.grouped_safe_when_applied());
        assert!(
            !capabilities.index_range_limit_pushdown_allowed(),
            "cursor-boundary mode with anchor-required policy must disable index-range pushdown",
        );
    }

    #[test]
    fn continuation_capabilities_keep_index_range_pushdown_for_anchor_mode() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::IndexRangeAnchor,
            ContinuationPolicy::new(true, true, true),
        );

        assert_eq!(capabilities.mode(), ContinuationMode::IndexRangeAnchor);
        assert!(capabilities.applied());
        assert!(capabilities.index_range_limit_pushdown_allowed());
    }
}
