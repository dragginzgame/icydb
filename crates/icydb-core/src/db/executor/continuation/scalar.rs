use crate::{
    db::{
        access::LoweredKey,
        cursor::{
            ContinuationSignature, CursorBoundary, PlannedCursor, RangeToken,
            decode_pk_cursor_boundary, range_token_anchor_key,
            range_token_from_validated_cursor_anchor,
        },
        direction::Direction,
        executor::{
            AccessScanContinuationInput,
            continuation::capabilities::ContinuationCapabilities,
            route::ContinuationMode,
            traversal::{effective_keep_count_for_limit, effective_page_offset_for_window},
        },
        query::plan::{AccessPlannedQuery, ContinuationPolicy},
    },
    error::InternalError,
    traits::EntityKind,
};

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
        stream_order_contract_safe: bool,
    ) -> Result<(), InternalError> {
        if scan_budget_hint.is_some() {
            if self.continuation_applied() {
                return Err(crate::db::error::executor_invariant(
                    "load page scan budget hint requires non-continuation execution",
                ));
            }
            if !stream_order_contract_safe {
                return Err(crate::db::error::executor_invariant(
                    "load page scan budget hint requires streaming-safe access shape",
                ));
            }
        }

        Ok(())
    }
}
