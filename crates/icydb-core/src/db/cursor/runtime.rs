//! Module: cursor::runtime
//! Responsibility: cursor-owned runtime continuation composition for scan and window semantics.
//! Does not own: route-mode policy derivation or index-store traversal internals.
//! Boundary: composes anchor/direction scan continuation with cursor-aware page windows.

use crate::{
    db::{
        cursor::{CursorBoundary, IndexScanContinuationInput},
        direction::Direction,
        index::RawIndexKey,
        query::plan::{AccessPlannedQuery, effective_offset_for_cursor_window},
    },
    error::InternalError,
};
use std::ops::Bound;

///
/// WindowCursorContract
///
/// Cursor-owned window progression contract for effective offset/limit windows.
/// Runtime consumers advance this contract once per existing row emission.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct WindowCursorContract {
    offset_remaining: usize,
    limit_remaining: Option<usize>,
}

impl WindowCursorContract {
    /// Build one window contract from canonical cursor-aware plan semantics.
    #[must_use]
    pub(in crate::db) fn from_plan(
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Self {
        let window_size = plan
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| page.offset);
        let effective_offset =
            effective_offset_for_cursor_window(window_size, cursor_boundary.is_some());
        let offset_remaining = usize::try_from(effective_offset).unwrap_or(usize::MAX);
        let limit_remaining = plan
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        Self {
            offset_remaining,
            limit_remaining,
        }
    }

    /// Build one unbounded window contract (no offset, no limit).
    #[must_use]
    pub(in crate::db) const fn unbounded() -> Self {
        Self {
            offset_remaining: 0,
            limit_remaining: None,
        }
    }

    /// Return whether the effective limit window is exhausted.
    #[must_use]
    pub(in crate::db) const fn exhausted(&self) -> bool {
        matches!(self.limit_remaining, Some(0))
    }

    /// Advance window state by one row and return whether the row is in-window.
    pub(in crate::db) const fn accept_existing_row(&mut self) -> bool {
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

/// Build one cursor-owned window contract from canonical plan semantics.
#[must_use]
pub(in crate::db) fn window_cursor_contract_for_plan(
    plan: &AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
) -> WindowCursorContract {
    WindowCursorContract::from_plan(plan, cursor_boundary)
}

///
/// LoopAction
///
/// Canonical continuation/runtime loop action.
/// `Skip` discards the current candidate, `Emit` keeps processing, and
/// `Stop` terminates the loop immediately.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum LoopAction {
    Skip,
    Emit,
    Stop,
}

///
/// ContinuationKeyRef
///
/// Typed key input contract for continuation-runtime pre-row gating.
/// Used by raw index scans to validate directional advancement.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ContinuationKeyRef<'a> {
    raw_key: &'a RawIndexKey,
}

impl<'a> ContinuationKeyRef<'a> {
    /// Build one scan-key reference for continuation runtime checks.
    #[must_use]
    pub(crate) const fn scan(raw_key: &'a RawIndexKey) -> Self {
        Self { raw_key }
    }
}

///
/// ContinuationRuntime
///
/// Cursor-owned composed continuation runtime.
/// Bundles scan-anchor semantics and page-window semantics under one object so
/// runtime loops consume one continuation contract instead of independent pieces.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ContinuationRuntime<'a> {
    scan: IndexScanContinuationInput<'a>,
    window: WindowCursorContract,
}

impl<'a> ContinuationRuntime<'a> {
    /// Build one composed continuation runtime from scan + window contracts.
    #[must_use]
    pub(in crate::db) const fn new(
        scan: IndexScanContinuationInput<'a>,
        window: WindowCursorContract,
    ) -> Self {
        Self { scan, window }
    }

    /// Build one window-only continuation runtime for post-access reducer loops.
    #[must_use]
    pub(in crate::db) const fn from_window(window: WindowCursorContract) -> Self {
        Self::new(
            IndexScanContinuationInput::new(None, Direction::Asc),
            window,
        )
    }

    /// Run pre-fetch loop gating before reading the next key from a stream.
    #[must_use]
    pub(in crate::db) const fn pre_fetch(&self) -> LoopAction {
        if self.window.exhausted() {
            return LoopAction::Stop;
        }

        LoopAction::Emit
    }

    /// Validate continuation-envelope compatibility and derive resumed scan bounds.
    pub(in crate::db) fn scan_bounds(
        &self,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    ) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), InternalError> {
        self.scan.resume_bounds(bounds)
    }

    /// Apply key-level continuation checks for one scan candidate.
    ///
    /// This method is intentionally side-effect free; runtime advancement
    /// happens only through `accept_row`.
    pub(in crate::db) fn accept_key(
        &self,
        key: ContinuationKeyRef<'_>,
    ) -> Result<LoopAction, InternalError> {
        self.scan.validate_candidate_advancement(key.raw_key)?;

        Ok(LoopAction::Emit)
    }

    /// Apply post-row window checks after row materialization.
    pub(in crate::db) const fn accept_row(&mut self) -> LoopAction {
        if self.window.exhausted() {
            return LoopAction::Stop;
        }
        if !self.window.accept_existing_row() {
            return LoopAction::Skip;
        }

        LoopAction::Emit
    }

    /// Borrow scan direction carried by this continuation runtime.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.scan.direction()
    }
}
