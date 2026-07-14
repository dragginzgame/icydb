//! Module: query::read_intent
//! Responsibility: hardcoded read-intent caps for public semantic terminals.
//! Does not own: planner proof, executor routing, or public policy builders.
//! Boundary: one internal authority for engine-owned read-intent limits.

use crate::db::query::admission::DEFAULT_BOUNDED_READ_MAX_ROWS;
use candid::CandidType;
use serde::Deserialize;

pub(in crate::db::query) const PUBLIC_PAGE_DEFAULT_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;
pub(in crate::db::query) const PUBLIC_PAGE_MAX_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;
pub(in crate::db::query) const COMPLETE_SMALL_MAX_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;
pub(in crate::db::query) const COMPLETE_SMALL_LOOKAHEAD_ROWS: u32 = 1;
pub(in crate::db::query) const COMPLETE_SMALL_EXECUTION_LIMIT: u32 =
    COMPLETE_SMALL_MAX_ROWS + COMPLETE_SMALL_LOOKAHEAD_ROWS;
pub(in crate::db::query) const ADMIN_BATCH_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;

/// Semantic read intent selected by a caller-facing terminal.
///
/// This is diagnostic metadata only. It does not grant access, choose planner
/// routes, or configure admission policy.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum ReadIntentKind {
    /// No semantic read intent was attached to this diagnostic payload.
    #[default]
    Unspecified,

    /// Low-level execution over the effective bounded row window.
    BoundedRowWindow,

    /// Boolean existence check.
    ExistenceCheck,

    /// Request-owned public cursor page.
    PublicPage,

    /// Complete small-set read that fails instead of silently truncating.
    CompleteSmallSet,

    /// Exact aggregate terminal such as count, sum, min, max, or average.
    ExactAggregate,

    /// Trusted/admin cursor batch with engine-owned batch size.
    TrustedAdminBatch,
}

/// Internal public-page request shape.
///
/// The requested limit is a caller preference, not a custom policy. IcyDB
/// clamps it to the engine-owned public page cap before admission/execution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db::query) struct PageRequest {
    limit: Option<u32>,
    cursor: Option<String>,
}

impl PageRequest {
    /// Build a first-page request with one requested page size.
    #[must_use]
    pub(in crate::db::query) const fn first(limit: u32) -> Self {
        Self {
            limit: Some(limit),
            cursor: None,
        }
    }

    /// Build a continuation request with one requested page size and cursor.
    #[must_use]
    pub(in crate::db::query) fn next(limit: u32, cursor: impl Into<String>) -> Self {
        Self {
            limit: Some(limit),
            cursor: Some(cursor.into()),
        }
    }

    pub(in crate::db::query) const fn effective_limit(&self) -> u32 {
        match self.limit {
            Some(0) => 1,
            Some(limit) if limit > PUBLIC_PAGE_MAX_ROWS => PUBLIC_PAGE_MAX_ROWS,
            Some(limit) => limit,
            None => PUBLIC_PAGE_DEFAULT_ROWS,
        }
    }

    pub(in crate::db::query) fn into_cursor(self) -> Option<String> {
        self.cursor
    }
}

/// Request-owned trusted/admin batch continuation shape.
///
/// The batch size is engine-owned. Callers may only supply an opaque cursor
/// for continuation, and the terminal remains gated to trusted read lanes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdminBatchRequest {
    cursor: Option<String>,
}

impl AdminBatchRequest {
    /// Build a first-batch request.
    #[must_use]
    pub const fn new() -> Self {
        Self { cursor: None }
    }

    /// Build a continuation request with an opaque cursor.
    #[must_use]
    pub fn next(cursor: impl Into<String>) -> Self {
        Self {
            cursor: Some(cursor.into()),
        }
    }

    /// Return this request with an opaque continuation cursor.
    #[must_use]
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Return the opaque continuation cursor, if supplied.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    pub(in crate::db::query) fn into_cursor(self) -> Option<String> {
        self.cursor
    }
}
