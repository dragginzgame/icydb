//! Module: cursor::runtime
//! Responsibility: cursor-owned scan continuation validation.
//! Does not own: route policy or index-store traversal.
//! Boundary: applies one validated directional anchor to raw index scans.

use crate::{
    db::{cursor::IndexScanContinuationInput, direction::Direction, index::RawIndexStoreKey},
    error::InternalError,
};
use std::ops::Bound;

/// Typed key input for continuation advancement checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ContinuationKeyRef<'a> {
    raw_key: &'a RawIndexStoreKey,
}

impl<'a> ContinuationKeyRef<'a> {
    /// Build one scan-key reference.
    #[must_use]
    pub(in crate::db) const fn scan(raw_key: &'a RawIndexStoreKey) -> Self {
        Self { raw_key }
    }
}

/// Cursor-owned scan continuation runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ContinuationRuntime<'a> {
    scan: IndexScanContinuationInput<'a>,
}

impl<'a> ContinuationRuntime<'a> {
    /// Build one scan continuation runtime.
    #[must_use]
    pub(in crate::db) const fn new(scan: IndexScanContinuationInput<'a>) -> Self {
        Self { scan }
    }

    /// Derive resumed physical bounds.
    pub(in crate::db) fn scan_bounds(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), InternalError> {
        self.scan.resume_bounds(bounds)
    }

    /// Enforce strict directional advancement for one candidate key.
    pub(in crate::db) fn accept_key(
        &self,
        key: ContinuationKeyRef<'_>,
    ) -> Result<(), InternalError> {
        self.scan.validate_candidate_advancement(key.raw_key)
    }

    /// Return the scan direction.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.scan.direction()
    }
}
