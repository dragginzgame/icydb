use crate::db::{index::RawIndexKey, query::contracts::cursor::CursorBoundary};

///
/// PlannedCursor
///
/// Executor-facing continuation state produced after cursor validation.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedCursor {
    boundary: Option<CursorBoundary>,
    index_range_anchor: Option<RawIndexKey>,
    initial_offset: u32,
}

impl PlannedCursor {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            boundary: None,
            index_range_anchor: None,
            initial_offset: 0,
        }
    }

    #[must_use]
    pub(in crate::db) const fn new(
        boundary: CursorBoundary,
        index_range_anchor: Option<RawIndexKey>,
        initial_offset: u32,
    ) -> Self {
        Self {
            boundary: Some(boundary),
            index_range_anchor,
            initial_offset,
        }
    }

    #[must_use]
    pub(in crate::db) const fn boundary(&self) -> Option<&CursorBoundary> {
        self.boundary.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn index_range_anchor(&self) -> Option<&RawIndexKey> {
        self.index_range_anchor.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.boundary.is_none() && self.index_range_anchor.is_none() && self.initial_offset == 0
    }
}

impl From<Option<CursorBoundary>> for PlannedCursor {
    fn from(value: Option<CursorBoundary>) -> Self {
        Self {
            boundary: value,
            index_range_anchor: None,
            initial_offset: 0,
        }
    }
}
