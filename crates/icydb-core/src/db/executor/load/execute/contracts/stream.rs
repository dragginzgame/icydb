use crate::db::executor::{ExecutionOptimization, OrderedKeyStream, OrderedKeyStreamBox};
use std::{cell::Cell, rc::Rc};

///
/// ResolvedExecutionKeyStream
///
/// Canonical key-stream resolution output for one load execution attempt.
/// Keeps fast-path metadata and fallback stream output on one shared boundary.
///

pub(in crate::db::executor) struct ResolvedExecutionKeyStream {
    key_stream: OrderedKeyStreamBox,
    optimization: Option<ExecutionOptimization>,
    rows_scanned_override: Option<usize>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
}

impl ResolvedExecutionKeyStream {
    /// Construct one resolved key-stream payload.
    #[must_use]
    pub(in crate::db::executor) fn new(
        key_stream: OrderedKeyStreamBox,
        optimization: Option<ExecutionOptimization>,
        rows_scanned_override: Option<usize>,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
    ) -> Self {
        Self {
            key_stream,
            optimization,
            rows_scanned_override,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped_counter,
        }
    }

    /// Decompose resolved key-stream payload into raw parts.
    #[must_use]
    #[expect(clippy::type_complexity)]
    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (
        OrderedKeyStreamBox,
        Option<ExecutionOptimization>,
        Option<usize>,
        bool,
        u64,
        Option<Rc<Cell<u64>>>,
    ) {
        (
            self.key_stream,
            self.optimization,
            self.rows_scanned_override,
            self.index_predicate_applied,
            self.index_predicate_keys_rejected,
            self.distinct_keys_deduped_counter,
        )
    }

    /// Borrow mutable ordered key stream.
    pub(in crate::db::executor) fn key_stream_mut(&mut self) -> &mut dyn OrderedKeyStream {
        self.key_stream.as_mut()
    }

    /// Return optional rows-scanned override.
    #[must_use]
    pub(in crate::db::executor) const fn rows_scanned_override(&self) -> Option<usize> {
        self.rows_scanned_override
    }

    /// Return resolved optimization label.
    #[must_use]
    pub(in crate::db::executor) const fn optimization(&self) -> Option<ExecutionOptimization> {
        self.optimization
    }

    /// Return whether index predicate was applied during access stream resolution.
    #[must_use]
    pub(in crate::db::executor) const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    /// Return count of index predicate key rejections during stream resolution.
    #[must_use]
    pub(in crate::db::executor) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    /// Return distinct deduplicated key count for this resolved stream.
    #[must_use]
    pub(in crate::db::executor) fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get())
    }
}
