//! Module: db::executor::pipeline::contracts::stream
//! Defines execution-stream contracts shared by scalar pipeline traversal.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::OrderedKeyStreamBox;

///
/// ResolvedExecutionKeyStream
///
/// Canonical key-stream resolution output for one load execution attempt.
/// Keeps fast-path metadata and fallback stream output on one shared boundary.
///

pub(in crate::db::executor) struct ResolvedExecutionKeyStream {
    key_stream: OrderedKeyStreamBox,
    rows_scanned_override: Option<usize>,
}

impl ResolvedExecutionKeyStream {
    /// Construct one resolved key-stream payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        key_stream: OrderedKeyStreamBox,
        rows_scanned_override: Option<usize>,
    ) -> Self {
        Self {
            key_stream,
            rows_scanned_override,
        }
    }

    /// Decorate the owned key stream while preserving resolution metadata.
    #[must_use]
    pub(in crate::db::executor) fn decorate_key_stream(
        self,
        decorate: impl FnOnce(OrderedKeyStreamBox) -> OrderedKeyStreamBox,
    ) -> Self {
        let Self {
            key_stream,
            rows_scanned_override,
        } = self;
        let key_stream = decorate(key_stream);

        Self {
            key_stream,
            rows_scanned_override,
        }
    }

    /// Borrow the concrete owned ordered key stream.
    pub(in crate::db::executor) const fn key_stream_mut(&mut self) -> &mut OrderedKeyStreamBox {
        &mut self.key_stream
    }

    /// Return optional rows-scanned override.
    #[must_use]
    pub(in crate::db::executor) const fn rows_scanned_override(&self) -> Option<usize> {
        self.rows_scanned_override
    }
}
