//! Module: index::key::ordered::error
//! Responsibility: canonical ordered-component encode error taxonomy.
//! Does not own: error class mapping outside ordered encoding.
//! Boundary: consumed by index-key build/predicate compile paths.

use crate::error::InternalError;

///
/// OrderedValueEncodeError
///
/// Canonical index-encoding failures for one `Value` component.
///

#[derive(Debug)]
pub(crate) enum OrderedValueEncodeError {
    NullNotIndexable,

    UnsupportedValueKind,

    SegmentTooLarge,

    AccountOwnerTooLarge,

    DecimalExponentOverflow,
}

impl From<OrderedValueEncodeError> for InternalError {
    fn from(_err: OrderedValueEncodeError) -> Self {
        Self::index_unsupported()
    }
}
