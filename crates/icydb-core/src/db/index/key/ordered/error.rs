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

impl std::fmt::Display for OrderedValueEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("index ordered value encode error")
    }
}

impl std::error::Error for OrderedValueEncodeError {}

impl From<OrderedValueEncodeError> for InternalError {
    fn from(_err: OrderedValueEncodeError) -> Self {
        Self::index_unsupported()
    }
}
