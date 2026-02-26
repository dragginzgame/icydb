use crate::error::InternalError;
use thiserror::Error as ThisError;

///
/// OrderedValueEncodeError
///
/// Canonical index-encoding failures for one `Value` component.
///

#[derive(Debug, ThisError)]
pub(crate) enum OrderedValueEncodeError {
    #[error("null values are not indexable")]
    NullNotIndexable,

    #[error("value kind '{kind}' is not canonically index-orderable")]
    UnsupportedValueKind { kind: &'static str },

    #[error("ordered segment exceeds max length: {len} bytes (limit {max})")]
    SegmentTooLarge { len: usize, max: usize },

    #[error("account owner principal exceeds max length: {len} bytes (limit {max})")]
    AccountOwnerTooLarge { len: usize, max: usize },

    #[error("decimal exponent overflow during canonical encoding")]
    DecimalExponentOverflow,
}

impl From<OrderedValueEncodeError> for InternalError {
    fn from(err: OrderedValueEncodeError) -> Self {
        Self::index_unsupported(format!(
            "index value is not canonically order-encodable: {err}"
        ))
    }
}
