//! Module: db::cursor::token::error
//! Responsibility: module-local ownership and contracts for db::cursor::token::error.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use thiserror::Error as ThisError;

///
/// TokenWireError
/// Cursor token wire encode/decode failures.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum TokenWireError {
    #[error("failed to encode cursor token: {0}")]
    Encode(String),

    #[error("failed to decode cursor token: {0}")]
    Decode(String),
}

impl TokenWireError {
    pub(in crate::db::cursor::token) fn encode(reason: impl Into<String>) -> Self {
        Self::Encode(reason.into())
    }

    pub(in crate::db::cursor::token) fn decode(reason: impl Into<String>) -> Self {
        Self::Decode(reason.into())
    }
}
