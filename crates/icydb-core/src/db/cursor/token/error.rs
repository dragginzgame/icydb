//! Module: db::cursor::token::error
//! Responsibility: cursor token wire encode/decode error taxonomy.
//! Does not own: higher-level cursor validation or continuation compatibility policy.
//! Boundary: local error surface for cursor token serialization helpers.

use thiserror::Error as ThisError;

///
/// TokenWireError
///
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
