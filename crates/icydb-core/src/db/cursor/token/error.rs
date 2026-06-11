//! Module: db::cursor::token::error
//! Responsibility: cursor token wire encode/decode error taxonomy.
//! Does not own: higher-level cursor validation or continuation policy.
//! Boundary: local error surface for cursor token serialization helpers.

///
/// TokenWireError
///
/// Cursor token wire encode/decode failures.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum TokenWireError {
    Encode,

    Decode,
}

impl TokenWireError {
    pub(in crate::db::cursor::token) const fn encode() -> Self {
        Self::Encode
    }

    pub(in crate::db::cursor::token) const fn decode() -> Self {
        Self::Decode
    }
}
