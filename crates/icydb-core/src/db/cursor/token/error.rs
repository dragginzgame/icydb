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

    #[error("unsupported cursor token version: {version}")]
    UnsupportedVersion { version: u8 },
}

impl TokenWireError {
    pub(in crate::db::cursor::token) fn encode(reason: impl Into<String>) -> Self {
        Self::Encode(reason.into())
    }

    pub(in crate::db::cursor::token) fn decode(reason: impl Into<String>) -> Self {
        Self::Decode(reason.into())
    }

    pub(in crate::db::cursor::token) const fn unsupported_version(version: u8) -> Self {
        Self::UnsupportedVersion { version }
    }
}
