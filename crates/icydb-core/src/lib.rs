//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
pub mod db;
pub mod hash;
pub mod index;
pub mod interface;
pub mod key;
pub mod macros;
pub mod obs;
pub mod serialize;
pub mod traits;
pub mod types;
pub mod value;
pub mod view;
pub mod visitor;

pub use index::IndexSpec;
pub use key::Key;
pub use serialize::{deserialize, serialize};
pub use value::Value;
pub use visitor::{sanitize, validate};

///
/// CONSTANTS
///

/// Maximum number of indexed fields allowed on an entity.
///
/// This limit keeps hashed index keys within bounded, storable sizes and
/// simplifies sizing tests in the stores.
pub const MAX_INDEX_FIELDS: usize = 4;

use candid::CandidType;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

///
/// Error
///
/// top level error should handle all sub-errors, but not expose the candid types
/// as that would be a lot of them
///

#[derive(CandidType, Debug, Deserialize, Serialize, ThisError)]
pub enum Error {
    #[error("{0}")]
    DbError(String),

    #[error("{0}")]
    InterfaceError(String),

    #[error("{0}")]
    SerializeError(String),

    #[error("{0}")]
    VisitorError(String),
}

macro_rules! from_to_string {
    ($from:ty, $variant:ident) => {
        impl From<$from> for Error {
            fn from(e: $from) -> Self {
                Error::$variant(e.to_string())
            }
        }
    };
}

from_to_string!(db::DbError, DbError);
from_to_string!(interface::InterfaceError, InterfaceError);
from_to_string!(serialize::SerializeError, SerializeError);
from_to_string!(visitor::VisitorError, VisitorError);
