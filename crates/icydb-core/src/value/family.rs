//! Semantic classification for `Value`.
//!
//! `ValueTag` is a hashing/serialization enum (repr(u8)).
//! This module adds a *semantic* layer so filter/coercion logic
//! can decide:
//!   - which values are numeric
//!   - which are textual
//!   - which are identifiers (`Ulid`, `Principal`, etc.)
//!   - which are collections
//!
//! This avoids hardcoding tag comparisons all over the evaluator.

/// High-level semantic classification of a `Value`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueFamily {
    Numeric,    // Int, Uint, Decimal, Float, Duration, Timestamp, …
    Textual,    // Text
    Identifier, // Ulid, Principal, Subaccount
    Enum,       // Enum(type, variant)
    Collection, // List
    Blob,       // Blob(Vec<u8>)
    Bool,
    Null, // Value::None
    Unit, // Value::Unit
    Unsupported,
}

impl ValueFamily {
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        matches!(self, Self::Numeric)
    }

    #[must_use]
    pub const fn is_textual(self) -> bool {
        matches!(self, Self::Textual)
    }

    #[must_use]
    pub const fn is_identifier(self) -> bool {
        matches!(self, Self::Identifier)
    }

    #[must_use]
    pub const fn is_collection(self) -> bool {
        matches!(self, Self::Collection)
    }

    #[must_use]
    pub const fn is_enum(self) -> bool {
        matches!(self, Self::Enum)
    }

    #[must_use]
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub const fn is_scalar(self) -> bool {
        // scalar = not collection, not unit
        !self.is_collection() && !matches!(self, Self::Unit)
    }
}

///
/// Extension trait mapping `ValueTag → ValueFamily`.
///

pub trait ValueFamilyExt {
    fn family(&self) -> ValueFamily;
}
