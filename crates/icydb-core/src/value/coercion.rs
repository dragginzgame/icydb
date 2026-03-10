//! Module: value::coercion
//! Responsibility: module-local ownership and contracts for value::coercion.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Coercion-routing family classification for `Value`.
//!
//! This module defines only coarse routing categories used by coercion tables.
//! It does not define scalar capabilities.

///
/// CoercionFamily
///
/// Coarse value classification used only for coercion routing.
/// This classification MUST NOT be used to infer numeric coercion,
/// arithmetic support, ordering support, or keyability.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoercionFamily {
    Numeric,    // Int, Uint, Decimal, Float, Duration, Timestamp, …
    Textual,    // Text
    Identifier, // Ulid, Principal, Subaccount
    Enum,       // Enum(type, variant)
    Collection, // List
    Blob,       // Blob(Vec<u8>)
    Bool,
    Null, // Value::Null
    Unit, // Value::Unit
}

///
/// CoercionFamilyExt
///
/// Maps a value to its coercion-routing family.
///
pub trait CoercionFamilyExt {
    /// Returns the coercion-routing family for this value.
    fn coercion_family(&self) -> CoercionFamily;
}
