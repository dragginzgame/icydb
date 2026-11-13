//! Module: value::coercion
//! Responsibility: coarse coercion-family classification for dynamic values.
//! Does not own: detailed scalar coercion rules or planner/operator semantics.
//! Boundary: lightweight routing surface used by coercion tables.

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
    /// Numeric-family routing, including temporal scalar representations.
    Numeric, // Decimal, Float, Duration, Timestamp, …
    /// Text values routed through textual coercions.
    Textual, // Text
    /// Identifier values such as principals, ULIDs and subaccounts.
    Identifier, // Ulid, Principal, Subaccount
    /// Nominal enum values routed by type and variant.
    Enum, // Enum(type, variant)
    /// Collection values routed through element operations.
    Collection, // List
    /// Explicit heterogeneous values with no coercion routes.
    Opaque,
    /// Opaque binary values.
    Blob, // Blob(Vec<u8>)
    /// Boolean values.
    Bool,
    /// Absent optional values.
    Null, // Value::Null
    /// Internal unit placeholders.
    Unit, // Value::Unit
}
