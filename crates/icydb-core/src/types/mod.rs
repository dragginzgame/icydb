//! Module: types
//!
//! Responsibility: engine operations over schema-owned scalar atoms and
//! core-owned identity types.
//! Does not own: canonical scalar representation, dynamic `Value` semantics,
//! or schema planning policy.
//! Boundary: exact scalar re-exports plus storage, generation, and runtime
//! behavior retained by the engine.

mod account;
mod duration;
mod identity;
mod principal;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod random;
mod subaccount;
mod timestamp;
mod ulid;
mod unit;

pub use account::*;
pub use duration::*;
pub use icydb_schema::{
    Blob, Date, Decimal, DecimalParts, Float32, Float32DecodeError, Float64, Float64DecodeError,
    NumericValue, ParseDecimalError, ParseDecimalErrorReason, ParseU256Error, TypeParseError, U256,
};
pub use identity::*;
pub use principal::*;
pub use subaccount::*;
pub use timestamp::*;
pub use ulid::*;
pub use unit::*;

pub(crate) use ulid::generate_ulid_at;

//
// Type Representation Overview
//
// - Float32 and Float64 normalize their primitive transport form
//   (finite only, -0.0 -> 0.0).
// - Timestamp, Principal, Ulid, Blob, Decimal and Unit own
//   their transport representations.
//
// Notes
// - Display for fixed‑point types prints normalized decimal (human‑readable),
//   not raw atomics.
// - Ulid uses exactly 16 bytes in Wire/binary Serde and canonical text in
//   human-readable Serde.

/// Schema-facing alias for a boolean.
pub type Bool = bool;
/// Schema-facing alias for a signed 8-bit integer.
pub type Int8 = i8;
/// Schema-facing alias for a signed 16-bit integer.
pub type Int16 = i16;
/// Schema-facing alias for a signed 32-bit integer.
pub type Int32 = i32;
/// Schema-facing alias for a signed 64-bit integer.
pub type Int64 = i64;
/// Schema-facing alias for an unsigned 8-bit integer.
pub type Nat8 = u8;
/// Schema-facing alias for an unsigned 16-bit integer.
pub type Nat16 = u16;
/// Schema-facing alias for an unsigned 32-bit integer.
pub type Nat32 = u32;
/// Schema-facing alias for an unsigned 64-bit integer.
pub type Nat64 = u64;
/// Schema-facing alias for an owned UTF-8 string.
pub type Text = String;

#[cfg(test)]
mod ownership_tests;
