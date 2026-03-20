//! Module: types
//!
//! Responsibility: module-local ownership and contracts for types.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod account;
mod blob;
mod date;
mod decimal;
mod duration;
mod float;
mod identity;
mod int;
mod nat;
mod principal;
mod subaccount;
mod timestamp;
mod ulid;
mod unit;

pub use account::*;
pub use blob::*;
pub use date::*;
pub use decimal::*;
pub use duration::*;
pub use float::*;
pub use identity::*;
pub use int::*;
pub use nat::*;
pub use principal::*;
pub use subaccount::*;
pub use timestamp::*;
pub use ulid::*;
pub use unit::*;

//
// Type Representation Overview
//
// - Float32 and Float64 normalize their primitive transport form
//   (finite only, -0.0 -> 0.0).
// - Timestamp, Principal, Ulid, Blob, Decimal, Nat, Int, and Unit are their
//   own transport representation.
//
// Notes
// - Display for fixed‑point types prints normalized decimal (human‑readable),
//   not raw atomics.
// - Ulid serde deserialization fails on invalid strings.

pub type Bool = bool;
pub type Int8 = i8;
pub type Int16 = i16;
pub type Int32 = i32;
pub type Int64 = i64;
pub type Nat8 = u8;
pub type Nat16 = u16;
pub type Nat32 = u32;
pub type Nat64 = u64;
pub type Text = String;
