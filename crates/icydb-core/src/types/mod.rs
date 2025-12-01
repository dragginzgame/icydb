mod account;
mod blob;
mod date;
mod decimal;
mod duration;
mod e18s;
mod e8s;
mod float;
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
pub use e8s::*;
pub use e18s::*;
pub use float::*;
pub use int::*;
pub use nat::*;
pub use principal::*;
pub use subaccount::*;
pub use timestamp::*;
pub use ulid::*;
pub use unit::*;

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

//
// TypeView Mapping Overview
//
// - Float32: view = f32 (sanitized; finite only, -0.0 → 0.0)
// - Float64: view = f64 (sanitized; finite only, -0.0 → 0.0)
// - E8s:     view = u64 (raw atomics)
// - E18s:    view = u128 (raw atomics)
// - Timestamp, Principal, Ulid, Blob, Decimal, Nat, Int, Unit: view = Self
//
// Notes
// - Display for fixed‑point types prints normalized decimal (human‑readable),
//   not raw atomics.
// - Ulid serde deserialization fails on invalid strings.
