//! Module: types
//!
//! Responsibility: engine operations over schema-owned scalar atoms and
//! core-owned identity types.
//! Does not own: canonical scalar representation, dynamic `Value` semantics,
//! or schema planning policy.
//! Boundary: exact scalar re-exports plus storage, generation, visitor, and
//! runtime behavior retained by the engine.

mod account;
mod blob;
mod date;
mod decimal;
mod duration;
mod float;
mod identity;
mod int_big;
mod nat_big;
mod principal;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod random;
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
pub use icydb_schema::NumericValue;
pub use icydb_schema::TypeParseError;
pub use identity::*;
pub use int_big::*;
pub use nat_big::*;
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
// - Timestamp, Principal, Ulid, Blob, Decimal, NatBig, IntBig, and Unit are
//   their own transport representation.
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

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[test]
    fn core_scalar_surface_is_the_exact_schema_owned_type_identity() {
        let _: Account = icydb_schema::Account::from(icydb_schema::Principal::anonymous());
        let _: Blob = icydb_schema::Blob::default();
        let _: Date = icydb_schema::Date::EPOCH;
        let _: Decimal = icydb_schema::Decimal::default();
        let _: Duration = icydb_schema::Duration::ZERO;
        let _: Float32 = icydb_schema::Float32::default();
        let _: Float64 = icydb_schema::Float64::default();
        let _: IntBig = icydb_schema::IntBig::default();
        let _: NatBig = icydb_schema::NatBig::default();
        let _: Principal = icydb_schema::Principal::anonymous();
        let _: Subaccount = icydb_schema::Subaccount::MIN;
        let _: Timestamp = icydb_schema::Timestamp::EPOCH;
        let _: Ulid = icydb_schema::Ulid::nil();
        let _: Unit = icydb_schema::Unit;
    }
}
