//! Bounded public schema-proposal contract for IcyDB.
//!
//! This crate owns proposal vocabulary only. It does not own accepted schema,
//! runtime planning, storage, clocks, generators, application callbacks, or
//! persisted row formats.

mod account;
mod atom;
mod bounds;
mod codec;
mod date;
mod decimal;
mod error;
mod expression;
mod fragment;
mod int_big;
mod key;
mod nat_big;
mod numeric_value;
mod proposal;
#[macro_use]
mod scalar_macros;
mod scalar;
mod subaccount;
mod time_atoms;
mod unit;

pub use account::*;
pub use atom::*;
pub use bounds::*;
pub use codec::*;
pub use date::*;
pub use decimal::*;
pub use error::*;
pub use expression::*;
pub use fragment::*;
pub use int_big::*;
pub use key::*;
pub use nat_big::*;
pub use numeric_value::*;
pub use proposal::*;
pub use scalar::*;
pub use subaccount::*;
pub use time_atoms::*;
pub use unit::*;

#[cfg(test)]
mod tests;
