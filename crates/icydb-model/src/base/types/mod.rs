//! Module: base::types
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

pub mod bytes;
pub mod color;
pub mod finance;
pub mod geo;
pub mod hash;
pub mod ic;
pub mod ident;
pub mod intl;
pub mod lang;
pub mod num;
pub mod time;
pub mod web;
