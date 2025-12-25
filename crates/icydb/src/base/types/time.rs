use crate::design::prelude::*;

///
/// CreatedAt
/// if zero gets sanitized to the current Timestamp
///

#[newtype(
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    ty(sanitizer(path = "base::sanitizer::time::CreatedAt"))
)]
pub struct CreatedAt {}

///
/// UpdatedAt
/// always gets sanitized to the current Timestamp
///

#[newtype(
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    ty(sanitizer(path = "base::sanitizer::time::UpdatedAt"))
)]
pub struct UpdatedAt {}

///
/// Milliseconds
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Milliseconds {}

///
/// Seconds
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Seconds {}

///
/// Minutes
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Minutes {}
