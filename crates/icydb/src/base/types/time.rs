use crate::design::prelude::*;

///
/// CreatedAt
///
/// Creation timestamp wrapper.
/// A zero value is sanitized to the current `Timestamp`.
///

#[newtype(
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    ty(sanitizer(path = "base::sanitizer::time::CreatedAt"))
)]
pub struct CreatedAt {}

///
/// UpdatedAt
///
/// Last-updated timestamp wrapper.
/// Always sanitized to the current `Timestamp`.
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
/// Duration wrapper expressed in milliseconds.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Milliseconds {}

///
/// Seconds
///
/// Duration wrapper expressed in seconds.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Seconds {}

///
/// Minutes
///
/// Duration wrapper expressed in minutes.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Minutes {}
