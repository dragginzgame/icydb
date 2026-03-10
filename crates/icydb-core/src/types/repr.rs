//! Module: types::repr
//! Responsibility: module-local ownership and contracts for types::repr.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

///
/// Repr
///
/// Internal representation boundary for scalar wrapper types.
///
pub(crate) trait Repr {
    type Inner;

    fn repr(&self) -> Self::Inner;
    fn from_repr(inner: Self::Inner) -> Self;
}
