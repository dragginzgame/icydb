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
