//! Module: traits::atomic
//! Responsibility: module-local ownership and contracts for traits::atomic.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

///
/// Atomic
///
/// Marker trait for values that are **indivisible** at the semantic layer.
///
/// Types implementing `Atomic` are treated as *full-replacement values* during
/// patch application: any update replaces the entire value rather than
/// performing a structural or field-wise merge.
///
/// This is appropriate for:
/// - primitive scalars
/// - numeric and fixed-point wrappers
/// - timestamps and durations
/// - domain types with no meaningful partial update semantics
///
/// Invariant:
/// Types implementing `Atomic` must correspond to `FieldValueKind::Atomic`.
///
/// This trait has no methods. It exists solely to declare value-level
/// indivisibility, which is *consumed* by higher-level mechanisms
/// (e.g. blanket `UpdateView` merge implementations).
///

pub trait Atomic: Sized {}

macro_rules! impl_atomic {
    ($($type:ty),* $(,)?) => {
        $(
            impl Atomic for $type {}
        )*
    };
}

impl_atomic!(bool, i8, i16, i32, i64, u8, u16, u32, u64, String);
