pub mod list;
pub mod map;
pub mod merge;
pub mod set;

pub use list::ListPatch;
pub use map::MapPatch;
pub use merge::{MergePatch, MergePatchError};
pub use set::SetPatch;

///
/// AtomicPatch
///
/// Marker trait for values whose patch semantics are **full replacement**.
///
/// Types implementing `AtomicPatch` are treated as indivisible at the patch layer:
/// their `MergePatch` implementation replaces the entire value rather than
/// performing a structural merge.
///
/// This is appropriate for:
/// - primitive values
/// - numeric wrappers
/// - timestamps
/// - fixed-point / scalar domain types
///
/// Invariant:
/// `AtomicPatch` types must correspond to `FieldValueKind::Atomic`.
///
/// This trait has no methods; it exists solely to opt a type into
/// overwrite-only merge semantics via a blanket `MergePatch` implementation.
///

pub trait AtomicPatch: Sized {}
