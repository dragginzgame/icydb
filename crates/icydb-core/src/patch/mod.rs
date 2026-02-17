pub(crate) mod list;
pub(crate) mod map;
pub(crate) mod merge;
pub(crate) mod set;

// re-exports
pub use list::ListPatch;
pub use map::MapPatch;
pub use merge::MergePatchError;
pub use set::SetPatch;
