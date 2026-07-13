use crate::model::field::FieldKind;

///
/// StorageStrategy
///
/// StorageStrategy is the private lane descriptor for persisted row codec
/// operations.
/// It intentionally describes the selected storage lane only; encoding,
/// decoding, traversal, and runtime-value bridging stay with their owning
/// helpers in sibling modules.
///

#[derive(Clone, Copy)]
pub(in crate::db::data::persisted_row::codec) enum StorageStrategy {
    Scalar,
    ByKind(FieldKind),
    Structured,
}
