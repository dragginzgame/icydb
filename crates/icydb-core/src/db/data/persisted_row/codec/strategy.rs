use crate::model::field::{FieldKind, FieldStorageDecode};

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

impl StorageStrategy {
    // Select the storage strategy represented by field metadata without
    // spreading FieldStorageDecode branching across encode/decode helpers.
    pub(in crate::db::data::persisted_row::codec) const fn from_field_storage(
        decode: FieldStorageDecode,
        kind: FieldKind,
    ) -> Self {
        match decode {
            FieldStorageDecode::ByKind => Self::ByKind(kind),
            FieldStorageDecode::Value => Self::Structured,
        }
    }
}
