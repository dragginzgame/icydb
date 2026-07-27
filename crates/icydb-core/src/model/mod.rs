//! Module: model
//!
//! Responsibility: runtime field-kind and storage-codec vocabulary.
//! Does not own: declarative schema ASTs or macro-time code generation surfaces.
//! Boundary: accepted schema construction consumes these field-level atoms;
//! generated entity or index models never enter runtime authority.
//!
//! This module contains only field-level runtime representations retained by
//! accepted schema, planning, execution, and storage.

pub(crate) mod field;
#[cfg(any(test, feature = "sql"))]
pub(crate) mod field_kind_semantics;

// re-exports
pub use field::{
    CompositeCodec, CompositeElementModel, CompositeFieldModel, CompositeShapeModel,
    DEFAULT_BIG_INT_MAX_BYTES, EnumVariantModel, FieldDatabaseDefault, FieldInsertGeneration,
    FieldKind, FieldModel, FieldStorageDecode, FieldWriteManagement,
};
#[cfg(any(test, feature = "sql"))]
pub(crate) use field_kind_semantics::canonicalize_grouped_having_numeric_literal_for_field_kind;
#[cfg(any(test, feature = "sql"))]
pub(crate) use field_kind_semantics::classify_field_kind;
