//! Module: model
//!
//! Responsibility: runtime schema-model types consumed by planning and execution.
//! Does not own: declarative schema ASTs or macro-time code generation surfaces.
//! Boundary: internal runtime model layer derived from typed entities and indexes.
//!
//! This module contains the runtime representations of schema-level concepts,
//! as opposed to their declarative or macro-time forms. Types in `model` are
//! instantiated and used directly by query planning, executors, and storage
//! layers.

pub(crate) mod entity;
pub(crate) mod field;
pub(crate) mod field_kind_semantics;
pub(crate) mod index;

// re-exports
pub use entity::{
    EntityModel, PrimaryKeyModel, PrimaryKeyModelFieldIter, PrimaryKeyModelFields,
    RelationEdgeModel,
};
pub use field::{
    DEFAULT_BIG_INT_MAX_BYTES, EnumVariantModel, FieldDatabaseDefault, FieldInsertGeneration,
    FieldKind, FieldModel, FieldStorageDecode, FieldWriteManagement, RelationEnforcement,
};
#[cfg(test)]
pub(crate) use field_kind_semantics::field_kind_has_identity_group_canonical_form;
pub(crate) use field_kind_semantics::{
    canonicalize_filter_literal_for_kind,
    canonicalize_grouped_having_numeric_literal_for_field_kind,
};
#[cfg(any(test, feature = "sql"))]
pub(crate) use field_kind_semantics::{
    canonicalize_strict_sql_literal_for_kind, classify_field_kind,
};
pub use index::{
    GeneratedIndexPredicateResolver, IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel,
    IndexPredicateMetadata,
};
