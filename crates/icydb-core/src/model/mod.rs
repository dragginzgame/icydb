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
pub(crate) mod index;

// re-exports
pub use entity::EntityModel;
pub use field::{EnumVariantModel, FieldKind, FieldModel, FieldStorageDecode, RelationStrength};
pub use index::{
    GeneratedIndexPredicateResolver, IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel,
    IndexPredicateMetadata,
};
