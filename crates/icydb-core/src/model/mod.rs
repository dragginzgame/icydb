//! Runtime data model definitions.
//!
//! This module contains the *runtime representations* of schema-level concepts,
//! as opposed to their declarative or macro-time forms. Types in `model` are
//! instantiated and used directly by query planning, executors, and storage
//! layers.
//!
//! Currently this includes index-related models, but the module is intended to
//! grow to encompass additional runtime schema nodes (e.g. entities, fields,
//! or constraints) as IcyDB’s internal model is made more explicit.
//!
//! In general:
//! - Schema / macro code defines *what exists*
//! - `model` defines *what runs*
//!
//! Model types are **internal** runtime artifacts derived from typed entities.
//! Downstream code should not construct them manually except in tests that
//! intentionally exercise invalid or edge-case schemas.

pub(crate) mod entity;
pub(crate) mod field;
pub(crate) mod index;

// re-exports
pub use entity::EntityModel;
pub use field::FieldModel;
pub use index::IndexModel;
