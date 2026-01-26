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
pub mod entity;
pub mod field;
pub mod index;
