//! Module: query::fingerprint
//! Responsibility: deterministic query-plan fingerprint/signature primitives.
//! Does not own: explain projection construction or query-plan validation.
//! Boundary: hash surface over `query::explain` models for plan identity checks.

pub(crate) mod fingerprint;
pub(crate) mod hash_parts;
mod shape_signature;
