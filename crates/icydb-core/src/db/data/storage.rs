//! Module: data::storage
//! Responsibility: short aliases for raw structural value-storage codecs.
//! Boundary: preserves the original structural-field functions while giving
//! callers a semantic namespace.

pub(in crate::db::data) mod decode {}

pub(in crate::db::data) mod encode {}
