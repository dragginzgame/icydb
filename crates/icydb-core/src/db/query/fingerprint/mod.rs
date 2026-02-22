//! Fingerprint/canonicalization components split from query plan internals.

pub(crate) mod canonical;
pub(crate) mod fingerprint;
pub(crate) mod hash_parts;

pub(crate) use crate::db::query::explain::ExplainPlan;
pub(crate) use crate::db::query::plan::LogicalPlan;
