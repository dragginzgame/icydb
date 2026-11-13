//! Schema-version and fingerprint admission policy.

#[cfg(feature = "sql")]
mod admission;

#[cfg(feature = "sql")]
pub(in crate::db::schema) use admission::{
    SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionClassification,
    SchemaAdmissionRejectionReason, classify_schema_admission_rejection,
};
