//! Module: db::executor::authority
//! Responsibility: executor-owned authority bundles and read-authority classification.
//! Does not own: execution kernels, store access, or typed API entrypoints.
//! Boundary: centralizes structural entity identity plus evidence-backed secondary read authority.

mod entity;
mod read;

pub use entity::EntityAuthority;
pub(in crate::db::executor) use read::{
    SecondaryReadAuthorityOwner, classify_secondary_read_authority_explain_labels,
    classify_secondary_read_existing_row_mode, derive_secondary_covering_authority_profile,
    secondary_read_authority_owner,
};
