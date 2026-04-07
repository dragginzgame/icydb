//! Module: db::executor::authority
//! Responsibility: executor-owned authority bundles and read-authority classification.
//! Does not own: execution kernels, store access, or typed API entrypoints.
//! Boundary: centralizes structural entity identity plus evidence-backed
//! secondary read authority.
//!
//! `0.70.2` keeps one explicit rule here:
//! - `ResolvedSecondaryReadAuthorityProfile` is the canonical behavior source
//! - flat classifier labels are projection-only compatibility surfaces
//!
//! Production code must not reintroduce classifier-driven correctness
//! decisions outside this module boundary.

mod entity;
mod read;

pub use entity::EntityAuthority;

// Re-export the current executor-local authority seams from the module root so
// sibling executor modules can stay coupled to this boundary instead of the
// file layout behind it.
#[allow(unused_imports)]
pub(in crate::db::executor) use read::resolve_secondary_read_authority_profile;
