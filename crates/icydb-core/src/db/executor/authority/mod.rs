//! Module: db::executor::authority
//! Responsibility: executor-owned authority bundles and read-authority classification.
//! Does not own: execution kernels, store access, or typed API entrypoints.
//! Boundary: centralizes structural entity identity plus evidence-backed
//! secondary read authority.
//!
//! `0.70.x` keeps one explicit rule here:
//! - `ResolvedSecondaryReadAuthorityProfile` is the canonical behavior source
//! - flat classifier labels are projection-only compatibility surfaces
//! - secondary read authority consumes immutable snapshot inputs from the
//!   registry boundary instead of borrowing live `StoreHandle` state here
//!
//! Production code must not reintroduce classifier-driven correctness
//! decisions outside this module boundary, and `authority/read.rs` must stay
//! snapshot-only for store-backed secondary read truth ingestion.

mod entity;
mod read;

pub use entity::EntityAuthority;

// Re-export the current executor-local authority seams from the module root so
// sibling executor modules can stay coupled to this boundary instead of the
// file layout behind it.
#[allow(unused_imports)]
pub(in crate::db::executor) use read::ResolvedSecondaryReadAuthorityProfile;
#[allow(unused_imports)]
pub(in crate::db::executor) use read::resolve_secondary_read_authority_profile;
