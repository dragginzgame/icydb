//! Module: lib
//! Responsibility: documentation-only crate root for IcyDB style examples.
//! Does not own: runtime behavior, workspace crate API, or production contracts.
//! Boundary: exposes a small catalog and planning surface used only by docs.

pub mod catalog;
pub mod diagnostic;
pub mod plan;

pub use catalog::{CatalogAdmission, CatalogAdmissionReport};
pub use diagnostic::{StyleDiagnostic, StyleDiagnosticCode};
pub use plan::{PlanRoute, PlanRouteKind};
