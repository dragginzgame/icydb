//! Module: catalog
//! Responsibility: accepted-schema admission example and owner-local state.
//! Does not own: SQL parsing, physical storage, or generated model fallback.
//! Boundary: validates catalog requests before publishing accepted snapshots.

mod admission;
mod snapshot;

#[cfg(test)]
mod tests;

use crate::{
    diagnostic::StyleDiagnostic,
    plan::{PlanRoute, PlanRouteKind},
};
use std::collections::BTreeMap;

pub use admission::{CatalogAdmission, CatalogAdmissionReport};
pub(crate) use snapshot::AcceptedSchemaSnapshot;

const MAX_INDEX_FIELDS: usize = 4;

///
/// CatalogExample
///
/// Owner-local catalog example used to demonstrate accepted snapshot flow.
/// The catalog owns runtime authority; generated model conveniences are not
/// used to reconstruct accepted state.
///

#[derive(Default)]
pub struct CatalogExample {
    snapshots: BTreeMap<String, AcceptedSchemaSnapshot>,
}

impl CatalogExample {
    /// Admit one accepted schema snapshot and return the route used to publish it.
    pub fn admit(
        &mut self,
        entity_name: impl Into<String>,
        version: u32,
    ) -> Result<CatalogAdmissionReport, StyleDiagnostic> {
        let admission = CatalogAdmission::new(entity_name, version)?;
        let snapshot = admission.accepted_snapshot();
        let route = PlanRoute::new(PlanRouteKind::CatalogMutation, admission.entity_name())?;

        self.snapshots
            .insert(admission.entity_name().to_owned(), snapshot);

        Ok(CatalogAdmissionReport::new(admission, route))
    }

    /// Return the accepted schema version for one entity when it is known.
    #[must_use]
    pub fn snapshot_version(&self, entity_name: &str) -> Option<u32> {
        self.snapshots
            .get(entity_name)
            .map(AcceptedSchemaSnapshot::version)
    }

    /// Return the accepted entity name stored for one catalog key.
    #[must_use]
    pub fn snapshot_entity_name(&self, entity_name: &str) -> Option<&str> {
        self.snapshots
            .get(entity_name)
            .map(AcceptedSchemaSnapshot::entity_name)
    }

    /// Return a read route for one entity without mutating accepted state.
    pub fn read_route(&self, entity_name: &str) -> Result<PlanRoute, StyleDiagnostic> {
        PlanRoute::new(PlanRouteKind::CatalogRead, entity_name)
    }

    /// Return the example index-field bound used by admission callers.
    #[must_use]
    pub const fn max_index_fields() -> usize {
        MAX_INDEX_FIELDS
    }
}
