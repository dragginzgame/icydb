//! Module: catalog::admission
//! Responsibility: catalog admission request and report contracts.
//! Does not own: snapshot storage or planning execution.
//! Boundary: turns caller input into owner-approved catalog facts.

use crate::{catalog::AcceptedSchemaSnapshot, diagnostic::StyleDiagnostic, plan::PlanRoute};

///
/// CatalogAdmission
///
/// Validated request to admit one accepted schema snapshot.
/// Admission owns input normalization but does not persist catalog state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAdmission {
    entity_name: String,
    version: u32,
}

impl CatalogAdmission {
    /// Build one validated catalog admission request.
    pub fn new(entity_name: impl Into<String>, version: u32) -> Result<Self, StyleDiagnostic> {
        let entity_name = entity_name.into();
        let entity_name = entity_name.trim();

        if entity_name.is_empty() {
            return Err(StyleDiagnostic::empty_entity_name());
        }

        if version == 0 {
            return Err(StyleDiagnostic::missing_snapshot_version());
        }

        Ok(Self {
            entity_name: entity_name.to_owned(),
            version,
        })
    }

    /// Return the accepted entity name.
    #[must_use]
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Return the accepted schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Convert this admission into an accepted snapshot owned by the catalog.
    #[must_use]
    pub(crate) fn accepted_snapshot(&self) -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(&self.entity_name, self.version)
    }
}

///
/// CatalogAdmissionReport
///
/// Result envelope returned after a catalog admission has been accepted.
/// The report carries the validated admission and the route selected for
/// publication without exposing catalog storage internals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAdmissionReport {
    admission: CatalogAdmission,
    route: PlanRoute,
}

impl CatalogAdmissionReport {
    /// Build one report from an accepted admission and route.
    #[must_use]
    pub const fn new(admission: CatalogAdmission, route: PlanRoute) -> Self {
        Self { admission, route }
    }

    /// Return the accepted admission.
    #[must_use]
    pub const fn admission(&self) -> &CatalogAdmission {
        &self.admission
    }

    /// Return the route chosen for publication.
    #[must_use]
    pub const fn route(&self) -> &PlanRoute {
        &self.route
    }
}
