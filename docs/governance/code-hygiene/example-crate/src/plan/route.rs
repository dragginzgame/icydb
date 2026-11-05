//! Module: plan::route
//! Responsibility: route labels and route-kind classification.
//! Does not own: catalog validation or execution-side effects.
//! Boundary: validates route labels before execution-facing code receives them.

#[cfg(test)]
mod tests;

use crate::diagnostic::StyleDiagnostic;

///
/// PlanRouteKind
///
/// Coarse route family selected by the owner module.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanRouteKind {
    CatalogMutation,
    CatalogRead,
}

impl PlanRouteKind {
    /// Return whether this route can mutate accepted catalog state.
    #[must_use]
    pub const fn is_write(self) -> bool {
        matches!(self, Self::CatalogMutation)
    }
}

///
/// PlanRoute
///
/// Validated route selected by a catalog owner before execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanRoute {
    kind: PlanRouteKind,
    label: String,
}

impl PlanRoute {
    /// Build one validated route.
    pub fn new(kind: PlanRouteKind, label: impl Into<String>) -> Result<Self, StyleDiagnostic> {
        let label = label.into();
        let label = label.trim();

        if label.is_empty() {
            return Err(StyleDiagnostic::empty_plan_route());
        }

        Ok(Self {
            kind,
            label: label.to_owned(),
        })
    }

    /// Return the route family.
    #[must_use]
    pub const fn kind(&self) -> PlanRouteKind {
        self.kind
    }

    /// Return the normalized route label.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }
}
