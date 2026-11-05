//! Test-only constructors and adapters for the parent module.

use super::*;

impl PlanError {
    pub(crate) fn into_kind(self) -> PlanErrorKind {
        self.kind
    }
}
