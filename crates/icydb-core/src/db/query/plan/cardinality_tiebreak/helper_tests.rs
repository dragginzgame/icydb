//! Test-only constructors and adapters for the parent module.

use super::*;

impl CardinalityTiebreakCandidate<'_> {
    /// Detach fixtures that outlive their locally prepared source plan.
    pub(in crate::db) fn into_owned(self) -> CardinalityTiebreakCandidate<'static> {
        CardinalityTiebreakCandidate {
            access: Cow::Owned(self.access.into_owned()),
            index: self.index,
            family: self.family,
            consumed_prefix_arity: self.consumed_prefix_arity,
        }
    }
}
