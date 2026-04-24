//! Module: access::execution_contract::strategy
//! Responsibility: access strategy object combining executable path contract and route class.
//! Does not own: planner access-plan selection or executor dispatch precedence semantics.
//! Boundary: centralizes access lowering plus route-class derivation under access ownership.

use crate::db::access::{
    AccessCapabilities,
    execution_contract::{
        ExecutableAccessPath, ExecutableAccessPlan, summary::summarize_executable_access_plan,
    },
    lowering::lower_executable_access_plan,
    plan::AccessPlan,
};
use std::fmt;

///
/// AccessStrategy
///
/// Pre-resolved access execution contract produced once from planner-selected
/// access shape and consumed by runtime layers. This keeps path lowering and
/// route-class derivation under one access-owned authority object.
///

#[derive(Clone, Eq, PartialEq)]
pub(in crate::db) struct AccessStrategy<'a, K> {
    executable: ExecutableAccessPlan<'a, K>,
    capabilities: AccessCapabilities,
}

impl<'a, K> AccessStrategy<'a, K> {
    /// Resolve one access strategy from one planner-selected access plan.
    #[must_use]
    pub(in crate::db) fn from_plan(plan: &'a AccessPlan<K>) -> Self {
        let executable = lower_executable_access_plan(plan);
        Self::from_executable(executable)
    }

    /// Resolve one access strategy from one already lowered executable access plan.
    #[must_use]
    pub(in crate::db) fn from_executable(executable: ExecutableAccessPlan<'a, K>) -> Self {
        let capabilities = executable.capabilities();
        Self {
            executable,
            capabilities,
        }
    }

    /// Borrow the lowered executable access contract.
    #[must_use]
    pub(in crate::db) const fn executable(&self) -> &ExecutableAccessPlan<'a, K> {
        &self.executable
    }

    /// Consume this strategy and return the lowered executable access contract.
    #[must_use]
    pub(in crate::db) fn into_executable(self) -> ExecutableAccessPlan<'a, K> {
        self.executable
    }

    /// Return access-owned capability snapshot.
    #[must_use]
    pub(in crate::db) const fn capabilities(&self) -> AccessCapabilities {
        self.capabilities
    }

    /// Borrow direct path payload when this strategy is single-path.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutableAccessPath<'a, K>> {
        self.executable.as_path()
    }
}

impl<K> AccessStrategy<'_, K>
where
    K: fmt::Debug,
{
    /// Return one concise debug summary of the resolved access strategy shape.
    #[must_use]
    pub(in crate::db) fn debug_summary(&self) -> String {
        summarize_executable_access_plan(&self.executable)
    }
}

impl<K> fmt::Debug for AccessStrategy<'_, K>
where
    K: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessStrategy")
            .field("summary", &self.debug_summary())
            .field("capabilities", &self.capabilities)
            .finish()
    }
}
