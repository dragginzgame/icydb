//! Module: access::execution_contract::strategy
//! Responsibility: access strategy object combining executable path contract and route class.
//! Does not own: planner access-plan selection or executor dispatch precedence semantics.
//! Boundary: centralizes access lowering plus route-class derivation under access ownership.

use crate::db::access::{
    execution_contract::{
        AccessRouteClass, ExecutableAccessPath, ExecutableAccessPlan,
        summary::summarize_executable_access_plan,
    },
    lowering::lower_executable_access_plan,
    plan::AccessPlan,
};
use crate::db::executor::planning::route::LoadOrderRouteContract;
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
    class: AccessRouteClass,
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
        let class = executable.class();
        Self { executable, class }
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

    /// Return access-owned route class capability snapshot.
    #[must_use]
    pub(in crate::db) const fn class(&self) -> AccessRouteClass {
        self.class
    }

    /// Borrow direct path payload when this strategy is single-path.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutableAccessPath<'a, K>> {
        self.executable.as_path()
    }

    /// Derive a load-window early-stop scan-budget hint for this access shape.
    ///
    /// This helper keeps access-shape mechanics (`ordered` stream support)
    /// centralized under `AccessStrategy`, while callers provide route-owned
    /// continuation and streaming-safety policy gates.
    #[must_use]
    pub(in crate::db) const fn load_window_early_stop_hint(
        &self,
        continuation_applied: bool,
        load_order_route_contract: LoadOrderRouteContract,
        fetch_count: Option<usize>,
    ) -> Option<usize> {
        if continuation_applied {
            return None;
        }
        if !load_order_route_contract.allows_streaming_load() {
            return None;
        }
        if !self.class().ordered() {
            return None;
        }

        fetch_count
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
            .field("class", &self.class)
            .finish()
    }
}
