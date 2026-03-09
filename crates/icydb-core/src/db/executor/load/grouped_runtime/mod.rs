//! Module: executor::load::grouped_runtime
//! Responsibility: runtime grouped pagination contracts and grouped continuation helpers.
//! Does not own: grouped planner policy derivation or route feasibility selection.
//! Boundary: provides grouped runtime primitives consumed by load/fold stages.

mod continuation;
mod runtime;

pub(in crate::db::executor::load) use continuation::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
};
pub(in crate::db::executor::load) use runtime::{
    GroupedExecutionContext, GroupedRuntimeProjection,
};
