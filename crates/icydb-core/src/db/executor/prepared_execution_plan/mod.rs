//! Module: executor::prepared_execution_plan
//! Responsibility: accepted structural prepared-plan contracts.
//! Does not own: typed entity wrappers or fluent terminal adapters.
//! Boundary: binds validated access plans to generic-free executor residents.

mod contracts;
mod core;
mod handoff;
mod load_plan;
mod shared_plan;

pub use core::ExecutionFamily;
pub(in crate::db::executor::prepared_execution_plan) use core::{
    PreparedExecutionPlanCore, build_prepared_execution_plan_core_with_lowered_access,
    build_prepared_execution_plan_core_with_schema_fingerprint,
};
pub(in crate::db::executor) use core::{PreparedGroupedRuntimeResidents, PreparedScalarPlanCore};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use handoff::SharedPreparedProjectionRuntimeHandoff;
pub(in crate::db::executor) use handoff::{
    PreparedAccessPlanHandoff, PreparedScalarRuntimeHandoff,
};
pub(in crate::db::executor) use load_plan::PreparedLoadPlan;
pub(in crate::db) use shared_plan::SharedPreparedExecutionPlan;
