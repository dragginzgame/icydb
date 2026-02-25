use crate::db::executor::route::ExecutionRoutePlan;

///
/// ExecutionPlan
///
/// Canonical route-to-kernel execution contract for read execution.
/// Phase 1 of 0.30 keeps this as a transparent alias to the existing
/// route payload so behavior remains unchanged while callsites converge.
///
pub(in crate::db::executor) type ExecutionPlan = ExecutionRoutePlan;
