use crate::db::executor::route::ExecutionRoutePlan;

///
/// ExecutionPlan
///
/// Canonical route-to-kernel execution contract for read execution.
/// This is route-owned policy output (mode, hints, fast-path ordering),
/// while `ExecutablePlan` remains the validated query/lowered-spec container.
/// Keeping this alias explicit preserves a distinct boundary at call sites.
///
pub(in crate::db::executor) type ExecutionPlan = ExecutionRoutePlan;
