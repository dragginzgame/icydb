//! Module: db::executor::diagnostics::outcome
//! Responsibility: executor-owned outcome labels for trace reporting.
//! Does not own: diagnostics DTO storage or trace projection formatting.
//! Boundary: runtime execution paths select these labels; diagnostics records them.

#[cfg_attr(
    doc,
    doc = "ExecutionOptimization\n\nLoad optimization selected at execution time, if any."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOptimization {
    PrimaryKey,
    PrimaryKeyTopNSeek,
    SecondaryOrderPushdown,
    SecondaryOrderTopNSeek,
    IndexRangeLimitPushdown,
}
