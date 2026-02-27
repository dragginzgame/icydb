//! Explain projection surface split from query plan internals.

mod explain;
#[cfg(test)]
pub(crate) use explain::ExplainOrderPushdown;
pub(crate) use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate,
};
