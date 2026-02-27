#![allow(unused_imports)]

pub(crate) use crate::db::query::plan::validate::{
    GroupPlanError, validate_group_query_semantics, validate_group_spec,
};
///
/// GROUPED QUERY SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped intent/planning/validation scaffold.
/// This module re-exports grouped contracts so grouped work does not stay
/// scattered across unrelated query modules.
///
pub(crate) use crate::db::query::plan::{
    GroupAggregateKind, GroupAggregateSpec, GroupSpec, GroupedPlan,
};
