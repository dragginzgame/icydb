//! Module: query::plan::expr::function_semantics
//! Responsibility: planner-owned scalar function taxonomy and semantic facets.
//! Does not own: parser identifier resolution, expression lowering, or runtime evaluation.
//! Boundary: central registry for scalar function category, null behavior, typing shape, and surfaces.

mod evaluation;
mod spec;
mod types;

#[cfg(feature = "sql")]
pub(in crate::db) use types::FunctionSurface;
pub(in crate::db::query::plan::expr) use types::{
    AggregateInputConstantFoldShape, BooleanFunctionShape, FieldPredicateFunctionKind,
    FunctionTypeInferenceShape, NullTestFunctionKind, ScalarEvalFunctionShape,
};
pub(in crate::db) use types::{NumericSubtype, TextPredicateFunctionKind};
