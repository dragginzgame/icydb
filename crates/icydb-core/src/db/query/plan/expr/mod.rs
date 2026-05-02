//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.
//!
//! Pipeline contract:
//!
//! canonicalize -> type_inference:
//! - input: planner `Expr` trees after affine rewrite and boolean
//!   canonicalization where boolean contexts use normalized `AND` / `OR`,
//!   bounded searched-`CASE` lowering, and explicit truth-admission wrappers.
//! - output: the same expression shape plus the guarantee that boolean
//!   normalization, CASE lowering, and constant boolean simplification are
//!   owned upstream, not rediscovered by type inference.
//! - forbidden: type inference must not reorder boolean trees, lower CASE,
//!   collapse truth wrappers, or choose runtime predicate coercions.
//! - ownership: canonicalize owns boolean shape/null-admission behavior;
//!   type inference owns schema field resolution and coarse expression type
//!   classification only.
//!
//! type_inference -> predicate_compile:
//! - input: canonical boolean expressions. Predicate subset derivation is
//!   intentionally schema-independent, so it consumes `CanonicalExpr` rather
//!   than `TypedExpr`; schema-aware legality remains owned by validation and
//!   type inference.
//! - output: runtime `Predicate` shells or no predicate subset when the
//!   normalized expression cannot be represented by the predicate runtime.
//! - forbidden: predicate compilation must not infer schema types, inspect
//!   field models, re-run function argument typing, canonicalize expressions,
//!   or rewrite expression shape.
//! - ownership: type inference owns type/nullability classification;
//!   predicate compilation owns only compile-ready boolean-shape admission and
//!   leaf-local runtime predicate coercion selection while lowering already
//!   canonical compare/function leaves.
//!
//! predicate_compile -> projection_eval:
//! - input: projection evaluation receives already-bound scalar expression
//!   arguments and builder preview expressions, not predicate compiler output.
//! - output: scalar `Value` results under SQL three-valued expression
//!   semantics, preserving checked numeric failures for executor paths.
//! - forbidden: projection evaluation must not canonicalize expressions,
//!   derive predicate subsets, normalize boolean trees, or import predicate
//!   runtime semantics.
//! - ownership: predicate compilation owns predicate runtime shape;
//!   projection evaluation owns scalar expression execution over values.
//!
//! Shared truth-value policy:
//! - `truth_value` owns TRUE-only admission for already-evaluated `Value`
//!   results in boolean contexts such as CASE branch selection, HAVING/filter
//!   evaluation, and aggregate FILTER checks.
//! - it is not a pipeline stage, does not rewrite expression shape, does not
//!   infer types, and does not compile predicates.
//! - projection evaluation may call it only after materializing a condition
//!   value; canonicalize and type inference must not call it.
//!
//! Stage artifacts:
//! - `CanonicalExpr` marks expressions that have crossed the canonicalization
//!   boundary.
//! - `TypedExpr` marks expressions that have crossed the type-inference
//!   boundary without allowing that stage to rewrite the expression tree.
//! - `PredicateCompilation` marks runtime predicates produced by predicate
//!   compilation from `CanonicalExpr`.
//!
//! Existing planner surfaces still expose `Expr` and `Predicate` where broader
//! subsystem APIs require them, but each stage now creates an explicit artifact
//! at its boundary so future tightening can migrate callers without inventing
//! parallel stage contracts.

mod aggregate_input;
mod ast;
mod canonicalize;
mod compiled_expr;
mod function_semantics;
mod path;
mod predicate;
mod preview;
mod projection;
mod projection_eval;
mod rewrite;
mod scalar;
mod truth_value;
mod type_inference;

pub(in crate::db) use aggregate_input::canonicalize_aggregate_input_expr;
pub(in crate::db) use ast::{
    Alias, BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function, PathSpec, UnaryOp,
    supported_order_expr_requires_index_satisfied_access,
};
#[cfg(test)]
pub(in crate::db) use ast::{
    render_supported_order_expr, supported_order_expr_field, supported_order_expr_is_plain_field,
};
#[cfg(test)]
pub(in crate::db) use canonicalize::normalize_bool_expr_artifact;
pub(in crate::db) use canonicalize::{
    CanonicalExpr, canonicalize_grouped_having_bool_expr, canonicalize_scalar_where_bool_expr,
    is_normalized_bool_expr, normalize_bool_expr, scalar_where_truth_condition_is_admitted,
    simplify_bool_expr_constants, truth_condition_binary_compare_op,
    truth_condition_compare_binary_op,
};
pub(in crate::db) use compiled_expr::{
    CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader, ProjectionEvalError,
    compile_grouped_projection_expr, compile_grouped_projection_plan, evaluate_grouped_having_expr,
};
pub(in crate::db::query::plan::expr) use function_semantics::{
    AggregateInputConstantFoldShape, BooleanFunctionShape, FieldPredicateFunctionKind,
    FunctionDeterminism, FunctionTypeInferenceShape, NullTestFunctionKind, ScalarEvalFunctionShape,
};
pub(in crate::db) use function_semantics::{
    FunctionSurface, NumericSubtype, TextPredicateFunctionKind,
};
pub(in crate::db) use path::CompiledPath;
pub(in crate::db) use predicate::{
    CompiledPredicate, derive_normalized_bool_expr_predicate_subset,
    normalized_bool_expr_from_predicate,
};
#[cfg(test)]
pub(in crate::db) use predicate::{
    canonicalize_runtime_predicate_via_bool_expr, predicate_to_runtime_bool_expr_for_test,
};
#[cfg(test)]
pub(in crate::db) use predicate::{
    compile_canonical_bool_expr_to_compiled_predicate, compile_normalized_bool_expr_to_predicate,
};
pub(in crate::db) use preview::eval_literal_only_expr_value;
#[cfg(test)]
pub(in crate::db) use projection::GroupedOrderExprClass;
pub(in crate::db::query) use projection::collect_unique_direct_projection_slots;
pub(in crate::db) use projection::{
    GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility, ProjectionField,
    ProjectionSelection, ProjectionSpec, classify_grouped_order_term_for_field,
    classify_grouped_top_k_order_term, grouped_top_k_order_term_requires_heap,
};
pub(in crate::db) use projection_eval::{
    ProjectionFunctionEvalError, eval_builder_expr_for_value_preview,
    eval_projection_function_call_checked,
};
pub(in crate::db) use rewrite::rewrite_affine_numeric_compare_expr;
pub(in crate::db) use scalar::{
    ScalarProjectionCaseArm, ScalarProjectionExpr, compile_scalar_projection_expr,
    compile_scalar_projection_plan,
};
pub(in crate::db) use truth_value::{
    admit_true_only_boolean_value, collapse_true_only_boolean_admission,
};
pub(in crate::db::query::plan::expr) use type_inference::{
    ExprCoarseTypeFamily, function_is_compare_operand_coarse_family,
};
pub(in crate::db) use type_inference::{ExprType, infer_expr_type};
