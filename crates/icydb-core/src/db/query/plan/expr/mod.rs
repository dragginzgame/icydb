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
//! - `CompiledPredicate` marks runtime predicates produced by predicate
//!   compilation from `CanonicalExpr`.
//!
//! Existing planner surfaces still expose `Expr` and `Predicate` where broader
//! subsystem APIs require them, but each stage now creates an explicit artifact
//! at its boundary so future tightening can migrate callers without inventing
//! parallel stage contracts.

mod aggregate_input;
mod ast;
mod canonicalize;
mod function_semantics;
mod predicate_bridge;
mod predicate_compile;
mod preview;
mod projection;
mod projection_eval;
mod rewrite;
mod scalar;
mod truth_value;
mod type_inference;

pub(in crate::db) use aggregate_input::*;
pub(crate) use ast::*;
pub(in crate::db) use canonicalize::*;
pub(crate) use function_semantics::*;
pub(in crate::db) use predicate_bridge::*;
pub(in crate::db) use predicate_compile::*;
pub(in crate::db) use preview::*;
pub(crate) use projection::*;
pub(in crate::db) use projection_eval::*;
pub(in crate::db) use rewrite::*;
pub(in crate::db) use scalar::*;
pub(in crate::db) use truth_value::*;
pub(crate) use type_inference::*;
