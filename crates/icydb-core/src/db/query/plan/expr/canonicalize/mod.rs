//! This module defines the canonical boolean IR for the planner.
//! All downstream predicate extraction and execution depend on this shape.
//!
//! Invariants:
//! - `normalize_bool_expr` preserves SQL three-valued logic inside
//!   subexpressions.
//! - associative `AND` / `OR` trees are flattened, deterministically sorted,
//!   deduplicated, and rebuilt as stable left-associated binary chains.
//! - scalar `WHERE` and grouped `HAVING` canonicalization remain distinct
//!   truth-admission behaviors.
//! - searched `CASE` lowering is bounded and only rewrites shapes proven
//!   equivalent under the owning boolean context.
//!
//! Planner boolean canonicalization pipeline:
//! 1. affine numeric rewrite
//! 2. boolean normalization
//! 3. CASE lowering
//! 4. constant simplification

mod case;
mod normalize;
mod truth_admission;

use crate::db::query::plan::expr::Expr;
use crate::db::query::plan::expr::canonicalize::{
    case::canonicalize_normalized_bool_case_in_bool_context, truth_admission::TruthWrapperScope,
};

pub(in crate::db) use normalize::{is_normalized_bool_expr, simplify_bool_expr_constants};
pub(in crate::db) use truth_admission::{
    scalar_where_truth_condition_is_admitted, truth_condition_binary_compare_op,
    truth_condition_compare_binary_op,
};

///
/// CanonicalExpr
///
/// Stage artifact for expressions that have passed the canonicalization
/// boundary. The wrapper keeps the invariant visible inside this stage while
/// planner and lowering boundaries continue to exchange plain `Expr` values.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CanonicalExpr {
    expr: Expr,
}

impl CanonicalExpr {
    // Build one canonical expression artifact after the entrypoint has already
    // performed the normal-form assertion for the owning context.
    const fn new(expr: Expr) -> Self {
        Self { expr }
    }

    // Rebuild one canonical artifact from a plain `Expr` only when the
    // expression already satisfies the canonical boolean IR invariant.
    pub(in crate::db::query::plan::expr) fn from_normalized_bool_expr(expr: &Expr) -> Option<Self> {
        is_normalized_bool_expr(expr).then(|| Self::new(expr.clone()))
    }

    /// Borrow the canonical expression for downstream stages that consume the
    /// stage artifact directly.
    pub(in crate::db) const fn as_expr(&self) -> &Expr {
        &self.expr
    }

    /// Return the canonical expression as a plain planner expression for
    /// boundaries that still exchange the shared expression tree directly.
    pub(in crate::db) fn into_expr(self) -> Expr {
        self.expr
    }
}

/// Normalize one planner-owned boolean expression and assert that the emitted
/// shape satisfies the canonical boolean IR invariant.
#[must_use]
pub(in crate::db) fn normalize_bool_expr_artifact(expr: Expr) -> CanonicalExpr {
    let expr = normalize::normalize_bool_expr_impl(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    CanonicalExpr::new(expr)
}

/// Normalize one planner-owned boolean expression and return the plain `Expr`
/// surface after producing the canonical stage artifact.
#[must_use]
pub(in crate::db) fn normalize_bool_expr(expr: Expr) -> Expr {
    normalize_bool_expr_artifact(expr).into_expr()
}

/// Canonicalize one scalar-WHERE boolean expression into the canonical stage
/// artifact used by downstream predicate subset derivation.
#[must_use]
pub(in crate::db) fn canonicalize_scalar_where_bool_expr_artifact(expr: Expr) -> CanonicalExpr {
    let expr = normalize_bool_expr(expr);
    debug_assert!(is_normalized_bool_expr(&expr));

    let expr = canonicalize_normalized_bool_case_in_bool_context(
        expr,
        true,
        Some(TruthWrapperScope::ScalarWhere),
    );
    let expr = normalize_bool_expr(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    CanonicalExpr::new(expr)
}

/// Canonicalize one scalar-WHERE boolean expression onto the shipped `0.107`
/// searched-`CASE` boolean seam after the shared structural normalization pass
/// has already settled the planner-owned tree shape.
#[must_use]
pub(in crate::db) fn canonicalize_scalar_where_bool_expr(expr: Expr) -> Expr {
    canonicalize_scalar_where_bool_expr_artifact(expr).into_expr()
}

/// Canonicalize one grouped-HAVING boolean expression into the canonical stage
/// artifact used by downstream grouped predicate and HAVING paths.
///
/// Unlike scalar `WHERE`, grouped `HAVING` does not collapse a final
/// `ELSE NULL` arm to `FALSE`. Grouped canonicalization therefore preserves
/// the explicit grouped boolean result tree unless the shipped searched-`CASE`
/// expansion is already semantically identical without null-arm collapse.
#[must_use]
pub(in crate::db) fn canonicalize_grouped_having_bool_expr_artifact(expr: Expr) -> CanonicalExpr {
    let expr = normalize_bool_expr(expr);
    debug_assert!(is_normalized_bool_expr(&expr));

    let expr = canonicalize_normalized_bool_case_in_bool_context(
        expr,
        false,
        Some(TruthWrapperScope::GroupedHaving),
    );
    let expr = normalize_bool_expr(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    CanonicalExpr::new(expr)
}

/// Canonicalize one grouped-HAVING boolean expression onto the bounded
/// searched-`CASE` boolean seam after the shared structural normalization pass
/// has already settled the planner-owned grouped tree shape.
#[must_use]
pub(in crate::db) fn canonicalize_grouped_having_bool_expr(expr: Expr) -> Expr {
    canonicalize_grouped_having_bool_expr_artifact(expr).into_expr()
}
