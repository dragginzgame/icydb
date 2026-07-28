#[cfg(any(test, feature = "query"))]
mod affine_numeric;

#[cfg(any(test, feature = "query"))]
pub(in crate::db) use affine_numeric::rewrite_affine_numeric_compare_expr;
