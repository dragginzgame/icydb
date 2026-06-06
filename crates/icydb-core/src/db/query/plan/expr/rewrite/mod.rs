#[cfg(any(test, feature = "sql"))]
mod affine_numeric;

#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use affine_numeric::rewrite_affine_numeric_compare_expr;
