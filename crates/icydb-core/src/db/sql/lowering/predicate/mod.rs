#[cfg(test)]
mod compile;
mod normalize;
mod validate;

use crate::db::{
    predicate::Predicate,
    query::plan::expr::Expr,
    query::plan::expr::derive_normalized_bool_expr_predicate_subset,
    sql::{
        lowering::{
            SqlLoweringError,
            expr::{SqlExprPhase, lower_sql_expr},
        },
        parser::SqlExpr,
    },
};

// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
// authority through the shared SQL-expression seam.
pub(in crate::db) fn lower_sql_where_expr(expr: &SqlExpr) -> Result<Predicate, SqlLoweringError> {
    let expr = lower_sql_where_bool_expr(expr)?;

    derive_where_predicate_subset(&expr).ok_or_else(SqlLoweringError::unsupported_where_expression)
}

// Lower one parser-owned SQL `WHERE` expression onto the shared boolean seam
// and derive the strongest predicate the current predicate compiler can
// express. When compilation support runs out, keep correctness on the
// semantic filter-expression path and fall back to `Predicate::True`.
pub(in crate::db::sql::lowering) fn lower_sql_where_expr_with_runtime_fallback(
    expr: &SqlExpr,
) -> Result<(Expr, Predicate), SqlLoweringError> {
    let expr = lower_sql_where_bool_expr(expr)?;
    let predicate = derive_where_predicate_subset(&expr).unwrap_or(Predicate::True);

    Ok((expr, predicate))
}

// Lower one parser-owned SQL scalar `WHERE` expression onto the shared boolean
// seam, including the bounded searched-`CASE` semantic canonicalization that
// belongs only to scalar row filters.
pub(in crate::db::sql::lowering) fn lower_sql_scalar_where_expr_with_runtime_fallback(
    expr: &SqlExpr,
) -> Result<(Expr, Predicate), SqlLoweringError> {
    let expr = lower_sql_scalar_where_bool_expr(expr)?;
    let predicate = derive_where_predicate_subset(&expr).unwrap_or(Predicate::True);

    Ok((expr, predicate))
}

// Lower one parser-owned SQL boolean expression onto the shared planner-owned
// WHERE boolean seam without compiling it into the runtime predicate layer.
pub(in crate::db::sql::lowering) fn lower_sql_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_where_bool_expr_internal(expr, false)
}

// Lower one parser-owned SQL scalar-row boolean expression through the
// bounded scalar searched-`CASE` canonicalization seam without changing the
// grouped or aggregate filter-expression surfaces.
pub(in crate::db::sql::lowering) fn lower_sql_scalar_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_where_bool_expr_internal(expr, true)
}

fn lower_sql_where_bool_expr_internal(
    expr: &SqlExpr,
    scalar_case_canonicalization: bool,
) -> Result<Expr, SqlLoweringError> {
    let expr = lower_sql_expr(expr, SqlExprPhase::PreAggregate)?;
    validate::validate_where_bool_expr(&expr)?;
    let expr = if scalar_case_canonicalization {
        normalize::normalize_scalar_where_bool_expr(expr)
    } else {
        normalize::normalize_where_bool_expr(expr)
    };

    debug_assert!(
        validate::validate_where_bool_expr(&expr).is_ok(),
        "WHERE normalization must not widen or narrow clause admissibility",
    );

    debug_assert!(normalize::is_normalized_where_bool_expr(&expr));

    Ok(expr)
}

// Derive the optional predicate subset for one already-admitted normalized
// WHERE expression without reopening clause admission.
fn derive_where_predicate_subset(expr: &Expr) -> Option<Predicate> {
    derive_normalized_bool_expr_predicate_subset(expr)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        query::plan::expr::{Expr, FieldId, Function},
        sql::{
            lowering::predicate::{
                derive_where_predicate_subset, lower_sql_where_bool_expr, lower_sql_where_expr,
            },
            parser::parse_sql,
        },
    };
    use crate::value::Value;

    fn parse_where_expr(sql: &str) -> crate::db::sql::parser::SqlExpr {
        let statement = parse_sql(sql).expect("SQL WHERE test statement should parse");
        let crate::db::sql::parser::SqlStatement::Select(select) = statement else {
            panic!("expected SELECT statement");
        };

        select
            .predicate
            .expect("SQL WHERE test statement should carry one predicate")
    }

    #[test]
    fn lower_sql_where_bool_expr_validates_before_normalization_for_casefold_targets() {
        let expr = parse_where_expr(
            "SELECT * FROM users WHERE UPPER(name) LIKE 'AL%' ORDER BY id ASC LIMIT 1",
        );

        let lowered =
            lower_sql_where_bool_expr(&expr).expect("UPPER(...) prefix LIKE should be admitted");
        let Expr::FunctionCall {
            function: Function::StartsWith,
            args,
        } = lowered
        else {
            panic!("UPPER(...) prefix LIKE should normalize onto STARTS_WITH(...)");
        };
        let [left, right] = args.as_slice() else {
            panic!("normalized STARTS_WITH(...) should keep two arguments");
        };
        let Expr::FunctionCall {
            function: Function::Lower,
            args,
        } = left
        else {
            panic!("casefold target should normalize onto LOWER(...)");
        };
        let [Expr::Field(field)] = args.as_slice() else {
            panic!("normalized LOWER(...) should keep the original field");
        };

        assert_eq!(field, &FieldId::new("name"));
        assert_eq!(right, &Expr::Literal(Value::Text("AL".to_string())));
    }

    #[test]
    fn derive_where_predicate_subset_returns_none_for_admitted_expression_only_shapes() {
        let expr = parse_where_expr(
            "SELECT * FROM users WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
        );
        let lowered = lower_sql_where_bool_expr(&expr)
            .expect("admitted expression-only WHERE shape should lower successfully");

        assert!(
            derive_where_predicate_subset(&lowered).is_none(),
            "predicate extraction should stay subset-only for admitted expression-owned WHERE shapes",
        );
    }

    #[test]
    fn lower_sql_where_expr_rejects_expression_only_shapes_on_strict_predicate_path() {
        let expr = parse_where_expr(
            "SELECT * FROM users WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
        );

        let err = lower_sql_where_expr(&expr).expect_err(
            "strict predicate-only WHERE lowering should reject expression-only shapes",
        );

        assert_eq!(
            err.to_string(),
            crate::db::sql::lowering::SqlLoweringError::unsupported_where_expression().to_string(),
            "strict predicate-only lowering should fail closed with the normal unsupported WHERE error",
        );
    }

    #[test]
    fn derive_where_predicate_subset_recovers_folded_constant_compare_shapes() {
        let expr = parse_where_expr(
            "SELECT * FROM users WHERE name = TRIM('alpha') AND NULLIF('alpha', 'alpha') IS NULL",
        );
        let lowered = lower_sql_where_bool_expr(&expr)
            .expect("foldable compare WHERE shape should lower successfully");
        let subset = derive_where_predicate_subset(&lowered)
            .expect("foldable compare WHERE shape should recover one predicate subset");

        assert!(
            matches!(
                subset,
                crate::db::predicate::Predicate::Compare(ref compare)
                    if compare.field() == "name"
                        && compare.op() == crate::db::predicate::CompareOp::Eq
                        && compare.value() == &Value::Text("alpha".to_string())
            ),
            "predicate subset derivation should stay available after legality is decided earlier",
        );
    }
}
