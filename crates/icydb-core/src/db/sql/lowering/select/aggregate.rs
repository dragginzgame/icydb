use crate::db::query::preparation::PreparationWork;
use crate::{
    db::{
        query::{
            builder::AggregateExpr,
            plan::{
                canonicalize_grouped_having_numeric_literal_for_group_field,
                expr::{BinaryOp, Expr, canonicalize_grouped_having_bool_expr},
                resolve_group_field_with_schema,
            },
        },
        schema::SchemaInfo,
        sql::{
            lowering::{
                AnalyzedLoweredExpr, LoweredExprAnalysis, SqlLoweringError,
                aggregate::resolve_having_aggregate_expr_index,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{SqlExpr, SqlProjection},
        },
    },
    value::Value,
};

/// Lower grouped SQL `HAVING` expressions onto planner-owned expressions.
///
/// This keeps grouped `HAVING` on the shared `SqlExpr -> Expr` seam and
/// canonicalizes numeric literals against grouped-key field kinds once the
/// concrete entity model is available.
pub(super) fn lower_having_clauses(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_aggregates: &[AggregateExpr],
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<Vec<Expr>, SqlLoweringError> {
    let clauses = lower_having_clauses_with_policy(
        having_exprs,
        projection,
        group_by_fields.is_empty(),
        work,
    )?;
    let mut lowered = Vec::with_capacity(clauses.len());
    for clause in clauses {
        register_having_analysis_aggregates(clause.analysis(), &mut |aggregate| {
            resolve_having_aggregate_expr_index(aggregate, grouped_aggregates)
        })?;
        lowered.push(canonicalize_grouped_having_expr_from_lowered_sql_clause(
            schema, clause, work,
        )?);
    }

    Ok(lowered)
}

/// Lower global aggregate SQL `HAVING` clauses onto planner-owned expressions
/// while registering any aggregate terminals needed only by `HAVING`.
pub(in crate::db::sql::lowering) fn lower_global_aggregate_having_expr<F>(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
    work: &PreparationWork<'_>,
) -> Result<Option<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let clauses = lower_having_clauses_with_policy(having_exprs, projection, false, work)?;
    if clauses.is_empty() {
        return Ok(None);
    }

    let mut canonicalized = Vec::with_capacity(clauses.len());
    for clause in clauses {
        if clause.analysis().references_direct_fields() {
            return Err(SqlLoweringError::unsupported_select_having());
        }
        register_having_analysis_aggregates(clause.analysis(), &mut resolve_aggregate_index)?;
        canonicalized.push(canonicalize_grouped_global_having_clause(clause, work)?);
    }

    Ok(Some(combine_having_clauses(canonicalized)))
}

fn lower_having_clauses_with_policy(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    require_group_by: bool,
    work: &PreparationWork<'_>,
) -> Result<Vec<LoweredHavingClause>, SqlLoweringError> {
    if having_exprs.is_empty() {
        return Ok(Vec::new());
    }
    if require_group_by {
        return Err(SqlLoweringError::having_requires_group_by());
    }

    let SqlProjection::Items(_) = projection else {
        return Err(SqlLoweringError::unsupported_select_having());
    };

    let mut lowered = Vec::with_capacity(having_exprs.len());
    for expr in having_exprs {
        let contains_omitted_else_case = expr.contains_omitted_else_case();
        lowered.push(LoweredHavingClause {
            contains_omitted_else_case,
            analyzed: lower_having_expr(expr, work)?,
        });
    }

    Ok(lowered)
}

///
/// LoweredHavingClause
///
/// One grouped/global HAVING clause paired with the original SQL omitted-ELSE
/// searched-CASE signal used to gate grouped admission.
///

struct LoweredHavingClause {
    contains_omitted_else_case: bool,
    analyzed: AnalyzedLoweredExpr,
}

impl LoweredHavingClause {
    const fn analysis(&self) -> &LoweredExprAnalysis {
        self.analyzed.analysis()
    }

    fn into_expr(self) -> Expr {
        self.analyzed.into_expr()
    }
}

fn lower_having_expr(
    expr: SqlExpr,
    work: &PreparationWork<'_>,
) -> Result<AnalyzedLoweredExpr, SqlLoweringError> {
    let expr = lower_sql_expr(&expr, SqlExprPhase::PostAggregate, work)?;

    Ok(AnalyzedLoweredExpr::new(expr))
}

fn register_having_analysis_aggregates<F>(
    analysis: &LoweredExprAnalysis,
    resolve_aggregate_index: &mut F,
) -> Result<(), SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    analysis
        .aggregate_refs()
        .iter()
        .try_for_each(|aggregate| resolve_aggregate_index(aggregate).map(|_| ()))
}

fn combine_having_clauses(clauses: Vec<Expr>) -> Expr {
    let mut clauses = clauses.into_iter();
    let mut expr = clauses.next().unwrap_or(Expr::Literal(Value::Bool(true)));
    for clause in clauses {
        expr = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(expr),
            right: Box::new(clause),
        };
    }

    expr
}

fn canonicalize_grouped_having_expr(
    schema: &SchemaInfo,
    mut expr: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    match &mut expr {
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                *arg = canonicalize_grouped_having_expr(schema, arg.take(), work)?;
            }
        }
        Expr::Unary { expr, .. } => {
            **expr = canonicalize_grouped_having_expr(schema, expr.take(), work)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                let [condition, result] = arm.children_mut();
                *condition = canonicalize_grouped_having_expr(schema, condition.take(), work)?;
                *result = canonicalize_grouped_having_expr(schema, result.take(), work)?;
            }
            **else_expr = canonicalize_grouped_having_expr(schema, else_expr.take(), work)?;
        }
        Expr::Binary { left, right, .. } => {
            **left = canonicalize_grouped_having_expr(schema, left.take(), work)?;
            **right = canonicalize_grouped_having_expr(schema, right.take(), work)?;
            let canonical_left =
                canonicalize_grouped_having_compare_literals(schema, left, right, work)?;
            let canonical_right =
                canonicalize_grouped_having_compare_literals(schema, right, left, work)?;
            if let Some(canonical) = canonical_left {
                **left = canonical;
            }
            if let Some(canonical) = canonical_right {
                **right = canonical;
            }
        }
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) | Expr::Literal(_) => {}
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            **expr = canonicalize_grouped_having_expr(schema, expr.take(), work)?;
        }
    }

    Ok(expr)
}

// Apply grouped semantic canonicalization across the bounded grouped searched-
// `CASE` family. Omitted-`ELSE` grouped `CASE` is admitted only
// when canonicalization eliminates raw planner `Case` nodes from the lowered
// grouped boolean candidate, proving it joined the shipped canonical family.
fn canonicalize_grouped_having_expr_from_lowered_sql_clause(
    schema: &SchemaInfo,
    clause: LoweredHavingClause,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let contains_omitted_else_case = clause.contains_omitted_else_case;
    let expr = canonicalize_grouped_having_expr(schema, clause.into_expr(), work)?;
    let canonical = canonicalize_grouped_having_bool_expr(expr, work)?;

    if contains_omitted_else_case && canonical.contains_case() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(canonical)
}

// Global aggregate HAVING has no grouped-key field literal canonicalization
// seam today, but explicit searched-CASE boolean canonicalization is still
// safe to apply before the global aggregate command freezes identity/explain.
// Omitted-`ELSE` global aggregate `CASE` uses the same proof gate
// as grouped SELECT HAVING: if canonical grouped boolean lowering still leaves
// raw planner `Case` nodes behind, the shape stays outside the admitted family.
fn canonicalize_grouped_global_having_clause(
    clause: LoweredHavingClause,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let contains_omitted_else_case = clause.contains_omitted_else_case;
    let canonical = canonicalize_grouped_having_bool_expr(clause.into_expr(), work)?;

    if contains_omitted_else_case && canonical.contains_case() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(canonical)
}

fn canonicalize_grouped_having_compare_literals(
    schema: &SchemaInfo,
    expr: &Expr,
    other: &Expr,
    work: &PreparationWork<'_>,
) -> Result<Option<Expr>, SqlLoweringError> {
    let Expr::Literal(value) = expr else {
        return Ok(None);
    };
    let field = match other {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::FieldPath(path) => path.path_spec().dotted_label(),
        _ => return Ok(None),
    };
    let Ok(group_field) = resolve_group_field_with_schema(schema, field.as_str()) else {
        return Ok(None);
    };
    let canonical = canonicalize_grouped_having_numeric_literal_for_group_field(
        schema,
        &group_field,
        value,
        work,
    )?;

    Ok(canonical.map(Expr::Literal))
}
