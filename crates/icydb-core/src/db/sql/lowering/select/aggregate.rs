use crate::{
    db::{
        query::{
            builder::AggregateExpr,
            plan::{
                canonicalize_grouped_having_numeric_literal_for_slot,
                expr::{BinaryOp, Expr, canonicalize_grouped_having_bool_expr},
                resolve_group_field_slot_with_schema,
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
    model::entity::EntityModel,
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
    model: &'static EntityModel,
    schema: &SchemaInfo,
) -> Result<Vec<Expr>, SqlLoweringError> {
    let clauses =
        lower_having_clauses_with_policy(having_exprs, projection, group_by_fields.is_empty())?;
    let mut lowered = Vec::with_capacity(clauses.len());
    for clause in clauses {
        register_having_analysis_aggregates(clause.analysis(), &mut |aggregate| {
            resolve_having_aggregate_expr_index(aggregate, grouped_aggregates)
        })?;
        lowered.push(canonicalize_grouped_having_expr_from_lowered_sql_clause(
            model, schema, clause,
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
) -> Result<Option<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let clauses = lower_having_clauses_with_policy(having_exprs, projection, false)?;
    if clauses.is_empty() {
        return Ok(None);
    }

    let mut canonicalized = Vec::with_capacity(clauses.len());
    for clause in clauses {
        if clause.analysis().references_direct_fields() {
            return Err(SqlLoweringError::unsupported_select_having());
        }
        register_having_analysis_aggregates(clause.analysis(), &mut resolve_aggregate_index)?;
        canonicalized.push(canonicalize_grouped_global_having_clause(clause)?);
    }

    Ok(Some(combine_having_clauses(canonicalized)))
}

fn lower_having_clauses_with_policy(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    require_group_by: bool,
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
            analyzed: lower_having_expr(expr)?,
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

fn lower_having_expr(expr: SqlExpr) -> Result<AnalyzedLoweredExpr, SqlLoweringError> {
    let expr = lower_sql_expr(&expr, SqlExprPhase::PostAggregate)?;

    Ok(AnalyzedLoweredExpr::new(expr, None))
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

fn combine_having_clauses(mut clauses: Vec<Expr>) -> Expr {
    if clauses.len() == 1 {
        return clauses
            .pop()
            .expect("single HAVING clause should remain present");
    }

    let mut expr = clauses.remove(0);
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
    model: &'static EntityModel,
    schema: &SchemaInfo,
    expr: Expr,
) -> Result<Expr, SqlLoweringError> {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) | Expr::Literal(_) => Ok(expr),
        Expr::FunctionCall { function, args } => Ok(Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| canonicalize_grouped_having_expr(model, schema, arg))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op,
            expr: Box::new(canonicalize_grouped_having_expr(model, schema, *expr)?),
        }),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Ok(Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    Ok(crate::db::query::plan::expr::CaseWhenArm::new(
                        canonicalize_grouped_having_expr(model, schema, arm.condition().clone())?,
                        canonicalize_grouped_having_expr(model, schema, arm.result().clone())?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(canonicalize_grouped_having_expr(model, schema, *else_expr)?),
        }),
        Expr::Binary { op, left, right } => {
            let left = canonicalize_grouped_having_expr(model, schema, *left)?;
            let right = canonicalize_grouped_having_expr(model, schema, *right)?;
            let canonical_left =
                canonicalize_grouped_having_compare_literals(model, schema, &left, &right)
                    .unwrap_or_else(|| left.clone());
            let canonical_right =
                canonicalize_grouped_having_compare_literals(model, schema, &right, &left)
                    .unwrap_or_else(|| right.clone());

            Ok(Expr::Binary {
                op,
                left: Box::new(canonical_left),
                right: Box::new(canonical_right),
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Ok(Expr::Alias {
            expr: Box::new(canonicalize_grouped_having_expr(model, schema, *expr)?),
            name,
        }),
    }
}

// Apply grouped semantic canonicalization across the bounded grouped searched-
// `CASE` family. Omitted-`ELSE` grouped `CASE` is admitted only
// when canonicalization eliminates raw planner `Case` nodes from the lowered
// grouped boolean candidate, proving it joined the shipped canonical family.
fn canonicalize_grouped_having_expr_from_lowered_sql_clause(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    clause: LoweredHavingClause,
) -> Result<Expr, SqlLoweringError> {
    let contains_omitted_else_case = clause.contains_omitted_else_case;
    let expr = canonicalize_grouped_having_expr(model, schema, clause.into_expr())?;
    let canonical = canonicalize_grouped_having_bool_expr(expr);

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
) -> Result<Expr, SqlLoweringError> {
    let contains_omitted_else_case = clause.contains_omitted_else_case;
    let canonical = canonicalize_grouped_having_bool_expr(clause.into_expr());

    if contains_omitted_else_case && canonical.contains_case() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(canonical)
}

fn canonicalize_grouped_having_compare_literals(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    expr: &Expr,
    other: &Expr,
) -> Option<Expr> {
    let (Expr::Literal(value), Expr::Field(field)) = (expr, other) else {
        return None;
    };
    let field_slot = resolve_group_field_slot_with_schema(model, schema, field.as_str()).ok()?;
    let canonical = canonicalize_grouped_having_numeric_literal_for_slot(&field_slot, value)?;

    Some(Expr::Literal(canonical))
}
