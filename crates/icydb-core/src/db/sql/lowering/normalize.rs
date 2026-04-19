use crate::db::sql::lowering::SqlLoweringError;
use crate::db::{
    predicate::Predicate,
    sql::{
        identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers, split_qualified_identifier,
        },
        parser::{
            SqlAggregateCall, SqlExpr, SqlOrderTerm, SqlProjection, SqlSelectItem,
            SqlSelectStatement,
        },
    },
};
use crate::value::Value;

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    // Plain local scalar selects already arrive in the canonical scope shape
    // used by the planner, so skip the full statement rebuild when there is
    // nothing left to rewrite.
    if select_statement_is_already_local_canonical(&statement) {
        return Ok(statement);
    }

    // Re-scope parsed identifiers onto the resolved entity surface after the
    // caller has already established entity ownership for this statement.
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement.predicate.map(|predicate| {
        adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice())
    });
    statement.order_by = normalize_select_order_terms(
        statement.order_by,
        &statement.projection,
        statement.projection_aliases.as_slice(),
        entity_scope.as_slice(),
    )?;
    statement.having = normalize_having_clauses(
        statement.having,
        &statement.projection,
        statement.projection_aliases.as_slice(),
        entity_scope.as_slice(),
    )?;

    Ok(statement)
}

// Detect the already-local scalar `SELECT` family that does not need entity
// scope or alias normalization before planner lowering.
fn select_statement_is_already_local_canonical(statement: &SqlSelectStatement) -> bool {
    if !statement.projection_aliases.iter().all(Option::is_none) {
        return false;
    }
    if !statement.having.is_empty() {
        return false;
    }
    if !identifier_list_is_already_local(statement.group_by.as_slice()) {
        return false;
    }
    if !select_projection_is_already_local_scalar(&statement.projection) {
        return false;
    }
    if statement
        .predicate
        .as_ref()
        .is_some_and(|predicate| !sql_expr_is_already_local_scalar(predicate))
    {
        return false;
    }

    order_terms_are_already_local_supported(statement.order_by.as_slice())
}

// Keep the fast path on local field and simple local aggregate items so alias
// or computed-expression normalization still goes through the full rewrite path.
fn select_projection_is_already_local_scalar(projection: &SqlProjection) -> bool {
    match projection {
        SqlProjection::All => true,
        SqlProjection::Items(items) => items.iter().all(select_item_is_already_local_projection),
    }
}

// Only bare local fields and local aggregate inputs participate in the no-op
// projection normalization path. Text, arithmetic, round, and free-form
// expression projections still go through the recursive rewrite boundary.
fn select_item_is_already_local_projection(item: &SqlSelectItem) -> bool {
    match item {
        SqlSelectItem::Field(field) => identifier_is_already_local(field.as_str()),
        SqlSelectItem::Aggregate(aggregate) => aggregate_call_is_already_local(aggregate),
        SqlSelectItem::Expr(_) => false,
    }
}

// Local aggregate calls can skip projection normalization when their admitted
// reduced input form is already scoped to bare field names or literals.
fn aggregate_call_is_already_local(aggregate: &SqlAggregateCall) -> bool {
    let input_is_local = aggregate
        .input
        .as_deref()
        .is_none_or(sql_expr_is_already_local_scalar);

    input_is_local
        && aggregate
            .filter_expr
            .as_deref()
            .is_none_or(sql_expr_is_already_local_scalar)
}

// Accept only the plain boolean expression family used by local scalar
// predicates so the fast path does not silently skip function, CASE, or
// aggregate normalization.
fn sql_expr_is_already_local_scalar(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Field(field) => identifier_is_already_local(field.as_str()),
        SqlExpr::Literal(_) => true,
        SqlExpr::Membership { expr, values, .. } => {
            sql_expr_is_already_local_scalar(expr)
                && values
                    .iter()
                    .all(|value| !matches!(value, Value::List(_) | Value::Map(_)))
        }
        SqlExpr::NullTest { expr, .. } | SqlExpr::Unary { expr, .. } => {
            sql_expr_is_already_local_scalar(expr)
        }
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_is_already_local_scalar(left) && sql_expr_is_already_local_scalar(right)
        }
        SqlExpr::Aggregate(_) | SqlExpr::FunctionCall { .. } | SqlExpr::Case { .. } => false,
    }
}

// Order normalization still owns alias rewriting, but already-local supported
// order terms do not need the full scope rewrite path when aliases are absent.
fn order_terms_are_already_local_supported(terms: &[SqlOrderTerm]) -> bool {
    terms
        .iter()
        .all(|term| sql_expr_fields_are_already_local(&term.field))
}

// Group-by lists can skip rescoping only when every identifier is already a
// bare local field.
fn identifier_list_is_already_local(fields: &[String]) -> bool {
    fields
        .iter()
        .all(|field| identifier_is_already_local(field.as_str()))
}

// Local identifiers are already in the planner-owned leaf form and do not need
// entity-scope reduction.
fn identifier_is_already_local(identifier: &str) -> bool {
    split_qualified_identifier(identifier).is_none()
}

// ORDER BY normalization can skip the recursive scope rewrite only when every
// SQL field leaf is already a local bare identifier.
fn sql_expr_fields_are_already_local(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Field(field) => identifier_is_already_local(field.as_str()),
        SqlExpr::Aggregate(aggregate) => aggregate_call_is_already_local(aggregate),
        SqlExpr::Literal(_) => true,
        SqlExpr::Membership { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => sql_expr_fields_are_already_local(expr),
        SqlExpr::FunctionCall { args, .. } => args.iter().all(sql_expr_fields_are_already_local),
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_fields_are_already_local(left) && sql_expr_fields_are_already_local(right)
        }
        SqlExpr::Case { arms, else_expr } => {
            arms.iter().all(|arm| {
                sql_expr_fields_are_already_local(&arm.condition)
                    && sql_expr_fields_are_already_local(&arm.result)
            }) && else_expr
                .as_ref()
                .is_none_or(|else_expr| sql_expr_fields_are_already_local(else_expr))
        }
    }
}

pub(in crate::db::sql::lowering) fn normalize_having_clauses(
    clauses: Vec<SqlExpr>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    entity_scope: &[String],
) -> Result<Vec<SqlExpr>, SqlLoweringError> {
    SqlIdentifierNormalizer::new(entity_scope)
        .normalize_having_clauses(clauses)
        .into_iter()
        .map(|clause| normalize_having_aliases(clause, projection, projection_aliases))
        .collect()
}

pub(in crate::db::sql::lowering) fn adapt_sql_predicate_identifiers_to_scope(
    mut predicate: SqlExpr,
    entity_scope: &[String],
) -> SqlExpr {
    if let SqlExpr::NullTest { expr, negated } = &predicate
        && let SqlExpr::Field(field) = expr.as_ref()
    {
        let rewritten = rewrite_field_identifiers(
            if *negated {
                Predicate::IsNotNull {
                    field: field.clone(),
                }
            } else {
                Predicate::IsNull {
                    field: field.clone(),
                }
            },
            |field| normalize_identifier(field, entity_scope),
        );
        predicate = match rewritten {
            Predicate::IsNull { field } => SqlExpr::NullTest {
                expr: Box::new(SqlExpr::Field(field)),
                negated: false,
            },
            Predicate::IsNotNull { field } => SqlExpr::NullTest {
                expr: Box::new(SqlExpr::Field(field)),
                negated: true,
            },
            _ => unreachable!("null-test identifier rewrite should stay on the null-test boundary"),
        };
    }

    SqlIdentifierNormalizer::new(entity_scope).normalize_sql_expr(predicate)
}

// Build one identifier scope used for reducing SQL-qualified field references
// (`entity.field`, `schema.entity.field`) into canonical planner field names.
pub(in crate::db::sql::lowering) fn sql_entity_scope_candidates(
    sql_entity: &str,
    expected_entity: &'static str,
) -> Vec<String> {
    let mut out = Vec::new();
    out.push(sql_entity.to_string());
    out.push(expected_entity.to_string());

    if let Some(last) = identifier_last_segment(sql_entity) {
        out.push(last.to_string());
    }
    if let Some(last) = identifier_last_segment(expected_entity) {
        out.push(last.to_string());
    }

    out
}

fn normalize_projection_identifiers(
    projection: SqlProjection,
    entity_scope: &[String],
) -> SqlProjection {
    SqlIdentifierNormalizer::new(entity_scope).normalize_projection(projection)
}

pub(in crate::db::sql::lowering) fn normalize_sql_expr_to_scope(
    expr: SqlExpr,
    entity_scope: &[String],
) -> SqlExpr {
    SqlIdentifierNormalizer::new(entity_scope).normalize_sql_expr(expr)
}

///
/// SqlIdentifierNormalizer
///
/// Local SQL identifier rewrite owner shared by projection and HAVING
/// normalization. This keeps recursive aggregate, operand, arithmetic, and
/// round rewrites on one boundary instead of rethreading `entity_scope`
/// through parallel helper families.
///

#[derive(Clone, Copy)]
struct SqlIdentifierNormalizer<'a> {
    entity_scope: &'a [String],
}

impl<'a> SqlIdentifierNormalizer<'a> {
    // Freeze one entity scope for all recursive SQL identifier rewrites so
    // projection and HAVING normalization share the same rewrite contract.
    const fn new(entity_scope: &'a [String]) -> Self {
        Self { entity_scope }
    }

    // Rewrite all identifiers inside one projection surface while preserving
    // the original SQL projection shape.
    fn normalize_projection(self, projection: SqlProjection) -> SqlProjection {
        match projection {
            SqlProjection::All => SqlProjection::All,
            SqlProjection::Items(items) => SqlProjection::Items(
                items
                    .into_iter()
                    .map(|item| self.normalize_select_item(item))
                    .collect(),
            ),
        }
    }

    // Rewrite grouped HAVING expressions with the same recursive identifier rules
    // used by projection normalization.
    fn normalize_having_clauses(self, clauses: Vec<SqlExpr>) -> Vec<SqlExpr> {
        clauses
            .into_iter()
            .map(|clause| self.normalize_sql_expr(clause))
            .collect()
    }

    // Rewrite one select item while preserving the parser-owned projection
    // family chosen for this SQL surface.
    fn normalize_select_item(self, item: SqlSelectItem) -> SqlSelectItem {
        match item {
            SqlSelectItem::Field(field) => SqlSelectItem::Field(self.normalize_identifier(field)),
            SqlSelectItem::Aggregate(aggregate) => {
                SqlSelectItem::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlSelectItem::Expr(expr) => SqlSelectItem::Expr(self.normalize_sql_expr(expr)),
        }
    }

    // Aggregate calls only rewrite their optional field target, so keep that
    // field-local transformation behind one owner-local helper.
    fn normalize_aggregate_call(self, aggregate: SqlAggregateCall) -> SqlAggregateCall {
        SqlAggregateCall {
            kind: aggregate.kind,
            input: aggregate
                .input
                .map(|input| Box::new(self.normalize_sql_expr(*input))),
            filter_expr: aggregate
                .filter_expr
                .map(|expr| Box::new(self.normalize_sql_expr(*expr))),
            distinct: aggregate.distinct,
        }
    }

    fn normalize_sql_expr(self, expr: SqlExpr) -> SqlExpr {
        match expr {
            SqlExpr::Field(field) => SqlExpr::Field(self.normalize_identifier_to_scope(field)),
            SqlExpr::Aggregate(aggregate) => {
                SqlExpr::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlExpr::Literal(literal) => SqlExpr::Literal(literal),
            SqlExpr::Membership {
                expr,
                values,
                negated,
            } => SqlExpr::Membership {
                expr: Box::new(self.normalize_sql_expr(*expr)),
                values,
                negated,
            },
            SqlExpr::NullTest { expr, negated } => SqlExpr::NullTest {
                expr: Box::new(self.normalize_sql_expr(*expr)),
                negated,
            },
            SqlExpr::FunctionCall { function, args } => SqlExpr::FunctionCall {
                function,
                args: args
                    .into_iter()
                    .map(|arg| self.normalize_sql_expr(arg))
                    .collect(),
            },
            SqlExpr::Unary { op, expr } => SqlExpr::Unary {
                op,
                expr: Box::new(self.normalize_sql_expr(*expr)),
            },
            SqlExpr::Binary { op, left, right } => SqlExpr::Binary {
                op,
                left: Box::new(self.normalize_sql_expr(*left)),
                right: Box::new(self.normalize_sql_expr(*right)),
            },
            SqlExpr::Case { arms, else_expr } => SqlExpr::Case {
                arms: arms
                    .into_iter()
                    .map(|arm| crate::db::sql::parser::SqlCaseArm {
                        condition: self.normalize_sql_expr(arm.condition),
                        result: self.normalize_sql_expr(arm.result),
                    })
                    .collect(),
                else_expr: else_expr.map(|else_expr| Box::new(self.normalize_sql_expr(*else_expr))),
            },
        }
    }

    // Preserve the parser/session distinction between entity-scope normalization
    // and planner-owned field names.
    fn normalize_identifier(self, identifier: String) -> String {
        normalize_identifier(identifier, self.entity_scope)
    }

    // Some SQL surfaces rewrite directly onto the resolved entity scope instead
    // of the broader helper used by order-expression normalization.
    fn normalize_identifier_to_scope(self, identifier: String) -> String {
        normalize_identifier_to_scope(identifier, self.entity_scope)
    }
}

// Normalize `HAVING` targets after identifier normalization so projection
// aliases reuse the same parser/session-owned rewrite boundary as `ORDER BY`.
fn normalize_having_aliases(
    expr: SqlExpr,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<SqlExpr, SqlLoweringError> {
    match expr {
        SqlExpr::Field(field) => {
            Ok(
                resolve_projection_having_alias(field.as_str(), projection, projection_aliases)
                    .unwrap_or(SqlExpr::Field(field)),
            )
        }
        SqlExpr::Aggregate(_) | SqlExpr::Literal(_) => Ok(expr),
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => Ok(SqlExpr::Membership {
            expr: Box::new(normalize_having_aliases(
                *expr,
                projection,
                projection_aliases,
            )?),
            values,
            negated,
        }),
        SqlExpr::NullTest { expr, negated } => Ok(SqlExpr::NullTest {
            expr: Box::new(normalize_having_aliases(
                *expr,
                projection,
                projection_aliases,
            )?),
            negated,
        }),
        SqlExpr::FunctionCall { function, args } => Ok(SqlExpr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| normalize_having_aliases(arg, projection, projection_aliases))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        SqlExpr::Unary { op, expr } => Ok(SqlExpr::Unary {
            op,
            expr: Box::new(normalize_having_aliases(
                *expr,
                projection,
                projection_aliases,
            )?),
        }),
        SqlExpr::Binary { op, left, right } => Ok(SqlExpr::Binary {
            op,
            left: Box::new(normalize_having_aliases(
                *left,
                projection,
                projection_aliases,
            )?),
            right: Box::new(normalize_having_aliases(
                *right,
                projection,
                projection_aliases,
            )?),
        }),
        SqlExpr::Case { arms, else_expr } => Ok(SqlExpr::Case {
            arms: arms
                .into_iter()
                .map(|arm| {
                    Ok(crate::db::sql::parser::SqlCaseArm {
                        condition: normalize_having_aliases(
                            arm.condition,
                            projection,
                            projection_aliases,
                        )?,
                        result: normalize_having_aliases(
                            arm.result,
                            projection,
                            projection_aliases,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: else_expr
                .map(|else_expr| {
                    normalize_having_aliases(*else_expr, projection, projection_aliases)
                        .map(Box::new)
                })
                .transpose()?,
        }),
    }
}

// Normalize `ORDER BY` targets after projection normalization so alias
// rewrites stay parser/session-owned and planner order semantics remain
// canonical.
fn normalize_select_order_terms(
    terms: Vec<SqlOrderTerm>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    entity_scope: &[String],
) -> Result<Vec<SqlOrderTerm>, SqlLoweringError> {
    terms
        .into_iter()
        .map(|term| {
            let field =
                match resolve_projection_order_alias(&term.field, projection, projection_aliases) {
                    Some(rewritten) => rewritten,
                    None => term.field,
                };

            Ok(SqlOrderTerm {
                field: normalize_sql_expr_to_scope(field, entity_scope),
                direction: term.direction,
            })
        })
        .collect()
}

// Resolve one `ORDER BY <alias>` target onto one already-supported projection
// order target. Unsupported aliases fail closed here rather than leaking new
// order semantics into planner lowering.
fn resolve_projection_order_alias(
    order_target: &SqlExpr,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Option<SqlExpr> {
    let SqlExpr::Field(order_target) = order_target else {
        return None;
    };
    let SqlProjection::Items(items) = projection else {
        return None;
    };

    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let Some(alias) = alias.as_deref() else {
            continue;
        };
        if !alias.eq_ignore_ascii_case(order_target) {
            continue;
        }

        let target = order_target_from_projection_item(item);

        return Some(target);
    }

    None
}

// Resolve one `HAVING <alias>` field reference onto the shared SQL expression
// tree carried by the aliased projection item.
fn resolve_projection_having_alias(
    alias_target: &str,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Option<SqlExpr> {
    let SqlProjection::Items(items) = projection else {
        return None;
    };

    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let Some(alias) = alias.as_deref() else {
            continue;
        };
        if !alias.eq_ignore_ascii_case(alias_target) {
            continue;
        }

        return Some(SqlExpr::from_select_item(item));
    }

    None
}

// Restrict alias rewrites to the exact order target family already accepted by
// the reduced SQL parser plus the internal bounded computed alias family.
fn order_target_from_projection_item(item: &SqlSelectItem) -> SqlExpr {
    match item {
        SqlSelectItem::Field(_) | SqlSelectItem::Aggregate(_) | SqlSelectItem::Expr(_) => {
            SqlExpr::from_select_item(item)
        }
    }
}

pub(in crate::db::sql::lowering) fn normalize_order_terms(
    terms: Vec<SqlOrderTerm>,
    entity_scope: &[String],
) -> Vec<SqlOrderTerm> {
    terms
        .into_iter()
        .map(|term| SqlOrderTerm {
            field: normalize_sql_expr_to_scope(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
}

pub(in crate::db::sql::lowering) fn normalize_identifier_list(
    fields: Vec<String>,
    entity_scope: &[String],
) -> Vec<String> {
    fields
        .into_iter()
        .map(|field| normalize_identifier(field, entity_scope))
        .collect()
}

// SQL lowering only adapts identifier qualification (`entity.field` -> `field`)
// and delegates predicate-tree traversal ownership to `db::predicate`.
fn normalize_identifier(identifier: String, entity_scope: &[String]) -> String {
    normalize_identifier_to_scope(identifier, entity_scope)
}

pub(in crate::db::sql::lowering) fn ensure_entity_matches_expected(
    sql_entity: &str,
    expected_entity: &'static str,
) -> Result<(), SqlLoweringError> {
    if identifiers_tail_match(sql_entity, expected_entity) {
        return Ok(());
    }

    Err(SqlLoweringError::entity_mismatch(
        sql_entity,
        expected_entity,
    ))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::sql::{
            lowering::normalize::select_statement_is_already_local_canonical,
            parser::{
                SqlAggregateCall, SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm,
                SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement, parse_sql,
            },
        },
        value::Value,
    };

    fn sql_order_expr(term: &str) -> SqlExpr {
        let sql = format!("SELECT id FROM NormalizeOrderEntity ORDER BY {term}");
        let SqlStatement::Select(statement) =
            parse_sql(&sql).expect("normalize ORDER BY term helper SQL should parse")
        else {
            unreachable!("normalize ORDER BY term helper should always produce one SELECT");
        };

        statement
            .order_by
            .into_iter()
            .next()
            .expect("normalize ORDER BY term helper SQL should carry one ORDER BY term")
            .field
    }

    #[test]
    fn local_scalar_select_is_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "PerfAuditUser".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("id".to_string()),
                SqlSelectItem::Field("age".to_string()),
            ]),
            projection_aliases: vec![None, None],
            predicate: Some(SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Ne,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(24))),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Ne,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(31))),
                }),
            }),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: None,
        };

        assert!(select_statement_is_already_local_canonical(&statement));
    }

    #[test]
    fn local_scalar_select_with_supported_order_expr_is_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "PerfAuditUser".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("id".to_string()),
                SqlSelectItem::Field("name".to_string()),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("LOWER(name)"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: None,
        };

        assert!(select_statement_is_already_local_canonical(&statement));
    }

    #[test]
    fn local_grouped_select_with_local_aggregate_is_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "PerfAuditUser".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: crate::db::sql::parser::SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(10),
            offset: None,
        };

        assert!(select_statement_is_already_local_canonical(&statement));
    }

    #[test]
    fn qualified_field_select_is_not_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "public.PerfAuditUser".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Field(
                "PerfAuditUser.id".to_string(),
            )]),
            projection_aliases: vec![None],
            predicate: Some(SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::Field("PerfAuditUser.age".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int(24))),
            }),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("PerfAuditUser.id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        };

        assert!(!select_statement_is_already_local_canonical(&statement));
    }
}
