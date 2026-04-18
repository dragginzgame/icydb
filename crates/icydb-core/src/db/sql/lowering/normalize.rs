use crate::db::sql::lowering::{SqlLoweringError, select::lower_select_item_expr};
use crate::db::{
    predicate::Predicate,
    query::builder::scalar_projection::render_scalar_projection_expr_sql_label,
    query::plan::expr::{
        parse_supported_order_expr, render_supported_order_expr,
        rewrite_supported_order_expr_fields,
    },
    sql::{
        identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers, split_qualified_identifier,
        },
        lowering::expr::SqlExprPhase,
        parser::{
            SqlAggregateCall, SqlAggregateInputExpr, SqlArithmeticProjectionCall, SqlExpr,
            SqlOrderTerm, SqlProjection, SqlProjectionOperand, SqlRoundProjectionCall,
            SqlRoundProjectionInput, SqlSelectItem, SqlSelectStatement, SqlTextFunctionCall,
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

    order_terms_are_already_local_fields(statement.order_by.as_slice())
}

// Keep the fast path narrow to the field-list scalar projection family so it
// cannot bypass alias or computed-expression normalization.
fn select_projection_is_already_local_scalar(projection: &SqlProjection) -> bool {
    match projection {
        SqlProjection::All => true,
        SqlProjection::Items(items) => items.iter().all(select_item_is_already_local_field),
    }
}

// Only bare local fields participate in the no-op projection normalization
// path. Any aggregate, text, arithmetic, round, or free-form expression still
// goes through the existing recursive rewrite boundary.
fn select_item_is_already_local_field(item: &SqlSelectItem) -> bool {
    match item {
        SqlSelectItem::Field(field) => identifier_is_already_local(field.as_str()),
        SqlSelectItem::Aggregate(_)
        | SqlSelectItem::TextFunction(_)
        | SqlSelectItem::Arithmetic(_)
        | SqlSelectItem::Round(_)
        | SqlSelectItem::Expr(_) => false,
    }
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
        SqlExpr::Aggregate(_)
        | SqlExpr::TextFunction(_)
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Round(_)
        | SqlExpr::Case { .. } => false,
    }
}

// Order normalization still owns alias and supported-expression rewriting, so
// the fast path only admits bare local field targets here.
fn order_terms_are_already_local_fields(terms: &[SqlOrderTerm]) -> bool {
    terms
        .iter()
        .all(|term| order_term_is_already_local_field(term.field.as_str()))
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

// The normalization fast path only accepts bare field order targets here. Any
// expression-like target still routes through the existing order-expression
// rewrite and canonical render path.
fn order_term_is_already_local_field(identifier: &str) -> bool {
    identifier_is_already_local(identifier)
        && identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
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
            SqlSelectItem::TextFunction(call) => {
                SqlSelectItem::TextFunction(self.normalize_text_function_call(call))
            }
            SqlSelectItem::Arithmetic(call) => {
                SqlSelectItem::Arithmetic(self.normalize_arithmetic_call(call))
            }
            SqlSelectItem::Round(call) => SqlSelectItem::Round(self.normalize_round_call(call)),
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
                .map(|input| Box::new(self.normalize_aggregate_input_expr(*input))),
            distinct: aggregate.distinct,
        }
    }

    // Aggregate inputs share the same reduced SQL field-rewrite contract as
    // projection and HAVING expressions.
    fn normalize_aggregate_input_expr(self, expr: SqlAggregateInputExpr) -> SqlAggregateInputExpr {
        match expr {
            SqlAggregateInputExpr::Field(field) => {
                SqlAggregateInputExpr::Field(self.normalize_identifier_to_scope(field))
            }
            SqlAggregateInputExpr::Literal(literal) => SqlAggregateInputExpr::Literal(literal),
            SqlAggregateInputExpr::Arithmetic(call) => {
                SqlAggregateInputExpr::Arithmetic(self.normalize_arithmetic_call(call))
            }
            SqlAggregateInputExpr::Round(call) => {
                SqlAggregateInputExpr::Round(self.normalize_round_call(call))
            }
            SqlAggregateInputExpr::Expr(expr) => {
                SqlAggregateInputExpr::Expr(self.normalize_sql_expr(expr))
            }
        }
    }

    fn normalize_sql_expr(self, expr: SqlExpr) -> SqlExpr {
        match expr {
            SqlExpr::Field(field) => SqlExpr::Field(self.normalize_identifier_to_scope(field)),
            SqlExpr::Aggregate(aggregate) => {
                SqlExpr::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlExpr::Literal(literal) => SqlExpr::Literal(literal),
            SqlExpr::TextFunction(call) => {
                SqlExpr::TextFunction(self.normalize_text_function_call(call))
            }
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
            SqlExpr::Round(call) => SqlExpr::Round(self.normalize_round_call(call)),
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

    // Projection operands stay narrow: only field and aggregate leaves need
    // identifier rewriting here.
    fn normalize_projection_operand(self, operand: SqlProjectionOperand) -> SqlProjectionOperand {
        match operand {
            SqlProjectionOperand::Field(field) => {
                SqlProjectionOperand::Field(self.normalize_identifier(field))
            }
            SqlProjectionOperand::Aggregate(aggregate) => {
                SqlProjectionOperand::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlProjectionOperand::Literal(literal) => SqlProjectionOperand::Literal(literal),
            SqlProjectionOperand::Arithmetic(call) => {
                SqlProjectionOperand::Arithmetic(Box::new(self.normalize_arithmetic_call(*call)))
            }
        }
    }

    // Arithmetic projection calls recurse through the two operand leaves while
    // preserving the parser-owned operator.
    fn normalize_arithmetic_call(
        self,
        call: SqlArithmeticProjectionCall,
    ) -> SqlArithmeticProjectionCall {
        SqlArithmeticProjectionCall {
            left: self.normalize_projection_operand(call.left),
            op: call.op,
            right: self.normalize_projection_operand(call.right),
        }
    }

    // Round projection input can be either a single operand or an arithmetic
    // subtree, so keep that branch local to one owner.
    fn normalize_round_call(self, call: SqlRoundProjectionCall) -> SqlRoundProjectionCall {
        SqlRoundProjectionCall {
            input: self.normalize_round_input(call.input),
            scale: call.scale,
        }
    }

    // Round-input normalization shares the same operand/arithmetic rewrite
    // rules used by the wider projection and HAVING surfaces.
    fn normalize_round_input(self, input: SqlRoundProjectionInput) -> SqlRoundProjectionInput {
        match input {
            SqlRoundProjectionInput::Operand(operand) => {
                SqlRoundProjectionInput::Operand(self.normalize_projection_operand(operand))
            }
            SqlRoundProjectionInput::Arithmetic(call) => {
                SqlRoundProjectionInput::Arithmetic(self.normalize_arithmetic_call(call))
            }
        }
    }

    // Text SQL functions only rewrite their field target, while literal
    // arguments stay parser-owned and already normalized as values.
    fn normalize_text_function_call(self, call: SqlTextFunctionCall) -> SqlTextFunctionCall {
        SqlTextFunctionCall {
            function: call.function,
            field: self.normalize_identifier(call.field),
            literal: call.literal,
            literal2: call.literal2,
            literal3: call.literal3,
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
        SqlExpr::Aggregate(_) | SqlExpr::Literal(_) | SqlExpr::TextFunction(_) => Ok(expr),
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
        SqlExpr::Round(call) => Ok(SqlExpr::Round(call)),
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
            let field = match resolve_projection_order_alias(
                term.field.as_str(),
                projection,
                projection_aliases,
            )? {
                Some(rewritten) => rewritten,
                None => term.field,
            };

            Ok(SqlOrderTerm {
                field: normalize_order_term_identifier(field, entity_scope),
                direction: term.direction,
            })
        })
        .collect()
}

// Resolve one `ORDER BY <alias>` target onto one already-supported projection
// order target. Unsupported aliases fail closed here rather than leaking new
// order semantics into planner lowering.
fn resolve_projection_order_alias(
    order_target: &str,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Option<String>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(None);
    };

    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let Some(alias) = alias.as_deref() else {
            continue;
        };
        if !alias.eq_ignore_ascii_case(order_target) {
            continue;
        }

        let Some(target) = order_target_from_projection_item(item) else {
            return Err(SqlLoweringError::unsupported_order_by_alias(order_target));
        };

        return Ok(Some(target));
    }

    Ok(None)
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
fn order_target_from_projection_item(item: &SqlSelectItem) -> Option<String> {
    let phase = if crate::db::sql::parser::SqlExpr::from_select_item(item).contains_aggregate() {
        SqlExprPhase::PostAggregate
    } else {
        SqlExprPhase::Scalar
    };

    match item {
        SqlSelectItem::Field(field) => Some(field.clone()),
        SqlSelectItem::Aggregate(_) => lower_select_item_expr(item, phase)
            .ok()
            .map(|expr| render_scalar_projection_expr_sql_label(&expr)),
        SqlSelectItem::TextFunction(_) => lower_select_item_expr(item, phase)
            .ok()
            .and_then(|expr| render_supported_order_expr(&expr)),
        SqlSelectItem::Arithmetic(_) | SqlSelectItem::Round(_) | SqlSelectItem::Expr(_) => {
            lower_select_item_expr(item, phase).ok().and_then(|expr| {
                render_supported_order_expr(&expr)
                    .or_else(|| Some(render_scalar_projection_expr_sql_label(&expr)))
            })
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
            field: normalize_order_term_identifier(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
}

fn normalize_order_term_identifier(identifier: String, entity_scope: &[String]) -> String {
    let Some(expression) = parse_supported_order_expr(identifier.as_str()) else {
        return normalize_identifier(identifier, entity_scope);
    };
    let rewritten = rewrite_supported_order_expr_fields(&expression, |field| {
        normalize_identifier(field.to_string(), entity_scope)
    })
    .expect("supported order expression rewrite must preserve the admitted order family");

    render_supported_order_expr(&rewritten)
        .expect("supported order expression rendering must preserve the admitted order family")
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
                SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlProjection,
                SqlSelectItem, SqlSelectStatement,
            },
        },
        value::Value,
    };

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
                field: "id".to_string(),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
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
                field: "PerfAuditUser.id".to_string(),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        };

        assert!(!select_statement_is_already_local_canonical(&statement));
    }
}
