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
            rewrite_field_identifiers,
        },
        lowering::expr::SqlExprPhase,
        parser::{
            SqlAggregateCall, SqlAggregateInputExpr, SqlArithmeticProjectionCall, SqlExpr,
            SqlHavingClause, SqlHavingValueExpr, SqlOrderTerm, SqlProjection, SqlProjectionOperand,
            SqlRoundProjectionCall, SqlRoundProjectionInput, SqlSelectItem, SqlSelectStatement,
            SqlTextFunctionCall,
        },
    },
};

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
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
    statement.having = normalize_having_clauses(statement.having, entity_scope.as_slice());

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_having_clauses(
    clauses: Vec<SqlHavingClause>,
    entity_scope: &[String],
) -> Vec<SqlHavingClause> {
    SqlIdentifierNormalizer::new(entity_scope).normalize_having_clauses(clauses)
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

    // Rewrite grouped HAVING clauses with the same recursive identifier rules
    // used by projection normalization.
    fn normalize_having_clauses(self, clauses: Vec<SqlHavingClause>) -> Vec<SqlHavingClause> {
        clauses
            .into_iter()
            .map(|clause| SqlHavingClause {
                left: self.normalize_having_value_expr(clause.left),
                op: clause.op,
                right: self.normalize_having_value_expr(clause.right),
            })
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

    // Rewrite one grouped HAVING value while preserving the post-aggregate SQL
    // expression family admitted by the parser.
    fn normalize_having_value_expr(self, expr: SqlHavingValueExpr) -> SqlHavingValueExpr {
        match expr {
            SqlHavingValueExpr::Field(field) => {
                SqlHavingValueExpr::Field(self.normalize_identifier_to_scope(field))
            }
            SqlHavingValueExpr::Aggregate(aggregate) => {
                SqlHavingValueExpr::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlHavingValueExpr::Literal(literal) => SqlHavingValueExpr::Literal(literal),
            SqlHavingValueExpr::Arithmetic(call) => {
                SqlHavingValueExpr::Arithmetic(self.normalize_arithmetic_call(call))
            }
            SqlHavingValueExpr::Round(call) => {
                SqlHavingValueExpr::Round(self.normalize_round_call(call))
            }
            SqlHavingValueExpr::Expr(expr) => {
                SqlHavingValueExpr::Expr(self.normalize_sql_expr(expr))
            }
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
