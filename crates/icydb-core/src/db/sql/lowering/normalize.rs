use crate::db::{
    predicate::Predicate,
    sql::{
        identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers,
        },
        lowering::SqlLoweringError,
        parser::{
            SqlAggregateCall, SqlAssignment, SqlDeleteStatement, SqlExpr, SqlOrderTerm,
            SqlProjection, SqlReturningProjection, SqlSelectItem, SqlSelectStatement,
            SqlUpdateStatement,
        },
    },
};

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    // Plain local scalar selects already arrive in the canonical scope shape
    // used by the planner, so skip the full statement rebuild when there is
    // nothing left to rewrite.
    if statement.is_already_local_canonical() {
        return Ok(statement);
    }

    // Re-scope parsed identifiers onto the resolved entity surface after the
    // caller has already established entity ownership for this statement.
    let entity_scope = sql_statement_scope_candidates(
        statement.entity.as_str(),
        expected_entity,
        statement.table_alias.as_deref(),
    );
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
    statement.table_alias = None;

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_delete_statement_to_expected_entity(
    mut statement: SqlDeleteStatement,
    expected_entity: &str,
) -> SqlDeleteStatement {
    let entity_scope = sql_statement_scope_candidates(
        statement.entity.as_str(),
        expected_entity,
        statement.table_alias.as_deref(),
    );
    statement.predicate = statement.predicate.map(|predicate| {
        adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice())
    });
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());
    statement.returning = statement
        .returning
        .map(|returning| normalize_returning_projection(returning, entity_scope.as_slice()));
    statement.table_alias = None;

    statement
}

pub(in crate::db::sql::lowering) fn normalize_update_statement_to_expected_entity(
    mut statement: SqlUpdateStatement,
    expected_entity: &str,
) -> SqlUpdateStatement {
    let entity_scope = sql_statement_scope_candidates(
        statement.entity.as_str(),
        expected_entity,
        statement.table_alias.as_deref(),
    );
    statement.assignments = normalize_assignments(statement.assignments, entity_scope.as_slice());
    statement.predicate = statement.predicate.map(|predicate| {
        adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice())
    });
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());
    statement.returning = statement
        .returning
        .map(|returning| normalize_returning_projection(returning, entity_scope.as_slice()));
    statement.table_alias = None;

    statement
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
// and optional single-table aliases into canonical planner field names.
fn sql_statement_scope_candidates(
    sql_entity: &str,
    expected_entity: &str,
    table_alias: Option<&str>,
) -> Vec<String> {
    let mut out = Vec::new();
    out.push(sql_entity.to_string());
    out.push(expected_entity.to_string());
    if let Some(alias) = table_alias {
        out.push(alias.to_string());
    }

    if let Some(last) = identifier_last_segment(sql_entity) {
        out.push(last.to_string());
    }
    if let Some(last) = identifier_last_segment(expected_entity) {
        out.push(last.to_string());
    }
    if let Some(alias) = table_alias
        && let Some(last) = identifier_last_segment(alias)
    {
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
            SqlSelectItem::Field(field) => {
                match self.normalize_sql_expr(SqlExpr::from_field_identifier(field)) {
                    SqlExpr::Field(field) => SqlSelectItem::Field(field),
                    expr => SqlSelectItem::Expr(expr),
                }
            }
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
            SqlExpr::Field(field) => normalize_field_identifier_expr_to_scope(
                self.normalize_identifier_to_scope(field),
                self.entity_scope,
            ),
            SqlExpr::FieldPath { root, segments } => {
                normalize_field_path_to_scope(root, segments, self.entity_scope)
            }
            SqlExpr::Aggregate(aggregate) => {
                SqlExpr::Aggregate(self.normalize_aggregate_call(aggregate))
            }
            SqlExpr::Literal(literal) => SqlExpr::Literal(literal),
            SqlExpr::Param { index } => SqlExpr::Param { index },
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
            SqlExpr::Like {
                expr,
                pattern,
                negated,
                casefold,
            } => SqlExpr::Like {
                expr: Box::new(self.normalize_sql_expr(*expr)),
                pattern,
                negated,
                casefold,
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

    // Some SQL surfaces rewrite directly onto the resolved entity scope instead
    // of the broader helper used by order-expression normalization.
    fn normalize_identifier_to_scope(self, identifier: String) -> String {
        normalize_identifier_to_scope(identifier, self.entity_scope)
    }
}

// Normalize `HAVING` targets after identifier normalization so projection
// aliases reuse the same lowering-owned rewrite boundary as `ORDER BY`.
#[expect(
    clippy::too_many_lines,
    reason = "recursive SQL expression normalization keeps every expression variant explicit"
)]
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
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. } => Ok(expr),
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
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            casefold,
        } => Ok(SqlExpr::Like {
            expr: Box::new(normalize_having_aliases(
                *expr,
                projection,
                projection_aliases,
            )?),
            pattern,
            negated,
            casefold,
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
// rewrites stay lowering-owned and planner order semantics remain
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
            let field = normalize_sql_expr_to_scope(term.field, entity_scope);
            let field = normalize_order_aliases(field, projection, projection_aliases);

            Ok(SqlOrderTerm {
                field: normalize_sql_expr_to_scope(field, entity_scope),
                direction: term.direction,
            })
        })
        .collect()
}

// Normalize `ORDER BY` expressions after projection normalization so aliases
// can participate as leaves inside larger arithmetic, CASE, and function order
// targets without inventing any new planner-owned semantics.
fn normalize_order_aliases(
    expr: SqlExpr,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> SqlExpr {
    match expr {
        SqlExpr::Field(field) => {
            resolve_projection_order_alias(field.as_str(), projection, projection_aliases)
                .unwrap_or(SqlExpr::Field(field))
        }
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. } => expr,
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => SqlExpr::Membership {
            expr: Box::new(normalize_order_aliases(
                *expr,
                projection,
                projection_aliases,
            )),
            values,
            negated,
        },
        SqlExpr::NullTest { expr, negated } => SqlExpr::NullTest {
            expr: Box::new(normalize_order_aliases(
                *expr,
                projection,
                projection_aliases,
            )),
            negated,
        },
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            casefold,
        } => SqlExpr::Like {
            expr: Box::new(normalize_order_aliases(
                *expr,
                projection,
                projection_aliases,
            )),
            pattern,
            negated,
            casefold,
        },
        SqlExpr::FunctionCall { function, args } => SqlExpr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| normalize_order_aliases(arg, projection, projection_aliases))
                .collect(),
        },
        SqlExpr::Unary { op, expr } => SqlExpr::Unary {
            op,
            expr: Box::new(normalize_order_aliases(
                *expr,
                projection,
                projection_aliases,
            )),
        },
        SqlExpr::Binary { op, left, right } => SqlExpr::Binary {
            op,
            left: Box::new(normalize_order_aliases(
                *left,
                projection,
                projection_aliases,
            )),
            right: Box::new(normalize_order_aliases(
                *right,
                projection,
                projection_aliases,
            )),
        },
        SqlExpr::Case { arms, else_expr } => SqlExpr::Case {
            arms: arms
                .into_iter()
                .map(|arm| crate::db::sql::parser::SqlCaseArm {
                    condition: normalize_order_aliases(
                        arm.condition,
                        projection,
                        projection_aliases,
                    ),
                    result: normalize_order_aliases(arm.result, projection, projection_aliases),
                })
                .collect(),
            else_expr: else_expr.map(|else_expr| {
                Box::new(normalize_order_aliases(
                    *else_expr,
                    projection,
                    projection_aliases,
                ))
            }),
        },
    }
}

// Resolve one `ORDER BY <alias>` leaf onto one already-supported projection
// order target. Recursive normalization owns larger expression shapes, while
// unsupported leaves still fail closed later during ordinary field lowering.
fn resolve_projection_order_alias(
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

fn normalize_assignments(
    assignments: Vec<SqlAssignment>,
    entity_scope: &[String],
) -> Vec<SqlAssignment> {
    assignments
        .into_iter()
        .map(|assignment| SqlAssignment {
            field: normalize_identifier(assignment.field, entity_scope),
            value: assignment.value,
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

fn normalize_returning_projection(
    projection: SqlReturningProjection,
    entity_scope: &[String],
) -> SqlReturningProjection {
    match projection {
        SqlReturningProjection::All => SqlReturningProjection::All,
        SqlReturningProjection::Fields(fields) => {
            SqlReturningProjection::Fields(normalize_identifier_list(fields, entity_scope))
        }
    }
}

// SQL lowering keeps string-only identifier normalization for surfaces that do
// not carry nested path semantics, such as GROUP BY and RETURNING field lists.
fn normalize_identifier(identifier: String, entity_scope: &[String]) -> String {
    normalize_identifier_to_scope(identifier, entity_scope)
}

// Normalize a parser-owned field leaf into either a scoped top-level field or
// a nested field path. Predicate parsing keeps dotted identifiers as field
// strings so this lowering boundary can distinguish `alias.field` from
// `field.subfield` after the statement's entity scope is known.
fn normalize_field_identifier_expr_to_scope(
    identifier: String,
    entity_scope: &[String],
) -> SqlExpr {
    let mut parts = identifier.split('.');
    let Some(root) = parts.next() else {
        return SqlExpr::Field(identifier);
    };

    let segments = parts.map(str::to_string).collect::<Vec<_>>();
    if segments.is_empty() {
        return SqlExpr::Field(root.to_string());
    }

    normalize_field_path_to_scope(root.to_string(), segments, entity_scope)
}

// Reduce the longest entity-qualified prefix from a parsed field path while
// preserving any remaining nested path as a parser-owned field-path leaf.
fn normalize_field_path_to_scope(
    root: String,
    segments: Vec<String>,
    entity_scope: &[String],
) -> SqlExpr {
    let mut parts = Vec::with_capacity(1 + segments.len());
    parts.push(root);
    parts.extend(segments);

    for split_at in (1..parts.len()).rev() {
        let qualifier = parts[..split_at].join(".");
        if entity_scope
            .iter()
            .any(|candidate| identifiers_tail_match(candidate.as_str(), qualifier.as_str()))
        {
            return sql_field_expr_from_parts(&parts[split_at..]);
        }
    }

    sql_field_expr_from_parts(parts.as_slice())
}

// Rebuild one normalized field/path from its already-split identifier parts.
fn sql_field_expr_from_parts(parts: &[String]) -> SqlExpr {
    match parts {
        [field] => SqlExpr::Field(field.clone()),
        [root, segments @ ..] => SqlExpr::FieldPath {
            root: root.clone(),
            segments: segments.to_vec(),
        },
        [] => unreachable!("field path normalization always keeps at least one segment"),
    }
}

pub(in crate::db::sql::lowering) fn ensure_entity_matches_expected(
    sql_entity: &str,
    expected_entity: &str,
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
        db::sql::parser::{
            SqlAggregateCall, SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm,
            SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement, parse_sql,
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
            table_alias: None,
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

        assert!(statement.is_already_local_canonical());
    }

    #[test]
    fn local_scalar_select_with_supported_order_expr_is_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "PerfAuditUser".to_string(),
            table_alias: None,
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

        assert!(statement.is_already_local_canonical());
    }

    #[test]
    fn local_grouped_select_with_local_aggregate_is_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "PerfAuditUser".to_string(),
            table_alias: None,
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

        assert!(statement.is_already_local_canonical());
    }

    #[test]
    fn qualified_field_select_is_not_already_local_canonical() {
        let statement = SqlSelectStatement {
            entity: "public.PerfAuditUser".to_string(),
            table_alias: None,
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

        assert!(!statement.is_already_local_canonical());
    }

    #[test]
    fn predicate_identifier_normalization_preserves_nested_field_paths() {
        let statement = SqlSelectStatement {
            entity: "users".to_string(),
            table_alias: Some("u".to_string()),
            projection: SqlProjection::All,
            projection_aliases: vec![],
            predicate: Some(SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(SqlExpr::Field("profile.rank".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(5))),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(SqlExpr::Field("u.age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(21))),
                }),
            }),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let normalized = super::normalize_select_statement_to_expected_entity(statement, "users")
            .expect("predicate identifiers should normalize");

        assert_eq!(
            normalized.predicate,
            Some(SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(SqlExpr::FieldPath {
                        root: "profile".to_string(),
                        segments: vec!["rank".to_string()],
                    }),
                    right: Box::new(SqlExpr::Literal(Value::Int(5))),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(21))),
                }),
            }),
        );
    }
}
