use crate::db::sql::{
    identifier::{identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope},
    lowering::SqlLoweringError,
    parser::{
        SqlAggregateCall, SqlAssignment, SqlDeleteStatement, SqlExpr, SqlOrderTerm, SqlProjection,
        SqlReturningProjection, SqlSelectItem, SqlSelectStatement, SqlUpdateStatement,
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
    );
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
) -> Vec<SqlExpr> {
    SqlIdentifierNormalizer::new(entity_scope)
        .normalize_having_clauses(clauses)
        .into_iter()
        .map(|clause| {
            normalize_scalar_aliases(clause, &|field| {
                resolve_projection_having_alias(field, projection, projection_aliases)
            })
        })
        .collect()
}

pub(in crate::db::sql::lowering) fn adapt_sql_predicate_identifiers_to_scope(
    mut predicate: SqlExpr,
    entity_scope: &[String],
) -> SqlExpr {
    if let SqlExpr::NullTest { expr, negated } = &predicate
        && let SqlExpr::Field(field) = expr.as_ref()
    {
        predicate = SqlExpr::NullTest {
            expr: Box::new(SqlExpr::Field(normalize_identifier(
                field.clone(),
                entity_scope,
            ))),
            negated: *negated,
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
                let mut expr = self.normalize_sql_expr(SqlExpr::from_field_identifier(field));
                match &mut expr {
                    SqlExpr::Field(field) => SqlSelectItem::Field(std::mem::take(field)),
                    _ => SqlSelectItem::Expr(expr),
                }
            }
            SqlSelectItem::Aggregate(mut aggregate) => {
                self.normalize_aggregate_call(&mut aggregate);
                SqlSelectItem::Aggregate(aggregate)
            }
            SqlSelectItem::Expr(expr) => SqlSelectItem::Expr(self.normalize_sql_expr(expr)),
        }
    }

    // Aggregate calls only rewrite their optional field target, so keep that
    // field-local transformation behind one owner-local helper.
    fn normalize_aggregate_call(self, aggregate: &mut SqlAggregateCall) {
        for child in [&mut aggregate.input, &mut aggregate.filter_expr]
            .into_iter()
            .flatten()
        {
            **child = self.normalize_sql_expr(std::mem::replace(
                child.as_mut(),
                SqlExpr::Literal(crate::value::Value::Null),
            ));
        }
    }

    fn normalize_sql_expr(self, mut expr: SqlExpr) -> SqlExpr {
        match &mut expr {
            SqlExpr::Field(field) => {
                return normalize_field_identifier_expr_to_scope(
                    self.normalize_identifier_to_scope(std::mem::take(field)),
                    self.entity_scope,
                );
            }
            SqlExpr::FieldPath { root, segments } => {
                return normalize_field_path_to_scope(
                    std::mem::take(root),
                    std::mem::take(segments),
                    self.entity_scope,
                );
            }
            SqlExpr::Aggregate(aggregate) => self.normalize_aggregate_call(aggregate),
            _ => {
                // Keep the existing boxes/vectors while transferring each child;
                // never move fields out of the cleanup-owning expression enum.
                expr.for_each_scalar_child_mut(&mut |child| {
                    *child = self.normalize_sql_expr(std::mem::replace(
                        child,
                        SqlExpr::Literal(crate::value::Value::Null),
                    ));
                });
            }
        }
        expr
    }

    // Some SQL surfaces rewrite directly onto the resolved entity scope instead
    // of the broader helper used by order-expression normalization.
    fn normalize_identifier_to_scope(self, identifier: String) -> String {
        normalize_identifier_to_scope(identifier, self.entity_scope)
    }
}

// Both alias policies rewrite scalar field leaves and leave aggregates opaque.
// Visit only original children: do not recursively expand an alias replacement.
fn normalize_scalar_aliases(
    mut expr: SqlExpr,
    resolve: &impl Fn(&str) -> Option<SqlExpr>,
) -> SqlExpr {
    if let SqlExpr::Field(field) = &expr {
        if let Some(replacement) = resolve(field) {
            return replacement;
        }
        return expr;
    }
    expr.for_each_scalar_child_mut(&mut |child| {
        *child = normalize_scalar_aliases(
            std::mem::replace(child, SqlExpr::Literal(crate::value::Value::Null)),
            resolve,
        );
    });
    expr
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
            let field = normalize_scalar_aliases(field, &|field| {
                resolve_projection_order_alias(field, projection, projection_aliases)
            });

            Ok(SqlOrderTerm {
                field: normalize_sql_expr_to_scope(field, entity_scope),
                direction: term.direction,
            })
        })
        .collect()
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
    let mut path_segments = identifier.split('.');
    let Some(root) = path_segments.next() else {
        return SqlExpr::Field(identifier);
    };

    let segments = path_segments.map(str::to_string).collect::<Vec<_>>();
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
    let mut path_segments = Vec::with_capacity(1 + segments.len());
    path_segments.push(root);
    path_segments.extend(segments);

    for split_at in (1..path_segments.len()).rev() {
        let qualifier = path_segments[..split_at].join(".");
        if entity_scope
            .iter()
            .any(|candidate| identifiers_tail_match(candidate.as_str(), qualifier.as_str()))
        {
            return sql_field_expr_from_segments(&path_segments[split_at..]);
        }
    }

    sql_field_expr_from_segments(path_segments.as_slice())
}

// Rebuild one normalized field/path from its already-split identifier segments.
fn sql_field_expr_from_segments(path_segments: &[String]) -> SqlExpr {
    match path_segments {
        [field] => SqlExpr::Field(field.clone()),
        [root, segments @ ..] => SqlExpr::FieldPath {
            root: root.clone(),
            segments: segments.to_vec(),
        },
        [] => SqlExpr::Field(String::new()),
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
mod tests;
