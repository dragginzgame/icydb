use crate::db::query::preparation::PreparationWork;
use crate::db::sql::lowering::copy::{copy_field_path_parts, copy_select_item_expr};
use crate::db::sql::lowering::expr::{charge_storage, copy_text};
use crate::db::sql::{
    identifier::{identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope},
    lowering::SqlLoweringError,
    parser::{
        SqlAggregateCall, SqlAssignment, SqlDeleteStatement, SqlExpr, SqlOrderTerm, SqlProjection,
        SqlReturningProjection, SqlSelectItem, SqlSelectStatement, SqlUpdateStatement,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &str,
    work: &PreparationWork<'_>,
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
        work,
    )?;
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice(), work)?;
    statement.group_by =
        normalize_identifier_list(statement.group_by, entity_scope.as_slice(), work)?;
    statement.predicate = statement
        .predicate
        .map(|predicate| {
            adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice(), work)
        })
        .transpose()?;
    statement.order_by = normalize_select_order_terms(
        statement.order_by,
        &statement.projection,
        statement.projection_aliases.as_slice(),
        entity_scope.as_slice(),
        work,
    )?;
    statement.having = normalize_having_clauses(
        statement.having,
        &statement.projection,
        statement.projection_aliases.as_slice(),
        entity_scope.as_slice(),
        work,
    )?;
    statement.table_alias = None;

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_delete_statement_to_expected_entity(
    mut statement: SqlDeleteStatement,
    expected_entity: &str,
    work: &PreparationWork<'_>,
) -> Result<SqlDeleteStatement, SqlLoweringError> {
    let entity_scope = sql_statement_scope_candidates(
        statement.entity.as_str(),
        expected_entity,
        statement.table_alias.as_deref(),
        work,
    )?;
    statement.predicate = statement
        .predicate
        .map(|predicate| {
            adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice(), work)
        })
        .transpose()?;
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice(), work)?;
    statement.returning = statement
        .returning
        .map(|returning| normalize_returning_projection(returning, entity_scope.as_slice(), work))
        .transpose()?;
    statement.table_alias = None;

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_update_statement_to_expected_entity(
    mut statement: SqlUpdateStatement,
    expected_entity: &str,
    work: &PreparationWork<'_>,
) -> Result<SqlUpdateStatement, SqlLoweringError> {
    let entity_scope = sql_statement_scope_candidates(
        statement.entity.as_str(),
        expected_entity,
        statement.table_alias.as_deref(),
        work,
    )?;
    statement.assignments =
        normalize_assignments(statement.assignments, entity_scope.as_slice(), work)?;
    statement.predicate = statement
        .predicate
        .map(|predicate| {
            adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice(), work)
        })
        .transpose()?;
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice(), work)?;
    statement.returning = statement
        .returning
        .map(|returning| normalize_returning_projection(returning, entity_scope.as_slice(), work))
        .transpose()?;
    statement.table_alias = None;

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_having_clauses(
    mut clauses: Vec<SqlExpr>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<Vec<SqlExpr>, SqlLoweringError> {
    let normalizer = SqlIdentifierNormalizer::new(entity_scope, work);
    for clause in &mut clauses {
        let original = std::mem::replace(clause, SqlExpr::Literal(crate::value::Value::Null));
        *clause = normalize_scalar_aliases(
            normalizer.normalize_sql_expr(original)?,
            projection,
            projection_aliases,
            work,
        )?;
    }
    Ok(clauses)
}

pub(in crate::db::sql::lowering) fn adapt_sql_predicate_identifiers_to_scope(
    mut predicate: SqlExpr,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    if let SqlExpr::NullTest { expr, .. } = &mut predicate
        && let SqlExpr::Field(field) = expr.as_mut()
    {
        // Preserve this surface's existing extra qualifier reduction without
        // copying the field or replacing its box.
        *field = normalize_identifier(std::mem::take(field), entity_scope, work)?;
    }

    SqlIdentifierNormalizer::new(entity_scope, work).normalize_sql_expr(predicate)
}

// Build one identifier scope used for reducing SQL-qualified field references
// and optional single-table aliases into canonical planner field names.
fn sql_statement_scope_candidates(
    sql_entity: &str,
    expected_entity: &str,
    table_alias: Option<&str>,
    work: &PreparationWork<'_>,
) -> Result<Vec<String>, SqlLoweringError> {
    // Every source has a final segment (possibly empty). Reserve once and keep
    // candidate order/duplicates because direct qualifier matching is ordered.
    let sources = [Some(sql_entity), Some(expected_entity), table_alias];
    let count = 2 * (2 + usize::from(table_alias.is_some()));
    charge_storage::<String>(count, work)?;
    let mut out = Vec::with_capacity(count);
    for source in sources.into_iter().flatten() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        out.push(copy_text(source, work)?);
    }
    for source in sources.into_iter().flatten() {
        work.charge(Resource::PredicateExpressionSteps, 1 + source.len() as u64)?;
        if let Some(last) = identifier_last_segment(source) {
            out.push(copy_text(last, work)?);
        }
    }
    Ok(out)
}

fn normalize_projection_identifiers(
    projection: SqlProjection,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlProjection, SqlLoweringError> {
    SqlIdentifierNormalizer::new(entity_scope, work).normalize_projection(projection)
}

pub(in crate::db::sql::lowering) fn normalize_sql_expr_to_scope(
    expr: SqlExpr,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    SqlIdentifierNormalizer::new(entity_scope, work).normalize_sql_expr(expr)
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
struct SqlIdentifierNormalizer<'a, 'request> {
    entity_scope: &'a [String],
    work: &'a PreparationWork<'request>,
}

impl<'a, 'request> SqlIdentifierNormalizer<'a, 'request> {
    // Freeze one entity scope for all recursive SQL identifier rewrites so
    // projection and HAVING normalization share the same rewrite contract.
    const fn new(entity_scope: &'a [String], work: &'a PreparationWork<'request>) -> Self {
        Self { entity_scope, work }
    }

    // Rewrite all identifiers inside one projection surface while preserving
    // the original SQL projection shape.
    fn normalize_projection(
        self,
        projection: SqlProjection,
    ) -> Result<SqlProjection, SqlLoweringError> {
        Ok(match projection {
            SqlProjection::All => SqlProjection::All,
            SqlProjection::Items(mut items) => {
                for item in &mut items {
                    let original = std::mem::replace(item, SqlSelectItem::Field(String::new()));
                    *item = self.normalize_select_item(original)?;
                }
                SqlProjection::Items(items)
            }
        })
    }

    // Rewrite one select item while preserving the parser-owned projection
    // family chosen for this SQL surface.
    fn normalize_select_item(self, item: SqlSelectItem) -> Result<SqlSelectItem, SqlLoweringError> {
        self.work.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match item {
            SqlSelectItem::Field(field) => {
                let mut expr =
                    normalize_field_identifier_expr_to_scope(field, self.entity_scope, self.work)?;
                match &mut expr {
                    SqlExpr::Field(field) => SqlSelectItem::Field(std::mem::take(field)),
                    _ => SqlSelectItem::Expr(expr),
                }
            }
            SqlSelectItem::Aggregate(mut aggregate) => {
                self.normalize_aggregate_call(&mut aggregate)?;
                SqlSelectItem::Aggregate(aggregate)
            }
            SqlSelectItem::Expr(expr) => SqlSelectItem::Expr(self.normalize_sql_expr(expr)?),
        })
    }

    // Input and FILTER expressions use the same identifier rewrite owner.
    fn normalize_aggregate_call(
        self,
        aggregate: &mut SqlAggregateCall,
    ) -> Result<(), SqlLoweringError> {
        for child in [&mut aggregate.input, &mut aggregate.filter_expr]
            .into_iter()
            .flatten()
        {
            **child = self.normalize_sql_expr(std::mem::replace(
                child.as_mut(),
                SqlExpr::Literal(crate::value::Value::Null),
            ))?;
        }
        Ok(())
    }

    fn normalize_sql_expr(self, mut expr: SqlExpr) -> Result<SqlExpr, SqlLoweringError> {
        self.work.charge(Resource::PredicateExpressionSteps, 1)?;
        match &mut expr {
            SqlExpr::Field(field) => {
                return normalize_field_identifier_expr_to_scope(
                    normalize_identifier(std::mem::take(field), self.entity_scope, self.work)?,
                    self.entity_scope,
                    self.work,
                );
            }
            SqlExpr::FieldPath { root, segments } => {
                return normalize_field_path_to_scope(
                    std::mem::take(root),
                    std::mem::take(segments),
                    self.entity_scope,
                    self.work,
                );
            }
            SqlExpr::Aggregate(aggregate) => self.normalize_aggregate_call(aggregate)?,
            _ => {
                // Keep the existing boxes/vectors while transferring each child;
                // never move fields out of the cleanup-owning expression enum.
                expr.try_for_each_scalar_child_mut(&mut |child| {
                    *child = self.normalize_sql_expr(std::mem::replace(
                        child,
                        SqlExpr::Literal(crate::value::Value::Null),
                    ))?;
                    Ok::<(), SqlLoweringError>(())
                })?;
            }
        }
        Ok(expr)
    }
}

// Both alias policies rewrite scalar field leaves and leave aggregates opaque.
// Visit only original children: do not recursively expand an alias replacement.
fn normalize_scalar_aliases(
    mut expr: SqlExpr,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if let SqlExpr::Field(field) = &expr {
        if let Some(replacement) =
            resolve_projection_alias(field, projection, projection_aliases, work)?
        {
            return Ok(replacement);
        }
        return Ok(expr);
    }
    expr.try_for_each_scalar_child_mut(&mut |child| {
        *child = normalize_scalar_aliases(
            std::mem::replace(child, SqlExpr::Literal(crate::value::Value::Null)),
            projection,
            projection_aliases,
            work,
        )?;
        Ok::<(), SqlLoweringError>(())
    })?;
    Ok(expr)
}

// Normalize `ORDER BY` targets after projection normalization so alias
// rewrites stay lowering-owned and planner order semantics remain
// canonical.
fn normalize_select_order_terms(
    mut terms: Vec<SqlOrderTerm>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<Vec<SqlOrderTerm>, SqlLoweringError> {
    for term in &mut terms {
        let original =
            std::mem::replace(&mut term.field, SqlExpr::Literal(crate::value::Value::Null));
        let field = normalize_sql_expr_to_scope(original, entity_scope, work)?;
        let field = normalize_scalar_aliases(field, projection, projection_aliases, work)?;
        term.field = normalize_sql_expr_to_scope(field, entity_scope, work)?;
    }
    Ok(terms)
}

// ORDER BY and HAVING share first-match, case-insensitive projection alias
// lookup. Their callers retain clause-specific normalization order; replacement
// expressions are copied once and never recursively expanded as aliases.
fn resolve_projection_alias(
    alias_target: &str,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    work: &PreparationWork<'_>,
) -> Result<Option<SqlExpr>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(None);
    };

    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(alias) = alias.as_deref() else {
            continue;
        };
        work.charge(
            Resource::PredicateExpressionSteps,
            alias.len().min(alias_target.len()) as u64,
        )?;
        if !alias.eq_ignore_ascii_case(alias_target) {
            continue;
        }

        return copy_select_item_expr(item, work).map(Some);
    }

    Ok(None)
}

pub(in crate::db::sql::lowering) fn normalize_order_terms(
    mut terms: Vec<SqlOrderTerm>,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<Vec<SqlOrderTerm>, SqlLoweringError> {
    for term in &mut terms {
        let original =
            std::mem::replace(&mut term.field, SqlExpr::Literal(crate::value::Value::Null));
        term.field = normalize_sql_expr_to_scope(original, entity_scope, work)?;
    }
    Ok(terms)
}

fn normalize_assignments(
    mut assignments: Vec<SqlAssignment>,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<Vec<SqlAssignment>, SqlLoweringError> {
    for assignment in &mut assignments {
        assignment.field =
            normalize_identifier(std::mem::take(&mut assignment.field), entity_scope, work)?;
    }
    Ok(assignments)
}

pub(in crate::db::sql::lowering) fn normalize_identifier_list(
    mut fields: Vec<String>,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<Vec<String>, SqlLoweringError> {
    for field in &mut fields {
        *field = normalize_identifier(std::mem::take(field), entity_scope, work)?;
    }
    Ok(fields)
}

fn normalize_returning_projection(
    projection: SqlReturningProjection,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlReturningProjection, SqlLoweringError> {
    Ok(match projection {
        SqlReturningProjection::All => SqlReturningProjection::All,
        SqlReturningProjection::Fields(fields) => {
            SqlReturningProjection::Fields(normalize_identifier_list(fields, entity_scope, work)?)
        }
    })
}

// SQL lowering keeps string-only identifier normalization for surfaces that do
// not carry nested path semantics, such as GROUP BY and RETURNING field lists.
fn normalize_identifier(
    identifier: String,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<String, SqlLoweringError> {
    // The public pure utility remains the matching authority. Charge a byte
    // allowance for its direct/full/tail comparisons, split and owned compaction
    // before calling it; lengths are O(1), and scope visits are charged first.
    work.charge(
        Resource::PredicateExpressionSteps,
        1 + 2 * identifier.len() as u64,
    )?;
    for candidate in entity_scope {
        work.charge(
            Resource::PredicateExpressionSteps,
            1 + 3 * (identifier.len() as u64 + candidate.len() as u64),
        )?;
    }
    Ok(normalize_identifier_to_scope(identifier, entity_scope))
}

// Normalize a parser-owned field leaf into either a scoped top-level field or
// a nested field path. Predicate parsing keeps dotted identifiers as field
// strings so this lowering boundary can distinguish `alias.field` from
// `field.subfield` after the statement's entity scope is known.
fn normalize_field_identifier_expr_to_scope(
    identifier: String,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    work.charge(
        Resource::PredicateExpressionSteps,
        1 + identifier.len() as u64,
    )?;
    let Some((root, tail)) = identifier.split_once('.') else {
        return Ok(SqlExpr::Field(identifier));
    };
    let (root, segments) = copy_field_path_parts(root, tail, work)?;
    normalize_field_path_to_scope(root, segments, entity_scope, work)
}

// Reduce the longest entity-qualified prefix from a parsed field path while
// preserving any remaining nested path as a parser-owned field-path leaf.
fn normalize_field_path_to_scope(
    root: String,
    mut segments: Vec<String>,
    entity_scope: &[String],
    work: &PreparationWork<'_>,
) -> Result<SqlExpr, SqlLoweringError> {
    // Full equality implies equal tails, so matching a joined qualifier is
    // exactly matching its final component. Search longest prefixes first,
    // without joined strings or a second root-plus-segments vector.
    for split_at in (1..=segments.len()).rev() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let qualifier_tail = if split_at == 1 {
            &root
        } else {
            &segments[split_at - 2]
        };
        for candidate in entity_scope {
            work.charge(
                Resource::PredicateExpressionSteps,
                1 + 3 * (candidate.len() as u64 + qualifier_tail.len() as u64),
            )?;
            if identifiers_tail_match(candidate, qualifier_tail) {
                // Charge initialized handle movement before compacting the
                // retained suffix; String payloads move without being copied.
                work.charge(
                    Resource::PredicateExpressionSteps,
                    (segments.len() as u64).saturating_mul(size_of::<String>() as u64),
                )?;
                let retained_root = std::mem::take(&mut segments[split_at - 1]);
                segments.drain(..split_at);
                return Ok(sql_field_expr_from_parts(retained_root, segments));
            }
        }
    }
    Ok(sql_field_expr_from_parts(root, segments))
}

// Preserve owned payloads and segment backing on both matching and local paths.
fn sql_field_expr_from_parts(root: String, segments: Vec<String>) -> SqlExpr {
    if segments.is_empty() {
        SqlExpr::Field(root)
    } else {
        SqlExpr::FieldPath { root, segments }
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

#[cfg(test)]
mod scope_tests;

#[cfg(test)]
mod alias_budget_tests;

#[cfg(test)]
mod identifier_budget_tests;
