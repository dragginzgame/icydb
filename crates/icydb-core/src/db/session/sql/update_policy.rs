//! Module: db::session::sql::update_policy
//! Responsibility: parser-shape classification and exposure-policy checks for
//! SQL `UPDATE` before a generated/public write surface can execute it.
//! Does not own: row mutation execution, field validation, or persistence.
//! Boundary: keeps public/generated UPDATE admission stricter than the broad
//! session write lane.

use crate::db::{
    QueryError,
    sql::parser::{
        SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlReturningProjection, SqlStatement,
        SqlUpdateStatement, parse_sql_with_attribution,
    },
};
use std::collections::BTreeSet;

/// SQL `UPDATE` exposure policy selected by a caller before execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlUpdateExposurePolicy {
    /// Current broad session/library write-lane policy.
    SessionWriteCurrent,
    /// Generated read/query endpoint policy. `UPDATE` is never admitted.
    GeneratedQuery,
    /// Generated schema DDL endpoint policy. `UPDATE` is never admitted.
    GeneratedDdl,
    /// Public-safe policy requiring complete primary-key equality in `WHERE`.
    PublicPrimaryKeyOnly,
    /// Public-safe bounded non-primary-key policy requiring explicit primary-key ordering.
    PublicBoundedDeterministic,
    /// Future admin/bulk policy; still rejects unsafe field assignment in this gate.
    AdminBulk,
}

/// Schema-derived field context needed to classify one `UPDATE`.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub struct SqlUpdatePolicyContext<'a> {
    /// Primary-key fields in canonical order.
    pub primary_key_fields: &'a [&'a str],
    /// Generated-owned fields that SQL `UPDATE` must not assign.
    pub generated_fields: &'a [&'a str],
    /// Managed/internal fields that SQL `UPDATE` must not assign.
    pub managed_fields: &'a [&'a str],
    /// Maximum admitted limit for the public bounded deterministic policy.
    pub max_public_bounded_limit: u32,
}

impl<'a> SqlUpdatePolicyContext<'a> {
    /// Build a context with no generated/managed fields and the default public bound.
    #[must_use]
    pub const fn new(primary_key_fields: &'a [&'a str]) -> Self {
        Self {
            primary_key_fields,
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: 100,
        }
    }
}

/// Assignment ownership classification for one parsed `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlUpdateAssignmentPolicy {
    /// Whether the statement assigns any primary-key field.
    pub mutates_primary_key: bool,
    /// Whether the statement assigns any generated-owned field.
    pub mutates_generated: bool,
    /// Whether the statement assigns any managed/internal field.
    pub mutates_managed: bool,
}

impl SqlUpdateAssignmentPolicy {
    const fn admitted(self) -> bool {
        !self.mutates_primary_key && !self.mutates_generated && !self.mutates_managed
    }
}

/// `WHERE` classification for one parsed `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlUpdateWherePolicy {
    /// The statement has no `WHERE` clause.
    Missing,
    /// The `WHERE` clause proves complete primary-key equality under v1 rules.
    PrimaryKeyEquality,
    /// The `WHERE` clause exists but does not prove primary-key equality.
    Other,
}

impl SqlUpdateWherePolicy {
    /// Return whether a `WHERE` clause was present.
    #[must_use]
    pub const fn has_where(self) -> bool {
        !matches!(self, Self::Missing)
    }

    /// Return whether v1 primary-key equality proof passed.
    #[must_use]
    pub const fn is_primary_key_equality(self) -> bool {
        matches!(self, Self::PrimaryKeyEquality)
    }
}

/// Explicit `ORDER BY` classification for one parsed `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlUpdateOrderPolicy {
    /// The statement has no explicit `ORDER BY`.
    Missing,
    /// The statement explicitly orders by canonical primary-key fields ascending.
    CanonicalPrimaryKey,
    /// The statement orders by canonical primary-key fields but uses descending order.
    DescendingPrimaryKey,
    /// The statement has another explicit ordering shape.
    Other,
}

/// Narrow write `RETURNING` classification for one parsed `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlUpdateReturningPolicy {
    /// No `RETURNING` clause.
    None,
    /// Narrow `RETURNING *`.
    NarrowAll,
    /// Narrow `RETURNING field, ...`.
    NarrowFields,
}

impl SqlUpdateReturningPolicy {
    /// Return whether the statement requests `RETURNING`.
    #[must_use]
    pub const fn is_requested(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Return whether the requested `RETURNING` shape is currently narrow.
    #[must_use]
    pub const fn is_narrow(self) -> bool {
        matches!(self, Self::NarrowAll | Self::NarrowFields)
    }
}

/// Parsed `UPDATE` classification before a caller-selected exposure policy is applied.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlUpdateStatementClassification {
    /// Target entity identifier.
    pub target_entity: String,
    /// Fields assigned by the `SET` list in parser order.
    pub assigned_fields: Vec<String>,
    /// Assignment ownership classification.
    pub assignment_policy: SqlUpdateAssignmentPolicy,
    /// `WHERE` proof classification.
    pub where_policy: SqlUpdateWherePolicy,
    /// Explicit `ORDER BY` classification.
    pub order_policy: SqlUpdateOrderPolicy,
    /// Parsed `LIMIT`, if supplied.
    pub limit: Option<u32>,
    /// Parsed `OFFSET`, if supplied.
    pub offset: Option<u32>,
    /// Narrow write `RETURNING` classification.
    pub returning_policy: SqlUpdateReturningPolicy,
}

impl SqlUpdateStatementClassification {
    /// Return whether the statement has an explicit positive `LIMIT`.
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        matches!(self.limit, Some(limit) if limit > 0)
    }

    /// Return whether the statement has explicit canonical ascending primary-key order.
    #[must_use]
    pub const fn has_explicit_canonical_primary_key_order(&self) -> bool {
        matches!(self.order_policy, SqlUpdateOrderPolicy::CanonicalPrimaryKey)
    }
}

/// Stable policy rejection for one classified SQL `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlUpdatePolicyRejection {
    /// The parsed statement is not `UPDATE`.
    NotUpdate,
    /// Generated query surfaces reject all `UPDATE`.
    GeneratedQueryRejectsUpdate,
    /// Generated DDL surfaces reject all `UPDATE`.
    GeneratedDdlRejectsUpdate,
    /// This policy requires a `WHERE` clause.
    MissingWhere,
    /// This policy rejects primary-key assignment.
    PrimaryKeyMutation,
    /// This policy rejects generated-owned field assignment.
    GeneratedFieldMutation,
    /// This policy rejects managed/internal field assignment.
    ManagedFieldMutation,
    /// The `WHERE` clause did not prove complete primary-key equality.
    PrimaryKeyProofFailed,
    /// This policy requires explicit canonical primary-key ordering.
    MissingCanonicalPrimaryKeyOrder,
    /// This policy rejects descending ordering in v1.
    DescendingOrder,
    /// This policy requires a positive `LIMIT`.
    MissingLimit,
    /// This policy rejects `OFFSET`.
    OffsetUnsupported,
    /// The supplied `LIMIT` exceeds the policy maximum.
    LimitTooHigh,
}

/// Result of classifying one SQL statement under an `UPDATE` exposure policy.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlUpdatePolicyReport {
    /// Parsed `UPDATE` classification when the statement is an `UPDATE`.
    pub classification: Option<SqlUpdateStatementClassification>,
    /// Policy rejection, or `None` when the selected policy admits the statement.
    pub rejection: Option<SqlUpdatePolicyRejection>,
}

impl SqlUpdatePolicyReport {
    /// Return whether the selected policy admits the statement.
    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        self.rejection.is_none()
    }

    const fn rejected(rejection: SqlUpdatePolicyRejection) -> Self {
        Self {
            classification: None,
            rejection: Some(rejection),
        }
    }
}

/// Classify one SQL statement under an explicit `UPDATE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided schema
/// field categories.
pub fn classify_sql_update_policy(
    sql: &str,
    policy: SqlUpdateExposurePolicy,
    context: SqlUpdatePolicyContext<'_>,
) -> Result<SqlUpdatePolicyReport, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(classify_sql_update_statement_policy(
        &statement, policy, context,
    ))
}

pub(in crate::db) fn classify_sql_update_statement_policy(
    statement: &SqlStatement,
    policy: SqlUpdateExposurePolicy,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdatePolicyReport {
    let SqlStatement::Update(statement) = statement else {
        return SqlUpdatePolicyReport::rejected(SqlUpdatePolicyRejection::NotUpdate);
    };

    let classification = classify_update_statement(statement, context);
    let rejection = update_policy_rejection(policy, &classification, context);

    SqlUpdatePolicyReport {
        classification: Some(classification),
        rejection,
    }
}

fn classify_update_statement(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdateStatementClassification {
    let assigned_fields = statement
        .assignments
        .iter()
        .map(|assignment| assignment.field.clone())
        .collect::<Vec<_>>();

    SqlUpdateStatementClassification {
        target_entity: statement.entity.clone(),
        assigned_fields,
        assignment_policy: assignment_policy(statement, context),
        where_policy: where_policy(statement, context),
        order_policy: order_policy(statement, context),
        limit: statement.limit,
        offset: statement.offset,
        returning_policy: returning_policy(statement),
    }
}

fn update_policy_rejection(
    policy: SqlUpdateExposurePolicy,
    classification: &SqlUpdateStatementClassification,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlUpdatePolicyRejection> {
    match policy {
        SqlUpdateExposurePolicy::GeneratedQuery => {
            return Some(SqlUpdatePolicyRejection::GeneratedQueryRejectsUpdate);
        }
        SqlUpdateExposurePolicy::GeneratedDdl => {
            return Some(SqlUpdatePolicyRejection::GeneratedDdlRejectsUpdate);
        }
        SqlUpdateExposurePolicy::SessionWriteCurrent
        | SqlUpdateExposurePolicy::PublicPrimaryKeyOnly
        | SqlUpdateExposurePolicy::PublicBoundedDeterministic
        | SqlUpdateExposurePolicy::AdminBulk => {}
    }

    if !classification.where_policy.has_where() {
        return Some(SqlUpdatePolicyRejection::MissingWhere);
    }

    if !classification.assignment_policy.admitted() {
        return unsafe_assignment_rejection(classification.assignment_policy);
    }

    match policy {
        SqlUpdateExposurePolicy::SessionWriteCurrent | SqlUpdateExposurePolicy::AdminBulk => None,
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => {
            if classification.where_policy.is_primary_key_equality() {
                None
            } else {
                Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed)
            }
        }
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => {
            bounded_policy_rejection(classification, context)
        }
        SqlUpdateExposurePolicy::GeneratedQuery | SqlUpdateExposurePolicy::GeneratedDdl => {
            unreachable!("generated policies returned before shared checks")
        }
    }
}

const fn unsafe_assignment_rejection(
    policy: SqlUpdateAssignmentPolicy,
) -> Option<SqlUpdatePolicyRejection> {
    if policy.mutates_primary_key {
        Some(SqlUpdatePolicyRejection::PrimaryKeyMutation)
    } else if policy.mutates_generated {
        Some(SqlUpdatePolicyRejection::GeneratedFieldMutation)
    } else if policy.mutates_managed {
        Some(SqlUpdatePolicyRejection::ManagedFieldMutation)
    } else {
        None
    }
}

const fn bounded_policy_rejection(
    classification: &SqlUpdateStatementClassification,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlUpdatePolicyRejection> {
    if classification.offset.is_some() {
        return Some(SqlUpdatePolicyRejection::OffsetUnsupported);
    }

    let Some(limit) = classification.limit else {
        return Some(SqlUpdatePolicyRejection::MissingLimit);
    };
    if limit == 0 {
        return Some(SqlUpdatePolicyRejection::MissingLimit);
    }
    if limit > context.max_public_bounded_limit {
        return Some(SqlUpdatePolicyRejection::LimitTooHigh);
    }

    match classification.order_policy {
        SqlUpdateOrderPolicy::CanonicalPrimaryKey => None,
        SqlUpdateOrderPolicy::DescendingPrimaryKey => {
            Some(SqlUpdatePolicyRejection::DescendingOrder)
        }
        SqlUpdateOrderPolicy::Missing | SqlUpdateOrderPolicy::Other => {
            Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder)
        }
    }
}

fn assignment_policy(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdateAssignmentPolicy {
    SqlUpdateAssignmentPolicy {
        mutates_primary_key: statement.assignments.iter().any(|assignment| {
            assignment_field_name(statement, assignment.field.as_str())
                .is_some_and(|field| contains_field(context.primary_key_fields, field))
        }),
        mutates_generated: statement.assignments.iter().any(|assignment| {
            assignment_field_name(statement, assignment.field.as_str())
                .is_some_and(|field| contains_field(context.generated_fields, field))
        }),
        mutates_managed: statement.assignments.iter().any(|assignment| {
            assignment_field_name(statement, assignment.field.as_str())
                .is_some_and(|field| contains_field(context.managed_fields, field))
        }),
    }
}

fn where_policy(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdateWherePolicy {
    let Some(predicate) = statement.predicate.as_ref() else {
        return SqlUpdateWherePolicy::Missing;
    };

    if primary_key_equality_proof(
        predicate,
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
        context.primary_key_fields,
    ) {
        SqlUpdateWherePolicy::PrimaryKeyEquality
    } else {
        SqlUpdateWherePolicy::Other
    }
}

fn order_policy(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
) -> SqlUpdateOrderPolicy {
    if statement.order_by.is_empty() {
        return SqlUpdateOrderPolicy::Missing;
    }
    if statement.order_by.len() != context.primary_key_fields.len() {
        return SqlUpdateOrderPolicy::Other;
    }

    let mut all_canonical = true;
    let mut saw_descending = false;
    for (term, primary_key) in statement
        .order_by
        .iter()
        .zip(context.primary_key_fields.iter().copied())
    {
        let ordered_field = simple_field_name(
            &term.field,
            statement.entity.as_str(),
            statement.table_alias.as_deref(),
        );
        all_canonical &= ordered_field.is_some_and(|field| field == primary_key);
        saw_descending |= matches!(term.direction, SqlOrderDirection::Desc);
    }

    if !all_canonical {
        SqlUpdateOrderPolicy::Other
    } else if saw_descending {
        SqlUpdateOrderPolicy::DescendingPrimaryKey
    } else {
        SqlUpdateOrderPolicy::CanonicalPrimaryKey
    }
}

const fn returning_policy(statement: &SqlUpdateStatement) -> SqlUpdateReturningPolicy {
    match &statement.returning {
        None => SqlUpdateReturningPolicy::None,
        Some(SqlReturningProjection::All) => SqlUpdateReturningPolicy::NarrowAll,
        Some(SqlReturningProjection::Fields(_)) => SqlUpdateReturningPolicy::NarrowFields,
    }
}

fn primary_key_equality_proof(
    predicate: &SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> bool {
    if primary_key_fields.is_empty() {
        return false;
    }

    let mut observed = BTreeSet::new();
    for leaf in conjunctive_leaves(predicate) {
        let Some(field) = primary_key_equality_field(leaf, entity, table_alias) else {
            return false;
        };
        if !contains_field(primary_key_fields, field) || !observed.insert(field.to_string()) {
            return false;
        }
    }

    primary_key_fields
        .iter()
        .all(|primary_key| observed.contains(*primary_key))
}

fn conjunctive_leaves(expr: &SqlExpr) -> Vec<&SqlExpr> {
    match expr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => {
            let mut leaves = conjunctive_leaves(left);
            leaves.extend(conjunctive_leaves(right));
            leaves
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => vec![expr],
    }
}

fn primary_key_equality_field<'a>(
    expr: &'a SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    let SqlExpr::Binary {
        op: SqlExprBinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };

    let left_field = simple_field_name(left, entity, table_alias);
    let right_field = simple_field_name(right, entity, table_alias);
    match (left_field, right_field) {
        (Some(field), None) => comparable_constant(right).then_some(field),
        (None, Some(field)) => comparable_constant(left).then_some(field),
        (Some(_), Some(_)) | (None, None) => None,
    }
}

fn simple_field_name<'a>(
    expr: &'a SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    match expr {
        SqlExpr::Field(field) => current_table_field_name(field.as_str(), entity, table_alias),
        SqlExpr::FieldPath { root, segments } if segments.len() == 1 => {
            let qualifier_matches =
                table_alias.is_some_and(|alias| root == alias) || root == entity;
            qualifier_matches.then_some(segments[0].as_str())
        }
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => None,
    }
}

fn assignment_field_name<'a>(statement: &SqlUpdateStatement, field: &'a str) -> Option<&'a str> {
    current_table_field_name(
        field,
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
    )
}

fn current_table_field_name<'a>(
    field: &'a str,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    let Some((qualifier, leaf)) = field.split_once('.') else {
        return Some(field);
    };
    if leaf.contains('.') {
        return None;
    }

    let qualifier_matches =
        table_alias.is_some_and(|alias| qualifier == alias) || qualifier == entity;
    qualifier_matches.then_some(leaf)
}

const fn comparable_constant(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Literal(_) | SqlExpr::Param { .. })
}

fn contains_field(fields: &[&str], field: &str) -> bool {
    fields.contains(&field)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIMARY_KEY: &[&str] = &["id"];

    fn context() -> SqlUpdatePolicyContext<'static> {
        SqlUpdatePolicyContext::new(PRIMARY_KEY)
    }

    fn classify(sql: &str, policy: SqlUpdateExposurePolicy) -> SqlUpdatePolicyReport {
        classify_sql_update_policy(sql, policy, context()).expect("SQL should parse")
    }

    #[test]
    fn update_policy_session_write_current_admits_broad_current_shape() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21",
            SqlUpdateExposurePolicy::SessionWriteCurrent,
        );

        assert!(report.is_admitted());
        let classification = report
            .classification
            .as_ref()
            .expect("admitted UPDATE should include classification");
        assert_eq!(classification.target_entity, "Character");
        assert_eq!(classification.assigned_fields, ["active"]);
        assert_eq!(classification.where_policy, SqlUpdateWherePolicy::Other);
    }

    #[test]
    fn update_policy_rejects_non_update_statement() {
        let report = classify(
            "SELECT id FROM Character",
            SqlUpdateExposurePolicy::SessionWriteCurrent,
        );

        assert_eq!(report.classification, None);
        assert_eq!(report.rejection, Some(SqlUpdatePolicyRejection::NotUpdate),);
    }

    #[test]
    fn update_policy_session_write_current_rejects_missing_where() {
        let report = classify(
            "UPDATE Character SET active = false",
            SqlUpdateExposurePolicy::SessionWriteCurrent,
        );

        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("UPDATE should still classify")
                .where_policy,
            SqlUpdateWherePolicy::Missing,
        );
        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::MissingWhere),
        );
    }

    #[test]
    fn update_policy_generated_query_rejects_update() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21",
            SqlUpdateExposurePolicy::GeneratedQuery,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::GeneratedQueryRejectsUpdate),
        );
    }

    #[test]
    fn update_policy_generated_ddl_rejects_update() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21",
            SqlUpdateExposurePolicy::GeneratedDdl,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::GeneratedDdlRejectsUpdate),
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_accepts_primary_key_equality() {
        let report = classify(
            "UPDATE Character SET age = 22 WHERE id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(report.is_admitted());
        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .where_policy,
            SqlUpdateWherePolicy::PrimaryKeyEquality,
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_accepts_alias_qualified_primary_key_equality() {
        let report = classify(
            "UPDATE Character c SET age = 22 WHERE c.id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(report.is_admitted());
        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .where_policy,
            SqlUpdateWherePolicy::PrimaryKeyEquality,
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_rejects_primary_key_assignment() {
        let report = classify(
            "UPDATE Character SET id = 2 WHERE id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .assignment_policy,
            SqlUpdateAssignmentPolicy {
                mutates_primary_key: true,
                mutates_generated: false,
                mutates_managed: false,
            },
        );
        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::PrimaryKeyMutation),
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_rejects_non_primary_key_where() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_rejects_extra_where_guard() {
        let report = classify(
            "UPDATE Character SET age = 22 WHERE id = 1 AND active = true",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_accepts_complete_composite_primary_key() {
        let context = SqlUpdatePolicyContext::new(&["tenant_id", "id"]);
        let report = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE tenant_id = 7 AND id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");

        assert!(report.is_admitted());
        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .where_policy,
            SqlUpdateWherePolicy::PrimaryKeyEquality,
        );
    }

    #[test]
    fn update_policy_public_primary_key_only_rejects_partial_composite_primary_key() {
        let context = SqlUpdatePolicyContext::new(&["tenant_id", "id"]);
        let report = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
        );
    }

    #[test]
    fn update_policy_classifies_narrow_returning_shapes() {
        let returning_all = classify(
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING *",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );
        let returning_fields = classify(
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id, age",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(returning_all.is_admitted());
        assert_eq!(
            returning_all
                .classification
                .as_ref()
                .expect("classification should be present")
                .returning_policy,
            SqlUpdateReturningPolicy::NarrowAll,
        );
        assert!(returning_fields.is_admitted());
        assert_eq!(
            returning_fields
                .classification
                .as_ref()
                .expect("classification should be present")
                .returning_policy,
            SqlUpdateReturningPolicy::NarrowFields,
        );
    }

    #[test]
    fn update_policy_public_bounded_accepts_explicit_primary_key_order_and_limit() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert!(report.is_admitted());
        let classification = report
            .classification
            .as_ref()
            .expect("admitted UPDATE should include classification");
        assert!(classification.is_bounded());
        assert!(classification.has_explicit_canonical_primary_key_order());
    }

    #[test]
    fn update_policy_public_bounded_rejects_implicit_primary_key_fallback() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 LIMIT 10",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
    }

    #[test]
    fn update_policy_public_bounded_rejects_non_primary_key_ordering() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY age LIMIT 10",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
    }

    #[test]
    fn update_policy_public_bounded_rejects_descending_order() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id DESC LIMIT 10",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::DescendingOrder),
        );
    }

    #[test]
    fn update_policy_public_bounded_rejects_excessive_limit() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 101",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::LimitTooHigh),
        );
    }

    #[test]
    fn update_policy_public_bounded_rejects_offset() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 OFFSET 1",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::OffsetUnsupported),
        );
    }

    #[test]
    fn update_policy_rejects_generated_and_managed_assignment() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &["slug"],
            managed_fields: &["updated_at"],
            max_public_bounded_limit: 100,
        };

        let generated = classify_sql_update_policy(
            "UPDATE Character SET slug = 'ada' WHERE id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");
        let managed = classify_sql_update_policy(
            "UPDATE Character SET updated_at = 1 WHERE id = 1",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");

        assert_eq!(
            generated.rejection,
            Some(SqlUpdatePolicyRejection::GeneratedFieldMutation),
        );
        assert_eq!(
            managed.rejection,
            Some(SqlUpdatePolicyRejection::ManagedFieldMutation),
        );
    }
}
