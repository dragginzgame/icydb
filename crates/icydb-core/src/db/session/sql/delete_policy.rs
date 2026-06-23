//! Module: db::session::sql::delete_policy
//! Responsibility: parser-shape classification and exposure-policy checks for
//! SQL `DELETE` before a generated/public write surface can execute it.
//! Does not own: delete execution, row materialization, or commit semantics.
//! Boundary: records public DELETE admission rules separately from the broad
//! session write lane.

use crate::db::{
    QueryError,
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteBoundedPolicyRejection, SqlWriteOrderProof, SqlWriteReturningBounds,
        SqlWriteReturningShape, SqlWriteStagedRowBoundKind, SqlWriteWhereProof,
        bounded_write_policy_rejection, classify_write_order_proof, classify_write_returning_shape,
        classify_write_where_proof, owned_write_field_names,
        sql_write_execution_bounds_for_staged_kind,
    },
    sql::parser::{SqlDeleteStatement, SqlStatement, parse_sql_with_attribution},
};

/// Default generated/public bounded SQL `DELETE` row target limit.
#[doc(hidden)]
pub(in crate::db) const DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT: u32 =
    DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT;

/// Default generated/public SQL `DELETE RETURNING` projection payload budget.
#[doc(hidden)]
pub(in crate::db) const DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES: u32 =
    DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES;

/// SQL `DELETE` exposure policy selected by a caller before execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlDeleteExposurePolicy {
    /// Current broad session/library write-lane policy.
    SessionWriteCurrent,
    /// Generated read/query endpoint policy. `DELETE` is never admitted.
    GeneratedQuery,
    /// Generated schema DDL endpoint policy. `DELETE` is never admitted.
    GeneratedDdl,
    /// Public-safe policy requiring complete primary-key equality in `WHERE`.
    PublicPrimaryKeyOnly,
    /// Public-safe bounded policy requiring `WHERE`, explicit primary-key order, and `LIMIT`.
    PublicBoundedDeterministic,
    /// Future admin/bulk policy; deliberately broad but explicit.
    AdminBulk,
}

/// Schema-derived field context needed to classify one `DELETE`.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub struct SqlDeletePolicyContext<'a> {
    /// Primary-key fields in canonical order.
    pub primary_key_fields: &'a [&'a str],
    /// Maximum admitted limit for the public bounded deterministic policy.
    pub max_public_bounded_limit: u32,
    /// Optional returned-row cap carried by validated plans with `RETURNING`.
    pub max_returning_rows: Option<u32>,
    /// Optional response-size cap carried by validated plans with `RETURNING`.
    pub max_returning_response_bytes: Option<u32>,
}

impl<'a> SqlDeletePolicyContext<'a> {
    /// Build a context with the default public DELETE bounds.
    #[must_use]
    pub const fn new(primary_key_fields: &'a [&'a str]) -> Self {
        Self {
            primary_key_fields,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: None,
        }
    }

    /// Build the default context used by schema-derived public/generated delete endpoints.
    #[must_use]
    pub const fn public_generated(primary_key_fields: &'a [&'a str]) -> Self {
        Self {
            primary_key_fields,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: Some(DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES),
        }
    }
}

/// `WHERE` classification for one parsed `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlDeleteWherePolicy {
    /// The statement has no `WHERE` clause.
    Missing,
    /// The `WHERE` clause proves complete primary-key equality under v1 rules.
    PrimaryKeyEquality,
    /// The `WHERE` clause exists but does not prove primary-key equality.
    Other,
}

impl SqlDeleteWherePolicy {
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

/// Explicit `ORDER BY` classification for one parsed `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlDeleteOrderPolicy {
    /// The statement has no explicit `ORDER BY`.
    Missing,
    /// The statement explicitly orders by canonical primary-key fields ascending.
    CanonicalPrimaryKey,
    /// The statement orders by canonical primary-key fields but uses descending order.
    DescendingPrimaryKey,
    /// The statement has another explicit ordering shape.
    Other,
}

impl SqlDeleteOrderPolicy {
    const fn write_order_proof(self) -> SqlWriteOrderProof {
        match self {
            Self::Missing => SqlWriteOrderProof::Missing,
            Self::CanonicalPrimaryKey => SqlWriteOrderProof::CanonicalPrimaryKey,
            Self::DescendingPrimaryKey => SqlWriteOrderProof::DescendingPrimaryKey,
            Self::Other => SqlWriteOrderProof::Other,
        }
    }
}

/// Narrow write `RETURNING` classification for one parsed `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlDeleteReturningPolicy {
    /// No `RETURNING` clause.
    None,
    /// Narrow `RETURNING *`.
    NarrowAll,
    /// Narrow `RETURNING field, ...`.
    NarrowFields,
}

impl SqlDeleteReturningPolicy {
    /// Return whether the statement requests `RETURNING`.
    #[must_use]
    pub const fn is_requested(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// `RETURNING` bounds carried by a validated delete plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlDeleteReturningBounds {
    /// Maximum rows the plan may return, when statically bounded by policy.
    pub max_rows: Option<u32>,
    /// Maximum encoded response bytes, when supplied by the caller surface.
    pub max_response_bytes: Option<u32>,
}

/// Runtime execution bounds carried by a policy-validated SQL `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlDeleteExecutionBounds {
    /// Maximum candidate rows the validated plan may stage before mutation.
    pub max_staged_rows: Option<u32>,
    /// Optional `RETURNING` row and response-size bounds.
    pub returning: SqlDeleteReturningBounds,
}

impl SqlDeleteReturningBounds {
    const fn from_write_bounds(bounds: SqlWriteReturningBounds) -> Self {
        Self {
            max_rows: bounds.max_rows,
            max_response_bytes: bounds.max_response_bytes,
        }
    }

    pub(in crate::db::session::sql) const fn write_bounds(self) -> SqlWriteReturningBounds {
        SqlWriteReturningBounds {
            max_rows: self.max_rows,
            max_response_bytes: self.max_response_bytes,
        }
    }
}

impl SqlDeleteExecutionBounds {
    const fn from_write_bounds(
        bounds: crate::db::session::sql::write_policy::SqlWriteExecutionBounds,
    ) -> Self {
        Self {
            max_staged_rows: bounds.max_staged_rows,
            returning: SqlDeleteReturningBounds::from_write_bounds(bounds.returning),
        }
    }
}

/// Parsed `DELETE` classification before a caller-selected exposure policy is applied.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlDeleteStatementClassification {
    /// Target entity identifier.
    pub target_entity: String,
    /// `WHERE` proof classification.
    pub where_policy: SqlDeleteWherePolicy,
    /// Explicit `ORDER BY` classification.
    pub order_policy: SqlDeleteOrderPolicy,
    /// Parsed `LIMIT`, if supplied.
    pub limit: Option<u32>,
    /// Parsed `OFFSET`, if supplied.
    pub offset: Option<u32>,
    /// Narrow write `RETURNING` classification.
    pub returning_policy: SqlDeleteReturningPolicy,
}

impl SqlDeleteStatementClassification {
    /// Return whether the statement has an explicit positive `LIMIT`.
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        matches!(self.limit, Some(limit) if limit > 0)
    }

    /// Return whether the statement has explicit canonical ascending primary-key order.
    #[must_use]
    pub const fn has_explicit_canonical_primary_key_order(&self) -> bool {
        matches!(self.order_policy, SqlDeleteOrderPolicy::CanonicalPrimaryKey)
    }
}

/// Validated non-executing SQL `DELETE` plan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlValidatedDeletePlan {
    /// Current broad session/library write-lane plan.
    SessionCurrent(SqlSessionCurrentDeletePlan),
    /// Public-safe one-row primary-key plan.
    PublicPrimaryKeyOnly(SqlPublicPrimaryKeyDeletePlan),
    /// Public-safe bounded deterministic plan.
    PublicBoundedDeterministic(SqlPublicBoundedDeletePlan),
    /// Future admin/bulk plan.
    AdminBulk(SqlAdminBulkDeletePlan),
}

impl SqlValidatedDeletePlan {
    /// Return the classification carried by this validated plan.
    #[must_use]
    pub const fn classification(&self) -> &SqlDeleteStatementClassification {
        match self {
            Self::SessionCurrent(plan) => &plan.classification,
            Self::PublicPrimaryKeyOnly(plan) => &plan.classification,
            Self::PublicBoundedDeterministic(plan) => &plan.classification,
            Self::AdminBulk(plan) => &plan.classification,
        }
    }

    /// Return the execution bounds carried by this validated plan.
    #[must_use]
    pub const fn execution_bounds(&self) -> SqlDeleteExecutionBounds {
        match self {
            Self::SessionCurrent(plan) => plan.execution_bounds,
            Self::PublicPrimaryKeyOnly(plan) => plan.execution_bounds,
            Self::PublicBoundedDeterministic(plan) => plan.execution_bounds,
            Self::AdminBulk(plan) => plan.execution_bounds,
        }
    }

    /// Return the `RETURNING` bounds carried by this validated plan.
    #[must_use]
    pub const fn returning_bounds(&self) -> SqlDeleteReturningBounds {
        self.execution_bounds().returning
    }

    /// Return the entity targeted by the policy-validated parsed delete statement.
    #[must_use]
    pub const fn statement_entity(&self) -> &str {
        match self {
            Self::SessionCurrent(plan) => plan.statement.entity.as_str(),
            Self::PublicPrimaryKeyOnly(plan) => plan.statement.entity.as_str(),
            Self::PublicBoundedDeterministic(plan) => plan.statement.entity.as_str(),
            Self::AdminBulk(plan) => plan.statement.entity.as_str(),
        }
    }
}

/// Validated plan for the current broad session/library delete lane.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlSessionCurrentDeletePlan {
    statement: SqlDeleteStatement,
    /// Shape classification admitted by the current session policy.
    pub classification: SqlDeleteStatementClassification,
    /// Runtime execution bounds attached to the admitted plan.
    pub execution_bounds: SqlDeleteExecutionBounds,
}

/// Validated plan for public primary-key-only delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicPrimaryKeyDeletePlan {
    statement: SqlDeleteStatement,
    /// Shape classification admitted by the primary-key policy.
    pub classification: SqlDeleteStatementClassification,
    /// Primary-key fields proven by the policy, in canonical order.
    pub primary_key_fields: Vec<String>,
    /// Runtime execution bounds attached to the admitted plan.
    pub execution_bounds: SqlDeleteExecutionBounds,
}

impl SqlPublicPrimaryKeyDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        &self.statement
    }
}

/// Validated plan for public bounded deterministic delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicBoundedDeletePlan {
    statement: SqlDeleteStatement,
    /// Shape classification admitted by the bounded deterministic policy.
    pub classification: SqlDeleteStatementClassification,
    /// Explicit positive limit admitted by the policy.
    pub limit: u32,
    /// Primary-key fields used for explicit canonical ordering.
    pub ordered_primary_key_fields: Vec<String>,
    /// Runtime execution bounds attached to the admitted plan.
    pub execution_bounds: SqlDeleteExecutionBounds,
}

impl SqlPublicBoundedDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        &self.statement
    }
}

/// Validated plan for future admin/bulk delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlAdminBulkDeletePlan {
    statement: SqlDeleteStatement,
    /// Shape classification admitted by the admin/bulk policy.
    pub classification: SqlDeleteStatementClassification,
    /// Runtime execution bounds attached to the admitted plan.
    pub execution_bounds: SqlDeleteExecutionBounds,
}

/// Stable policy rejection for one classified SQL `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlDeletePolicyRejection {
    /// The parsed statement is not `DELETE`.
    NotDelete,
    /// Generated query surfaces reject all `DELETE`.
    GeneratedQueryRejectsDelete,
    /// Generated DDL surfaces reject all `DELETE`.
    GeneratedDdlRejectsDelete,
    /// This public policy requires a `WHERE` clause.
    MissingWhere,
    /// The `WHERE` clause did not prove complete primary-key equality.
    PrimaryKeyProofFailed,
    /// This public policy requires explicit canonical primary-key ordering.
    MissingCanonicalPrimaryKeyOrder,
    /// This public policy rejects descending ordering in v1.
    DescendingOrder,
    /// This public policy requires a positive `LIMIT`.
    MissingLimit,
    /// This public policy rejects `OFFSET`.
    OffsetUnsupported,
    /// The supplied `LIMIT` exceeds the policy maximum.
    LimitTooHigh,
}

impl SqlDeletePolicyRejection {
    const fn from_bounded_write_rejection(rejection: SqlWriteBoundedPolicyRejection) -> Self {
        match rejection {
            SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder => {
                Self::MissingCanonicalPrimaryKeyOrder
            }
            SqlWriteBoundedPolicyRejection::DescendingOrder => Self::DescendingOrder,
            SqlWriteBoundedPolicyRejection::MissingLimit => Self::MissingLimit,
            SqlWriteBoundedPolicyRejection::OffsetUnsupported => Self::OffsetUnsupported,
            SqlWriteBoundedPolicyRejection::LimitTooHigh => Self::LimitTooHigh,
        }
    }
}

/// Result of classifying one SQL statement under a `DELETE` exposure policy.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlDeletePolicyReport {
    /// Parsed `DELETE` classification when the statement is a `DELETE`.
    pub classification: Option<SqlDeleteStatementClassification>,
    /// Typed validated plan when the selected policy admits the statement.
    pub plan: Option<SqlValidatedDeletePlan>,
    /// Policy rejection, or `None` when the selected policy admits the statement.
    pub rejection: Option<SqlDeletePolicyRejection>,
}

impl SqlDeletePolicyReport {
    /// Return whether the selected policy admits the statement.
    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        self.rejection.is_none()
    }

    const fn rejected(rejection: SqlDeletePolicyRejection) -> Self {
        Self {
            classification: None,
            plan: None,
            rejection: Some(rejection),
        }
    }
}

/// Classify one SQL statement under an explicit `DELETE` exposure policy.
///
/// This helper parses and inspects statement shape only. It does not execute
/// mutation work or validate field existence beyond the caller-provided primary
/// key context.
pub fn classify_sql_delete_policy(
    sql: &str,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> Result<SqlDeletePolicyReport, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(classify_sql_delete_statement_policy(
        &statement, policy, context,
    ))
}

pub(in crate::db) fn classify_sql_delete_statement_policy(
    statement: &SqlStatement,
    policy: SqlDeleteExposurePolicy,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeletePolicyReport {
    let SqlStatement::Delete(statement) = statement else {
        return SqlDeletePolicyReport::rejected(SqlDeletePolicyRejection::NotDelete);
    };

    let classification = classify_delete_statement(statement, context);
    let rejection = delete_policy_rejection(policy, &classification, context);
    let plan = rejection
        .is_none()
        .then(|| validated_delete_plan(statement, policy, &classification, context));

    SqlDeletePolicyReport {
        classification: Some(classification),
        plan,
        rejection,
    }
}

fn classify_delete_statement(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeleteStatementClassification {
    SqlDeleteStatementClassification {
        target_entity: statement.entity.clone(),
        where_policy: where_policy(statement, context),
        order_policy: order_policy(statement, context),
        limit: statement.limit,
        offset: statement.offset,
        returning_policy: returning_policy(statement),
    }
}

fn delete_policy_rejection(
    policy: SqlDeleteExposurePolicy,
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> Option<SqlDeletePolicyRejection> {
    match policy {
        SqlDeleteExposurePolicy::GeneratedQuery => {
            return Some(SqlDeletePolicyRejection::GeneratedQueryRejectsDelete);
        }
        SqlDeleteExposurePolicy::GeneratedDdl => {
            return Some(SqlDeletePolicyRejection::GeneratedDdlRejectsDelete);
        }
        SqlDeleteExposurePolicy::SessionWriteCurrent
        | SqlDeleteExposurePolicy::PublicPrimaryKeyOnly
        | SqlDeleteExposurePolicy::PublicBoundedDeterministic
        | SqlDeleteExposurePolicy::AdminBulk => {}
    }

    match policy {
        SqlDeleteExposurePolicy::SessionWriteCurrent | SqlDeleteExposurePolicy::AdminBulk => None,
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => {
            if let Some(rejection) = public_delete_where_rejection(classification) {
                return Some(rejection);
            }
            if !classification.where_policy.is_primary_key_equality() {
                return Some(SqlDeletePolicyRejection::PrimaryKeyProofFailed);
            }

            None
        }
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => {
            public_delete_where_rejection(classification)
                .or_else(|| bounded_policy_rejection(classification, context))
        }
        SqlDeleteExposurePolicy::GeneratedQuery | SqlDeleteExposurePolicy::GeneratedDdl => {
            unreachable!("generated policies returned before shared checks")
        }
    }
}

const fn public_delete_where_rejection(
    classification: &SqlDeleteStatementClassification,
) -> Option<SqlDeletePolicyRejection> {
    if classification.where_policy.has_where() {
        None
    } else {
        Some(SqlDeletePolicyRejection::MissingWhere)
    }
}

const fn bounded_policy_rejection(
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> Option<SqlDeletePolicyRejection> {
    match bounded_write_policy_rejection(
        classification.offset,
        classification.limit,
        context.max_public_bounded_limit,
        classification.order_policy.write_order_proof(),
    ) {
        Some(rejection) => Some(SqlDeletePolicyRejection::from_bounded_write_rejection(
            rejection,
        )),
        None => None,
    }
}

fn validated_delete_plan(
    statement: &SqlDeleteStatement,
    policy: SqlDeleteExposurePolicy,
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> SqlValidatedDeletePlan {
    let execution_bounds = execution_bounds(policy, classification, context);
    match policy {
        SqlDeleteExposurePolicy::SessionWriteCurrent => {
            SqlValidatedDeletePlan::SessionCurrent(SqlSessionCurrentDeletePlan {
                statement: statement.clone(),
                classification: classification.clone(),
                execution_bounds,
            })
        }
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => {
            SqlValidatedDeletePlan::PublicPrimaryKeyOnly(SqlPublicPrimaryKeyDeletePlan {
                statement: statement.clone(),
                classification: classification.clone(),
                primary_key_fields: owned_write_field_names(context.primary_key_fields),
                execution_bounds,
            })
        }
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => {
            SqlValidatedDeletePlan::PublicBoundedDeterministic(SqlPublicBoundedDeletePlan {
                statement: statement.clone(),
                classification: classification.clone(),
                limit: classification
                    .limit
                    .expect("bounded policy admitted a limit"),
                ordered_primary_key_fields: owned_write_field_names(context.primary_key_fields),
                execution_bounds,
            })
        }
        SqlDeleteExposurePolicy::AdminBulk => {
            SqlValidatedDeletePlan::AdminBulk(SqlAdminBulkDeletePlan {
                statement: statement.clone(),
                classification: classification.clone(),
                execution_bounds,
            })
        }
        SqlDeleteExposurePolicy::GeneratedQuery | SqlDeleteExposurePolicy::GeneratedDdl => {
            unreachable!("generated policies never produce validated delete plans")
        }
    }
}

fn execution_bounds(
    policy: SqlDeleteExposurePolicy,
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeleteExecutionBounds {
    SqlDeleteExecutionBounds::from_write_bounds(sql_write_execution_bounds_for_staged_kind(
        staged_row_bound_kind(policy),
        classification.limit,
        classification.returning_policy.is_requested(),
        context.max_returning_rows,
        context.max_returning_response_bytes,
    ))
}

fn staged_row_bound_kind(policy: SqlDeleteExposurePolicy) -> SqlWriteStagedRowBoundKind {
    match policy {
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => SqlWriteStagedRowBoundKind::One,
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => SqlWriteStagedRowBoundKind::Limit,
        SqlDeleteExposurePolicy::SessionWriteCurrent | SqlDeleteExposurePolicy::AdminBulk => {
            SqlWriteStagedRowBoundKind::Unbounded
        }
        SqlDeleteExposurePolicy::GeneratedQuery | SqlDeleteExposurePolicy::GeneratedDdl => {
            unreachable!("generated policies never produce validated delete plans")
        }
    }
}

fn where_policy(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeleteWherePolicy {
    match classify_write_where_proof(
        statement.predicate.as_ref(),
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
        context.primary_key_fields,
    ) {
        SqlWriteWhereProof::Missing => SqlDeleteWherePolicy::Missing,
        SqlWriteWhereProof::PrimaryKeyEquality => SqlDeleteWherePolicy::PrimaryKeyEquality,
        SqlWriteWhereProof::Other => SqlDeleteWherePolicy::Other,
    }
}

fn order_policy(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlDeleteOrderPolicy {
    match classify_write_order_proof(
        statement.order_by.as_slice(),
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
        context.primary_key_fields,
    ) {
        SqlWriteOrderProof::Missing => SqlDeleteOrderPolicy::Missing,
        SqlWriteOrderProof::CanonicalPrimaryKey => SqlDeleteOrderPolicy::CanonicalPrimaryKey,
        SqlWriteOrderProof::DescendingPrimaryKey => SqlDeleteOrderPolicy::DescendingPrimaryKey,
        SqlWriteOrderProof::Other => SqlDeleteOrderPolicy::Other,
    }
}

const fn returning_policy(statement: &SqlDeleteStatement) -> SqlDeleteReturningPolicy {
    match classify_write_returning_shape(statement.returning.as_ref()) {
        SqlWriteReturningShape::None => SqlDeleteReturningPolicy::None,
        SqlWriteReturningShape::NarrowAll => SqlDeleteReturningPolicy::NarrowAll,
        SqlWriteReturningShape::NarrowFields => SqlDeleteReturningPolicy::NarrowFields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIMARY_KEY: &[&str] = &["id"];

    fn context() -> SqlDeletePolicyContext<'static> {
        SqlDeletePolicyContext::new(PRIMARY_KEY)
    }

    fn classify(sql: &str, policy: SqlDeleteExposurePolicy) -> SqlDeletePolicyReport {
        classify_sql_delete_policy(sql, policy, context()).expect("SQL should parse")
    }

    fn expect_plan(report: &SqlDeletePolicyReport) -> &SqlValidatedDeletePlan {
        report
            .plan
            .as_ref()
            .expect("admitted policy should produce a validated plan")
    }

    fn assert_no_plan(report: &SqlDeletePolicyReport) {
        assert!(
            report.plan.is_none(),
            "rejected policy should not expose a partially usable plan",
        );
    }

    #[test]
    fn delete_policy_session_write_current_admits_broad_current_shape() {
        let report = classify(
            "DELETE FROM Character",
            SqlDeleteExposurePolicy::SessionWriteCurrent,
        );

        assert!(report.is_admitted());
        let classification = report
            .classification
            .as_ref()
            .expect("admitted DELETE should include classification");
        assert_eq!(classification.target_entity, "Character");
        assert_eq!(classification.where_policy, SqlDeleteWherePolicy::Missing);
        assert!(matches!(
            expect_plan(&report),
            SqlValidatedDeletePlan::SessionCurrent(_),
        ));
        assert_eq!(expect_plan(&report).statement_entity(), "Character");
    }

    #[test]
    fn delete_policy_rejects_non_delete_statement() {
        let report = classify(
            "SELECT id FROM Character",
            SqlDeleteExposurePolicy::SessionWriteCurrent,
        );

        assert_eq!(report.classification, None);
        assert_eq!(report.rejection, Some(SqlDeletePolicyRejection::NotDelete),);
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_generated_query_rejects_delete() {
        let report = classify(
            "DELETE FROM Character WHERE id = 1",
            SqlDeleteExposurePolicy::GeneratedQuery,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::GeneratedQueryRejectsDelete),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_generated_ddl_rejects_delete() {
        let report = classify(
            "DELETE FROM Character WHERE id = 1",
            SqlDeleteExposurePolicy::GeneratedDdl,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::GeneratedDdlRejectsDelete),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_primary_key_only_accepts_primary_key_equality() {
        let report = classify(
            "DELETE FROM Character WHERE id = 1",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(report.is_admitted());
        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .where_policy,
            SqlDeleteWherePolicy::PrimaryKeyEquality,
        );
        let SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan) = expect_plan(&report) else {
            panic!("primary-key policy should produce only the primary-key plan variant");
        };
        assert_eq!(plan.primary_key_fields, ["id"]);
    }

    #[test]
    fn delete_policy_public_primary_key_only_accepts_alias_qualified_primary_key_equality() {
        let report = classify(
            "DELETE FROM Character c WHERE c.id = 1",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(report.is_admitted());
        assert_eq!(
            report
                .classification
                .as_ref()
                .expect("classification should be present")
                .where_policy,
            SqlDeleteWherePolicy::PrimaryKeyEquality,
        );
    }

    #[test]
    fn delete_policy_public_primary_key_only_rejects_missing_where() {
        let report = classify(
            "DELETE FROM Character",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::MissingWhere),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_primary_key_only_rejects_non_primary_key_where() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::PrimaryKeyProofFailed),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_primary_key_only_rejects_extra_where_guard() {
        let report = classify(
            "DELETE FROM Character WHERE id = 1 AND active = true",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::PrimaryKeyProofFailed),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_primary_key_only_accepts_complete_composite_primary_key() {
        let context = SqlDeletePolicyContext::new(&["tenant_id", "id"]);
        let report = classify_sql_delete_policy(
            "DELETE FROM Character WHERE tenant_id = 7 AND id = 1",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");

        assert!(report.is_admitted());
        let SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan) = expect_plan(&report) else {
            panic!("composite primary-key proof should produce a primary-key plan");
        };
        assert_eq!(plan.primary_key_fields, ["tenant_id", "id"]);
    }

    #[test]
    fn delete_policy_public_bounded_accepts_explicit_primary_key_order_and_limit() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert!(report.is_admitted());
        let classification = report
            .classification
            .as_ref()
            .expect("admitted DELETE should include classification");
        assert!(classification.is_bounded());
        assert!(classification.has_explicit_canonical_primary_key_order());
        let SqlValidatedDeletePlan::PublicBoundedDeterministic(plan) = expect_plan(&report) else {
            panic!("bounded policy should produce only the bounded plan variant");
        };
        assert_eq!(plan.limit, 10);
        assert_eq!(plan.ordered_primary_key_fields, ["id"]);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_missing_where() {
        let report = classify(
            "DELETE FROM Character ORDER BY id LIMIT 10",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::MissingWhere),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_implicit_primary_key_fallback() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21 LIMIT 10",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_non_primary_key_ordering() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21 ORDER BY age LIMIT 10",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_descending_order() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21 ORDER BY id DESC LIMIT 10",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::DescendingOrder),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_excessive_limit() {
        let excessive_limit = DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT
            .checked_add(1)
            .expect("test default public bounded delete limit should fit u32");
        let report = classify(
            format!("DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT {excessive_limit}")
                .as_str(),
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::LimitTooHigh),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_public_bounded_rejects_offset() {
        let report = classify(
            "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 OFFSET 1",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlDeletePolicyRejection::OffsetUnsupported),
        );
        assert_no_plan(&report);
    }

    #[test]
    fn delete_policy_classifies_narrow_returning_shapes() {
        let returning_all = classify(
            "DELETE FROM Character WHERE id = 1 RETURNING *",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );
        let returning_fields = classify(
            "DELETE FROM Character WHERE id = 1 RETURNING id, age",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        );

        assert!(returning_all.is_admitted());
        assert_eq!(
            returning_all
                .classification
                .as_ref()
                .expect("classification should be present")
                .returning_policy,
            SqlDeleteReturningPolicy::NarrowAll,
        );
        assert!(returning_fields.is_admitted());
        assert_eq!(
            returning_fields
                .classification
                .as_ref()
                .expect("classification should be present")
                .returning_policy,
            SqlDeleteReturningPolicy::NarrowFields,
        );
    }

    #[test]
    fn delete_policy_validated_plans_carry_execution_and_returning_bounds() {
        let context = SqlDeletePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: Some(4096),
        };
        let primary_key = classify_sql_delete_policy(
            "DELETE FROM Character WHERE id = 1 RETURNING id",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");
        let bounded = classify_sql_delete_policy(
            "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
            context,
        )
        .expect("SQL should parse");

        assert_eq!(
            expect_plan(&primary_key).returning_bounds(),
            SqlDeleteReturningBounds {
                max_rows: Some(1),
                max_response_bytes: Some(4096),
            },
        );
        assert_eq!(
            expect_plan(&bounded).returning_bounds(),
            SqlDeleteReturningBounds {
                max_rows: Some(10),
                max_response_bytes: Some(4096),
            },
        );
        assert_eq!(
            expect_plan(&primary_key).execution_bounds().max_staged_rows,
            Some(1),
        );
        assert_eq!(
            expect_plan(&bounded).execution_bounds().max_staged_rows,
            Some(10),
        );
    }

    #[test]
    fn delete_policy_public_generated_context_carries_default_returning_byte_bound() {
        let context = SqlDeletePolicyContext::public_generated(PRIMARY_KEY);
        let report = classify_sql_delete_policy(
            "DELETE FROM Character WHERE id = 1 RETURNING id",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");

        assert_eq!(
            expect_plan(&report).returning_bounds(),
            SqlDeleteReturningBounds {
                max_rows: Some(1),
                max_response_bytes: Some(DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES),
            },
        );
    }

    #[test]
    fn delete_policy_validated_plans_lower_configured_returning_row_bound() {
        let context = SqlDeletePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: Some(2),
            max_returning_response_bytes: None,
        };
        let primary_key = classify_sql_delete_policy(
            "DELETE FROM Character WHERE id = 1 RETURNING id",
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");
        let bounded = classify_sql_delete_policy(
            "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
            context,
        )
        .expect("SQL should parse");

        assert_eq!(
            expect_plan(&primary_key).returning_bounds(),
            SqlDeleteReturningBounds {
                max_rows: Some(1),
                max_response_bytes: None,
            },
        );
        assert_eq!(
            expect_plan(&bounded).returning_bounds(),
            SqlDeleteReturningBounds {
                max_rows: Some(2),
                max_response_bytes: None,
            },
        );
    }

    #[test]
    fn delete_policy_admin_bulk_produces_only_admin_plan_variant() {
        let report = classify("DELETE FROM Character", SqlDeleteExposurePolicy::AdminBulk);

        assert!(report.is_admitted());
        assert!(matches!(
            expect_plan(&report),
            SqlValidatedDeletePlan::AdminBulk(_),
        ));
    }
}
