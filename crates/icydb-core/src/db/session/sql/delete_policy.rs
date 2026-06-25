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
        SqlWriteAdmissionLane, SqlWriteBoundedPlanProof, SqlWriteBoundedPolicyRejection,
        SqlWriteExecutionBounds, SqlWritePlanCore, SqlWritePolicyBounds,
        SqlWritePrimaryKeyPlanProof, SqlWriteReturningBounds, SqlWriteStatementShape,
        SqlWriteStatementShapeInput, classify_write_statement_shape,
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

impl SqlDeleteExposurePolicy {
    fn validated_admission_lane(self) -> SqlWriteAdmissionLane {
        match self {
            Self::PublicPrimaryKeyOnly => SqlWriteAdmissionLane::PrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteAdmissionLane::BoundedDeterministic,
            Self::SessionWriteCurrent | Self::AdminBulk => SqlWriteAdmissionLane::Bulk,
            Self::GeneratedQuery | Self::GeneratedDdl => {
                unreachable!("generated policies never produce validated delete plans")
            }
        }
    }
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

    const fn write_bounds(self) -> SqlWritePolicyBounds {
        SqlWritePolicyBounds::new(
            self.max_public_bounded_limit,
            self.max_returning_rows,
            self.max_returning_response_bytes,
        )
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

/// Parsed `DELETE` classification before a caller-selected exposure policy is applied.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlDeleteStatementClassification {
    /// Target entity identifier.
    pub target_entity: String,
    /// Shared parser write-shape classification.
    pub write_shape: SqlWriteStatementShape,
}

type SqlDeletePlanCore = SqlWritePlanCore<SqlDeleteStatement, SqlDeleteStatementClassification>;

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
            Self::SessionCurrent(plan) => plan.core.classification(),
            Self::PublicPrimaryKeyOnly(plan) => plan.core.classification(),
            Self::PublicBoundedDeterministic(plan) => plan.core.classification(),
            Self::AdminBulk(plan) => plan.core.classification(),
        }
    }

    /// Return the execution bounds carried by this validated plan.
    #[must_use]
    pub const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        match self {
            Self::SessionCurrent(plan) => plan.core.execution_bounds(),
            Self::PublicPrimaryKeyOnly(plan) => plan.core.execution_bounds(),
            Self::PublicBoundedDeterministic(plan) => plan.core.execution_bounds(),
            Self::AdminBulk(plan) => plan.core.execution_bounds(),
        }
    }

    /// Return the `RETURNING` bounds carried by this validated plan.
    #[must_use]
    pub const fn returning_bounds(&self) -> SqlWriteReturningBounds {
        self.execution_bounds().returning
    }

    /// Return the entity targeted by the policy-validated parsed delete statement.
    #[must_use]
    pub const fn statement_entity(&self) -> &str {
        match self {
            Self::SessionCurrent(plan) => plan.core.statement().entity.as_str(),
            Self::PublicPrimaryKeyOnly(plan) => plan.core.statement().entity.as_str(),
            Self::PublicBoundedDeterministic(plan) => plan.core.statement().entity.as_str(),
            Self::AdminBulk(plan) => plan.core.statement().entity.as_str(),
        }
    }
}

/// Validated plan for the current broad session/library delete lane.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlSessionCurrentDeletePlan {
    core: SqlDeletePlanCore,
}

/// Validated plan for public primary-key-only delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicPrimaryKeyDeletePlan {
    core: SqlDeletePlanCore,
    primary_key_proof: SqlWritePrimaryKeyPlanProof,
}

impl SqlPublicPrimaryKeyDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this primary-key plan.
    #[must_use]
    pub const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }

    /// Return the primary-key fields proven by policy, in canonical order.
    #[must_use]
    pub const fn primary_key_fields(&self) -> &[String] {
        self.primary_key_proof.primary_key_fields()
    }
}

/// Validated plan for public bounded deterministic delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicBoundedDeletePlan {
    core: SqlDeletePlanCore,
    bounded_proof: SqlWriteBoundedPlanProof,
}

impl SqlPublicBoundedDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this bounded deterministic plan.
    #[must_use]
    pub const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }

    #[cfg(test)]
    pub(in crate::db) const fn set_execution_bounds_for_tests(
        &mut self,
        execution_bounds: SqlWriteExecutionBounds,
    ) {
        self.core.set_execution_bounds_for_tests(execution_bounds);
    }

    /// Return the explicit limit admitted by the bounded deterministic policy.
    #[must_use]
    pub const fn limit(&self) -> u32 {
        self.bounded_proof.limit()
    }

    /// Return the ordered primary-key fields proven by policy.
    #[must_use]
    pub const fn ordered_primary_key_fields(&self) -> &[String] {
        self.bounded_proof.ordered_primary_key_fields()
    }
}

/// Validated plan for future admin/bulk delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlAdminBulkDeletePlan {
    core: SqlDeletePlanCore,
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
        write_shape: classify_write_shape(statement, context),
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
            if !classification
                .write_shape
                .where_proof
                .is_primary_key_equality()
            {
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
    if classification.write_shape.where_proof.has_where() {
        None
    } else {
        Some(SqlDeletePolicyRejection::MissingWhere)
    }
}

const fn bounded_policy_rejection(
    classification: &SqlDeleteStatementClassification,
    context: SqlDeletePolicyContext<'_>,
) -> Option<SqlDeletePolicyRejection> {
    match classification
        .write_shape
        .bounded_policy_rejection_for_bounds(context.write_bounds())
    {
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
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
            })
        }
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly => {
            SqlValidatedDeletePlan::PublicPrimaryKeyOnly(SqlPublicPrimaryKeyDeletePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
                primary_key_proof: SqlWritePrimaryKeyPlanProof::from_field_names(
                    context.primary_key_fields,
                ),
            })
        }
        SqlDeleteExposurePolicy::PublicBoundedDeterministic => {
            SqlValidatedDeletePlan::PublicBoundedDeterministic(SqlPublicBoundedDeletePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
                bounded_proof: SqlWriteBoundedPlanProof::from_admitted_shape(
                    &classification.write_shape,
                    context.primary_key_fields,
                ),
            })
        }
        SqlDeleteExposurePolicy::AdminBulk => {
            SqlValidatedDeletePlan::AdminBulk(SqlAdminBulkDeletePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
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
) -> SqlWriteExecutionBounds {
    classification
        .write_shape
        .execution_bounds_for_admission_lane(
            policy.validated_admission_lane(),
            context.write_bounds(),
        )
}

fn classify_write_shape(
    statement: &SqlDeleteStatement,
    context: SqlDeletePolicyContext<'_>,
) -> SqlWriteStatementShape {
    classify_write_statement_shape(SqlWriteStatementShapeInput {
        predicate: statement.predicate.as_ref(),
        entity: statement.entity.as_str(),
        table_alias: statement.table_alias.as_deref(),
        order_by: statement.order_by.as_slice(),
        limit: statement.limit,
        offset: statement.offset,
        returning: statement.returning.as_ref(),
        primary_key_fields: context.primary_key_fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::session::sql::write_policy::{
        SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteWhereProof,
    };

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
        assert_eq!(
            classification.write_shape.where_proof,
            SqlWriteWhereProof::Missing
        );
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::PrimaryKeyEquality,
        );
        let SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan) = expect_plan(&report) else {
            panic!("primary-key policy should produce only the primary-key plan variant");
        };
        assert_eq!(plan.primary_key_fields(), ["id"]);
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::PrimaryKeyEquality,
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
        assert_eq!(plan.primary_key_fields(), ["tenant_id", "id"]);
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
        assert!(classification.write_shape.is_bounded());
        assert!(
            classification
                .write_shape
                .has_explicit_canonical_primary_key_order()
        );
        let SqlValidatedDeletePlan::PublicBoundedDeterministic(plan) = expect_plan(&report) else {
            panic!("bounded policy should produce only the bounded plan variant");
        };
        assert_eq!(plan.limit(), 10);
        assert_eq!(plan.ordered_primary_key_fields(), ["id"]);
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
                .write_shape
                .returning_shape,
            SqlWriteReturningShape::NarrowAll,
        );
        assert!(returning_fields.is_admitted());
        assert_eq!(
            returning_fields
                .classification
                .as_ref()
                .expect("classification should be present")
                .write_shape
                .returning_shape,
            SqlWriteReturningShape::NarrowFields,
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
            SqlWriteReturningBounds {
                max_rows: Some(1),
                max_response_bytes: Some(4096),
            },
        );
        assert_eq!(
            expect_plan(&bounded).returning_bounds(),
            SqlWriteReturningBounds {
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
            SqlWriteReturningBounds {
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
            SqlWriteReturningBounds {
                max_rows: Some(1),
                max_response_bytes: None,
            },
        );
        assert_eq!(
            expect_plan(&bounded).returning_bounds(),
            SqlWriteReturningBounds {
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
