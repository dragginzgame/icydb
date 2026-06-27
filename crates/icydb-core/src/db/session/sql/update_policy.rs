//! Module: db::session::sql::update_policy
//! Responsibility: parser-shape classification and exposure-policy checks for
//! SQL `UPDATE` before a generated/public write surface can execute it.
//! Does not own: row mutation execution, field validation, or persistence.
//! Boundary: keeps public/generated UPDATE admission stricter than the broad
//! session write lane.

use crate::db::{
    QueryError,
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteAdmissionLane, SqlWriteBoundedPlanProof, SqlWriteBoundedPolicyRejection,
        SqlWriteExecutionBounds, SqlWritePlanCore, SqlWritePolicyBounds,
        SqlWritePrimaryKeyPlanProof, SqlWriteReturningBounds, SqlWriteStatementShape,
        SqlWriteStatementShapeInput, classify_write_statement_shape, contains_field,
        current_table_field_name,
    },
    sql::parser::{SqlStatement, SqlUpdateStatement, parse_sql_with_attribution},
};

/// Default generated/public bounded SQL `UPDATE` row target limit.
#[doc(hidden)]
pub(in crate::db) const DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT: u32 =
    DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT;

/// Default generated/public SQL `UPDATE RETURNING` projection payload budget.
#[doc(hidden)]
pub(in crate::db) const DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES: u32 =
    DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES;

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

impl SqlUpdateExposurePolicy {
    const fn validated_admission_lane(self) -> Option<SqlWriteAdmissionLane> {
        Some(match self {
            Self::PublicPrimaryKeyOnly => SqlWriteAdmissionLane::PrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteAdmissionLane::BoundedDeterministic,
            Self::SessionWriteCurrent | Self::AdminBulk => SqlWriteAdmissionLane::Bulk,
            Self::GeneratedQuery | Self::GeneratedDdl => return None,
        })
    }
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
    /// Optional returned-row cap carried by validated plans with `RETURNING`.
    pub max_returning_rows: Option<u32>,
    /// Optional response-size cap carried by validated plans with `RETURNING`.
    pub max_returning_response_bytes: Option<u32>,
}

impl<'a> SqlUpdatePolicyContext<'a> {
    /// Build a context with no generated/managed fields and the default public bound.
    #[must_use]
    pub const fn new(primary_key_fields: &'a [&'a str]) -> Self {
        Self {
            primary_key_fields,
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
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

    /// Build the default context used by schema-derived public/generated update endpoints.
    #[must_use]
    pub(in crate::db) const fn public_generated(
        primary_key_fields: &'a [&'a str],
        generated_fields: &'a [&'a str],
        managed_fields: &'a [&'a str],
    ) -> Self {
        Self {
            primary_key_fields,
            generated_fields,
            managed_fields,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: Some(DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES),
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
    /// Shared parser write-shape classification.
    pub write_shape: SqlWriteStatementShape,
}

type SqlUpdatePlanCore = SqlWritePlanCore<SqlUpdateStatement, SqlUpdateStatementClassification>;

/// Validated non-executing SQL `UPDATE` plan.
///
/// Generated/public executors should consume one of these policy-specific
/// variants instead of a raw parsed `SqlUpdateStatement` plus a separate
/// "already checked" report.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlValidatedUpdatePlan {
    /// Current broad session/library write-lane plan.
    SessionCurrent(SqlSessionCurrentUpdatePlan),
    /// Public-safe one-row primary-key plan.
    PublicPrimaryKeyOnly(SqlPublicPrimaryKeyUpdatePlan),
    /// Public-safe bounded deterministic plan.
    PublicBoundedDeterministic(SqlPublicBoundedUpdatePlan),
    /// Future admin/bulk plan.
    AdminBulk(SqlAdminBulkUpdatePlan),
}

impl SqlValidatedUpdatePlan {
    /// Return the classification carried by this validated plan.
    #[must_use]
    pub const fn classification(&self) -> &SqlUpdateStatementClassification {
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

    /// Return the entity targeted by the policy-validated parsed update statement.
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

/// Validated plan for the current broad session/library update lane.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlSessionCurrentUpdatePlan {
    core: SqlUpdatePlanCore,
}

/// Validated plan for public primary-key-only update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicPrimaryKeyUpdatePlan {
    core: SqlUpdatePlanCore,
    primary_key_proof: SqlWritePrimaryKeyPlanProof,
}

impl SqlPublicPrimaryKeyUpdatePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlUpdateStatement {
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

/// Validated plan for public bounded deterministic update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicBoundedUpdatePlan {
    core: SqlUpdatePlanCore,
    bounded_proof: SqlWriteBoundedPlanProof,
}

impl SqlPublicBoundedUpdatePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlUpdateStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this bounded deterministic plan.
    #[must_use]
    pub const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
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

/// Validated plan for future admin/bulk update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlAdminBulkUpdatePlan {
    core: SqlUpdatePlanCore,
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

impl SqlUpdatePolicyRejection {
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

/// Result of classifying one SQL statement under an `UPDATE` exposure policy.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlUpdatePolicyReport {
    /// Parsed `UPDATE` classification when the statement is an `UPDATE`.
    pub classification: Option<SqlUpdateStatementClassification>,
    /// Typed validated plan when the selected policy admits the statement.
    pub plan: Option<SqlValidatedUpdatePlan>,
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
            plan: None,
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
    let plan = if rejection.is_none() {
        validated_update_plan(statement, policy, &classification, context)
    } else {
        None
    };

    SqlUpdatePolicyReport {
        classification: Some(classification),
        plan,
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
        write_shape: classify_write_shape(statement, context),
    }
}

const fn update_policy_rejection(
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

    if !classification.write_shape.where_proof.has_where() {
        return Some(SqlUpdatePolicyRejection::MissingWhere);
    }

    if !classification.assignment_policy.admitted() {
        return unsafe_assignment_rejection(classification.assignment_policy);
    }

    match policy {
        SqlUpdateExposurePolicy::SessionWriteCurrent | SqlUpdateExposurePolicy::AdminBulk => None,
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => {
            if !classification
                .write_shape
                .where_proof
                .is_primary_key_equality()
            {
                return Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed);
            }

            None
        }
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => {
            bounded_policy_rejection(classification, context)
        }
        SqlUpdateExposurePolicy::GeneratedQuery | SqlUpdateExposurePolicy::GeneratedDdl => {
            generated_policy_rejection(policy)
        }
    }
}

fn validated_update_plan(
    statement: &SqlUpdateStatement,
    policy: SqlUpdateExposurePolicy,
    classification: &SqlUpdateStatementClassification,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlValidatedUpdatePlan> {
    let execution_bounds = execution_bounds(policy, classification, context)?;
    match policy {
        SqlUpdateExposurePolicy::SessionWriteCurrent => Some(
            SqlValidatedUpdatePlan::SessionCurrent(SqlSessionCurrentUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
            }),
        ),
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly => Some(
            SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(SqlPublicPrimaryKeyUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
                primary_key_proof: SqlWritePrimaryKeyPlanProof::from_field_names(
                    context.primary_key_fields,
                ),
            }),
        ),
        SqlUpdateExposurePolicy::PublicBoundedDeterministic => Some(
            SqlValidatedUpdatePlan::PublicBoundedDeterministic(SqlPublicBoundedUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
                bounded_proof: SqlWriteBoundedPlanProof::from_admitted_shape(
                    &classification.write_shape,
                    context.primary_key_fields,
                )?,
            }),
        ),
        SqlUpdateExposurePolicy::AdminBulk => {
            Some(SqlValidatedUpdatePlan::AdminBulk(SqlAdminBulkUpdatePlan {
                core: SqlWritePlanCore::from_borrowed(statement, classification, execution_bounds),
            }))
        }
        SqlUpdateExposurePolicy::GeneratedQuery | SqlUpdateExposurePolicy::GeneratedDdl => None,
    }
}

fn execution_bounds(
    policy: SqlUpdateExposurePolicy,
    classification: &SqlUpdateStatementClassification,
    context: SqlUpdatePolicyContext<'_>,
) -> Option<SqlWriteExecutionBounds> {
    let admission_lane = policy.validated_admission_lane()?;
    Some(
        classification
            .write_shape
            .execution_bounds_for_admission_lane(admission_lane, context.write_bounds()),
    )
}

const fn generated_policy_rejection(
    policy: SqlUpdateExposurePolicy,
) -> Option<SqlUpdatePolicyRejection> {
    match policy {
        SqlUpdateExposurePolicy::GeneratedQuery => {
            Some(SqlUpdatePolicyRejection::GeneratedQueryRejectsUpdate)
        }
        SqlUpdateExposurePolicy::GeneratedDdl => {
            Some(SqlUpdatePolicyRejection::GeneratedDdlRejectsUpdate)
        }
        SqlUpdateExposurePolicy::SessionWriteCurrent
        | SqlUpdateExposurePolicy::PublicPrimaryKeyOnly
        | SqlUpdateExposurePolicy::PublicBoundedDeterministic
        | SqlUpdateExposurePolicy::AdminBulk => None,
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
    match classification
        .write_shape
        .bounded_policy_rejection_for_bounds(context.write_bounds())
    {
        Some(rejection) => Some(SqlUpdatePolicyRejection::from_bounded_write_rejection(
            rejection,
        )),
        None => None,
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

fn classify_write_shape(
    statement: &SqlUpdateStatement,
    context: SqlUpdatePolicyContext<'_>,
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

fn assignment_field_name<'a>(statement: &SqlUpdateStatement, field: &'a str) -> Option<&'a str> {
    current_table_field_name(
        field,
        statement.entity.as_str(),
        statement.table_alias.as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::session::sql::write_policy::{
        SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteWhereProof,
    };

    const PRIMARY_KEY: &[&str] = &["id"];

    fn context() -> SqlUpdatePolicyContext<'static> {
        SqlUpdatePolicyContext::new(PRIMARY_KEY)
    }

    fn classify(sql: &str, policy: SqlUpdateExposurePolicy) -> SqlUpdatePolicyReport {
        classify_sql_update_policy(sql, policy, context()).expect("SQL should parse")
    }

    fn expect_plan(report: &SqlUpdatePolicyReport) -> &SqlValidatedUpdatePlan {
        report
            .plan
            .as_ref()
            .expect("admitted policy should produce a validated plan")
    }

    fn assert_no_plan(report: &SqlUpdatePolicyReport) {
        assert!(
            report.plan.is_none(),
            "rejected policy should not expose a partially usable plan",
        );
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
        assert_eq!(
            classification.write_shape.where_proof,
            SqlWriteWhereProof::Other
        );
        assert!(matches!(
            expect_plan(&report),
            SqlValidatedUpdatePlan::SessionCurrent(_),
        ));
        assert_eq!(expect_plan(&report).statement_entity(), "Character");
    }

    #[test]
    fn update_policy_rejects_non_update_statement() {
        let report = classify(
            "SELECT id FROM Character",
            SqlUpdateExposurePolicy::SessionWriteCurrent,
        );

        assert_eq!(report.classification, None);
        assert_eq!(report.rejection, Some(SqlUpdatePolicyRejection::NotUpdate),);
        assert_no_plan(&report);
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::Missing,
        );
        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::MissingWhere),
        );
        assert_no_plan(&report);
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
        assert_no_plan(&report);
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
        assert_no_plan(&report);
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::PrimaryKeyEquality,
        );
        let SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan) = expect_plan(&report) else {
            panic!("primary-key policy should produce only the primary-key plan variant");
        };
        assert_eq!(plan.primary_key_fields(), ["id"]);
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::PrimaryKeyEquality,
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
        assert_no_plan(&report);
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
        assert_no_plan(&report);
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
        assert_no_plan(&report);
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
                .write_shape
                .where_proof,
            SqlWriteWhereProof::PrimaryKeyEquality,
        );
        let SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan) = expect_plan(&report) else {
            panic!("composite primary-key proof should produce a primary-key plan");
        };
        assert_eq!(plan.primary_key_fields(), ["tenant_id", "id"]);
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
        assert_no_plan(&report);
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
    fn update_policy_validated_plans_carry_execution_and_returning_bounds() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: Some(4096),
        };
        let primary_key = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");
        let bounded = classify_sql_update_policy(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
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
    fn update_policy_validated_plans_lower_configured_returning_row_bound() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: Some(2),
            max_returning_response_bytes: None,
        };
        let primary_key = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("SQL should parse");
        let bounded = classify_sql_update_policy(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
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
        assert!(classification.write_shape.is_bounded());
        assert!(
            classification
                .write_shape
                .has_explicit_canonical_primary_key_order()
        );
        let SqlValidatedUpdatePlan::PublicBoundedDeterministic(plan) = expect_plan(&report) else {
            panic!("bounded policy should produce only the bounded plan variant");
        };
        assert_eq!(plan.limit(), 10);
        assert_eq!(plan.ordered_primary_key_fields(), ["id"]);
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
        assert_no_plan(&report);
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
        assert_no_plan(&report);
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
        assert_no_plan(&report);
    }

    #[test]
    fn update_policy_public_bounded_rejects_excessive_limit() {
        let excessive_limit = DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT
            .checked_add(1)
            .expect("test default public bounded update limit should fit u32");
        let report = classify(
            format!(
                "UPDATE Character SET active = false WHERE age = 21 ORDER BY id \
                 LIMIT {excessive_limit}",
            )
            .as_str(),
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        );

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::LimitTooHigh),
        );
        assert_no_plan(&report);
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
        assert_no_plan(&report);
    }

    #[test]
    fn update_policy_admin_bulk_produces_only_admin_plan_variant() {
        let report = classify(
            "UPDATE Character SET active = false WHERE age = 21",
            SqlUpdateExposurePolicy::AdminBulk,
        );

        assert!(report.is_admitted());
        assert!(matches!(
            expect_plan(&report),
            SqlValidatedUpdatePlan::AdminBulk(_),
        ));
    }

    #[test]
    fn update_policy_rejects_generated_and_managed_assignment() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &["slug"],
            managed_fields: &["updated_at"],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: None,
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
        assert_no_plan(&generated);
        assert_no_plan(&managed);
    }

    #[test]
    fn update_policy_allows_schema_owned_returning_fields_on_public_surfaces() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &["slug"],
            managed_fields: &["updated_at"],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: None,
        };
        let cases = [
            (
                "UPDATE Character SET age = 22 WHERE id = 1 RETURNING *",
                SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            ),
            (
                "UPDATE Character SET age = 22 WHERE id = 1 RETURNING slug",
                SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            ),
            (
                "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 \
                 RETURNING updated_at",
                SqlUpdateExposurePolicy::PublicBoundedDeterministic,
            ),
        ];

        for (sql, policy) in cases {
            let report = classify_sql_update_policy(sql, policy, context)
                .expect("schema-owned RETURNING SQL should parse");

            assert!(
                report.is_admitted(),
                "public returning follows accepted row projection visibility",
            );
            assert!(report.plan.is_some());
        }
    }

    #[test]
    fn update_policy_preserves_shape_rejections_with_schema_owned_returning_fields() {
        let context = SqlUpdatePolicyContext {
            primary_key_fields: PRIMARY_KEY,
            generated_fields: &["slug"],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: None,
        };
        let primary_key = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE age = 21 RETURNING *",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            context,
        )
        .expect("primary-key policy rejection SQL should parse");
        let bounded = classify_sql_update_policy(
            "UPDATE Character SET age = 22 WHERE age = 21 LIMIT 10 RETURNING *",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
            context,
        )
        .expect("bounded policy rejection SQL should parse");

        assert_eq!(
            primary_key.rejection,
            Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
        );
        assert_eq!(
            bounded.rejection,
            Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
        assert_no_plan(&primary_key);
        assert_no_plan(&bounded);
    }
}
