//! SQL `UPDATE` policy model, proofs, bounds, and public DTOs.
//! Does not own: SQL parsing or policy classification execution.

use crate::db::{
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteBoundedPlanProof, SqlWriteBoundedPolicyRejection, SqlWriteExecutionBounds,
        SqlWriteExposureClass, SqlWritePlanCore, SqlWritePolicyBounds, SqlWritePrimaryKeyPlanProof,
        SqlWriteReturningBounds, SqlWriteShapePolicyRejection, SqlWriteStatementShape,
    },
    sql::parser::SqlUpdateStatement,
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
    pub(super) const fn exposure_class(self) -> SqlWriteExposureClass {
        match self {
            Self::SessionWriteCurrent => SqlWriteExposureClass::SessionWriteCurrent,
            Self::GeneratedQuery => SqlWriteExposureClass::GeneratedQuery,
            Self::GeneratedDdl => SqlWriteExposureClass::GeneratedDdl,
            Self::PublicPrimaryKeyOnly => SqlWriteExposureClass::PublicPrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteExposureClass::PublicBoundedDeterministic,
            Self::AdminBulk => SqlWriteExposureClass::AdminBulk,
        }
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

    pub(super) const fn write_bounds(self) -> SqlWritePolicyBounds {
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
    pub(super) const fn admitted(self) -> bool {
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

pub(super) type SqlUpdatePlanCore =
    SqlWritePlanCore<SqlUpdateStatement, SqlUpdateStatementClassification>;

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
    pub(super) core: SqlUpdatePlanCore,
}

/// Validated plan for public primary-key-only update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicPrimaryKeyUpdatePlan {
    pub(super) core: SqlUpdatePlanCore,
    pub(super) primary_key_proof: SqlWritePrimaryKeyPlanProof,
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
    pub(super) core: SqlUpdatePlanCore,
    pub(super) bounded_proof: SqlWriteBoundedPlanProof,
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

/// Validated plan for future admin/bulk update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlAdminBulkUpdatePlan {
    pub(super) core: SqlUpdatePlanCore,
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

    pub(super) const fn from_write_shape_rejection(
        rejection: SqlWriteShapePolicyRejection,
    ) -> Self {
        match rejection {
            SqlWriteShapePolicyRejection::MissingWhere => Self::MissingWhere,
            SqlWriteShapePolicyRejection::PrimaryKeyProofFailed => Self::PrimaryKeyProofFailed,
            SqlWriteShapePolicyRejection::Bounded(rejection) => {
                Self::from_bounded_write_rejection(rejection)
            }
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

    pub(super) const fn rejected(rejection: SqlUpdatePolicyRejection) -> Self {
        Self {
            classification: None,
            plan: None,
            rejection: Some(rejection),
        }
    }
}
