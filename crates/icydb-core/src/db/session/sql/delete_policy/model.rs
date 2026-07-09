//! SQL `DELETE` policy model, proofs, bounds, and public DTOs.
//! Does not own: SQL parsing or policy classification execution.

use crate::db::{
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteBoundedPlanProof, SqlWriteBoundedPolicyRejection, SqlWriteExecutionBounds,
        SqlWriteExposureClass, SqlWritePlanCore, SqlWritePolicyBounds, SqlWritePrimaryKeyPlanProof,
        SqlWriteReturningBounds, SqlWriteShapePolicyRejection, SqlWriteStatementShape,
    },
    sql::parser::SqlDeleteStatement,
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

    pub(super) const fn write_bounds(self) -> SqlWritePolicyBounds {
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

pub(super) type SqlDeletePlanCore =
    SqlWritePlanCore<SqlDeleteStatement, SqlDeleteStatementClassification>;

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
    pub(super) core: SqlDeletePlanCore,
}

/// Validated plan for public primary-key-only delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlPublicPrimaryKeyDeletePlan {
    pub(super) core: SqlDeletePlanCore,
    pub(super) primary_key_proof: SqlWritePrimaryKeyPlanProof,
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
    pub(super) core: SqlDeletePlanCore,
    pub(super) bounded_proof: SqlWriteBoundedPlanProof,
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
    pub(super) core: SqlDeletePlanCore,
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

    pub(super) const fn rejected(rejection: SqlDeletePolicyRejection) -> Self {
        Self {
            classification: None,
            plan: None,
            rejection: Some(rejection),
        }
    }
}
