//! SQL `UPDATE` policy model, proofs, bounds, and public DTOs.
//! Does not own: SQL parsing or policy classification execution.

#[cfg(test)]
use crate::db::session::sql::write_policy::SqlWriteReturningBounds;
use crate::db::{
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteBoundedPolicyRejection, SqlWriteExecutionBounds, SqlWriteExposureClass,
        SqlWritePlanCore, SqlWritePolicyBounds, SqlWriteShapePolicyRejection,
        SqlWriteStatementShape,
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
pub(in crate::db) enum SqlUpdateExposurePolicy {
    /// Public-safe policy requiring complete primary-key equality in `WHERE`.
    PublicPrimaryKeyOnly,
    /// Public-safe bounded non-primary-key policy requiring explicit primary-key ordering.
    PublicBoundedDeterministic,
}

impl SqlUpdateExposurePolicy {
    pub(super) const fn exposure_class(self) -> SqlWriteExposureClass {
        match self {
            Self::PublicPrimaryKeyOnly => SqlWriteExposureClass::PublicPrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteExposureClass::PublicBoundedDeterministic,
        }
    }
}

/// Schema-derived field context needed to classify one `UPDATE`.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub(in crate::db) struct SqlUpdatePolicyContext<'a> {
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
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn new(primary_key_fields: &'a [&'a str]) -> Self {
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
pub(in crate::db) struct SqlUpdateAssignmentPolicy {
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
pub(in crate::db) struct SqlUpdateStatementClassification {
    /// Target entity identifier.
    pub target_entity: String,
    /// Fields assigned by the `SET` list in parser order.
    pub assigned_fields: Vec<String>,
    /// Assignment ownership classification.
    pub assignment_policy: SqlUpdateAssignmentPolicy,
    /// Shared parser write-shape classification.
    pub write_shape: SqlWriteStatementShape,
}

pub(super) type SqlUpdatePlanCore = SqlWritePlanCore<SqlUpdateStatement>;

/// Validated non-executing SQL `UPDATE` plan.
///
/// Generated/public executors should consume one of these policy-specific
/// variants instead of a raw parsed `SqlUpdateStatement` plus a separate
/// "already checked" report.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlValidatedUpdatePlan {
    /// Public-safe one-row primary-key plan.
    PublicPrimaryKeyOnly(SqlPublicPrimaryKeyUpdatePlan),
    /// Public-safe bounded deterministic plan.
    PublicBoundedDeterministic(SqlPublicBoundedUpdatePlan),
}

impl SqlValidatedUpdatePlan {
    /// Return the execution bounds carried by this validated plan.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        match self {
            Self::PublicPrimaryKeyOnly(plan) => plan.core.execution_bounds(),
            Self::PublicBoundedDeterministic(plan) => plan.core.execution_bounds(),
        }
    }

    /// Return the `RETURNING` bounds carried by this validated plan.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn returning_bounds(&self) -> SqlWriteReturningBounds {
        self.execution_bounds().returning
    }
}

/// Validated plan for public primary-key-only update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlPublicPrimaryKeyUpdatePlan {
    pub(super) core: SqlUpdatePlanCore,
}

impl SqlPublicPrimaryKeyUpdatePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlUpdateStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this primary-key plan.
    #[must_use]
    pub(in crate::db) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }
}

/// Validated plan for public bounded deterministic update.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlPublicBoundedUpdatePlan {
    pub(super) core: SqlUpdatePlanCore,
}

impl SqlPublicBoundedUpdatePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlUpdateStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this bounded deterministic plan.
    #[must_use]
    pub(in crate::db) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }

    #[cfg(test)]
    pub(in crate::db) const fn set_execution_bounds_for_tests(
        &mut self,
        execution_bounds: SqlWriteExecutionBounds,
    ) {
        self.core.set_execution_bounds_for_tests(execution_bounds);
    }
}

/// Stable policy rejection for one classified SQL `UPDATE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlUpdatePolicyRejection {
    /// The parsed statement is not `UPDATE`.
    NotUpdate,
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
    /// This policy rejects descending ordering.
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
pub(in crate::db) struct SqlUpdatePolicyReport {
    /// Parsed `UPDATE` classification when the statement is an `UPDATE`.
    pub classification: Option<SqlUpdateStatementClassification>,
    /// Typed validated plan when the selected policy admits the statement.
    pub plan: Option<SqlValidatedUpdatePlan>,
    /// Policy rejection, or `None` when the selected policy admits the statement.
    pub rejection: Option<SqlUpdatePolicyRejection>,
}

impl SqlUpdatePolicyReport {
    /// Return whether the selected policy admits the statement.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn is_admitted(&self) -> bool {
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
