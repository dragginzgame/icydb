//! SQL `DELETE` policy model, proofs, bounds, and typed outcomes.
//! Does not own: SQL parsing or policy classification execution.

#[cfg(test)]
use crate::db::session::sql::write_policy::SqlWriteReturningBounds;
use crate::db::{
    session::sql::write_policy::{
        DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
        SqlWriteExecutionBounds, SqlWriteExposureClass, SqlWritePlanCore, SqlWritePolicyBounds,
        SqlWriteShapePolicyRejection,
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
pub(in crate::db) enum SqlDeleteExposurePolicy {
    /// Public-safe policy requiring complete primary-key equality in `WHERE`.
    PublicPrimaryKeyOnly,
    /// Public-safe bounded policy requiring `WHERE`, explicit primary-key order, and `LIMIT`.
    PublicBoundedDeterministic,
}

impl SqlDeleteExposurePolicy {
    pub(super) const fn exposure_class(self) -> SqlWriteExposureClass {
        match self {
            Self::PublicPrimaryKeyOnly => SqlWriteExposureClass::PublicPrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteExposureClass::PublicBoundedDeterministic,
        }
    }
}

/// Schema-derived field context needed to classify one `DELETE`.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub(in crate::db) struct SqlDeletePolicyContext<'a> {
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
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn new(primary_key_fields: &'a [&'a str]) -> Self {
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
    pub(in crate::db) const fn public_generated(primary_key_fields: &'a [&'a str]) -> Self {
        Self {
            primary_key_fields,
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes: Some(DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES),
        }
    }
}

pub(super) type SqlDeletePlanCore = SqlWritePlanCore<SqlDeleteStatement>;

/// Validated non-executing SQL `DELETE` plan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlValidatedDeletePlan {
    /// Public-safe one-row primary-key plan.
    PublicPrimaryKeyOnly(SqlPublicPrimaryKeyDeletePlan),
    /// Public-safe bounded deterministic plan.
    PublicBoundedDeterministic(SqlPublicBoundedDeletePlan),
}

impl SqlValidatedDeletePlan {
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

/// Validated plan for public primary-key-only delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlPublicPrimaryKeyDeletePlan {
    pub(super) core: SqlDeletePlanCore,
}

impl SqlPublicPrimaryKeyDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this primary-key plan.
    #[must_use]
    pub(in crate::db) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }
}

/// Validated plan for public bounded deterministic delete.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlPublicBoundedDeletePlan {
    pub(super) core: SqlDeletePlanCore,
}

impl SqlPublicBoundedDeletePlan {
    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlDeleteStatement {
        self.core.statement()
    }

    /// Return the execution bounds carried by this bounded deterministic plan.
    #[must_use]
    pub(in crate::db) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.core.execution_bounds()
    }
}

/// Stable policy rejection for one classified SQL `DELETE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlDeletePolicyRejection {
    /// The parsed statement is not `DELETE`.
    NotDelete,
    /// Shared write-shape policy rejected the statement.
    WriteShape(SqlWriteShapePolicyRejection),
}

/// Mutually exclusive validated plan or stable rejection for one SQL `DELETE` policy.
#[doc(hidden)]
pub(in crate::db) type SqlDeletePolicyResult =
    Result<SqlValidatedDeletePlan, SqlDeletePolicyRejection>;
