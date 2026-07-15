//! Module: db::session::sql::write_policy::model
//! Responsibility: shared SQL write policy DTOs, proofs, and admission lanes.
//! Does not own: SQL parser expression inspection or statement-family policy.
//! Boundary: carries proven write shape and execution bounds into UPDATE/DELETE gates.

use super::bounds::{bounded_write_policy_rejection, sql_write_execution_bounds_for_staged_kind};

pub(in crate::db::session::sql) const DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT: u32 = 100;
pub(in crate::db::session::sql) const DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES: u32 =
    1_048_576;

/// Shared `WHERE` proof classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlWriteWhereProof {
    /// The statement has no `WHERE` clause.
    Missing,
    /// The `WHERE` clause proves complete primary-key equality.
    PrimaryKeyEquality,
    /// The `WHERE` clause exists but does not prove primary-key equality.
    Other,
}

impl SqlWriteWhereProof {
    /// Return whether a `WHERE` clause was present.
    #[must_use]
    pub(in crate::db) const fn has_where(self) -> bool {
        !matches!(self, Self::Missing)
    }

    /// Return whether primary-key equality proof passed.
    #[must_use]
    pub(in crate::db) const fn is_primary_key_equality(self) -> bool {
        matches!(self, Self::PrimaryKeyEquality)
    }
}

/// Shared `ORDER BY` proof classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlWriteOrderProof {
    /// The statement has no explicit `ORDER BY`.
    Missing,
    /// The statement explicitly orders by canonical primary-key fields ascending.
    CanonicalPrimaryKey,
    /// The statement orders by canonical primary-key fields but uses descending order.
    DescendingPrimaryKey,
    /// The statement has another explicit ordering shape.
    Other,
}

impl SqlWriteOrderProof {
    /// Return whether the statement has explicit canonical ascending primary-key order.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn is_canonical_primary_key(self) -> bool {
        matches!(self, Self::CanonicalPrimaryKey)
    }
}

/// Shared narrow `RETURNING` classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) enum SqlWriteReturningShape {
    /// No `RETURNING` clause.
    None,
    /// Narrow `RETURNING *`.
    NarrowAll,
    /// Narrow `RETURNING field, ...`.
    NarrowFields,
}

impl SqlWriteReturningShape {
    /// Return whether the statement requests `RETURNING`.
    #[must_use]
    pub(in crate::db) const fn is_requested(self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteBoundedPolicyRejection {
    MissingCanonicalPrimaryKeyOrder,
    DescendingOrder,
    MissingLimit,
    OffsetUnsupported,
    LimitTooHigh,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteExposureClass {
    PublicPrimaryKeyOnly,
    PublicBoundedDeterministic,
}

impl SqlWriteExposureClass {
    const fn admission_lane(self) -> SqlWriteAdmissionLane {
        match self {
            Self::PublicPrimaryKeyOnly => SqlWriteAdmissionLane::PrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteAdmissionLane::BoundedDeterministic,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteShapePolicyRejection {
    MissingWhere,
    PrimaryKeyProofFailed,
    Bounded(SqlWriteBoundedPolicyRejection),
}

/// Shared `RETURNING` bounds carried by policy-validated SQL write plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlWriteReturningBounds {
    /// Maximum rows the plan may return, when statically bounded by policy.
    pub max_rows: Option<u32>,
    /// Maximum encoded response bytes, when supplied by the caller surface.
    pub max_response_bytes: Option<u32>,
}

/// Shared execution bounds carried by policy-validated SQL write plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlWriteExecutionBounds {
    /// Maximum candidate rows the validated plan may stage before mutation.
    pub max_staged_rows: Option<u32>,
    /// Optional `RETURNING` row and response-size bounds.
    pub returning: SqlWriteReturningBounds,
}

/// Shared parsed write shape used by UPDATE and DELETE exposure policies.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub(in crate::db) struct SqlWriteStatementShape {
    /// `WHERE` proof classification.
    pub where_proof: SqlWriteWhereProof,
    /// Explicit `ORDER BY` proof classification.
    pub order_proof: SqlWriteOrderProof,
    /// Parsed `LIMIT`, if supplied.
    pub limit: Option<u32>,
    /// Parsed `OFFSET`, if supplied.
    pub offset: Option<u32>,
    /// Narrow write `RETURNING` classification.
    pub returning_shape: SqlWriteReturningShape,
}

impl SqlWriteStatementShape {
    /// Return whether the statement has an explicit positive `LIMIT`.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn is_bounded(&self) -> bool {
        matches!(self.limit, Some(limit) if limit > 0)
    }

    /// Return whether the statement has explicit canonical ascending primary-key order.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn has_explicit_canonical_primary_key_order(&self) -> bool {
        self.order_proof.is_canonical_primary_key()
    }

    const fn bounded_policy_rejection(
        &self,
        max_limit: u32,
    ) -> Option<SqlWriteBoundedPolicyRejection> {
        bounded_write_policy_rejection(self.offset, self.limit, max_limit, self.order_proof)
    }

    pub(in crate::db::session::sql) const fn bounded_policy_rejection_for_bounds(
        &self,
        bounds: SqlWritePolicyBounds,
    ) -> Option<SqlWriteBoundedPolicyRejection> {
        self.bounded_policy_rejection(bounds.public_bounded_limit)
    }

    pub(in crate::db::session::sql) const fn required_where_rejection(
        &self,
    ) -> Option<SqlWriteShapePolicyRejection> {
        if self.where_proof.has_where() {
            None
        } else {
            Some(SqlWriteShapePolicyRejection::MissingWhere)
        }
    }

    pub(in crate::db::session::sql) const fn primary_key_policy_rejection(
        &self,
    ) -> Option<SqlWriteShapePolicyRejection> {
        if let Some(rejection) = self.required_where_rejection() {
            return Some(rejection);
        }
        if self.where_proof.is_primary_key_equality() {
            None
        } else {
            Some(SqlWriteShapePolicyRejection::PrimaryKeyProofFailed)
        }
    }

    pub(in crate::db::session::sql) const fn bounded_deterministic_policy_rejection(
        &self,
        bounds: SqlWritePolicyBounds,
    ) -> Option<SqlWriteShapePolicyRejection> {
        if let Some(rejection) = self.required_where_rejection() {
            return Some(rejection);
        }
        match self.bounded_policy_rejection_for_bounds(bounds) {
            Some(rejection) => Some(SqlWriteShapePolicyRejection::Bounded(rejection)),
            None => None,
        }
    }

    pub(in crate::db::session::sql) const fn execution_bounds_for_admission_lane(
        &self,
        admission_lane: SqlWriteAdmissionLane,
        bounds: SqlWritePolicyBounds,
    ) -> SqlWriteExecutionBounds {
        sql_write_execution_bounds_for_staged_kind(
            admission_lane.staged_row_bound_kind(),
            self.limit,
            self.returning_shape.is_requested(),
            bounds.returning_rows,
            bounds.returning_response_bytes,
        )
    }

    pub(in crate::db::session::sql) const fn execution_bounds_for_exposure_class(
        &self,
        exposure_class: SqlWriteExposureClass,
        bounds: SqlWritePolicyBounds,
    ) -> SqlWriteExecutionBounds {
        self.execution_bounds_for_admission_lane(exposure_class.admission_lane(), bounds)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWritePolicyBounds {
    pub(in crate::db::session::sql) public_bounded_limit: u32,
    pub(in crate::db::session::sql) returning_rows: Option<u32>,
    pub(in crate::db::session::sql) returning_response_bytes: Option<u32>,
}

impl SqlWritePolicyBounds {
    pub(in crate::db::session::sql) const fn new(
        public_bounded_limit: u32,
        returning_rows: Option<u32>,
        returning_response_bytes: Option<u32>,
    ) -> Self {
        Self {
            public_bounded_limit,
            returning_rows,
            returning_response_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWritePlanCore<S> {
    statement: S,
    execution_bounds: SqlWriteExecutionBounds,
}

impl<S> SqlWritePlanCore<S> {
    pub(in crate::db::session::sql) const fn new(
        statement: S,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Self {
        Self {
            statement,
            execution_bounds,
        }
    }

    pub(in crate::db::session::sql) const fn statement(&self) -> &S {
        &self.statement
    }

    pub(in crate::db::session::sql) const fn execution_bounds(&self) -> SqlWriteExecutionBounds {
        self.execution_bounds
    }

    #[cfg(test)]
    pub(in crate::db) const fn set_execution_bounds_for_tests(
        &mut self,
        execution_bounds: SqlWriteExecutionBounds,
    ) {
        self.execution_bounds = execution_bounds;
    }
}

impl<S: Clone> SqlWritePlanCore<S> {
    pub(in crate::db::session::sql) fn from_borrowed(
        statement: &S,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Self {
        Self::new(statement.clone(), execution_bounds)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteAdmissionLane {
    PrimaryKeyOnly,
    BoundedDeterministic,
}

impl SqlWriteAdmissionLane {
    pub(super) const fn staged_row_bound_kind(self) -> SqlWriteStagedRowBoundKind {
        match self {
            Self::PrimaryKeyOnly => SqlWriteStagedRowBoundKind::One,
            Self::BoundedDeterministic => SqlWriteStagedRowBoundKind::Limit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SqlWriteStagedRowBoundKind {
    One,
    Limit,
}
