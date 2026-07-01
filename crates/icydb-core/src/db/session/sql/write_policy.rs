//! Module: db::session::sql::write_policy
//! Responsibility: shared SQL write-shape proofs used by policy classifiers.
//! Does not own: statement-family admission or mutation execution.
//! Boundary: proves primary-key `WHERE`, canonical order, and `RETURNING`
//! shapes consistently for UPDATE and DELETE policy gates.

use crate::db::sql::parser::{
    SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlReturningProjection,
};
use std::collections::BTreeSet;

pub(in crate::db::session::sql) const DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT: u32 = 100;
pub(in crate::db::session::sql) const DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES: u32 =
    1_048_576;

/// Shared `WHERE` proof classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlWriteWhereProof {
    /// The statement has no `WHERE` clause.
    Missing,
    /// The `WHERE` clause proves complete primary-key equality under v1 rules.
    PrimaryKeyEquality,
    /// The `WHERE` clause exists but does not prove primary-key equality.
    Other,
}

impl SqlWriteWhereProof {
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

/// Shared `ORDER BY` proof classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlWriteOrderProof {
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
    #[must_use]
    pub const fn is_canonical_primary_key(self) -> bool {
        matches!(self, Self::CanonicalPrimaryKey)
    }
}

/// Shared narrow `RETURNING` classification for SQL write policy gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlWriteReturningShape {
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
    pub const fn is_requested(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Return whether the requested `RETURNING` shape is currently narrow.
    #[must_use]
    pub const fn is_narrow(self) -> bool {
        matches!(self, Self::NarrowAll | Self::NarrowFields)
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
pub(in crate::db::session::sql) enum SqlGeneratedWritePolicyKind {
    Query,
    Ddl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteExposureClass {
    SessionWriteCurrent,
    GeneratedQuery,
    GeneratedDdl,
    PublicPrimaryKeyOnly,
    PublicBoundedDeterministic,
    AdminBulk,
}

impl SqlWriteExposureClass {
    pub(in crate::db::session::sql) const fn generated_policy_kind(
        self,
    ) -> Option<SqlGeneratedWritePolicyKind> {
        match self {
            Self::GeneratedQuery => Some(SqlGeneratedWritePolicyKind::Query),
            Self::GeneratedDdl => Some(SqlGeneratedWritePolicyKind::Ddl),
            Self::SessionWriteCurrent
            | Self::PublicPrimaryKeyOnly
            | Self::PublicBoundedDeterministic
            | Self::AdminBulk => None,
        }
    }

    const fn admission_lane(self) -> Option<SqlWriteAdmissionLane> {
        Some(match self {
            Self::PublicPrimaryKeyOnly => SqlWriteAdmissionLane::PrimaryKeyOnly,
            Self::PublicBoundedDeterministic => SqlWriteAdmissionLane::BoundedDeterministic,
            Self::SessionWriteCurrent | Self::AdminBulk => SqlWriteAdmissionLane::Bulk,
            Self::GeneratedQuery | Self::GeneratedDdl => return None,
        })
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
pub struct SqlWriteReturningBounds {
    /// Maximum rows the plan may return, when statically bounded by policy.
    pub max_rows: Option<u32>,
    /// Maximum encoded response bytes, when supplied by the caller surface.
    pub max_response_bytes: Option<u32>,
}

/// Shared execution bounds carried by policy-validated SQL write plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlWriteExecutionBounds {
    /// Maximum candidate rows the validated plan may stage before mutation.
    pub max_staged_rows: Option<u32>,
    /// Optional `RETURNING` row and response-size bounds.
    pub returning: SqlWriteReturningBounds,
}

/// Shared parsed write shape used by UPDATE and DELETE exposure policies.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlWriteStatementShape {
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
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        matches!(self.limit, Some(limit) if limit > 0)
    }

    /// Return whether the statement has explicit canonical ascending primary-key order.
    #[must_use]
    pub const fn has_explicit_canonical_primary_key_order(&self) -> bool {
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
    ) -> Option<SqlWriteExecutionBounds> {
        match exposure_class.admission_lane() {
            Some(admission_lane) => {
                Some(self.execution_bounds_for_admission_lane(admission_lane, bounds))
            }
            None => None,
        }
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
pub(in crate::db::session::sql) struct SqlWritePlanCore<S, C> {
    statement: S,
    classification: C,
    execution_bounds: SqlWriteExecutionBounds,
}

impl<S, C> SqlWritePlanCore<S, C> {
    pub(in crate::db::session::sql) const fn new(
        statement: S,
        classification: C,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Self {
        Self {
            statement,
            classification,
            execution_bounds,
        }
    }

    pub(in crate::db::session::sql) const fn statement(&self) -> &S {
        &self.statement
    }

    pub(in crate::db::session::sql) const fn classification(&self) -> &C {
        &self.classification
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

impl<S: Clone, C: Clone> SqlWritePlanCore<S, C> {
    pub(in crate::db::session::sql) fn from_borrowed(
        statement: &S,
        classification: &C,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Self {
        Self::new(statement.clone(), classification.clone(), execution_bounds)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWritePrimaryKeyPlanProof {
    primary_key_fields: Vec<String>,
}

impl SqlWritePrimaryKeyPlanProof {
    pub(in crate::db::session::sql) const fn new(primary_key_fields: Vec<String>) -> Self {
        Self { primary_key_fields }
    }

    pub(in crate::db::session::sql) fn from_field_names(primary_key_fields: &[&str]) -> Self {
        Self::new(owned_write_field_names(primary_key_fields))
    }

    pub(in crate::db::session::sql) const fn primary_key_fields(&self) -> &[String] {
        self.primary_key_fields.as_slice()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWriteBoundedPlanProof {
    limit: u32,
    ordered_primary_key_fields: Vec<String>,
}

impl SqlWriteBoundedPlanProof {
    pub(in crate::db::session::sql) const fn new(
        limit: u32,
        ordered_primary_key_fields: Vec<String>,
    ) -> Self {
        Self {
            limit,
            ordered_primary_key_fields,
        }
    }

    pub(in crate::db::session::sql) const fn limit(&self) -> u32 {
        self.limit
    }

    pub(in crate::db::session::sql) const fn ordered_primary_key_fields(&self) -> &[String] {
        self.ordered_primary_key_fields.as_slice()
    }

    pub(in crate::db::session::sql) fn from_admitted_shape(
        shape: &SqlWriteStatementShape,
        ordered_primary_key_fields: &[&str],
    ) -> Option<Self> {
        Some(Self::new(
            shape.limit?,
            owned_write_field_names(ordered_primary_key_fields),
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteAdmissionLane {
    PrimaryKeyOnly,
    BoundedDeterministic,
    Bulk,
}

impl SqlWriteAdmissionLane {
    const fn staged_row_bound_kind(self) -> SqlWriteStagedRowBoundKind {
        match self {
            Self::PrimaryKeyOnly => SqlWriteStagedRowBoundKind::One,
            Self::BoundedDeterministic => SqlWriteStagedRowBoundKind::Limit,
            Self::Bulk => SqlWriteStagedRowBoundKind::Unbounded,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlWriteStagedRowBoundKind {
    One,
    Limit,
    Unbounded,
}

pub(in crate::db::session::sql) struct SqlWriteStatementShapeInput<'a> {
    pub(in crate::db::session::sql) predicate: Option<&'a SqlExpr>,
    pub(in crate::db::session::sql) entity: &'a str,
    pub(in crate::db::session::sql) table_alias: Option<&'a str>,
    pub(in crate::db::session::sql) order_by: &'a [SqlOrderTerm],
    pub(in crate::db::session::sql) limit: Option<u32>,
    pub(in crate::db::session::sql) offset: Option<u32>,
    pub(in crate::db::session::sql) returning: Option<&'a SqlReturningProjection>,
    pub(in crate::db::session::sql) primary_key_fields: &'a [&'a str],
}

pub(in crate::db::session::sql) fn classify_write_statement_shape(
    input: SqlWriteStatementShapeInput<'_>,
) -> SqlWriteStatementShape {
    SqlWriteStatementShape {
        where_proof: classify_write_where_proof(
            input.predicate,
            input.entity,
            input.table_alias,
            input.primary_key_fields,
        ),
        order_proof: classify_write_order_proof(
            input.order_by,
            input.entity,
            input.table_alias,
            input.primary_key_fields,
        ),
        limit: input.limit,
        offset: input.offset,
        returning_shape: classify_write_returning_shape(input.returning),
    }
}

pub(in crate::db::session::sql) fn classify_write_where_proof(
    predicate: Option<&SqlExpr>,
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> SqlWriteWhereProof {
    let Some(predicate) = predicate else {
        return SqlWriteWhereProof::Missing;
    };

    if primary_key_equality_proof(predicate, entity, table_alias, primary_key_fields) {
        SqlWriteWhereProof::PrimaryKeyEquality
    } else {
        SqlWriteWhereProof::Other
    }
}

pub(in crate::db::session::sql) fn classify_write_order_proof(
    order_by: &[SqlOrderTerm],
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> SqlWriteOrderProof {
    if order_by.is_empty() {
        return SqlWriteOrderProof::Missing;
    }
    if order_by.len() != primary_key_fields.len() {
        return SqlWriteOrderProof::Other;
    }

    let mut all_canonical = true;
    let mut saw_descending = false;
    for (term, primary_key) in order_by.iter().zip(primary_key_fields.iter().copied()) {
        let ordered_field = simple_field_name(&term.field, entity, table_alias);
        all_canonical &= ordered_field.is_some_and(|field| field == primary_key);
        saw_descending |= matches!(term.direction, SqlOrderDirection::Desc);
    }

    if !all_canonical {
        SqlWriteOrderProof::Other
    } else if saw_descending {
        SqlWriteOrderProof::DescendingPrimaryKey
    } else {
        SqlWriteOrderProof::CanonicalPrimaryKey
    }
}

pub(in crate::db::session::sql) const fn classify_write_returning_shape(
    returning: Option<&SqlReturningProjection>,
) -> SqlWriteReturningShape {
    match returning {
        None => SqlWriteReturningShape::None,
        Some(SqlReturningProjection::All) => SqlWriteReturningShape::NarrowAll,
        Some(SqlReturningProjection::Fields(_)) => SqlWriteReturningShape::NarrowFields,
    }
}

pub(in crate::db::session::sql) fn current_table_field_name<'a>(
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

pub(in crate::db::session::sql) fn contains_field(fields: &[&str], field: &str) -> bool {
    fields.contains(&field)
}

pub(in crate::db::session::sql) fn owned_write_field_names(fields: &[&str]) -> Vec<String> {
    fields.iter().map(|field| (*field).to_owned()).collect()
}

pub(in crate::db::session::sql) const fn combined_optional_row_bound(
    policy_max_rows: Option<u32>,
    configured_max_rows: Option<u32>,
) -> Option<u32> {
    match (policy_max_rows, configured_max_rows) {
        (Some(policy), Some(configured)) => Some(if policy < configured {
            policy
        } else {
            configured
        }),
        (Some(policy), None) => Some(policy),
        (None, Some(configured)) => Some(configured),
        (None, None) => None,
    }
}

const fn sql_write_staged_row_bound(
    kind: SqlWriteStagedRowBoundKind,
    limit: Option<u32>,
) -> Option<u32> {
    match kind {
        SqlWriteStagedRowBoundKind::One => Some(1),
        SqlWriteStagedRowBoundKind::Limit => limit,
        SqlWriteStagedRowBoundKind::Unbounded => None,
    }
}

const fn bounded_write_policy_rejection(
    offset: Option<u32>,
    limit: Option<u32>,
    max_limit: u32,
    order_proof: SqlWriteOrderProof,
) -> Option<SqlWriteBoundedPolicyRejection> {
    if offset.is_some() {
        return Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported);
    }

    let Some(limit) = limit else {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    };
    if limit == 0 {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    }
    if limit > max_limit {
        return Some(SqlWriteBoundedPolicyRejection::LimitTooHigh);
    }

    match order_proof {
        SqlWriteOrderProof::CanonicalPrimaryKey => None,
        SqlWriteOrderProof::DescendingPrimaryKey => {
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder)
        }
        SqlWriteOrderProof::Missing | SqlWriteOrderProof::Other => {
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder)
        }
    }
}

const fn sql_write_execution_bounds(
    max_staged_rows: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    let max_rows = if returning_requested {
        combined_optional_row_bound(max_staged_rows, max_returning_rows)
    } else {
        None
    };

    SqlWriteExecutionBounds {
        max_staged_rows,
        returning: SqlWriteReturningBounds {
            max_rows,
            max_response_bytes: max_returning_response_bytes,
        },
    }
}

const fn sql_write_execution_bounds_for_staged_kind(
    staged_row_bound_kind: SqlWriteStagedRowBoundKind,
    limit: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    sql_write_execution_bounds(
        sql_write_staged_row_bound(staged_row_bound_kind, limit),
        returning_requested,
        max_returning_rows,
        max_returning_response_bytes,
    )
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

const fn comparable_constant(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Literal(_) | SqlExpr::Param { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn literal(value: i64) -> SqlExpr {
        SqlExpr::Literal(Value::Int64(value))
    }

    fn field(name: &str) -> SqlExpr {
        SqlExpr::Field(name.to_string())
    }

    fn aliased_field(alias: &str, name: &str) -> SqlExpr {
        SqlExpr::FieldPath {
            root: alias.to_string(),
            segments: vec![name.to_string()],
        }
    }

    fn equals(left: SqlExpr, right: SqlExpr) -> SqlExpr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn and(left: SqlExpr, right: SqlExpr) -> SqlExpr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn sql_write_staged_row_bound_maps_shared_policy_kinds() {
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::One, Some(10)),
            Some(1),
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Limit, Some(10)),
            Some(10),
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Limit, None),
            None,
        );
        assert_eq!(
            sql_write_staged_row_bound(SqlWriteStagedRowBoundKind::Unbounded, Some(10)),
            None,
        );
    }

    #[test]
    fn sql_write_admission_lane_maps_to_execution_bounds() {
        let shape = SqlWriteStatementShape {
            where_proof: SqlWriteWhereProof::Other,
            order_proof: SqlWriteOrderProof::CanonicalPrimaryKey,
            limit: Some(10),
            offset: None,
            returning_shape: SqlWriteReturningShape::NarrowFields,
        };
        let bounds = SqlWritePolicyBounds::new(10, Some(7), Some(1024));

        assert_eq!(
            shape
                .execution_bounds_for_admission_lane(SqlWriteAdmissionLane::PrimaryKeyOnly, bounds,)
                .max_staged_rows,
            Some(1),
        );
        assert_eq!(
            shape
                .execution_bounds_for_admission_lane(
                    SqlWriteAdmissionLane::BoundedDeterministic,
                    bounds,
                )
                .max_staged_rows,
            Some(10),
        );
        assert_eq!(
            shape
                .execution_bounds_for_admission_lane(
                    SqlWriteAdmissionLane::BoundedDeterministic,
                    bounds,
                )
                .returning
                .max_rows,
            Some(7),
        );
        assert_eq!(
            shape
                .execution_bounds_for_admission_lane(SqlWriteAdmissionLane::Bulk, bounds,)
                .max_staged_rows,
            None,
        );
    }

    #[test]
    fn sql_write_execution_bounds_for_staged_kind_combines_policy_and_returning_caps() {
        let bounded = sql_write_execution_bounds_for_staged_kind(
            SqlWriteStagedRowBoundKind::Limit,
            Some(10),
            true,
            Some(3),
            Some(1024),
        );

        assert_eq!(bounded.max_staged_rows, Some(10));
        assert_eq!(bounded.returning.max_rows, Some(3));
        assert_eq!(bounded.returning.max_response_bytes, Some(1024));

        let primary_key_only = sql_write_execution_bounds_for_staged_kind(
            SqlWriteStagedRowBoundKind::One,
            Some(10),
            false,
            Some(3),
            Some(1024),
        );

        assert_eq!(primary_key_only.max_staged_rows, Some(1));
        assert_eq!(primary_key_only.returning.max_rows, None);
        assert_eq!(primary_key_only.returning.max_response_bytes, Some(1024));
    }

    #[test]
    fn bounded_write_policy_rejection_keeps_public_priority_order() {
        assert_eq!(
            bounded_write_policy_rejection(
                Some(1),
                None,
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, None, 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
            Some(SqlWriteBoundedPolicyRejection::MissingLimit),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(0),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::MissingLimit),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, Some(11), 10, SqlWriteOrderProof::Other),
            Some(SqlWriteBoundedPolicyRejection::LimitTooHigh),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(10),
                10,
                SqlWriteOrderProof::DescendingPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, Some(10), 10, SqlWriteOrderProof::Missing),
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(10),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            None,
        );
    }

    #[test]
    fn sql_write_execution_bounds_combines_staged_and_returning_limits() {
        let bounded_returning = sql_write_execution_bounds(Some(10), true, Some(4), Some(1024));
        assert_eq!(bounded_returning.max_staged_rows, Some(10));
        assert_eq!(bounded_returning.returning.max_rows, Some(4));
        assert_eq!(bounded_returning.returning.max_response_bytes, Some(1024));

        let staged_only_returning = sql_write_execution_bounds(Some(10), true, None, Some(1024));
        assert_eq!(staged_only_returning.returning.max_rows, Some(10));

        let configured_only_returning = sql_write_execution_bounds(None, true, Some(4), Some(1024));
        assert_eq!(configured_only_returning.returning.max_rows, Some(4));

        let no_returning = sql_write_execution_bounds(Some(10), false, Some(4), Some(1024));
        assert_eq!(no_returning.max_staged_rows, Some(10));
        assert_eq!(no_returning.returning.max_rows, None);
        assert_eq!(no_returning.returning.max_response_bytes, Some(1024));
    }

    #[test]
    fn classify_write_order_proof_requires_full_canonical_primary_key_order() {
        let asc_id = SqlOrderTerm {
            field: SqlExpr::Field("id".to_string()),
            direction: SqlOrderDirection::Asc,
        };
        let desc_id = SqlOrderTerm {
            field: SqlExpr::Field("id".to_string()),
            direction: SqlOrderDirection::Desc,
        };
        let asc_other = SqlOrderTerm {
            field: SqlExpr::Field("name".to_string()),
            direction: SqlOrderDirection::Asc,
        };

        assert_eq!(
            classify_write_order_proof(&[], "Token", None, &["id"]),
            SqlWriteOrderProof::Missing,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&asc_id), "Token", None, &["id"]),
            SqlWriteOrderProof::CanonicalPrimaryKey,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&desc_id), "Token", None, &["id"]),
            SqlWriteOrderProof::DescendingPrimaryKey,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&asc_other), "Token", None, &["id"]),
            SqlWriteOrderProof::Other,
        );
        assert_eq!(
            classify_write_order_proof(&[asc_id], "Token", None, &["id", "version"]),
            SqlWriteOrderProof::Other,
        );
    }

    #[test]
    fn classify_write_where_proof_requires_complete_primary_key_literal_equality() {
        let complete = and(
            equals(field("id"), literal(1)),
            equals(field("version"), literal(2)),
        );
        assert_eq!(
            classify_write_where_proof(Some(&complete), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::PrimaryKeyEquality,
        );

        let alias_complete = and(
            equals(aliased_field("t", "id"), literal(1)),
            equals(literal(2), aliased_field("t", "version")),
        );
        assert_eq!(
            classify_write_where_proof(
                Some(&alias_complete),
                "Token",
                Some("t"),
                &["id", "version"],
            ),
            SqlWriteWhereProof::PrimaryKeyEquality,
        );

        let partial = equals(field("id"), literal(1));
        assert_eq!(
            classify_write_where_proof(Some(&partial), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::Other,
        );

        let duplicate = and(
            equals(field("id"), literal(1)),
            equals(field("id"), literal(2)),
        );
        assert_eq!(
            classify_write_where_proof(Some(&duplicate), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::Other,
        );

        let field_to_field = equals(field("id"), field("version"));
        assert_eq!(
            classify_write_where_proof(Some(&field_to_field), "Token", None, &["id"]),
            SqlWriteWhereProof::Other,
        );
        assert_eq!(
            classify_write_where_proof(None, "Token", None, &["id"]),
            SqlWriteWhereProof::Missing,
        );
    }
}
