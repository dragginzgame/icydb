use crate::{
    db::{
        access::{AccessPath, IndexRangePathRef},
        contracts::ReadConsistency,
        direction::Direction,
        query::predicate::Predicate,
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::ops::{Deref, DerefMut};

///
/// QueryMode
///
/// Discriminates load vs delete intent at planning time.
/// Encodes mode-specific fields so invalid states are unrepresentable.
/// Mode checks are explicit and stable at execution time.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMode {
    Load(LoadSpec),
    Delete(DeleteSpec),
}

impl QueryMode {
    /// True if this mode represents a load intent.
    #[must_use]
    pub const fn is_load(&self) -> bool {
        match self {
            Self::Load(_) => true,
            Self::Delete(_) => false,
        }
    }

    /// True if this mode represents a delete intent.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        match self {
            Self::Delete(_) => true,
            Self::Load(_) => false,
        }
    }
}

///
/// LoadSpec
///
/// Mode-specific fields for load intents.
/// Encodes pagination without leaking into delete intents.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

impl LoadSpec {
    /// Create an empty load spec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

///
/// DeleteSpec
///
/// Mode-specific fields for delete intents.
/// Encodes delete limits without leaking into load intents.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeleteSpec {
    pub limit: Option<u32>,
}

impl DeleteSpec {
    /// Create an empty delete spec.
    #[must_use]
    pub const fn new() -> Self {
        Self { limit: None }
    }
}

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderSpec
/// Executor-facing ordering specification.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// LogicalPlan
///
/// Pure logical query intent produced by the planner.
///
/// A `LogicalPlan` represents the access-independent query semantics:
/// predicate/filter, ordering, distinct behavior, pagination/delete windows,
/// and read-consistency mode.
///
/// Design notes:
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access strategy
///
/// This struct is the logical compiler stage output and intentionally excludes
/// access-path details.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalPlan {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional distinct semantics over ordered rows.
    pub(crate) distinct: bool,

    /// Optional delete bound (delete intents only).
    pub(crate) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(crate) consistency: ReadConsistency,
}

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen `AccessPlan`.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery<K> {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<K>,
}

impl<K> AccessPlannedQuery<K> {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) const fn from_parts(logical: LogicalPlan, access: AccessPlan<K>) -> Self {
        Self { logical, access }
    }

    /// Decompose into logical + access stages.
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<K>) {
        (self.logical, self.access)
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: ReadConsistency) -> Self {
        Self {
            logical: LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            },
            access: AccessPlan::path(access),
        }
    }
}

impl<K> Deref for AccessPlannedQuery<K> {
    type Target = LogicalPlan;

    fn deref(&self) -> &Self::Target {
        &self.logical
    }
}

impl<K> DerefMut for AccessPlannedQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.logical
    }
}

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is runtime-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPlan<K> {
    Path(Box<AccessPath<K>>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
    /// Construct a plan from one concrete access path.
    #[must_use]
    pub(crate) fn path(path: AccessPath<K>) -> Self {
        Self::Path(Box::new(path))
    }

    /// Construct a plan that forces a full scan.
    #[must_use]
    pub(crate) fn full_scan() -> Self {
        Self::path(AccessPath::FullScan)
    }

    /// Borrow the concrete path when this plan is a single-path node.
    #[must_use]
    pub(crate) fn as_path(&self) -> Option<&AccessPath<K>> {
        match self {
            Self::Path(path) => Some(path.as_ref()),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }

    /// Return true when this plan is exactly one full-scan path.
    #[must_use]
    pub(crate) const fn is_single_full_scan(&self) -> bool {
        matches!(self, Self::Path(path) if path.is_full_scan())
    }

    /// Borrow index-prefix access details when this is a single IndexPrefix path.
    #[must_use]
    pub(crate) fn as_index_prefix_path(&self) -> Option<(&IndexModel, &[Value])> {
        self.as_path().and_then(AccessPath::as_index_prefix)
    }

    /// Borrow index-range access details when this is a single IndexRange path.
    #[must_use]
    pub(crate) fn as_index_range_path(&self) -> Option<IndexRangePathRef<'_>> {
        self.as_path().and_then(AccessPath::as_index_range)
    }

    /// Walk the tree and return the first encountered IndexRange details.
    #[must_use]
    pub(crate) fn first_index_range_details(&self) -> Option<(&'static str, usize)> {
        match self {
            Self::Path(path) => path.index_range_details(),
            Self::Union(children) | Self::Intersection(children) => {
                children.iter().find_map(Self::first_index_range_details)
            }
        }
    }
}

impl<K> From<AccessPath<K>> for AccessPlan<K> {
    fn from(value: AccessPath<K>) -> Self {
        Self::path(value)
    }
}

type OrderFieldRef<'a> = (&'a str, Direction);

fn direction_from_order(direction: OrderDirection) -> Direction {
    if direction == OrderDirection::Desc {
        Direction::Desc
    } else {
        Direction::Asc
    }
}

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
        .collect()
}

///
/// SecondaryOrderPushdownEligibility
///
/// Access-layer eligibility decision for secondary-index ORDER BY pushdown.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownEligibility {
    Eligible {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// PushdownApplicability
///
/// Explicit applicability state for secondary-index ORDER BY pushdown.
///
/// This avoids overloading `Option<SecondaryOrderPushdownEligibility>` and
/// keeps "not applicable" separate from "applicable but rejected".
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PushdownApplicability {
    NotApplicable,
    Applicable(SecondaryOrderPushdownEligibility),
}

impl PushdownApplicability {
    /// Return true when this applicability state is eligible for secondary-order pushdown.
    #[must_use]
    pub(crate) const fn is_eligible(&self) -> bool {
        matches!(
            self,
            Self::Applicable(SecondaryOrderPushdownEligibility::Eligible { .. })
        )
    }
}

///
/// PushdownSurfaceEligibility
///
/// Shared conversion boundary from core eligibility into surface-facing
/// projections used by explain and trace layers.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownSurfaceEligibility<'a> {
    EligibleSecondaryIndex {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected {
        reason: &'a SecondaryOrderPushdownRejection,
    },
}

impl<'a> From<&'a SecondaryOrderPushdownEligibility> for PushdownSurfaceEligibility<'a> {
    fn from(value: &'a SecondaryOrderPushdownEligibility) -> Self {
        match value {
            SecondaryOrderPushdownEligibility::Eligible { index, prefix_len } => {
                Self::EligibleSecondaryIndex {
                    index,
                    prefix_len: *prefix_len,
                }
            }
            SecondaryOrderPushdownEligibility::Rejected(reason) => Self::Rejected { reason },
        }
    }
}

///
/// SecondaryOrderPushdownRejection
///
/// Deterministic reason why secondary-index ORDER BY pushdown is not eligible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownRejection {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    AccessPathIndexRangeUnsupported {
        index: &'static str,
        prefix_len: usize,
    },
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    MissingPrimaryKeyTieBreak {
        field: String,
    },
    PrimaryKeyDirectionNotAscending {
        field: String,
    },
    MixedDirectionNotEligible {
        field: String,
    },
    OrderFieldsDoNotMatchIndex {
        index: &'static str,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_from_parts(model, order_fields.as_deref(), &plan.access)
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan shape.
pub(crate) fn assess_secondary_order_pushdown_from_parts<K>(
    model: &EntityModel,
    order_fields: Option<&[OrderFieldRef<'_>]>,
    access_plan: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order_fields) = order_fields else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if order_fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index,
                    prefix_len,
                },
            );
        }

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };

    if let Some((index, values)) = access.as_index_prefix() {
        if values.len() > index.fields.len() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: values.len(),
                    index_field_len: index.fields.len(),
                },
            );
        }

        assess_secondary_order_pushdown_for_applicable_shape(
            model,
            order_fields,
            index.name,
            index.fields,
            values.len(),
        )
    } else if let Some((index, prefix_len)) = access.index_range_details() {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len },
        )
    } else {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        )
    }
}

/// Evaluate pushdown eligibility for plans that are already known to be
/// structurally applicable (ORDER BY + single index-prefix access path).
///
/// `EnforceAndReject` is used for defensive assessors, while
/// `AssumeValidated` keeps validated-path invariants as debug assertions.
enum PkTieBreakPolicy {
    EnforceAndReject,
    AssumeValidated,
}

// Core matcher shared by defensive and validated pushdown assessors.
fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[OrderFieldRef<'_>],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
    pk_policy: PkTieBreakPolicy,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };

    match pk_policy {
        PkTieBreakPolicy::EnforceAndReject => {
            if *last_field != model.primary_key.name {
                return SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                        field: model.primary_key.name.to_string(),
                    },
                );
            }
        }
        PkTieBreakPolicy::AssumeValidated => {
            debug_assert_eq!(
                *last_field, model.primary_key.name,
                "validated plan must include PK tie-break"
            );
        }
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);

    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);

    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

// Evaluate pushdown eligibility for plans that are already known to be
// structurally applicable (ORDER BY + single index-prefix access path).
fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[OrderFieldRef<'_>],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(
        model,
        order_fields,
        index_name,
        index_fields,
        prefix_len,
        PkTieBreakPolicy::EnforceAndReject,
    )
}

#[cfg(test)]
fn applicability_from_eligibility(
    eligibility: SecondaryOrderPushdownEligibility,
) -> PushdownApplicability {
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
///
/// Returns `PushdownApplicability::NotApplicable` for non-applicable shapes:
/// - no ORDER BY fields
/// - access path is not a secondary index path
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_if_applicable_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}

/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
///
/// Returns `PushdownApplicability::NotApplicable` for non-applicable shapes:
/// - no ORDER BY fields
/// - access path is not a secondary index path
#[cfg(test)]
pub(crate) fn assess_secondary_order_pushdown_if_applicable_from_parts<K>(
    model: &EntityModel,
    order_fields: Option<&[OrderFieldRef<'_>]>,
    access_plan: &AccessPlan<K>,
) -> PushdownApplicability {
    applicability_from_eligibility(assess_secondary_order_pushdown_from_parts(
        model,
        order_fields,
        access_plan,
    ))
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
///
/// This variant keeps applicability explicit and assumes validated invariants
/// with debug assertions, while preserving safe fallbacks in release builds.
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_if_applicable_validated_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
///
/// This variant keeps applicability explicit and assumes validated invariants
/// with debug assertions, while preserving safe fallbacks in release builds.
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated_from_parts<K>(
    model: &EntityModel,
    order_fields: Option<&[OrderFieldRef<'_>]>,
    access_plan: &AccessPlan<K>,
) -> PushdownApplicability {
    let Some(order_fields) = order_fields else {
        return PushdownApplicability::NotApplicable;
    };
    debug_assert!(
        !order_fields.is_empty(),
        "validated plan must not contain an empty ORDER BY specification"
    );

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
            return PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index,
                    prefix_len,
                },
            ));
        }

        return PushdownApplicability::NotApplicable;
    };

    if let Some((index, values)) = access.as_index_prefix() {
        debug_assert!(
            values.len() <= index.fields.len(),
            "validated plan must keep index-prefix bounds within declared index fields"
        );

        return PushdownApplicability::Applicable(
            assess_secondary_order_pushdown_for_validated_shape(
                model,
                order_fields,
                index.name,
                index.fields,
                values.len(),
            ),
        );
    }

    if let Some((index, prefix_len)) = access.index_range_details() {
        return PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len },
        ));
    }

    PushdownApplicability::NotApplicable
}

// Evaluate pushdown eligibility for validated plans without re-checking
// upstream ORDER/access-shape invariants.
fn assess_secondary_order_pushdown_for_validated_shape(
    model: &EntityModel,
    order_fields: &[OrderFieldRef<'_>],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(
        model,
        order_fields,
        index_name,
        index_fields,
        prefix_len,
        PkTieBreakPolicy::AssumeValidated,
    )
}
